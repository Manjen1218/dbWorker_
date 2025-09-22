#!/usr/bin/python3

import multiprocessing
from multiprocessing import Queue, Value, Lock, Event
import time
from typing import Dict, Any, List, Tuple, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import threading
import os
import sys
import json
import signal
from multiprocessing import Manager
import csv
from io import StringIO
import xxhash
import atexit

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.MySQLHelper import MySQLHelper
from core.Logger import Logger
from workers import worker_ult as wult
from ParseEngine.engine.ParseEngineData import ParseEngineData


class DatabaseConnectionManager:
    """
    Enhanced database connection manager that ensures cleanup under all circumstances.
    """
    def __init__(self, db_config: Dict[str, Any], logger: Logger, process_name: str = "Unknown"):
        self.db_config = db_config
        self.logger = logger
        self.process_name = process_name
        self.db = None
        self.connected = False
        self.cleanup_registered = False
        
    def connect(self) -> MySQLHelper:
        """Connect and register cleanup handlers."""
        try:
            self.db = MySQLHelper(self.db_config)
            if self.db.connect():
                self.connected = True
                self._register_cleanup()
                return self.db
            else:
                raise Exception("Failed to connect to database")
        except Exception as e:
            self.logger.log_error(f"Database connection failed in {self.process_name}: {e}")
            self.cleanup()
            raise
    
    def _register_cleanup(self):
        """Register cleanup handlers for various termination scenarios."""
        if self.cleanup_registered:
            return
            
        # Register atexit handler (handles normal exit and some exceptions)
        atexit.register(self.cleanup)
        
        # Enhanced signal handlers for graceful cleanup
        def enhanced_signal_handler(signum, frame):
            self.logger.log_warn(f"{self.process_name} received signal {signum}, performing database cleanup")
            self.cleanup()
            # Re-raise as KeyboardInterrupt for proper handling
            raise KeyboardInterrupt(f"Interrupted by signal {signum}")
        
        signal.signal(signal.SIGTERM, enhanced_signal_handler)
        signal.signal(signal.SIGINT, enhanced_signal_handler)
        
        # For Unix systems, also handle SIGQUIT
        if hasattr(signal, 'SIGQUIT'):
            signal.signal(signal.SIGQUIT, enhanced_signal_handler)
        
        self.cleanup_registered = True
    
    def cleanup(self):
        """Force cleanup database connection."""
        if self.connected and self.db:
            try:
                self.logger.log_info(f"Forcing database connection cleanup in {self.process_name}")
                self.db.close()
                self.connected = False
            except Exception as e:
                # Even if cleanup fails, mark as disconnected
                self.logger.log_error(f"Error during database cleanup in {self.process_name}: {e}")
                self.connected = False
    
    def __enter__(self):
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


def database_writer_process(
    data_queues: List[multiprocessing.Queue],
    exception_queues: List[multiprocessing.Queue],
    db_config: Dict[str, Any],
    dbname: str,
    tablename: str,
    exception_db_config: Dict[str, Any],
    exception_table: str,
    batch_size: int,
    commit_threshold: int,
    exception_check_interval: int,
    logger_config: Dict[str, Any],
    worker_finished_event: multiprocessing.Event,
    total_files_to_process: int,
    processed_counter: multiprocessing.Value,
    batch_collection_size: int = 1000,
    batch_time_threshold: float = 5.0
) -> Tuple[List[str], List[str]]:
    """
    Enhanced database writer with guaranteed connection cleanup and table locking.
    """
    logg = Logger()
    logg.log_info("Enhanced database writer process started with guaranteed cleanup")
    
    # Enhanced database connection managers for both main and exception databases
    main_db_manager = DatabaseConnectionManager(db_config, logg, "DatabaseWriter-Main")
    exception_db_manager = DatabaseConnectionManager(exception_db_config, logg, "DatabaseWriter-Exception")
    
    # Tracking variables
    unlogged_parsed_files = []
    unlogged_unparsed_files = []
    insert_success_count = 0
    total_processed_count = 0
    processing_start_time = time.time()
    
    # Collection variables
    collection_buffer = []
    last_collection_time = time.time()
    collection_rounds = 0
    consecutive_empty_rounds = 0
    
    num_processes = len(data_queues)
    current_process_idx = 0
    
    logg.log_info(f"Monitoring {num_processes} workers, expecting {total_files_to_process} files")
    
    def process_collection_buffer():
        nonlocal collection_buffer, insert_success_count, total_processed_count
        
        if not collection_buffer:
            return
        
        logg.log_info(f"Processing buffer: {len(collection_buffer)} records")
        
        # Use database lock manager for safe operations with guaranteed cleanup
        try:
            with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, "INSERT", logg) as locked_db:
                success_count = process_data_batch(locked_db, tablename, collection_buffer, logg)
                
                if success_count > 0:
                    locked_db.commit_and_refresh_cursor()
                    
                insert_success_count += success_count
                total_processed_count += len(collection_buffer)
                
                # Update shared counter for global progress tracking
                with processed_counter.get_lock():
                    processed_counter.value += len(collection_buffer)
                    current_global_progress = processed_counter.value
                
                collection_buffer = []
                
                progress_pct = (current_global_progress / max(total_files_to_process, 1)) * 100
                logg.log_info(f'Progress: {current_global_progress}/{total_files_to_process} ({progress_pct:.1f}%)')
                
        except TableLockedException:
            logg.log_warn(f"Database table {dbname}.{tablename} is locked by another process, skipping this batch")
            # Don't clear buffer, will retry next time
        except Exception as e:
            logg.log_error(f"Error in database operation: {e}")
            # Clear buffer to prevent infinite retry
            collection_buffer = []
    
    def process_exception_queues():
        """Process exception queues with enhanced database connection management."""
        total_exceptions = 0
        
        for i, queue in enumerate(exception_queues):
            queue_exceptions = []
            
            # Get all available exceptions from this queue
            while True:
                try:
                    exception_record = queue.get_nowait()
                    if exception_record is None:
                        break
                    queue_exceptions.append(exception_record)
                except:
                    break
            
            # Insert exceptions if any, using enhanced connection management
            if queue_exceptions:
                try:
                    with DatabaseLockManager(MySQLHelper(exception_db_config), "exception", exception_table, "INSERT", logg) as exception_db:
                        for record in queue_exceptions:
                            exception_db.insert_if_not_exist(exception_table, record)
                        exception_db.commit_and_refresh_cursor()
                        total_exceptions += len(queue_exceptions)
                        logg.log_info(f"Processed {len(queue_exceptions)} exceptions from process {i}")
                        
                except TableLockedException:
                    logg.log_warn(f"Exception table {exception_table} is locked, skipping exception processing for process {i}")
                except Exception as e:
                    logg.log_error(f"Failed to process exceptions from process {i}: {e}")
        
        if total_exceptions > 0:
            logg.log_info(f"Total exceptions processed: {total_exceptions}")
    
    try:
        while True:
            try:
                round_collected = 0
                
                # Collect from all queues
                for _ in range(num_processes):
                    try:
                        data = data_queues[current_process_idx].get_nowait()
                        if data is not None:
                            collection_buffer.append(data)
                            round_collected += 1
                    except:
                        pass
                    current_process_idx = (current_process_idx + 1) % num_processes
                
                collection_rounds += 1
                
                # Track empty rounds
                if round_collected == 0:
                    consecutive_empty_rounds += 1
                else:
                    consecutive_empty_rounds = 0
                
                # Process buffer if thresholds met
                time_elapsed = time.time() - last_collection_time
                if (len(collection_buffer) >= batch_collection_size or 
                    (time_elapsed >= batch_time_threshold and collection_buffer)):
                    process_collection_buffer()
                    last_collection_time = time.time()
                
                # Check exception queues periodically
                if collection_rounds % 50 == 0:
                    process_exception_queues()
                
                # === Primary Termination Condition ===
                with processed_counter.get_lock():
                    current_global_progress = processed_counter.value
                
                if current_global_progress >= total_files_to_process:
                    logg.log_info(f"ALL FILES PROCESSED: {current_global_progress}/{total_files_to_process}")
                    if collection_buffer:
                        process_collection_buffer()
                    break
                
                # Secondary condition: workers finished with reasonable empty rounds
                if worker_finished_event.is_set() and consecutive_empty_rounds > 50:
                    logg.log_info(f"Workers finished with {consecutive_empty_rounds} empty rounds")
                    if collection_buffer:
                        process_collection_buffer()
                    with processed_counter.get_lock():
                        if processed_counter.value >= total_files_to_process:
                            break
                    if consecutive_empty_rounds > 100:
                        logg.log_warn("Forcing termination after extended empty rounds")
                        break
                
                # Sleep only if no data
                if round_collected == 0:
                    time.sleep(0.01)
                
                # Status logging
                if collection_rounds % 500 == 0:
                    elapsed = time.time() - processing_start_time
                    logg.log_info(f'Status - Global progress: {current_global_progress}/{total_files_to_process}, '
                                f'Buffer: {len(collection_buffer)}, Time: {elapsed:.1f}s')
                    
            except KeyboardInterrupt:
                logg.log_warn("Database writer interrupted, performing enhanced cleanup")
                break
                
    except KeyboardInterrupt:
        logg.log_warn("Database writer shutting down due to interrupt")
    except Exception as e:
        logg.log_error(f"Critical error in database writer: {e}")
    finally:
        # Final cleanup with enhanced error handling
        if collection_buffer:
            logg.log_info("Final buffer processing with guaranteed cleanup")
            try:
                # Try to process final buffer with a fresh connection
                with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, "INSERT", logg) as final_db:
                    success_count = process_data_batch(final_db, tablename, collection_buffer, logg)
                    if success_count > 0:
                        final_db.commit_and_refresh_cursor()
                        
                    # Update final progress
                    with processed_counter.get_lock():
                        processed_counter.value += len(collection_buffer)
                        
            except Exception as e:
                logg.log_error(f"Error in final buffer processing: {e}")
        
        # Process final exceptions with enhanced cleanup
        try:
            process_exception_queues()
        except Exception as e:
            logg.log_error(f"Error in final exception processing: {e}")
        
        # Final stats
        with processed_counter.get_lock():
            final_progress = processed_counter.value
        
        total_elapsed = time.time() - processing_start_time
        logg.log_info(f"Database writer completed with guaranteed cleanup: {final_progress}/{total_files_to_process} records in {total_elapsed:.1f}s")
        
        # Explicit cleanup call (redundant but safe)
        main_db_manager.cleanup()
        exception_db_manager.cleanup()
    
    return unlogged_parsed_files, unlogged_unparsed_files


# Keep all other functions unchanged - they don't need database connection cleanup
def process_files_for_global(
    dbconfig: MySQLHelper,
    dbname: str,
    tablename: str,
    rule_dict: Dict[str, Any],
    folder: str,
    recently_days: int,
    time_column: str,
    logg: Logger,
    thread_pool_size: int = 8,
    batch_check_size: int = 100
) -> Tuple[bool, List[str], List[str]]:
    # Original function unchanged - operates on already-connected db
    with wult.DurationTimer(logg, "Get local file list"):
        filelist = wult.get_local_filelist(folder, recently_days)
        logg.log_info(f'Local file count: {len(filelist)}')

    failed_parse_files = []
    failed_update_files = []

    # Ensure 'hash_value' column exists in the table, and create index on (wo, hash_value)
    db = MySQLHelper(dbconfig)
    db.connect()
    db_schema = db.get_table_schema(tablename)

    if 'hash_value' not in db_schema: 
        logg.log_info(f"Adding 'hash_value' column to {tablename}")
        alter_sql = f"ALTER TABLE `{tablename}` ADD COLUMN `hash_value` VARCHAR(32)"
        db.execute_query(alter_sql)
        db.commit_and_refresh_cursor()
        logg.log_ok(f"'hash_value' column added to {tablename}")

    # Create index on (wo, hash_value) if not exists
    index_name = f"_idx"
    # Check if index exists
    index_exists_sql = f"SHOW INDEX FROM `{tablename}` WHERE Key_name = %s"
    exists, result = db.execute_query(index_exists_sql, (index_name,), fetch='one')
    if not (exists and result):
        logg.log_info(f"Creating index {index_name} on ({tablename}.wo, {tablename}.hash_value)")
        create_index_sql = f"CREATE INDEX `{index_name}` ON `{tablename}` (`wo`, `hash_value`)"
        db.execute_query(create_index_sql)
        db.commit_and_refresh_cursor()
        logg.log_ok(f"Index {index_name} created on ({tablename}.wo, {tablename}.hash_value)")
    
    def parse_worker(filename):
        try:
            parser = ParseEngineData()
            if parser.parse_engine(filename, rule_dict):
                data = dict(parser.items())
                if data['assembly_data'] == "None":
                    data['assembly_data'] = None
                if data.get('wo') is not None and data.get('assembly_data') is not None:
                    wo = data['wo']
                    assembly_data = data['assembly_data']
                    csv_reader = csv.reader(StringIO(assembly_data))
                    first_column_values = sorted([row[0] for row in csv_reader if row])
                    combined = ''.join(first_column_values)
                    combined = wo + combined
                    hash_value = xxhash.xxh32(combined).hexdigest()
                    data['hash_value'] = hash_value
                    data['fullpath'] = filename
                    return ('success', filename, data)
                else:
                    return ('parse_fail', filename, None)
            else:
                logg.log_parse_fail(filename, reason="parser.parse_engine returned False")
                return ('parse_fail', filename, None)
        except Exception as e:
            logg.log_error(f"Exception during parsing: {e}, fullpath: {filename}")
            return ('parse_fail', filename, None)

    with wult.DurationTimer(logg, "Parsing files"):
        with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
            futures = [executor.submit(parse_worker, filename) for filename in filelist]
            for future in as_completed(futures):
                status, filename, data = future.result()
                if status == 'success' and data:
                    wo = data.get('wo')
                    hash_value = data.get('hash_value')
                    # Query the DB for existence of this (wo, hash_value)
                    sql = f"SELECT * FROM `{tablename}` WHERE wo=%s and hash_value=%s LIMIT 1"
                    exists = db.execute_query(sql, (wo, hash_value), fetch='one')[1]
                    if not exists and data.get('wo') is not None and data.get('assembly_data') is not None:
                        # Remove 'fullpath' from data before inserting into the database
                        data_to_insert = data.copy()
                        data_to_insert.pop('fullpath', None)
                        db.insert_data(tablename, data_to_insert)
                    elif exists:
                            # Fetch all columns for this row
                            db_schema = db.get_table_schema(tablename)
                            column_names = list(db_schema.keys())
                            db_row = dict(zip(column_names, exists))
                            update_fields = {}
                            for col in column_names:
                                if col in data and db_row.get(col) is None and data.get(col) is not None:
                                    update_fields[col] = data.get(col)
                            if update_fields:
                                set_clause = ', '.join([f"{k}=%s" for k in update_fields.keys()])
                                update_sql = f"UPDATE `{tablename}` SET {set_clause} WHERE wo=%s and hash_value=%s"
                                params = list(update_fields.values()) + [wo, hash_value]
                                try:
                                    db.execute_query(update_sql, params)
                                    db.commit_and_refresh_cursor()
                                except Exception as e:
                                    logg.log_error(f"Failed to update columns for {filename}: {e}")
                                    failed_update_files.append(filename)
                else:
                    failed_parse_files.append(filename)

    db.close()
    all_success = len(failed_parse_files) == 0 and len(failed_update_files) == 0
    return all_success, failed_parse_files, failed_update_files


def worker_process(
    process_id: int,
    file_queue: multiprocessing.Queue,
    data_queue: multiprocessing.Queue,
    exception_queue: multiprocessing.Queue,
    rule_dict: Dict[str, Any],
    threads_per_process: int,
    logger_config: Dict[str, Any],
    worker_finished_counter: multiprocessing.Value,
    worker_finished_lock: multiprocessing.Lock,
    worker_finished_event: multiprocessing.Event,
    total_workers: int,
    processed_counter: multiprocessing.Value,
    total_files_to_process: int
) -> None:
    """
    Worker process - no database connections, only file parsing.
    """
    # Setup process-specific logger
    logg = Logger()
    logg.log_info(f"Worker process {process_id} started with {threads_per_process} threads")
    logg.log_info(f"Worker {process_id} aware of target: {total_files_to_process} files")
    
    # Setup signal handler for graceful interruption
    def signal_handler(signum, frame):
        logg.log_warn(f"Worker {process_id} received signal {signum}, shutting down")
        raise KeyboardInterrupt("Interrupted by signal")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    def parse_worker(filename):
        """Worker function to parse a single file."""
        try:
            parser = ParseEngineData()
            if parser.parse_engine(filename, rule_dict):
                data = dict(parser.items())
                
                # Check required fields
                required_fields = ['wo', 'tbeg', 'tend']
                missing_fields = []
                invalid_fields = []
                
                for field in required_fields:
                    if field not in data:
                        missing_fields.append(field)
                    elif data[field] is None or (isinstance(data[field], str) and data[field].strip() == ''):
                        invalid_fields.append(f"{field}={repr(data[field])}")
                
                if missing_fields or invalid_fields:
                    error_parts = []
                    if missing_fields:
                        error_parts.append(f"Missing fields: {missing_fields}")
                    if invalid_fields:
                        error_parts.append(f"Invalid fields: {invalid_fields}")
                    error_msg = "; ".join(error_parts)
                    
                    exception_record = {
                        "fullpath": filename,
                        "reason": "missing_required_fields" if missing_fields else "invalid_field_values",
                        "exception_context": error_msg
                    }
                    return ('exception', filename, exception_record)
                
                return ('success', filename, data)
            else:
                exception_record = {
                    "fullpath": filename,
                    "reason": "empty_file",
                    "exception_context": "File is empty or unparseable"
                }
                return ('exception', filename, exception_record)

        except Exception as e_inner:
            exception_record = {
                "fullpath": filename,
                "reason": "parsing_error",
                "exception_context": str(e_inner)
            }
            return ('exception', filename, exception_record)
    
    files_processed = 0
    
    try:
        with ThreadPoolExecutor(max_workers=threads_per_process) as executor:
            while True:
                try:
                    # Check global progress periodically
                    if files_processed % 10 == 0:
                        with processed_counter.get_lock():
                            global_progress = processed_counter.value
                        if global_progress >= total_files_to_process:
                            logg.log_info(f"Worker {process_id}: Global target reached ({global_progress}/{total_files_to_process}), exiting")
                            break
                    
                    # Get files to process
                    files_batch = []
                    for _ in range(threads_per_process * 2):
                        try:
                            filename = file_queue.get(timeout=0.5)
                            if filename is None:  # Poison pill
                                if files_batch:
                                    break
                                else:
                                    logg.log_info(f"Worker {process_id} received stop signal")
                                    with worker_finished_lock:
                                        worker_finished_counter.value += 1
                                        if worker_finished_counter.value >= total_workers:
                                            worker_finished_event.set()
                                    return
                            files_batch.append(filename)
                        except:
                            if not files_batch:
                                continue
                            break
                    
                    if not files_batch:
                        continue
                    
                    # Submit files for parsing
                    futures = [executor.submit(parse_worker, filename) for filename in files_batch]
                    
                    # Process results
                    for future in as_completed(futures):
                        try:
                            status, filename, result = future.result()
                            if status == 'success':
                                data_queue.put(result)
                            elif status == 'exception':
                                exception_queue.put(result)
                            files_processed += 1
                        except Exception as e:
                            logg.log_error(f"Future execution error in process {process_id}: {e}")
                            files_processed += 1
                    
                    # Progress logging
                    if files_processed % 100 == 0:
                        logg.log_info(f"Worker process {process_id} processed {files_processed} files")
                        
                except KeyboardInterrupt:
                    logg.log_warn(f"Worker {process_id} interrupted")
                    break
                except Exception as e:
                    logg.log_error(f"Error in worker process {process_id}: {e}")
                    continue
                    
    except KeyboardInterrupt:
        logg.log_warn(f"Worker {process_id} shutting down due to interrupt")
    finally:
        with worker_finished_lock:
            if worker_finished_counter.value < total_workers:
                worker_finished_counter.value += 1
                if worker_finished_counter.value >= total_workers:
                    worker_finished_event.set()
        
        logg.log_info(f"Worker process {process_id} finished with {files_processed} files processed")


def process_data_batch(db: MySQLHelper, tablename: str, batch: List[Dict[str, Any]], logg: Logger) -> int:
    """
    Process a batch of data for database insertion.
    """
    if not batch:
        return 0
    
    try:
        # Group by field structure to handle different schemas
        from collections import defaultdict
        grouped_batches = defaultdict(list)
        
        for data in batch:
            # Create a signature based on sorted field names
            field_signature = tuple(sorted(data.keys()))
            grouped_batches[field_signature].append(data)
        
        success_count = 0
        
        # Process each group separately
        for field_signature, group_batch in grouped_batches.items():
            try:
                # Try batch insert for this group
                success = db.add_data_batch(tablename, group_batch)
                
                if success:
                    success_count += len(group_batch)
                else:
                    # If batch insert fails, fall back to individual inserts
                    logg.log_warn(f"Batch insert failed for {len(group_batch)} records with {len(field_signature)} columns")
                    for data in group_batch:
                        try:
                            result = db.insert_or_update(tablename, data)
                            if result is True:
                                success_count += 1
                            else:
                                logg.log_error(f"Failed to insert record: {result}")
                        except Exception as e:
                            logg.log_error(f"Individual insert failed: {e}")
            
            except Exception as batch_error:
                logg.log_error(f"Batch processing error for group with {len(group_batch)} records: {batch_error}")
        
        return success_count
        
    except Exception as e:
        logg.log_error(f"Error processing data batch: {e}")
        return 0


def process_exception_queues(
    exception_queues: List[multiprocessing.Queue],
    exception_db: MySQLHelper,
    exception_table: str,
    logg: Logger
) -> None:
    """
    Process all exception queues and insert records into exception database.
    """
    total_exceptions = 0
    
    for i, queue in enumerate(exception_queues):
        queue_exceptions = []
        
        # Get all available exceptions from this queue
        while True:
            try:
                exception_record = queue.get_nowait()
                if exception_record is None:  # Poison pill
                    break
                queue_exceptions.append(exception_record)
            except:
                break
        
        # Insert exceptions if any
        if queue_exceptions:
            try:
                for record in queue_exceptions:
                    exception_db.insert_if_not_exist(exception_table, record)
                exception_db.commit_and_refresh_cursor()
                total_exceptions += len(queue_exceptions)
                logg.log_info(f"Processed {len(queue_exceptions)} exceptions from process {i}")
            except Exception as e:
                logg.log_error(f"Failed to process exceptions from process {i}: {e}")
    
    if total_exceptions > 0:
        logg.log_info(f"Total exceptions processed: {total_exceptions}")


def process_files(
    db_config: Dict[str, Any],
    dbname: str,
    tablename: str,
    rule_dict: Dict[str, Any],
    folder: str,
    recently_days: int,
    time_column: str,
    logg: Logger,
    exception_db_config: Dict[str, Any],
    exception_table: str,
    num_processes: int = 7,
    threads_per_process: int = 8,
    batch_size: int = 30,
    commit_threshold: int = 150,
    exception_check_interval: int = 100,
    batch_collection_size: int = 2000,
    batch_time_threshold: float = 5.0
) -> Tuple[bool, List[str], List[str]]:
    """
    Enhanced process_files with table locking and short connections.
    """
    # Get file lists using short connection
    with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, "READ", logg) as db:
        with wult.DurationTimer(logg, "Get local file list"):
            filelist = wult.get_local_filelist(folder, recently_days)
            logg.log_info(f'Local file count: {len(filelist)}')

        with wult.DurationTimer(logg, "Get db exist fullpath"):
            existing_paths = db.get_existing_fullpaths(tablename, time_column, recently_days + 1)
            logg.log_info(f'DB exist file count: {len(existing_paths)}')

            difference_files = list(set(filelist) - set(existing_paths))
            difference_files.sort(key=lambda x: x.split('/')[6], reverse=True)
            logg.log_info(f'Difference file count: {len(difference_files)}')

    if not difference_files:
        logg.log_ok("No new files to process")
        return True, [], []

    # Store the exact number of files to process
    total_files_to_process = len(difference_files)
    logg.log_info(f"EXACT TARGET: {total_files_to_process} files to process")

    # Create Manager for better shared memory management
    manager = multiprocessing.Manager()
    
    # Create queues
    file_queue = multiprocessing.Queue()
    data_queues = [multiprocessing.Queue() for _ in range(num_processes)]
    exception_queues = [multiprocessing.Queue() for _ in range(num_processes)]
    
    # Enhanced synchronization with global progress counter
    worker_finished_counter = multiprocessing.Value('i', 0)
    worker_finished_lock = multiprocessing.Lock()
    worker_finished_event = multiprocessing.Event()
    processed_counter = multiprocessing.Value('i', 0)  # Global progress counter

    # Populate file queue
    for filename in difference_files:
        file_queue.put(filename)

    # Add poison pills
    for _ in range(num_processes):
        file_queue.put(None)

    # Start processes
    processes = []

    with wult.DurationTimer(logg, "Multi-process parsing and database writing"):
        # Start workers
        for i in range(num_processes):
            process = multiprocessing.Process(
                target=worker_process,
                args=(
                    i, file_queue, data_queues[i], exception_queues[i],
                    rule_dict, threads_per_process, {},
                    worker_finished_counter, worker_finished_lock,
                    worker_finished_event, num_processes,
                    processed_counter,  # Pass the global counter
                    total_files_to_process  # Pass total files count
                )
            )
            process.start()
            processes.append(process)
            logg.log_info(f"Started worker process {i}")

        # Start enhanced database writer with guaranteed cleanup
        db_writer = multiprocessing.Process(
            target=database_writer_process,
            args=(
                data_queues, exception_queues, db_config, dbname, tablename,
                exception_db_config, exception_table, batch_size,
                commit_threshold, exception_check_interval, {},
                worker_finished_event, total_files_to_process,
                processed_counter,  # Pass the global counter
                batch_collection_size, batch_time_threshold
            )
        )
        db_writer.start()
        logg.log_info("Started enhanced database writer with guaranteed cleanup")

        # Monitor progress
        start_time = time.time()
        last_progress_check = start_time
        max_wait_time = max(300, total_files_to_process * 0.2)  # Dynamic max wait
        
        try:
            while True:
                current_time = time.time()
                elapsed = current_time - start_time
                
                # Check progress every 10 seconds
                if current_time - last_progress_check >= 10:
                    with processed_counter.get_lock():
                        current_progress = processed_counter.value
                    
                    progress_pct = (current_progress / total_files_to_process) * 100
                    logg.log_info(f"Monitor: {current_progress}/{total_files_to_process} ({progress_pct:.1f}%) processed in {elapsed:.1f}s")
                    
                    # Check if all files are processed
                    if current_progress >= total_files_to_process:
                        logg.log_ok(f"TARGET REACHED: All {total_files_to_process} files processed!")
                        time.sleep(2)  # Give a moment for final commits
                        break
                    
                    last_progress_check = current_time
                
                # Check if processes are still alive
                alive_workers = sum(1 for p in processes if p.is_alive())
                db_writer_alive = db_writer.is_alive()
                
                if not db_writer_alive:
                    logg.log_info("Database writer finished")
                    break
                
                if alive_workers == 0 and not db_writer_alive:
                    logg.log_info("All processes finished")
                    break
                
                # Safety timeout
                if elapsed > max_wait_time:
                    with processed_counter.get_lock():
                        current_progress = processed_counter.value
                    if current_progress < total_files_to_process:
                        logg.log_warn(f"Timeout reached with {total_files_to_process - current_progress} files remaining")
                    break
                
                time.sleep(1)
        
        finally:
            # Enhanced graceful shutdown with guaranteed cleanup
            logg.log_info("Initiating enhanced graceful shutdown with guaranteed cleanup...")
            
            # Send termination signals
            for i, process in enumerate(processes):
                if process.is_alive():
                    process.terminate()
                    logg.log_info(f"Sent termination signal to worker {i}")
            
            if db_writer.is_alive():
                db_writer.terminate()
                logg.log_info("Sent termination signal to database writer")
            
            # Wait for processes to finish with enhanced cleanup
            for i, process in enumerate(processes):
                process.join(timeout=10)
                if process.is_alive():
                    logg.log_warn(f"Force killing worker {i}")
                    process.kill()
                    process.join()

            db_writer.join(timeout=15)
            if db_writer.is_alive():
                logg.log_warn("Force killing database writer")
                db_writer.kill()
                db_writer.join()

    # Final progress check
    with processed_counter.get_lock():
        final_progress = processed_counter.value
    
    success = (final_progress >= total_files_to_process)
    if success:
        logg.log_ok(f"Processing completed successfully: {final_progress}/{total_files_to_process} files")
    else:
        logg.log_warn(f"Processing incomplete: {final_progress}/{total_files_to_process} files")
    
    return success, [], []


class TableLockedException(Exception):
    """Custom exception for when a table is locked."""
    pass


class DatabaseLockManager:
    """
    Enhanced context manager with guaranteed cleanup for database operations with table locking.
    """
    
    def __init__(self, db_helper: MySQLHelper, database_name: str, table_name: str, 
                 operation_type: str, logger, lock_db_name: str = "mysql"):
        """
        Initialize the enhanced lock manager with guaranteed cleanup.
        """
        self.db_helper = db_helper
        self.database_name = database_name
        self.table_name = table_name
        self.operation_type = operation_type
        self.logger = logger
        self.lock_db_name = lock_db_name
        self.lock_acquired = False
        self.connection_opened = False
        self.cleanup_registered = False
        
    def _register_cleanup(self):
        """Register cleanup handlers."""
        if self.cleanup_registered:
            return
            
        atexit.register(self._force_cleanup)
        self.cleanup_registered = True
        
    def _force_cleanup(self):
        """Force cleanup regardless of state."""
        try:
            if self.lock_acquired and self.connection_opened:
                self.db_helper.use_database(self.lock_db_name)
                self.db_helper.release_table_lock(self.database_name, self.table_name)
                
            if self.connection_opened:
                self.db_helper.close()
                
        except Exception as e:
            self.logger.log_error(f"Error during force cleanup: {e}")
        finally:
            self.lock_acquired = False
            self.connection_opened = False
        
    def __enter__(self):
        """
        Enhanced enter context with guaranteed cleanup registration.
        """
        try:
            # Step 1: Connect to database
            if not self.db_helper.connect():
                raise Exception("Failed to connect to database")
            self.connection_opened = True
            
            # new: re-new connect status
            self.db_helper.refresh_connection()
            
            # Step 2: use db
            self.db_helper.use_database(self.lock_db_name)
            
            # Step 3: create lock
            self.db_helper.create_lock_table()
            
            # new: clean cache
            self.db_helper.clear_table_locks_cache()
            
            # Step 4: Try to acquire table lock
            if not self.db_helper.acquire_table_lock(
                self.database_name, self.table_name, self.operation_type
            ):
                self.logger.log_warn(f"Table {self.database_name}.{self.table_name} is currently locked by another process, skipping operation")
                raise TableLockedException(f"Table {self.database_name}.{self.table_name} is locked")
            
            self.lock_acquired = True
            
            # Step 5: Now switch to the target database for actual operations
            self.db_helper.use_database(self.database_name)
            
            return self.db_helper
            
        except Exception as e:
            # Cleanup on failure
            self.__exit__(None, None, None)
            raise e
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Enhanced exit context with guaranteed cleanup.
        """
        self._force_cleanup()