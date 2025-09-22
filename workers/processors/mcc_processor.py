#!/usr/bin/python3

import multiprocessing
from multiprocessing import Queue, Value, Lock, Event
import time
from typing import Dict, Any, List, Tuple, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import signal
from collections import defaultdict
from datetime import datetime, timedelta

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.MySQLHelper import MySQLHelper
from core.Logger import Logger
from ParseEngine.engine.ParseEngineData import ParseEngineData


def mcc_process(
    db_config: Dict[str, Any],
    dbname: str,
    tablename: str,
    mcc_rule_dict: Dict[str, Any],
    time_column: str = "tbeg",
    logg: Logger = None,
    recently_days: int = 7,
    num_workers: int = 7,
    threads_per_worker: int = 8
) -> bool:
    """
    Process MCC files by finding corresponding .mcc files for existing database records.
    Only processes records within the recent time window where ALL mcc fields are NA/NULL.
    
    Args:
        db_config: Database configuration
        dbname: Database name
        tablename: Table name to update
        mcc_rule_dict: MCC rule dictionary containing parsing rules
        time_column: Time column for filtering recent records (default: "tbeg")
        logg: Logger instance
        recently_days: How many recent days of data to process
        num_workers: Number of worker processes
        threads_per_worker: Number of threads per worker
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not logg:
        logg = Logger()
        
    logg.log_info(f"Starting MCC processing for table: {tablename}, recent {recently_days} days")
    
    # Variables for process management
    processes = []
    db_writer = None
    
    try:
        # Step 1: Add MCC fields to table schema if needed
        with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, "MCC_SCHEMA", logg) as locked_db:
            current_schema = locked_db.get_table_schema(tablename)
            
            # Extract MCC fields from rule_dict
            mcc_schema_fields = {}
            mcc_field_names = []
            for field, rules in mcc_rule_dict.items():
                if 'dbtype' in rules:
                    mcc_field_names.append(field)
                    # Check if this field doesn't exist in current schema
                    if field not in current_schema:
                        mcc_schema_fields[field] = rules['dbtype']
            
            if not mcc_field_names:
                logg.log_error("No MCC fields defined in rule")
                return False
            
            if mcc_schema_fields:
                logg.log_info(f"Adding MCC fields to table: {list(mcc_schema_fields.keys())}")
                warnings, schema_updated = locked_db.sync_table_schema(tablename, mcc_schema_fields)
                for w in warnings:
                    logg.log_warn(w)
                
                if not schema_updated:
                    logg.log_error("Failed to add MCC fields to table")
                    return False
                    
                logg.log_ok("MCC fields added successfully")
            else:
                logg.log_info("MCC fields already exist in table")
        
        # Step 2: Get records from database that need MCC processing
        with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, "MCC_READ", logg) as locked_db:
            # Calculate date threshold (recently_days ago)
            date_threshold = datetime.now() - timedelta(days=recently_days)
            date_threshold_str = date_threshold.strftime('%Y-%m-%d')
            
            # Build condition to check if ALL MCC fields are NULL/NA
            mcc_null_conditions = []
            for field in mcc_field_names:
                mcc_null_conditions.append(f"(`{field}` IS NULL OR `{field}` = '' OR `{field}` = 'NA' OR mcc_err_check IS NULL OR mcc_err_upload IS NULL)")
            
            # All MCC fields must be NULL/NA (AND condition)
            all_mcc_null = " AND ".join(mcc_null_conditions)
            
            # Time filter: only records within recent days
            time_filter = f"`{time_column}` >= '{date_threshold_str}'"
            
            # Combined WHERE clause
            where_clause = f"({all_mcc_null}) AND ({time_filter})"
            
            sql = f"SELECT * FROM `{tablename}` WHERE {where_clause}"
            logg.log_info(f"Fetching records needing MCC processing from {date_threshold_str}...")
            
            success, records = locked_db.execute_query(sql, fetch='all')
            if not success or not records:
                logg.log_info("No records need MCC processing")
                return True
            
            # Convert to dictionaries
            columns = [desc[0] for desc in locked_db.cursor.description]
            records_dict = [dict(zip(columns, row)) for row in records]
            
            logg.log_info(f"Found {len(records_dict)} records needing MCC processing")
        
        # Step 3: Prepare work items by transforming paths
        work_items = []
        for record in records_dict:
            original_fullpath = record.get('fullpath')
            if not original_fullpath:
                logg.log_warn(f"Record missing fullpath, skipping: WO={record.get('wo')}")
                continue
            
            # Transform the path: .cap -> .mcc, /PT/ -> /PT-MCC/
            mcc_fullpath = original_fullpath.replace('.cap', '.mcc').replace('/PT/', '/PT-MCC/')
            
            work_items.append({
                'original_record': record,
                'original_fullpath': original_fullpath,
                'mcc_fullpath': mcc_fullpath,
                'wo': record.get('wo')
            })
        
        if not work_items:
            logg.log_info("No valid work items to process")
            return True
        
        total_items_to_process = len(work_items)
        logg.log_info(f"EXACT TARGET: {total_items_to_process} records to process for MCC")
        
        # Step 4: Process with multi-processing
        # Create queues
        work_queue = multiprocessing.Queue()
        data_queues = [multiprocessing.Queue() for _ in range(num_workers)]
        
        # Synchronization primitives
        worker_finished_counter = multiprocessing.Value('i', 0)
        worker_finished_lock = multiprocessing.Lock()
        worker_finished_event = multiprocessing.Event()
        processed_counter = multiprocessing.Value('i', 0)
        
        # Populate work queue
        for item in work_items:
            work_queue.put(item)
        
        # Add poison pills
        for _ in range(num_workers):
            work_queue.put(None)
        
        # Start worker processes
        for i in range(num_workers):
            p = multiprocessing.Process(
                target=mcc_worker_process,
                args=(
                    i, work_queue, data_queues[i], mcc_rule_dict,
                    mcc_field_names, threads_per_worker,
                    worker_finished_counter, worker_finished_lock,
                    worker_finished_event, num_workers,
                    processed_counter, total_items_to_process
                )
            )
            p.start()
            processes.append(p)
        
        # Start database writer
        db_writer = multiprocessing.Process(
            target=mcc_database_writer_process,
            args=(
                data_queues, db_config, dbname, tablename,
                worker_finished_event, total_items_to_process,
                processed_counter,
                1000, 5.0
            )
        )
        db_writer.start()
        
        logg.log_info(f"Started {num_workers} MCC workers and 1 database writer")
        
        # Monitor progress
        start_time = time.time()
        last_progress_check = start_time
        max_wait_time = max(300, total_items_to_process * 0.1)
        
        while True:
            current_time = time.time()
            elapsed = current_time - start_time
            
            if current_time - last_progress_check >= 10:
                with processed_counter.get_lock():
                    current_progress = processed_counter.value
                
                progress_pct = (current_progress / total_items_to_process) * 100
                alive_workers = sum(1 for p in processes if p.is_alive())
                writer_alive = db_writer.is_alive()
                
                logg.log_info(f"MCC monitor: {current_progress}/{total_items_to_process} ({progress_pct:.1f}%) "
                             f"processed in {elapsed:.1f}s, {alive_workers} workers alive, writer: {writer_alive}")
                
                if current_progress >= total_items_to_process:
                    logg.log_ok(f"TARGET REACHED: All {total_items_to_process} records processed!")
                    time.sleep(2)
                    break
                
                last_progress_check = current_time
            
            alive_workers = sum(1 for p in processes if p.is_alive())
            writer_alive = db_writer.is_alive()
            
            if not writer_alive:
                logg.log_info("Database writer finished")
                break
            
            if alive_workers == 0 and not writer_alive:
                logg.log_info("All processes finished")
                break
            
            if elapsed > max_wait_time:
                with processed_counter.get_lock():
                    current_progress = processed_counter.value
                if current_progress < total_items_to_process:
                    logg.log_warn(f"Timeout reached with {total_items_to_process - current_progress} records remaining")
                break
            
            time.sleep(1)
        
        # Final verification
        with processed_counter.get_lock():
            final_progress = processed_counter.value
        
        total_elapsed = time.time() - start_time
        success = (final_progress >= total_items_to_process)
        
        if success:
            logg.log_ok(f"MCC processing completed: {final_progress}/{total_items_to_process} records in {total_elapsed:.1f}s")
        else:
            logg.log_warn(f"MCC processing incomplete: {final_progress}/{total_items_to_process} records in {total_elapsed:.1f}s")
        
        return success
        
    except Exception as e:
        logg.log_error(f"Error during MCC processing: {e}")
        return False
    
    finally:
        # CRITICAL: Always ensure processes are terminated
        logg.log_info("Initiating graceful shutdown...")
        
        # Terminate worker processes
        for i, process in enumerate(processes):
            if process and process.is_alive():
                try:
                    process.terminate()
                    logg.log_info(f"Sent termination signal to MCC worker {i}")
                except Exception as e:
                    logg.log_error(f"Error terminating worker {i}: {e}")
        
        # Terminate database writer
        if db_writer and db_writer.is_alive():
            try:
                db_writer.terminate()
                logg.log_info("Sent termination signal to MCC writer")
            except Exception as e:
                logg.log_error(f"Error terminating MCC writer: {e}")
        
        # Wait for graceful shutdown
        for i, process in enumerate(processes):
            if process:
                try:
                    process.join(timeout=10)
                    if process.is_alive():
                        logg.log_warn(f"Force killing MCC worker {i}")
                        process.kill()
                        process.join()
                except Exception as e:
                    logg.log_error(f"Error joining worker {i}: {e}")

        if db_writer:
            try:
                db_writer.join(timeout=15)
                if db_writer.is_alive():
                    logg.log_warn("Force killing MCC writer")
                    db_writer.kill()
                    db_writer.join()
            except Exception as e:
                logg.log_error(f"Error joining MCC writer: {e}")


def mcc_worker_process(
    process_id: int,
    work_queue: multiprocessing.Queue,
    data_queue: multiprocessing.Queue,
    mcc_rule_dict: Dict[str, Any],
    mcc_field_names: List[str],
    threads_per_process: int,
    worker_finished_counter: multiprocessing.Value,
    worker_finished_lock: multiprocessing.Lock,
    worker_finished_event: multiprocessing.Event,
    total_workers: int,
    processed_counter: multiprocessing.Value,
    total_items_to_process: int
) -> None:
    """
    MCC worker process that checks for .mcc files and parses them or sets NA.
    Creates complete record copies with updated MCC fields.
    """
    logg = Logger()
    logg.log_info(f"MCC worker {process_id} started, target: {total_items_to_process} items")
    
    def signal_handler(signum, frame):
        logg.log_warn(f"MCC worker {process_id} received signal {signum}")
        raise KeyboardInterrupt("Interrupted by signal")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    def process_mcc_item(work_item: Dict[str, Any]):
        """Process a single MCC work item."""
        try:
            mcc_fullpath = work_item['mcc_fullpath']
            original_record = work_item['original_record']
            wo = work_item['wo']
            
            # Copy the entire original record
            updated_record = original_record.copy()
            
            # Check if MCC file exists
            if os.path.exists(mcc_fullpath):
                logg.log_info(f"Found MCC file: {mcc_fullpath}")
                
                # Parse the MCC file with selective fields
                parser = ParseEngineData(selective_fields=mcc_field_names)
                if parser.parse_engine(mcc_fullpath, mcc_rule_dict):
                    parsed_data = dict(parser.items())
                    
                    # Update only MCC fields in the record
                    for field in mcc_field_names:
                        if field in parsed_data:
                            updated_record[field] = parsed_data[field]
                        else:
                            updated_record[field] = "NA"
                    
                    logg.log_info(f"Successfully parsed MCC file for WO: {wo}")
                else:
                    # Parsing failed, set all MCC fields to NA
                    logg.log_warn(f"Failed to parse MCC file: {mcc_fullpath}")
                    for field in mcc_field_names:
                        updated_record[field] = "NA"
            else:
                # MCC file doesn't exist, set all MCC fields to NA
                logg.log_info(f"MCC file not found: {mcc_fullpath}")
                for field in mcc_field_names:
                    updated_record[field] = "NA"
            
            return updated_record
            
        except Exception as e:
            logg.log_error(f"Error processing MCC item: {e}")
            # Return original record with MCC fields set to NA on error
            error_record = work_item['original_record'].copy()
            for field in mcc_field_names:
                error_record[field] = "NA"
            return error_record
    
    items_processed = 0
    
    try:
        with ThreadPoolExecutor(max_workers=threads_per_process) as executor:
            while True:
                try:
                    # Check global progress
                    if items_processed % 10 == 0:
                        with processed_counter.get_lock():
                            global_progress = processed_counter.value
                        if global_progress >= total_items_to_process:
                            logg.log_info(f"MCC worker {process_id}: Target reached")
                            break
                    
                    # Get work batch
                    work_batch = []
                    for _ in range(threads_per_process * 2):
                        try:
                            item = work_queue.get(timeout=0.5)
                            if item is None:  # Poison pill
                                if work_batch:
                                    break
                                else:
                                    logg.log_info(f"MCC worker {process_id} received stop signal")
                                    with worker_finished_lock:
                                        worker_finished_counter.value += 1
                                        if worker_finished_counter.value >= total_workers:
                                            worker_finished_event.set()
                                    return
                            work_batch.append(item)
                        except:
                            if not work_batch:
                                continue
                            break
                    
                    if not work_batch:
                        continue
                    
                    # Process batch
                    futures = []
                    for work_item in work_batch:
                        future = executor.submit(process_mcc_item, work_item)
                        futures.append(future)
                    
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if result:
                                data_queue.put(result)
                            items_processed += 1
                        except Exception as e:
                            logg.log_error(f"Future error in MCC worker {process_id}: {e}")
                            items_processed += 1
                    
                    if items_processed % 100 == 0:
                        logg.log_info(f"MCC worker {process_id}: {items_processed} items processed")
                        
                except KeyboardInterrupt:
                    logg.log_warn(f"MCC worker {process_id} interrupted")
                    break
                except Exception as e:
                    logg.log_error(f"Error in MCC worker {process_id}: {e}")
                    continue
                    
    except KeyboardInterrupt:
        logg.log_warn(f"MCC worker {process_id} shutting down")
    finally:
        with worker_finished_lock:
            if worker_finished_counter.value < total_workers:
                worker_finished_counter.value += 1
                if worker_finished_counter.value >= total_workers:
                    worker_finished_event.set()
        logg.log_info(f"MCC worker {process_id} finished: {items_processed} items")


def mcc_database_writer_process(
    data_queues: List[multiprocessing.Queue],
    db_config: Dict[str, Any],
    dbname: str,
    tablename: str,
    worker_finished_event: multiprocessing.Event,
    total_items_to_process: int,
    processed_counter: multiprocessing.Value,
    batch_collection_size: int = 1000,
    batch_time_threshold: float = 5.0
) -> int:
    """
    Database writer for MCC processing with short connection strategy.
    Uses delete + insert pattern for each batch.
    """
    logg = Logger()
    logg.log_info(f"MCC DB writer started, expecting {total_items_to_process} items")
    
    def signal_handler(signum, frame):
        logg.log_warn(f"MCC writer received signal {signum}")
        raise KeyboardInterrupt("Interrupted by signal")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    collection_buffer = []
    last_collection_time = time.time()
    total_processed = 0
    consecutive_empty_rounds = 0
    num_processes = len(data_queues)
    current_process_idx = 0
    start_time = time.time()
    
    def process_buffer():
        nonlocal collection_buffer, total_processed
        
        if not collection_buffer:
            return 0

        buffer_size = len(collection_buffer)
        logg.log_info(f"Processing MCC buffer: {buffer_size} records (short connection)")
        
        try:
            # Short connection: only connect when needed
            with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, "MCC_UPDATE", logg) as locked_db:
                # Extract fullpaths for batch deletion
                buffer_fullpaths = []
                for record in collection_buffer:
                    fullpath = record.get('fullpath')
                    if fullpath:
                        buffer_fullpaths.append(fullpath)
                
                if not buffer_fullpaths:
                    logg.log_warn("No valid fullpaths in buffer")
                    collection_buffer = []
                    return 0
                
                # Step 1: Batch delete existing records
                placeholders = ', '.join(['%s'] * len(buffer_fullpaths))
                delete_sql = f"DELETE FROM `{tablename}` WHERE `fullpath` IN ({placeholders})"
                
                success, _ = locked_db.execute_query(delete_sql, buffer_fullpaths)
                if not success:
                    logg.log_error("Failed to delete existing records")
                    collection_buffer = []
                    return 0
                
                logg.log_info(f"Deleted {len(buffer_fullpaths)} existing records")
                
                # Step 2: Batch insert updated records
                # Group by field structure for batch insert
                from collections import defaultdict
                grouped_batches = defaultdict(list)
                
                for record in collection_buffer:
                    field_signature = tuple(sorted(record.keys()))
                    grouped_batches[field_signature].append(record)
                
                insert_success_count = 0
                for field_signature, group_batch in grouped_batches.items():
                    try:
                        # Use batch insert
                        success = locked_db.add_data_batch(tablename, group_batch)
                        if success:
                            insert_success_count += len(group_batch)
                        else:
                            # Fallback to individual inserts if batch fails
                            logg.log_warn(f"Batch insert failed for {len(group_batch)} MCC records, trying individual inserts")
                            for record in group_batch:
                                try:
                                    success = locked_db.insert_data(tablename, record)
                                    if success:
                                        insert_success_count += 1
                                except Exception as e:
                                    logg.log_error(f"Individual insert failed: {e}")
                    except Exception as e:
                        logg.log_error(f"Error inserting MCC batch: {e}")
                
                # Commit and close connection immediately
                locked_db.commit_and_refresh_cursor()
                logg.log_info(f"Successfully inserted {insert_success_count}/{len(collection_buffer)} MCC records")
                
                # Connection will be closed automatically by context manager
        
        except TableLockedException:
            logg.log_warn(f"Table {dbname}.{tablename} is locked, will retry MCC updates")
            return 0
        except Exception as e:
            logg.log_error(f"Error processing MCC buffer: {e}")
            collection_buffer = []
            return 0
        
        total_processed += buffer_size
        collection_buffer = []
        
        # Update global progress counter
        with processed_counter.get_lock():
            processed_counter.value += buffer_size
            current_global_progress = processed_counter.value
        
        progress_pct = (current_global_progress / max(total_items_to_process, 1)) * 100
        logg.log_info(f"MCC progress: {current_global_progress}/{total_items_to_process} ({progress_pct:.1f}%)")
        
        return buffer_size
    
    try:
        while True:
            try:
                round_collected = 0
                
                # Collect from all worker queues
                for _ in range(num_processes):
                    try:
                        data = data_queues[current_process_idx].get_nowait()
                        if data is not None:
                            collection_buffer.append(data)
                            round_collected += 1
                    except:
                        pass
                    current_process_idx = (current_process_idx + 1) % num_processes
                
                if round_collected == 0:
                    consecutive_empty_rounds += 1
                else:
                    consecutive_empty_rounds = 0
                
                # Process buffer if threshold met
                time_elapsed = time.time() - last_collection_time
                if (len(collection_buffer) >= batch_collection_size or 
                    (time_elapsed >= batch_time_threshold and collection_buffer)):
                    process_buffer()
                    last_collection_time = time.time()
                
                # Check global progress counter
                with processed_counter.get_lock():
                    current_global_progress = processed_counter.value
                
                if current_global_progress >= total_items_to_process:
                    logg.log_info(f"ALL MCC ITEMS PROCESSED: {current_global_progress}/{total_items_to_process}")
                    if collection_buffer:
                        process_buffer()
                    break
                
                # Secondary condition: workers finished
                if worker_finished_event.is_set() and consecutive_empty_rounds > 50:
                    logg.log_info(f"Workers finished with {consecutive_empty_rounds} empty rounds")
                    if collection_buffer:
                        process_buffer()
                    with processed_counter.get_lock():
                        if processed_counter.value >= total_items_to_process:
                            break
                    if consecutive_empty_rounds > 100:
                        logg.log_warn("Forcing termination after extended empty rounds")
                        break
                
                if round_collected == 0:
                    time.sleep(0.01)
                    
            except KeyboardInterrupt:
                logg.log_warn("MCC writer interrupted")
                break
                
    finally:
        if collection_buffer:
            logg.log_info("Final MCC buffer processing")
            process_buffer()
        
        with processed_counter.get_lock():
            final_progress = processed_counter.value
        
        total_elapsed = time.time() - start_time
        if final_progress > 0:
            avg_rate = final_progress / total_elapsed
            completion_pct = (final_progress / total_items_to_process) * 100
            logg.log_info(f"MCC writer completed: {final_progress}/{total_items_to_process} "
                         f"({completion_pct:.1f}%) in {total_elapsed:.1f}s ({avg_rate:.1f} items/sec)")
        else:
            logg.log_info(f"MCC writer completed: 0 items processed in {total_elapsed:.1f}s")
    
    return total_processed


class TableLockedException(Exception):
    """Custom exception for when a table is locked."""
    pass


class DatabaseLockManager:
    """
    Context manager for database operations with table locking and short connections.
    """
    
    def __init__(self, db_helper: MySQLHelper, database_name: str, table_name: str, 
                 operation_type: str, logger, lock_db_name: str = "mysql"):
        """
        Initialize the lock manager.
        
        :param db_helper: MySQLHelper instance (will be connected fresh)
        :param database_name: Target database name for operations
        :param table_name: Target table name for operations  
        :param operation_type: Type of operation (INSERT, DELETE, RE_SCHEMA, etc.)
        :param logger: Logger instance
        :param lock_db_name: Database to store lock table (default: mysql system db)
        """
        self.db_helper = db_helper
        self.database_name = database_name
        self.table_name = table_name
        self.operation_type = operation_type
        self.logger = logger
        self.lock_db_name = lock_db_name  # Store locks in system database
        self.lock_acquired = False
        self.connection_opened = False
        
    def __enter__(self):
        """
        Enter context: connect to database and acquire table lock.
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
        Exit context: release lock and close connection.
        """
        try:
            if self.lock_acquired:
                # Switch back to lock database to release the lock
                if self.connection_opened:
                    self.db_helper.use_database(self.lock_db_name)
                    self.db_helper.release_table_lock(self.database_name, self.table_name)
                    self.lock_acquired = False
                
            if self.connection_opened:
                self.db_helper.close()
                self.connection_opened = False
                
        except Exception as e:
            self.logger.log_error(f"Error during cleanup: {e}")