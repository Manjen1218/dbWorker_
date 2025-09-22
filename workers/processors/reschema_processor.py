#!/usr/bin/python3

import multiprocessing
from multiprocessing import Queue, Value, Lock, Event
import time
from typing import Dict, Any, List, Tuple, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import signal
import atexit
import threading

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.MySQLHelper import MySQLHelper
from core.Logger import Logger
from ParseEngine.engine.ParseEngineData import ParseEngineData


class DatabaseConnectionManager:
    """
    Enhanced database connection manager that ensures cleanup under all circumstances.
    """
    def __init__(self, db_config: Dict[str, Any], logger: Logger):
        self.db_config = db_config
        self.logger = logger
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
            self.logger.log_error(f"Database connection failed: {e}")
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
            self.logger.log_warn(f"Received signal {signum}, performing database cleanup")
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
                self.logger.log_info("Forcing database connection cleanup")
                self.db.close()
                self.connected = False
            except Exception as e:
                # Even if cleanup fails, mark as disconnected
                self.logger.log_error(f"Error during database cleanup: {e}")
                self.connected = False
    
    def __enter__(self):
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


def perform_reschema(
    db_config: Dict[str, Any],
    dbname: str,
    tablename: str,
    rule_dict: Dict[str, Any],
    new_fields: List[str],
    time_column: str,
    logg: Logger,
    num_workers: int = 7,
    threads_per_worker: int = 8
) -> bool:
    """
    Enhanced re-schema operation with guaranteed connection cleanup.
    """
    logg.log_info(f"Starting enhanced re-schema for table {tablename} with new fields: {new_fields}")
    
    # Get records needing re-schema using enhanced connection manager
    try:
        with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, "RE_SCHEMA_READ", logg) as db:
            # Build query to get ALL records with NULL values in new fields
            where_conditions = " OR ".join([f"`{field}` IS NULL OR `{field}` = ''" for field in new_fields])
            sql = f"SELECT * FROM `{tablename}` WHERE ({where_conditions})"
            
            success, records = db.execute_query(sql, fetch='all')
            if not success or not records:
                logg.log_info("No records need re-schema update")
                return True
            
            # Get column names
            columns = [desc[0] for desc in db.cursor.description]
            records_dict = [dict(zip(columns, row)) for row in records]
            
            logg.log_info(f"Found {len(records_dict)} records needing re-schema")
    except TableLockedException:
        logg.log_warn(f"Table {dbname}.{tablename} is locked during re-schema read, skipping re-schema")
        return False
    except Exception as e:
        logg.log_error(f"Error reading records for re-schema: {e}")
        return False
    
    # Create queues
    fullpath_queue = multiprocessing.Queue()
    data_queues = [multiprocessing.Queue() for _ in range(num_workers)]
    
    # Enhanced synchronization with global progress counter
    worker_finished_counter = multiprocessing.Value('i', 0)
    worker_finished_lock = multiprocessing.Lock()
    worker_finished_event = multiprocessing.Event()
    processed_counter = multiprocessing.Value('i', 0)  # Global progress counter
    
    # Populate work queue
    valid_records_count = 0
    for record in records_dict:
        if 'fullpath' in record and record['fullpath']:
            fullpath_queue.put((record['fullpath'], record))
            valid_records_count += 1
    
    logg.log_info(f"EXACT TARGET: {valid_records_count} records to re-schema")
    
    # Add poison pills
    for _ in range(num_workers):
        fullpath_queue.put(None)
    
    # Start worker processes
    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(
            target=reschema_worker_process,
            args=(
                i, fullpath_queue, data_queues[i], rule_dict,
                new_fields, threads_per_worker,
                worker_finished_counter, worker_finished_lock,
                worker_finished_event, num_workers,
                processed_counter, valid_records_count,
                db_config, dbname, tablename
            )
        )
        p.start()
        processes.append(p)
    
    # Start enhanced database writer
    db_writer = multiprocessing.Process(
        target=reschema_database_writer_process,
        args=(
            data_queues, db_config, dbname, tablename,
            worker_finished_event, valid_records_count,
            processed_counter,
            1000, 5.0
        )
    )
    db_writer.start()
    
    logg.log_info(f"Started {num_workers} re-schema workers and 1 database writer")
    
    # Monitor progress
    start_time = time.time()
    last_progress_check = start_time
    max_wait_time = max(300, valid_records_count * 0.1)
    
    try:
        while True:
            current_time = time.time()
            elapsed = current_time - start_time
            
            # Check progress every 10 seconds
            if current_time - last_progress_check >= 10:
                with processed_counter.get_lock():
                    current_progress = processed_counter.value
                
                progress_pct = (current_progress / valid_records_count) * 100
                alive_workers = sum(1 for p in processes if p.is_alive())
                writer_alive = db_writer.is_alive()
                
                logg.log_info(f"Re-schema monitor: {current_progress}/{valid_records_count} ({progress_pct:.1f}%) "
                             f"processed in {elapsed:.1f}s, {alive_workers} workers alive, writer: {writer_alive}")
                
                # Check if all records are processed
                if current_progress >= valid_records_count:
                    logg.log_ok(f"TARGET REACHED: All {valid_records_count} records processed!")
                    time.sleep(2)  # Give a moment for final commits
                    break
                
                last_progress_check = current_time
            
            # Check if processes are still alive
            alive_workers = sum(1 for p in processes if p.is_alive())
            writer_alive = db_writer.is_alive()
            
            if not writer_alive:
                logg.log_info("Database writer finished")
                break
            
            if alive_workers == 0 and not writer_alive:
                logg.log_info("All processes finished")
                break
            
            # Safety timeout
            if elapsed > max_wait_time:
                with processed_counter.get_lock():
                    current_progress = processed_counter.value
                if current_progress < valid_records_count:
                    logg.log_warn(f"Timeout reached with {valid_records_count - current_progress} records remaining")
                break
            
            time.sleep(1)
    
    finally:
        # Enhanced graceful shutdown with guaranteed cleanup
        logg.log_info("Initiating enhanced graceful shutdown with guaranteed cleanup...")
        
        # Send termination signals
        for i, process in enumerate(processes):
            if process.is_alive():
                process.terminate()
                logg.log_info(f"Sent termination signal to re-schema worker {i}")
        
        if db_writer.is_alive():
            db_writer.terminate()
            logg.log_info("Sent termination signal to re-schema writer")
        
        # Wait for processes to finish with enhanced cleanup
        for i, process in enumerate(processes):
            process.join(timeout=10)
            if process.is_alive():
                logg.log_warn(f"Force killing re-schema worker {i}")
                process.kill()
                process.join()

        db_writer.join(timeout=15)
        if db_writer.is_alive():
            logg.log_warn("Force killing re-schema writer")
            db_writer.kill()
            db_writer.join()
    
    # Final verification
    with processed_counter.get_lock():
        final_progress = processed_counter.value
    
    total_elapsed = time.time() - start_time
    success = (final_progress >= valid_records_count)
    
    if success:
        logg.log_ok(f"Re-schema completed: {final_progress}/{valid_records_count} records in {total_elapsed:.1f}s")
    else:
        logg.log_warn(f"Re-schema incomplete: {final_progress}/{valid_records_count} records in {total_elapsed:.1f}s")
    
    return success


def reschema_worker_process(
    process_id: int,
    fullpath_queue: multiprocessing.Queue,
    data_queue: multiprocessing.Queue,
    rule_dict: Dict[str, Any],
    new_fields: List[str],
    threads_per_process: int,
    worker_finished_counter: multiprocessing.Value,
    worker_finished_lock: multiprocessing.Lock,
    worker_finished_event: multiprocessing.Event,
    total_workers: int,
    processed_counter: multiprocessing.Value,  # Global progress counter
    total_records_to_process: int,  # Total records expected
    db_config: Dict[str, Any],
    dbname: str,
    tablename: str
) -> None:
    """
    Enhanced re-schema worker with guaranteed database connection cleanup.
    """
    logg = Logger()
    logg.log_info(f"Re-schema worker {process_id} started, target: {total_records_to_process} records")
    
    # Enhanced database connection manager
    db_manager = DatabaseConnectionManager(db_config, logg)
    
    try:
        # Connect to database with enhanced cleanup
        with db_manager as db:
            db.use_database(dbname)
            
            def parse_selective_fields(fullpath: str, existing_record: Dict[str, Any]):
                """Parse only the new fields needed for re-schema."""
                try:
                    if not os.path.exists(fullpath):
                        logg.log_warn(f"File no longer exists: {fullpath}")
                        return None
                    
                    parser = ParseEngineData(selective_fields=new_fields)
                    
                    if parser.parse_engine(fullpath, rule_dict):
                        parsed_data = dict(parser.items())
                        
                        # Merge with existing record
                        updated_record = existing_record.copy()
                        for field in new_fields:
                            if field in parsed_data:
                                if existing_record.get(field) is None or existing_record.get(field) == '':
                                    updated_record[field] = parsed_data[field]
                        
                        return updated_record
                    else:
                        return None
                        
                except Exception as e:
                    logg.log_error(f"Error parsing {fullpath}: {e}")
                    return None
            
            files_processed = 0
            
            with ThreadPoolExecutor(max_workers=threads_per_process) as executor:
                while True:
                    try:
                        # Check global progress periodically
                        if files_processed % 10 == 0:
                            with processed_counter.get_lock():
                                global_progress = processed_counter.value
                            if global_progress >= total_records_to_process:
                                logg.log_info(f"Re-schema worker {process_id}: Target reached ({global_progress}/{total_records_to_process})")
                                break
                        
                        # Get work batch
                        work_batch = []
                        for _ in range(threads_per_process * 2):
                            try:
                                item = fullpath_queue.get(timeout=0.5)
                                if item is None:  # Poison pill
                                    if work_batch:
                                        break
                                    else:
                                        logg.log_info(f"Re-schema worker {process_id} received stop signal")
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
                        for fullpath, existing_record in work_batch:
                            future = executor.submit(parse_selective_fields, fullpath, existing_record)
                            futures.append(future)
                        
                        for future in as_completed(futures):
                            try:
                                result = future.result()
                                if result:
                                    data_queue.put(result)
                                files_processed += 1
                            except Exception as e:
                                logg.log_error(f"Future error in worker {process_id}: {e}")
                                files_processed += 1
                        
                        if files_processed % 100 == 0:
                            logg.log_info(f"Re-schema worker {process_id}: {files_processed} files processed")
                            
                    except KeyboardInterrupt:
                        logg.log_warn(f"Re-schema worker {process_id} interrupted, performing cleanup")
                        break
                    except Exception as e:
                        logg.log_error(f"Error in re-schema worker {process_id}: {e}")
                        continue
                        
    except KeyboardInterrupt:
        logg.log_warn(f"Re-schema worker {process_id} shutting down")
    except Exception as e:
        logg.log_error(f"Critical error in worker {process_id}: {e}")
    finally:
        # Guaranteed cleanup handled by DatabaseConnectionManager
        with worker_finished_lock:
            if worker_finished_counter.value < total_workers:
                worker_finished_counter.value += 1
                if worker_finished_counter.value >= total_workers:
                    worker_finished_event.set()
        logg.log_info(f"Re-schema worker {process_id} finished: {files_processed} files")


def reschema_database_writer_process(
    data_queues: List[multiprocessing.Queue],
    db_config: Dict[str, Any],
    dbname: str,
    tablename: str,
    worker_finished_event: multiprocessing.Event,
    total_records_to_process: int,
    processed_counter: multiprocessing.Value,  # Global progress counter
    batch_collection_size: int = 1000,
    batch_time_threshold: float = 5.0
) -> int:
    """
    Enhanced database writer for re-schema with guaranteed connection cleanup.
    """
    logg = Logger()
    logg.log_info(f"Re-schema DB writer started, expecting {total_records_to_process} records")
    
    # Enhanced database connection manager
    db_manager = DatabaseConnectionManager(db_config, logg)
    
    collection_buffer = []
    last_collection_time = time.time()
    total_processed = 0
    consecutive_empty_rounds = 0
    num_processes = len(data_queues)
    current_process_idx = 0
    start_time = time.time()
    
    logg.log_info(f"Re-schema writer monitoring {num_processes} workers")
    
    try:
        # Connect to database with enhanced cleanup
        with db_manager as db:
            db.use_database(dbname)
            
            def process_buffer():
                nonlocal collection_buffer, total_processed
                
                if not collection_buffer:
                    return 0

                buffer_size = len(collection_buffer)
                logg.log_info(f"Processing re-schema buffer: {buffer_size} records")
                
                # Extract fullpaths from buffer
                buffer_fullpaths = [r.get('fullpath') for r in collection_buffer if r.get('fullpath')]
                
                if buffer_fullpaths:
                    # Delete existing records first
                    success = db.delete_by_fullpaths_batch(tablename, buffer_fullpaths)
                    if success:
                        logg.log_info(f"Deleted {len(buffer_fullpaths)} existing records")
                    
                    # Now insert updated records
                    for record in collection_buffer:
                        db.insert_data(tablename, record)
                    
                    db.commit_and_refresh_cursor()
                
                total_processed += buffer_size
                collection_buffer = []
                
                # Update global progress counter
                with processed_counter.get_lock():
                    processed_counter.value += buffer_size
                    current_global_progress = processed_counter.value
                
                progress_pct = (current_global_progress / max(total_records_to_process, 1)) * 100
                logg.log_info(f"Re-schema progress: {current_global_progress}/{total_records_to_process} ({progress_pct:.1f}%)")
                
                return buffer_size
            
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
                    
                    # === Primary Termination Condition ===
                    # Check global progress counter
                    with processed_counter.get_lock():
                        current_global_progress = processed_counter.value
                    
                    if current_global_progress >= total_records_to_process:
                        logg.log_info(f"ALL RECORDS PROCESSED: {current_global_progress}/{total_records_to_process}")
                        if collection_buffer:
                            process_buffer()
                        break
                    
                    # Secondary condition: workers finished with reasonable empty rounds
                    if worker_finished_event.is_set() and consecutive_empty_rounds > 50:
                        logg.log_info(f"Workers finished with {consecutive_empty_rounds} empty rounds")
                        if collection_buffer:
                            process_buffer()
                        # Double check global counter
                        with processed_counter.get_lock():
                            if processed_counter.value >= total_records_to_process:
                                break
                        if consecutive_empty_rounds > 100:
                            logg.log_warn("Forcing termination after extended empty rounds")
                            break
                    
                    # Sleep only if no data
                    if round_collected == 0:
                        time.sleep(0.01)
                    
                    # Status logging
                    if consecutive_empty_rounds > 0 and consecutive_empty_rounds % 500 == 0:
                        elapsed = time.time() - start_time
                        logg.log_info(f'Re-schema writer status - Progress: {current_global_progress}/{total_records_to_process}, '
                                    f'Buffer: {len(collection_buffer)}, Time: {elapsed:.1f}s, Empty rounds: {consecutive_empty_rounds}')
                        
                except KeyboardInterrupt:
                    logg.log_warn("Re-schema writer interrupted, performing cleanup")
                    break
                    
    except KeyboardInterrupt:
        logg.log_warn("Re-schema writer shutting down")
    except Exception as e:
        logg.log_error(f"Critical error in writer: {e}")
    finally:
        # Final processing - connection cleanup handled by DatabaseConnectionManager
        if collection_buffer:
            logg.log_info("Final re-schema buffer processing")
            # Try to process final buffer, but if connection is lost, that's ok
            try:
                with DatabaseConnectionManager(db_config, logg) as final_db:
                    final_db.use_database(dbname)
                    
                    buffer_fullpaths = [r.get('fullpath') for r in collection_buffer if r.get('fullpath')]
                    if buffer_fullpaths:
                        success = final_db.delete_by_fullpaths_batch(tablename, buffer_fullpaths)
                        if success:
                            for record in collection_buffer:
                                final_db.insert_data(tablename, record)
                            final_db.commit_and_refresh_cursor()
                        
                        # Update final progress
                        with processed_counter.get_lock():
                            processed_counter.value += len(collection_buffer)
                            
            except Exception as e:
                logg.log_error(f"Error in final buffer processing: {e}")
        
        # Final stats
        with processed_counter.get_lock():
            final_progress = processed_counter.value
        
        total_elapsed = time.time() - start_time
        if final_progress > 0:
            avg_rate = final_progress / total_elapsed
            completion_pct = (final_progress / total_records_to_process) * 100
            logg.log_info(f"Re-schema writer completed: {final_progress}/{total_records_to_process} "
                         f"({completion_pct:.1f}%) in {total_elapsed:.1f}s ({avg_rate:.1f} records/sec)")
        else:
            logg.log_info(f"Re-schema writer completed: 0 records processed in {total_elapsed:.1f}s")
    
    return total_processed

def perform_reschema_without_lock_check(
    db_config: Dict[str, Any],
    dbname: str,
    tablename: str,
    rule_dict: Dict[str, Any],
    new_fields: List[str],
    time_column: str,
    logg: Logger,
    num_workers: int = 7,
    threads_per_worker: int = 8,
    batch_limit: int = 1000000 
) -> bool:
    """
    function after checking schema and without check lock again.
    """
    logg.log_info(f"Starting immediate re-schema for table {tablename} with new fields: {new_fields}")
    
    temp_db = MySQLHelper(db_config)
    try:
        temp_db.connect()
        temp_db.use_database(dbname)
        
        # Build query to get records with NULL values in new fields
        where_conditions = " OR ".join([f"`{field}` IS NULL OR `{field}` = ''" for field in new_fields])
        sql = f"SELECT * FROM `{tablename}` WHERE ({where_conditions}) LIMIT {batch_limit}"
        
        success, records = temp_db.execute_query(sql, fetch='all')
        if not success or not records:
            logg.log_info("No records need re-schema update")
            return True
        
        # Get column names
        columns = [desc[0] for desc in temp_db.cursor.description]
        records_dict = [dict(zip(columns, row)) for row in records]
        
        logg.log_info(f"Found {len(records_dict)} records needing immediate re-schema")
        
    except Exception as e:
        logg.log_error(f"Error reading records for re-schema: {e}")
        return False
    finally:
        temp_db.close()
    
    # Create queues for multiprocessing
    fullpath_queue = multiprocessing.Queue()
    data_queues = [multiprocessing.Queue() for _ in range(num_workers)]
    
    # Synchronization primitives
    worker_finished_counter = multiprocessing.Value('i', 0)
    worker_finished_lock = multiprocessing.Lock()
    worker_finished_event = multiprocessing.Event()
    processed_counter = multiprocessing.Value('i', 0)
    
    # Populate work queue with valid records
    valid_records_count = 0
    for record in records_dict:
        if 'fullpath' in record and record['fullpath']:
            if os.path.exists(record['fullpath']):
                fullpath_queue.put((record['fullpath'], record))
                valid_records_count += 1
            else:
                logg.log_warn(f"File not found for re-schema: {record['fullpath']}")
    
    if valid_records_count == 0:
        logg.log_info("No valid files found for re-schema")
        return True
    
    logg.log_info(f"EXACT TARGET: {valid_records_count} valid records to re-schema")
    
    # Add poison pills
    for _ in range(num_workers):
        fullpath_queue.put(None)
    
    # Start worker processes
    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(
            target=reschema_worker_process,
            args=(
                i, fullpath_queue, data_queues[i], rule_dict,
                new_fields, threads_per_worker,
                worker_finished_counter, worker_finished_lock,
                worker_finished_event, num_workers,
                processed_counter, valid_records_count,
                db_config, dbname, tablename
            )
        )
        p.start()
        processes.append(p)
        logg.log_info(f"Started re-schema worker {i}")
    
    # Start database writer
    db_writer = multiprocessing.Process(
        target=reschema_database_writer_process_without_lock,
        args=(
            data_queues, db_config, dbname, tablename,
            worker_finished_event, valid_records_count,
            processed_counter,
            1000, 5.0  # batch_collection_size, batch_time_threshold
        )
    )
    db_writer.start()
    logg.log_info("Started re-schema database writer (no lock check)")
    
    # Monitor progress
    start_time = time.time()
    last_progress_check = start_time
    max_wait_time = max(300, valid_records_count * 0.1) 
    
    try:
        while True:
            current_time = time.time()
            elapsed = current_time - start_time
            
            # Check progress every 10 seconds
            if current_time - last_progress_check >= 10:
                with processed_counter.get_lock():
                    current_progress = processed_counter.value
                
                if valid_records_count > 0:
                    progress_pct = (current_progress / valid_records_count) * 100
                else:
                    progress_pct = 100.0
                    
                alive_workers = sum(1 for p in processes if p.is_alive())
                writer_alive = db_writer.is_alive()
                
                logg.log_info(f"Re-schema (no-lock) monitor: {current_progress}/{valid_records_count} ({progress_pct:.1f}%) "
                             f"in {elapsed:.1f}s, {alive_workers} workers alive, writer: {writer_alive}")
                
                # Check if all records are processed
                if current_progress >= valid_records_count:
                    logg.log_ok(f"TARGET REACHED: All {valid_records_count} records processed!")
                    time.sleep(2)  # Give a moment for final commits
                    break
                
                last_progress_check = current_time
            
            # Check if processes are still alive
            alive_workers = sum(1 for p in processes if p.is_alive())
            writer_alive = db_writer.is_alive()
            
            if not writer_alive:
                logg.log_info("Database writer finished")
                with processed_counter.get_lock():
                    if processed_counter.value >= valid_records_count:
                        break
            
            if alive_workers == 0 and not writer_alive:
                logg.log_info("All processes finished")
                break
            
            # Safety timeout
            if elapsed > max_wait_time:
                with processed_counter.get_lock():
                    current_progress = processed_counter.value
                if current_progress < valid_records_count:
                    logg.log_warn(f"Timeout reached with {valid_records_count - current_progress} records remaining")
                break
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        logg.log_warn("Re-schema monitoring interrupted")
    
    finally:
        # Graceful shutdown
        logg.log_info("Initiating graceful shutdown for re-schema processes...")
        
        # Send termination signals
        for i, process in enumerate(processes):
            if process.is_alive():
                process.terminate()
                logg.log_info(f"Sent termination signal to re-schema worker {i}")
        
        if db_writer.is_alive():
            db_writer.terminate()
            logg.log_info("Sent termination signal to re-schema writer")
        
        # Wait for processes to finish
        for i, process in enumerate(processes):
            process.join(timeout=10)
            if process.is_alive():
                logg.log_warn(f"Force killing re-schema worker {i}")
                process.kill()
                process.join()

        db_writer.join(timeout=15)
        if db_writer.is_alive():
            logg.log_warn("Force killing re-schema writer")
            db_writer.kill()
            db_writer.join()
    
    # Final verification
    with processed_counter.get_lock():
        final_progress = processed_counter.value
    
    total_elapsed = time.time() - start_time
    success = (final_progress >= valid_records_count)
    
    if success:
        logg.log_ok(f"Immediate re-schema completed: {final_progress}/{valid_records_count} records in {total_elapsed:.1f}s")
    else:
        logg.log_warn(f"Immediate re-schema incomplete: {final_progress}/{valid_records_count} records in {total_elapsed:.1f}s")
    
    if final_progress < len(records_dict):
        remaining = len(records_dict) - final_progress
        logg.log_info(f"Note: {remaining} records may still need re-schema in future runs")
    
    return success

def reschema_database_writer_process_without_lock(
    data_queues: List[multiprocessing.Queue],
    db_config: Dict[str, Any],
    dbname: str,
    tablename: str,
    worker_finished_event: multiprocessing.Event,
    total_records_to_process: int,
    processed_counter: multiprocessing.Value,
    batch_collection_size: int = 1000,
    batch_time_threshold: float = 5.0
) -> int:
    """
    Database writer for immediate re-schema without check lock
    """
    logg = Logger()
    logg.log_info(f"Re-schema DB writer (no-lock) started, expecting {total_records_to_process} records")
    
    collection_buffer = []
    last_collection_time = time.time()
    total_processed = 0
    consecutive_empty_rounds = 0
    num_processes = len(data_queues)
    current_process_idx = 0
    start_time = time.time()
    
    def process_buffer_with_short_connection():
        nonlocal collection_buffer, total_processed
        
        if not collection_buffer:
            return 0
        
        buffer_size = len(collection_buffer)
        logg.log_info(f"Processing re-schema buffer (no-lock): {buffer_size} records")
        
        temp_db = MySQLHelper(db_config)
        try:
            temp_db.connect()
            temp_db.use_database(dbname)
            
            # Extract fullpaths from buffer
            buffer_fullpaths = [r.get('fullpath') for r in collection_buffer if r.get('fullpath')]
            
            if buffer_fullpaths:
                # Delete existing records first
                success = temp_db.delete_by_fullpaths_batch(tablename, buffer_fullpaths)
                if success:
                    logg.log_info(f"Deleted {len(buffer_fullpaths)} existing records")
                
                # Insert updated records
                success_count = 0
                for record in collection_buffer:
                    try:
                        if temp_db.insert_data(tablename, record):
                            success_count += 1
                    except Exception as e:
                        logg.log_error(f"Failed to insert record: {e}")
                
                temp_db.commit_and_refresh_cursor()
                logg.log_info(f"Inserted {success_count}/{buffer_size} records")
            
            total_processed += buffer_size
            collection_buffer = []
            
            # Update global progress counter
            with processed_counter.get_lock():
                processed_counter.value += buffer_size
                current_global_progress = processed_counter.value
            
            progress_pct = (current_global_progress / max(total_records_to_process, 1)) * 100
            logg.log_info(f"Re-schema progress (no-lock): {current_global_progress}/{total_records_to_process} ({progress_pct:.1f}%)")
            
            return buffer_size
            
        except Exception as e:
            logg.log_error(f"Error processing buffer: {e}")
            collection_buffer = []  # Clear buffer to avoid infinite retry
            return 0
        finally:
            temp_db.close()
    
    try:
        while True:
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
                process_buffer_with_short_connection()
                last_collection_time = time.time()
            
            # Check global progress counter
            with processed_counter.get_lock():
                current_global_progress = processed_counter.value
            
            if current_global_progress >= total_records_to_process:
                logg.log_info(f"ALL RECORDS PROCESSED: {current_global_progress}/{total_records_to_process}")
                if collection_buffer:
                    process_buffer_with_short_connection()
                break
            
            # Check if workers finished
            if worker_finished_event.is_set() and consecutive_empty_rounds > 50:
                logg.log_info(f"Workers finished with {consecutive_empty_rounds} empty rounds")
                if collection_buffer:
                    process_buffer_with_short_connection()
                with processed_counter.get_lock():
                    if processed_counter.value >= total_records_to_process:
                        break
                if consecutive_empty_rounds > 100:
                    logg.log_warn("Forcing termination after extended empty rounds")
                    break
            
            if round_collected == 0:
                time.sleep(0.01)
                
    except KeyboardInterrupt:
        logg.log_warn("Re-schema writer (no-lock) interrupted")
    except Exception as e:
        logg.log_error(f"Critical error in writer: {e}")
    finally:
        # Final buffer processing
        if collection_buffer:
            logg.log_info("Final buffer processing")
            process_buffer_with_short_connection()
        
        with processed_counter.get_lock():
            final_progress = processed_counter.value
        
        total_elapsed = time.time() - start_time
        logg.log_info(f"Re-schema writer (no-lock) completed: {final_progress}/{total_records_to_process} in {total_elapsed:.1f}s")
    
    return total_processed


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