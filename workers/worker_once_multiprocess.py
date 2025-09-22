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

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.MySQLHelper import MySQLHelper
from core.Logger import Logger
from workers import worker_ult as wult
from ParseEngine.engine.ParseEngineData import ParseEngineData

from processors.file_processor import process_files, process_files_for_global, DatabaseLockManager, TableLockedException
from processors.reschema_processor import perform_reschema, perform_reschema_without_lock_check
from processors.mcc_processor import mcc_process


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


def check_table_lock_status(db_config: Dict[str, Any], database_name: str, 
                           table_name: str, logg: Logger) -> bool:
    """
    Check if a table is currently locked.
    
    :param db_config: Database configuration
    :param database_name: Database name
    :param table_name: Table name
    :param logg: Logger instance
    :return: True if table is locked, False otherwise
    """
    temp_db = MySQLHelper(db_config)
    try:
        temp_db.connect()
        
        # new force refresh connect
        temp_db.refresh_connection()
        
        temp_db.use_database("mysql")
        temp_db.create_lock_table()
        
        # new clear cache
        temp_db.clear_table_locks_cache()
        
        is_locked = temp_db.is_table_locked(database_name, table_name)
        return is_locked

    except Exception as e:
        logg.log_error(f"Error checking table lock status: {e}")
        return False
    finally:
        temp_db.close()


def load_dbsetting(path: str = 'db_setting.json') -> Dict[str, Any]:
    """
    Load database configuration from a JSON file.

    :param path: Path to db_setting.json file.
    :return: Parsed database configuration dictionary.
    """
    with open(path, 'r', encoding='UTF-8') as f:
        return json.load(f)


def cleanup_all_resolved_exceptions(
    db_config: Dict[str, Any],
    exception_db: MySQLHelper, 
    dbname: str,
    tablename: str, 
    exception_table: str, 
    time_column: str,
    recently_days: int,
    logg: Logger
) -> int:
    """
    Clean up all records that exist in main database but still remain in exception table with locking.
    """
    try:
        # Get all exception fullpaths
        success, exception_records = exception_db.execute_query(
            f"SELECT DISTINCT fullpath FROM `{exception_table}`", fetch='all'
        )
        

        if not success or not exception_records:
            logg.log_info("No exception records found to cleanup")
            return 0
            
        exception_fullpaths = [record[0] for record in exception_records]
        logg.log_info(f"Found {len(exception_fullpaths)} exception records to check for resolution")
        
        # Check which files have been successfully processed using short connection
        temp_db = MySQLHelper(db_config)
        temp_db.connect()
        temp_db.use_database("mysql")
        temp_db.clear_table_locks_cache()
        temp_db.close()

        with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, "READ", logg) as db:
            extended_days = max(recently_days * 2, 30)
            existing_fullpaths = db.get_existing_fullpaths(tablename, time_column, extended_days)

        # Find resolved exception records
        resolved_fullpaths = [fp for fp in exception_fullpaths if fp in existing_fullpaths]
        
        if not resolved_fullpaths:
            logg.log_info("No resolved exception records found")
            return 0
            
        logg.log_info(f"Found {len(resolved_fullpaths)} resolved exception records to cleanup")
        
        # Batch delete resolved exception records
        cleanup_count = 0
        batch_size = 100
        
        for i in range(0, len(resolved_fullpaths), batch_size):
            batch = resolved_fullpaths[i:i + batch_size]
            
            if exception_db.delete_by_fullpaths_batch(exception_table, batch):
                cleanup_count += len(batch)
                logg.log_info(f"Cleaned up batch {i//batch_size + 1}: {len(batch)} records")
            else:
                logg.log_error(f"Failed to cleanup batch {i//batch_size + 1}")
        
        exception_db.commit_and_refresh_cursor()
        logg.log_info(f"Successfully cleaned up {cleanup_count} resolved exception records from {exception_table}")
        
        return cleanup_count
        
    except Exception as e:
        logg.log_error(f"Exception cleanup failed: {e}")
        return 0


def process_test_station_with_cleanup(
    db_config: Dict[str, Any],
    dbname: str,
    test_station: str,
    ts_info: Dict[str, Any],
    default_recently_days: int,
    default_enable: bool,
    logg: Logger,
    exception_db_config: Dict[str, Any],
    exception_table: str,
    schema_updated: bool = False,
) -> None:
    """
    Process rule-based logic for a given test station with table locking.
    Now supports optional mcc_rule for dual processing.
    """
    if not ts_info.get('enable', default_enable):
        logg.log_warn(f"  - <{test_station}> disabled, skipped")
        return

    rule = ts_info['rule']
    tablename = ts_info['tablename']
    folder = ts_info['folder']
    recently_days = ts_info.get('recently_days', default_recently_days)
    time_column = ts_info.get('time_column', 'tbeg')
    
    mcc_rule = ts_info.get('mcc_rule')
    has_mcc = mcc_rule is not None

    logg.log_sub_title(f"Test Station: {test_station}")
    logg.log_info(f"Rule: {rule}")
    logg.log_info(f"Table: {tablename}")
    logg.log_info(f"Folder: {folder}")
    if has_mcc:
        logg.log_info(f"MCC Rule: {mcc_rule}")
    logg.log_info(f"Recently Days: {recently_days}")

    if not os.path.isfile(rule):
        logg.log_warn(f"  - <{rule}> does not exist, skipped")
        return
    
    if has_mcc and not os.path.isfile(mcc_rule):
        logg.log_warn(f"  - <{mcc_rule}> does not exist, skipped")
        return

    # Check if table is locked before starting any operations
    if check_table_lock_status(db_config, dbname, tablename, logg):
        logg.log_warn(f"Table {dbname}.{tablename} is currently locked by another process, skipping test station {test_station}")
        return
    
    # Continue with existing logic but use DatabaseLockManager for schema operations
    with wult.DurationTimer(logg, f"Load rule and create/sync table: {tablename}"):
        with open(rule, 'r', encoding='UTF-8') as f:
            rule_dict = json.load(f)
        schema_fields = wult.generate_schema_from_rule(rule_dict)
        
        need_reschema = False
        new_fields_to_populate = []
        
        try:
            # check reschema
            with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, 
                                    "SCHEMA_CHECK", logg, lock_db_name="mysql") as locked_db:
                locked_db.create_table_if_nexists(tablename, schema_fields)
                
                schema_dict = {k: v["dbtype"] for k, v in rule_dict.items() if "dbtype" in v}
                current_schema = locked_db.get_table_schema(tablename)
                rule_fields = set(rule_dict.keys())
                current_fields = set(current_schema.keys())
                new_fields = list(rule_fields - current_fields)
                
                warnings, schema_updated = locked_db.sync_table_schema(tablename, schema_dict)
                for w in warnings: logg.log_warn(w)
                
                # check if need re-schema
                if schema_updated and new_fields:
                    need_reschema = True
                    new_fields_to_populate = new_fields
                    logg.log_info(f"Schema updated with new fields: {new_fields}, will perform re-schema")
                
                # Check for unused fields
                db_schema_cols = set(locked_db.get_table_schema(tablename).keys())
                json_defined_cols = set(schema_dict.keys())
                unused_in_the_future = db_schema_cols - json_defined_cols
                if unused_in_the_future:
                    logg.log_warn(f"Potential unused fields in DB not defined in JSON: {unused_in_the_future}")
                    
                logg.log_ok(f"Table {tablename} schema check completed.")
            
                
        except TableLockedException:
            logg.log_warn(f"Table {dbname}.{tablename} is locked during schema check, skipping test station {test_station}")
            return
        except Exception as e:
            logg.log_error(f"Error during schema operations: {e}")
            return
    
    # re-schema
    if need_reschema:
        logg.log_info(f"Performing immediate re-schema for {len(new_fields_to_populate)} new fields")
        
        with wult.DurationTimer(logg, f"Immediate re-schema for new fields"):
            success = perform_reschema_without_lock_check(
                db_config, dbname, tablename, rule_dict, new_fields_to_populate,
                time_column, logg,
                num_workers=7,
                threads_per_worker=8
            )
            
            if success:
                logg.log_ok(f"Re-schema completed successfully for {tablename}")
            else:
                logg.log_error(f"Re-schema failed for {tablename}")
    else:
        logg.log_info("No re-schema needed")

    use_wo_grouping = ts_info.get('use_wo_grouping', False)
    
    # Process new files only after successful re-schema
    if use_wo_grouping:
        process_info = process_files_for_global(
            db_config, dbname, tablename, rule_dict, folder,
            recently_days, time_column, logg
        )
    else:
        process_info = process_files(
            db_config, dbname, tablename, rule_dict, folder, recently_days,
            time_column, logg, exception_db_config, exception_table
        )
    
    # Handle process results
    if process_info:
        all_logged, unlogged_parsed_files, unlogged_unparsed_files = process_info
        if all_logged:
            logg.log_ok(f"All files for Test Station '{test_station}' logged into Database '{dbname}'")
        else:
            failed_count = len(unlogged_parsed_files) + len(unlogged_unparsed_files)
            logg.log_warn(f"{failed_count} files failed to be logged into Database '{dbname}'")
            if unlogged_parsed_files:
                logg.log_warn(f"{len(unlogged_parsed_files)} files failed during data insertion")
            if unlogged_unparsed_files:
                logg.log_warn(f"{len(unlogged_unparsed_files)} files failed during parsing")
    
    # MCC Processing
    if has_mcc:
        time.sleep(60)
        logg.log_info(f"Starting MCC processing for test station: {test_station}")
        if not os.path.isfile(mcc_rule):
            logg.log_warn(f"MCC rule file does not exist: {mcc_rule}, skipping MCC processing")
        else:
            try:
                with open(mcc_rule, 'r', encoding='UTF-8') as f:
                    mcc_rule_dict = json.load(f)
                
                with wult.DurationTimer(logg, f"MCC processing for {tablename}"):
                    success = mcc_process(
                        db_config, dbname, tablename, mcc_rule_dict,
                        time_column, logg, recently_days,
                        num_workers=7,
                        threads_per_worker=8
                    )
                    
                    if success:
                        logg.log_ok(f"MCC processing completed successfully for {tablename}")
                    else:
                        logg.log_error(f"MCC processing failed for {tablename}")
                        
            except Exception as mcc_error:
                logg.log_error(f"Error during MCC processing for {test_station}: {mcc_error}")
    else:
        logg.log_info("No MCC processing required - mcc_rule")
    
    # Deduplication with locking
    logg.log_info(f"Starting deduplication for table: {tablename}")
    
    try:
        temp_db = MySQLHelper(db_config)
        temp_db.connect()
        temp_db.use_database("mysql")
        temp_db.clear_table_locks_cache()
        temp_db.close()

        with DatabaseLockManager(MySQLHelper(db_config), dbname, tablename, "DEDUPLICATION", logg) as locked_db:
            duplicate_count_before = locked_db.get_duplicate_count(tablename)
            if duplicate_count_before > 0:
                logg.log_info(f"Found {duplicate_count_before} duplicate records in {tablename}")
                
                with wult.DurationTimer(logg, f"Deduplicating table {tablename}"):
                    success, removed_count = locked_db.deduplicate_table_records(tablename, keep='first')
                    
                    if success and removed_count > 0:
                        logg.log_ok(f"Deduplication completed: removed {removed_count} duplicate records from {tablename}")
                    elif success and removed_count == 0:
                        logg.log_info(f"No duplicates found in {tablename} during deduplication")
                    else:
                        logg.log_error(f"Failed to deduplicate table {tablename}")
            else:
                logg.log_info(f"No duplicates found in {tablename}")
    except TableLockedException:
        logg.log_warn(f"Table {dbname}.{tablename} is locked by another process during deduplication, skipping deduplication for {test_station}")
    except Exception as dedup_error:
        logg.log_error(f"Error during deduplication of {tablename}: {dedup_error}")

    # Comprehensive exception cleanup with locking
    if exception_db_config and exception_table:
        logg.log_info(f"Starting comprehensive exception cleanup for test station: {test_station}")
        
        try:
            temp_db = MySQLHelper(db_config)
            temp_db.connect()
            temp_db.use_database("mysql")
            temp_db.clear_table_locks_cache()
            temp_db.close()

            with DatabaseLockManager(MySQLHelper(exception_db_config), "exception", exception_table, "DELETE", logg) as exception_db:
                cleanup_count = cleanup_all_resolved_exceptions(
                    db_config, exception_db, dbname, tablename, exception_table, 
                    time_column, recently_days, logg
                )
                
                if cleanup_count > 0:
                    logg.log_ok(f"Exception cleanup completed: {cleanup_count} resolved records removed")
                else:
                    logg.log_info("Exception cleanup completed: no resolved records found")
        except TableLockedException:
            logg.log_warn(f"Exception table {exception_table} is locked by another process, skipping exception cleanup for {test_station}")
        except Exception as cleanup_error:
            logg.log_error(f"Exception cleanup failed for {test_station}: {cleanup_error}")


def process_sku_with_comprehensive_cleanup(
    db_config: Dict[str, Any],  # Changed from MySQLHelper to config
    sku_title: str,
    sku_info: Dict[str, Any],
    default_recently_days: int,
    default_enable: bool,
    logg: Logger,
    exception_db_config: Dict[str, Any],  # Changed from MySQLHelper to config
    exception_table: str = None
) -> None:
    """
    Loop through and process all test stations of the given SKU with comprehensive exception cleanup.
    
    :param db_config: Database configuration dictionary
    :param sku_title: Identifier of the SKU (e.g., "K2v5_JRD03_R")
    :param sku_info: Configuration dictionary for that SKU
    :param default_recently_days: Fallback recently_days for test stations
    :param default_enable: Fallback enable flag
    :param logg: Logger instance
    :param exception_db_config: Database configuration for exception database
    :param exception_table: Name of the table to insert or delete records in exception database
    """
    if not sku_info.get('enable', default_enable):
        logg.log_warn(f"SKU <{sku_title}> disabled, skipped")
        return

    dbname = sku_info['dbname']
    exception_table = dbname
    
    # Create a temporary connection to switch database
    temp_db = MySQLHelper(db_config)
    try:
        temp_db.connect()
        temp_db.use_database(dbname)
        logg.log_title(f"DB_NAME : {dbname}")
    finally:
        temp_db.close()

    # Process each test station
    for test_station, ts_info in sku_info['test_station'].items():
        process_test_station_with_cleanup(
            db_config,  # Pass config, not db instance
            dbname, 
            test_station, 
            ts_info, 
            default_recently_days, 
            default_enable, 
            logg, 
            exception_db_config,  # Pass config, not db instance
            exception_table
        )
    
    # Final SKU-level exception cleanup
    logg.log_info(f"Starting final SKU-level exception cleanup for: {sku_title}")
    try:
        # Connect to exception database for cleanup
        exception_db = MySQLHelper(exception_db_config)
        exception_db.connect()
        exception_db.use_database("exception")
        
        # Get total exception count before cleanup
        success, total_before = exception_db.execute_query(
            f"SELECT COUNT(*) FROM `{exception_table}`", fetch='one'
        )
        total_before_count = total_before[0] if success and total_before else 0
        
        if total_before_count > 0:
            logg.log_info(f"Total exception records before final cleanup: {total_before_count}")
            
            # Perform one final comprehensive cleanup for all tables in this SKU
            total_cleanup_count = 0
            for test_station, ts_info in sku_info['test_station'].items():
                if ts_info.get('enable', default_enable):
                    tablename = ts_info['tablename']
                    time_column = ts_info.get('time_column', 'tbeg')
                    recently_days = ts_info.get('recently_days', default_recently_days)
                    
                    cleanup_count = cleanup_all_resolved_exceptions(
                        db_config,  # Pass config
                        exception_db,  # Pass connected exception_db instance
                        dbname, 
                        tablename, 
                        exception_table,
                        time_column, 
                        recently_days, 
                        logg
                    )
                    total_cleanup_count += cleanup_count
            
            logg.log_ok(f"SKU {sku_title} final cleanup: {total_cleanup_count} exception records removed")
        else:
            logg.log_info(f"No exception records found for SKU {sku_title}")
        
        exception_db.close()
            
    except Exception as final_cleanup_error:
        logg.log_error(f"Final SKU cleanup failed for {sku_title}: {final_cleanup_error}")


def create_exception_db(exception_db: MySQLHelper, dbsetting: dict):
    """
    Create/use the 'exception' database and ensure exception tables exist for each SKU.

    :param exception_db: MySQLHelper instance for performing DB operations, specific in exception database.
    :param db_setting: Dictionary loaded from db_setting.json, containing SKU/test_station configurations.
    """
    exception_db.use_database("exception")
    exception_schema = {
        "fullpath": "VARCHAR(200)",
        "reason": "VARCHAR(40)",
        "exception_context": "TEXT"
    }
    schema_fields = ", ".join([f"`{k}` {v}" for k, v in exception_schema.items()])

    for sku_title, sku_info in dbsetting['sku_parser_rule'].items():
        exception_table = sku_info['dbname']
        exception_db.create_table_if_nexists(exception_table, schema_fields)

    # Also create a default/undefined SKU table
    exception_table = "undefined_sku"
    exception_db.create_table_if_nexists(exception_table, schema_fields)    


def cleanup_stale_locks(db_config: Dict[str, Any], logg: Logger, max_lock_age_minutes: int = 60) -> None:
    """
    Clean up stale/orphaned locks that may have been left by previous crashed processes.
    
    :param db_config: Database configuration
    :param logg: Logger instance
    :param max_lock_age_minutes: Maximum age for locks before considering them stale
    """
    temp_db = MySQLHelper(db_config)
    try:
        temp_db.connect()
        temp_db.use_database("mysql")
        
        # Check if table_locks table exists
        check_table_sql = "SHOW TABLES LIKE 'table_locks'"
        success, result = temp_db.execute_query(check_table_sql, fetch='one')
        
        if not success or not result:
            logg.log_info("No table_locks table found - no cleanup needed")
            return
        
        # Get all current locks
        get_locks_sql = """
        SELECT database_name, table_name, lock_holder, operation_type, created_at 
        FROM table_locks 
        ORDER BY created_at
        """
        success, locks = temp_db.execute_query(get_locks_sql, fetch='all')
        
        if not success:
            logg.log_warn("Failed to query existing locks")
            return
            
        if not locks:
            logg.log_info("No existing locks found")
            return
            
        logg.log_info(f"Found {len(locks)} existing locks")
        
        # Calculate stale lock threshold
        from datetime import datetime, timedelta
        stale_threshold = datetime.now() - timedelta(minutes=max_lock_age_minutes)
        
        stale_locks = []
        current_locks = []
        
        for lock in locks:
            database_name, table_name, lock_holder, operation_type, created_at = lock
            
            # If created_at is None or too old, consider it stale
            if created_at is None or created_at < stale_threshold:
                stale_locks.append((database_name, table_name, lock_holder, operation_type, created_at))
            else:
                current_locks.append((database_name, table_name, lock_holder, operation_type, created_at))
        
        # Report current state
        if current_locks:
            logg.log_info(f"Current active locks ({len(current_locks)}):")
            for database_name, table_name, lock_holder, operation_type, created_at in current_locks:
                logg.log_info(f"  - {database_name}.{table_name} by {lock_holder} ({operation_type}) at {created_at}")
        
        if stale_locks:
            logg.log_warn(f"Found {len(stale_locks)} stale locks to cleanup:")
            for database_name, table_name, lock_holder, operation_type, created_at in stale_locks:
                logg.log_warn(f"  - {database_name}.{table_name} by {lock_holder} ({operation_type}) at {created_at}")
            
            # Clean up stale locks
            cleanup_count = 0
            for database_name, table_name, lock_holder, operation_type, created_at in stale_locks:
                delete_sql = """
                DELETE FROM table_locks 
                WHERE database_name = %s AND table_name = %s AND lock_holder = %s
                """
                success, _ = temp_db.execute_query(delete_sql, (database_name, table_name, lock_holder))
                if success:
                    cleanup_count += 1
                    logg.log_info(f"Cleaned up stale lock: {database_name}.{table_name}")
                else:
                    logg.log_error(f"Failed to cleanup lock: {database_name}.{table_name}")
            
            temp_db.commit_and_refresh_cursor()
            logg.log_ok(f"Successfully cleaned up {cleanup_count} stale locks")
        else:
            logg.log_info("No stale locks found")
            
    except Exception as e:
        logg.log_error(f"Error during lock cleanup: {e}")
    finally:
        temp_db.close()


def force_cleanup_all_locks(db_config: Dict[str, Any], logg: Logger) -> None:
    """
    Force cleanup ALL locks - use with caution, only when you're sure no other processes are running.
    
    :param db_config: Database configuration
    :param logg: Logger instance
    """
    logg.log_warn("FORCE CLEANUP: Removing ALL table locks - use with caution!")
    
    temp_db = MySQLHelper(db_config)
    try:
        temp_db.connect()
        temp_db.use_database("mysql")
        
        # Check if table exists
        check_table_sql = "SHOW TABLES LIKE 'table_locks'"
        success, result = temp_db.execute_query(check_table_sql, fetch='one')
        
        if not success or not result:
            logg.log_info("No table_locks table found")
            return
        
        # Get count before cleanup
        count_sql = "SELECT COUNT(*) FROM table_locks"
        success, result = temp_db.execute_query(count_sql, fetch='one')
        lock_count = result[0] if success and result else 0
        
        if lock_count == 0:
            logg.log_info("No locks to cleanup")
            return
            
        logg.log_warn(f"Found {lock_count} locks to force cleanup")
        
        # Clear all locks
        clear_sql = "DELETE FROM table_locks"
        success, _ = temp_db.execute_query(clear_sql)
        
        if success:
            temp_db.commit_and_refresh_cursor()
            logg.log_ok(f"Force cleanup completed: removed {lock_count} locks")
        else:
            logg.log_error("Force cleanup failed")
            
    except Exception as e:
        logg.log_error(f"Error during force cleanup: {e}")
    finally:
        temp_db.close()


def show_current_locks(db_config: Dict[str, Any], logg: Logger) -> None:
    """
    Show all current table locks for debugging.
    
    :param db_config: Database configuration
    :param logg: Logger instance
    """
    temp_db = MySQLHelper(db_config)
    try:
        temp_db.connect()
        temp_db.use_database("mysql")
        
        # Check if table exists
        check_table_sql = "SHOW TABLES LIKE 'table_locks'"
        success, result = temp_db.execute_query(check_table_sql, fetch='one')
        
        if not success or not result:
            logg.log_info("No table_locks table found")
            return
        
        # Get all locks
        get_locks_sql = """
        SELECT database_name, table_name, lock_holder, operation_type, created_at 
        FROM table_locks 
        ORDER BY created_at DESC
        """
        success, locks = temp_db.execute_query(get_locks_sql, fetch='all')
        
        if not success:
            logg.log_error("Failed to query locks")
            return
            
        if not locks:
            logg.log_info("No current locks found")
            return
            
        logg.log_info(f"Current table locks ({len(locks)}):")
        for database_name, table_name, lock_holder, operation_type, created_at in locks:
            logg.log_info(f"  - {database_name}.{table_name}")
            logg.log_info(f"    Holder: {lock_holder}")
            logg.log_info(f"    Operation: {operation_type}")
            logg.log_info(f"    Created: {created_at}")
            logg.log_info("")
            
    except Exception as e:
        logg.log_error(f"Error showing locks: {e}")
    finally:
        temp_db.close()

def main() -> None:
    """
    Entry point: load configuration and process all SKUs and test stations.
    """
    logg = Logger()

    try:
        # Load configuration
        if len(sys.argv) > 2 and sys.argv[1] == '--config':
            dbpath = sys.argv[2]
        else:
            dbpath = 'db_setting/db_setting_test.json'
        
        dbsetting = load_dbsetting(dbpath)
        default_recently_days = dbsetting.get('default_recently_days', 7)
        default_enable = dbsetting.get('default_enable', True)


        if("global" not in dbpath):
            # CRITICAL: Clean up ALL locks before starting any operations
            logg.log_title("Lock Cleanup Phase")
            
            # Check for command line lock cleanup options
            if len(sys.argv) > 1:
                if '--show-locks' in sys.argv:
                    show_current_locks(dbsetting, logg)
                    return
                elif '--keep-locks' in sys.argv:
                    logg.log_warn("Keeping existing locks as requested (--keep-locks)")
                elif '--cleanup-stale-locks' in sys.argv:
                    cleanup_stale_locks(dbsetting, logg, max_lock_age_minutes=30)
                else:
                    # Default behavior: clean ALL locks for fresh start
                    force_cleanup_all_locks(dbsetting, logg)
            else:
                # Default behavior: clean ALL locks for fresh start
                force_cleanup_all_locks(dbsetting, logg)
            
            # Setup exception DB and tables using short connection
            logg.log_title("Database Setup Phase")
            exception_db = MySQLHelper(dbsetting)
            exception_db.connect()
            create_exception_db(exception_db, dbsetting)
            exception_db.close()
            
            # clean cache
            temp_db = MySQLHelper(dbsetting)
            temp_db.connect()
            temp_db.use_database("mysql")
            temp_db.clear_table_locks_cache()
            temp_db.close()
            
            # Process each SKU
            logg.log_title("Processing Phase")
            for sku_title, sku_info in dbsetting['sku_parser_rule'].items():
                process_sku_with_comprehensive_cleanup(
                    dbsetting,  # Pass the database config dict
                    sku_title, 
                    sku_info, 
                    default_recently_days, 
                    default_enable, 
                    logg, 
                    dbsetting  # Pass config for exception_db too
                )

            logg.log_ok("All tasks completed successfully.")

        else:
            db = MySQLHelper(dbsetting)
            db.connect()
            for sku_title, sku_info in dbsetting['sku_parser_rule'].items():
                process_sku_with_comprehensive_cleanup(
                    dbsetting,  # Pass the database config dict
                    sku_title, 
                    sku_info, 
                    default_recently_days, 
                    default_enable, 
                    logg, 
                    dbsetting  # Pass config for exception_db too
                )
            db.close()
            logg.log_ok("All tasks completed successfully.")


    except Exception as e:
        logg.log_error(f"Fatal error: {e}")
        import traceback
        logg.log_error(f"Traceback: {traceback.format_exc()}")
        
        # Emergency cleanup on fatal error
        try:
            logg.log_warn("Performing emergency lock cleanup due to fatal error")
            cleanup_stale_locks(dbsetting, logg, max_lock_age_minutes=0)  # Clean all locks
        except:
            pass

    
if __name__ == '__main__':
    main()