#!/usr/bin/python3

import os
import time
import json
import sys
import threading
import queue
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import xxhash 
from io import StringIO
import csv

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.MySQLHelper import MySQLHelper
from core.Logger import Logger
from workers import worker_ult as wult
from ParseEngine.engine.ParseEngineData import ParseEngineData

def load_dbsetting(path: str = 'db_setting.json') -> Dict[str, Any]:
    """
    Load database configuration from a JSON file.

    :param path: Path to db_setting.json file.
    :return: Parsed database configuration dictionary.
    """
    with open(path, 'r', encoding='UTF-8') as f:
        return json.load(f)

def process_files_for_global(
    db: MySQLHelper,
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

    with wult.DurationTimer(logg, "Get local file list"):
        filelist = wult.get_local_filelist(folder, recently_days)
        logg.log_info(f'Local file count: {len(filelist)}')

    failed_parse_files = []
    failed_update_files = []

    db_schema = db.get_table_schema(tablename)
    if 'hash_value' not in db_schema:
        logg.log_info(f"Adding 'hash_value' column to {tablename}")
        alter_sql = f"ALTER TABLE `{tablename}` ADD COLUMN `hash_value` VARCHAR(32)"
        db.execute_query(alter_sql)
        db.commit_and_refresh_cursor()
        logg.log_ok(f"'hash_value' column added to {tablename}")

    index_exists_sql = f"SHOW INDEX FROM `{tablename}` WHERE Key_name = '_idx'"
    exists, result = db.execute_query(index_exists_sql, fetch='one')
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

    all_success = len(failed_parse_files) == 0 and len(failed_update_files) == 0
    return all_success, failed_parse_files, failed_update_files

def process_files(
    db: MySQLHelper,
    dbname: str,
    tablename: str,
    rule_dict: Dict[str, Any],
    folder: str,
    recently_days: int,
    time_column: str,
    logg: Logger,
    exception_db: MySQLHelper,
    exception_table: str,
    new_fields: Dict[str, Any],
    schema_updated: bool = False,
    thread_pool_size: int = 8,
    batch_size: int = 30,
    commit_threshold: int = 150,
    time_check_interval: float = 2.0,
    commit_time_threshold: float = 8.0
) -> None:
    """
    Parse and insert new files not yet in the database.
    Multi-threaded parsing with batch insert.

    :param db: MySQLHelper instance for DB interaction.
    :param tablename: Table to insert records into.
    :param rule_dict: JSON rule used for parsing structure.
    :param folder: Directory path to read files from.
    :param recently_days: Lookback range (in days) to filter files.
    :param time_column: Column used for timestamp filtering in DB.
    :param logg: Logger instance for logging info/warnings/errors.
    :param exception_db: MySQLHelper instance for performing DB operations, specific in exception database.
    :param exception_table: Name of the table to insert or delete records in exception database.
    :param schema_updated: Flag indicating if the schema was updated.
    :param thread_pool_size: Number of worker threads for parsing (default: 8).
    :param batch_size: Number of items to batch insert (default: 20).
    :param commit_threshold: Number of successful inserts before commit (default: 150).
    :param time_check_interval: Interval for time-based checks (default: 2.0 seconds).
    :param commit_time_threshold: Time threshold for commits (default: 8.0 seconds).
    """
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
                
                # If there are missing or invalid fields, treat as parse failure
                if missing_fields or invalid_fields:
                    error_parts = []
                    if missing_fields:
                        error_parts.append(f"Missing fields: {missing_fields}")
                    if invalid_fields:
                        error_parts.append(f"Invalid fields: {invalid_fields}")
                    error_msg = "; ".join(error_parts)
                    
                    logg.log_parse_fail(filename, reason=error_msg)
                    
                    with exception_lock:
                        exception_records.append({
                            "fullpath": filename,
                            "reason": "missing_required_fields" if missing_fields else "invalid_field_values",
                            "exception_context": error_msg
                        })
                    return ('parse_fail', filename, None)
                
                # All required fields are present and valid
                return ('success', filename, data)
            else:
                logg.log_parse_fail(filename, reason="parser.parse_engine returned False")
                with exception_lock:
                    exception_records.append({
                        "fullpath": filename,
                        "reason": "empty_file",
                        "exception_context": "File is empty or unparseable"
                    })
                return ('parse_fail', filename, None)

        except KeyError as e_inner:
            logg.log_error(f"Missing key during parsing: {e_inner}, fullpath: {filename}")
            with exception_lock:
                exception_records.append({
                    "fullpath": filename,
                    "reason": "parsing_error",
                    "exception_context": f"Missing key: {e_inner}"
                })
            return ('parse_fail', filename, None)

        except Exception as e_inner:
            logg.log_error(f"Exception occurred during parsing: {e_inner}, fullpath: {filename}")
            with exception_lock:
                exception_records.append({
                    "fullpath": filename,
                    "reason": "unknown",
                    "exception_context": str(e_inner)
                })
            return ('parse_fail', filename, None)

    with wult.DurationTimer(logg, "Get local file list"):
        filelist = wult.get_local_filelist(folder, recently_days)
        logg.log_info(f'Local file count: {len(filelist)}')

    with wult.DurationTimer(logg, "Get db exist fullpath"):
        existing_paths = db.get_existing_fullpaths(tablename, time_column, recently_days + 1)
        logg.log_info(f'DB exist file count: {len(existing_paths)}')

        difference_files = list(set(filelist) - set(existing_paths))
        difference_files.sort(key=lambda x: x.split('/')[6], reverse=True)
        logg.log_info(f'Difference file count: {len(difference_files)}')

    # Only skip if schema was not updated and there are no new files
    if not schema_updated:
        if not difference_files:
            logg.log_ok("Data is up to date. No new files to insert.")
            return
    else:
        logg.log_info("Schema was updated. Will re-parse and re-insert old data in batches.")

        # Get all old file paths from DB
        old_fullpaths = list(db.get_existing_fullpaths(tablename, time_column, 3000))
        logg.log_info(f"Found {len(old_fullpaths)} old records to re-parse.")

        batch_size = 500
        total = len(old_fullpaths)
        processing_time = time.time()
        db_schema = db.get_table_schema(tablename)
        for i in range(0, total, batch_size):
            batch_files = old_fullpaths[i:i+batch_size]

            # Fetch old data for these fullpaths
            placeholders = ','.join(['%s'] * len(batch_files))
            select_sql = f"SELECT * FROM `{tablename}` WHERE fullpath IN ({placeholders})"
            success, rows = db.execute_query(select_sql, batch_files, fetch='all')
            if not success:
                logg.log_error("Failed to fetch old data for reschema batch")
                continue
            col_names = list(db_schema.keys())
            old_data_dict = {row[col_names.index('fullpath')]: dict(zip(col_names, row)) for row in rows}

            # Parse only new fields for each file in batch
            def parse_new_fields(fullpath):
                parser = ParseEngineData(selective_fields=new_fields)
                if parser.parse_engine(fullpath, new_fields):
                    data = dict(parser.items())
                    return {field: data.get(field) for field in new_fields}
                else:
                    return {field: None for field in new_fields}

            new_data_dict = {}
            with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
                future_map = {executor.submit(parse_new_fields, fp): fp for fp in batch_files}
                for future in as_completed(future_map):
                    fp = future_map[future]
                    try:
                        new_data_dict[fp] = future.result()
                    except Exception as e:
                        logg.log_error(f"Failed to parse {fp} for new fields: {e}")
                        new_data_dict[fp] = {field: None for field in new_fields}

            # Merge old data with new fields
            merged_batch = []
            for fp in batch_files:
                merged = old_data_dict.get(fp, {}).copy()
                merged.update(new_data_dict.get(fp, {}))
                merged_batch.append(merged)

            # Delete old records in batch
            if not db.delete_by_fullpaths_batch(tablename, batch_files):
                logg.log_error(f"Failed to delete batch {i//batch_size+1}")
                continue

            # Insert merged records in batch
            for d in merged_batch:
                for k in col_names:
                    if k not in d:
                        d[k] = None
                for k in list(d.keys()):
                    if k not in col_names:
                        d.pop(k)
            try:
                success = db.add_data_batch(tablename, merged_batch)
                if not success:
                    logg.log_error(f"Failed to insert batch {i//batch_size+1}")
            except Exception as e:
                logg.log_error(f"Batch insert error: {e}")

            if i % 100 == 0 or i + batch_size >= total:
                elapsed = time.time() - processing_time
                percent = (i + batch_size) * 100 / total if total > 0 else 0
                logg.log_info(f'[Reschema] Duration: {elapsed:.1f}s, {percent:.1f}%, {i+batch_size}/{total}')

        logg.log_info("Old data re-parsed, deleted, and re-inserted in batches after schema update.")

    unlogged_parsed_files = []
    unlogged_unparsed_files = []
    all_logged = False
    
    # Collect les abnormal
    exception_records = []
    exception_lock = threading.Lock()
    
    # Multi-threading components
    data_queue = queue.Queue()
    queue_lock = threading.Lock()
    insert_success_count = 0
    insert_success_lock = threading.Lock()
    last_commit_time = time.time()
    commit_time_lock = threading.Lock()
    stop_consumer = threading.Event() 
    
    def consumer_thread():
        """Consumer thread that processes items from the queue."""
        nonlocal insert_success_count, last_commit_time
        
        while not stop_consumer.is_set() or not data_queue.empty():
            batch = []
            
            # Check queue size and time triggers
            with queue_lock:
                current_queue_size = data_queue.qsize()
                # Batch size trigger
                if current_queue_size >= batch_size:
                    for _ in range(batch_size):
                        try:
                            item = data_queue.get_nowait()
                            batch.append(item)
                        except queue.Empty:
                            break
                # Time trigger
                elif current_queue_size > 0 and time.time() - last_commit_time >= time_check_interval:
                    while not data_queue.empty():
                        try:
                            item = data_queue.get_nowait()
                            batch.append(item)
                            if len(batch) >= batch_size:
                                break
                        except queue.Empty:
                            break        
            
            # Process batch
            if batch:
                # Group by field structure to handle different schemas
                from collections import defaultdict
                grouped_batches = defaultdict(list)
                
                for data in batch:
                    # Create a signature based on sorted field names
                    field_signature = tuple(sorted(data.keys()))
                    grouped_batches[field_signature].append(data)
                
                # Process each group separately
                for field_signature, group_batch in grouped_batches.items():
                    try:
                        success = db.add_data_batch(tablename, group_batch)
                        if success:
                            with insert_success_lock:
                                insert_success_count += len(group_batch)
                        else:
                            # If batch insert fails, fall back to individual inserts
                            logg.log_warn(f"Batch insert failed for {len(group_batch)} records with fields: {len(field_signature)} columns")
                            for data in group_batch:
                                fullpath = data.get("fullpath")
                                try:
                                    result = db.insert_or_update(tablename, data)
                                    if result is True:
                                        with insert_success_lock:
                                            insert_success_count += 1
                                    else:
                                        # insert fail
                                        logg.log_insert_exception(fullpath, "insert_data", data, "insert or update fail")
                                        unlogged_parsed_files.append(fullpath)
                                        with exception_lock:
                                            exception_records.append({
                                                "fullpath": fullpath,
                                                "reason": "db_insert_fail",
                                                "exception_context": str(result)
                                            })
                                except Exception as e:
                                    logg.log_insert_exception(fullpath, "insert_data", data, e)
                                    unlogged_parsed_files.append(fullpath)
                                    with exception_lock:
                                        exception_records.append({
                                            "fullpath": fullpath,
                                            "reason": "db_insert_fail",
                                            "exception_context": str(e)
                                        })
                    except Exception as batch_error:
                        logg.log_error(f"Batch processing error for group with {len(group_batch)} records: {batch_error}")
                        # Fall back to individual processing on batch error
                        for data in group_batch:
                            fullpath = data.get("fullpath")
                            unlogged_parsed_files.append(fullpath)
                            with exception_lock:
                                exception_records.append({
                                    "fullpath": fullpath,
                                    "reason": "batch_processing_error",
                                    "exception_context": str(batch_error)
                                })

            with insert_success_lock:
                current_success_count = insert_success_count
            
            with commit_time_lock:
                time_since_commit = time.time() - last_commit_time
            
            # Commit based on count or time
            if current_success_count >= commit_threshold or (current_success_count > 0 and time_since_commit >= commit_time_threshold):
                db.commit_and_refresh_cursor()
                with insert_success_lock:
                    insert_success_count = 0
                with commit_time_lock:
                    last_commit_time = time.time()
                #logg.log_info(f"Committed transaction: {current_success_count} records")
            if not batch:
                time.sleep(0.1)
    
    # Start consumer thread
    consumer = threading.Thread(target=consumer_thread)
    consumer.start()
    
    processing_time = time.time()
    total = len(difference_files)
    
    with wult.DurationTimer(logg, "Parsing and inserting files"):
        with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
            futures = []
            for filename in difference_files:
                future = executor.submit(parse_worker, filename)
                futures.append(future)
            for i, future in enumerate(as_completed(futures)):
                if i % 100 == 0:
                    elapsed = time.time() - processing_time
                    percent = i * 100 / total if total > 0 else 0
                    logg.log_info(f'Duration: {elapsed:.1f}s, {percent:.1f}%, {i}/{total}')
                
                try:
                    status, filename, data = future.result()
                    if status == 'success':
                        with queue_lock:
                            data_queue.put(data)
                    else:
                        unlogged_unparsed_files.append(filename)
                except Exception as e:
                    logg.log_error(f"Future execution error: {e}")
                    unlogged_unparsed_files.append(filename)
    
    # Signal consumer to stop and wait for completion
    stop_consumer.set()
    consumer.join()
    
    # Final commit if needed
    with insert_success_lock:
        if insert_success_count > 0:
            db.commit_and_refresh_cursor()
            logg.log_info(f"Final commit: {insert_success_count} records")
    
    # Cleanup resolved exceptions before processing new ones
    if exception_records and exception_db and exception_table:   
        try:
            successful_fullpaths = []
            for filename in difference_files:
                if filename not in unlogged_parsed_files and filename not in unlogged_unparsed_files:
                    successful_fullpaths.append(filename)
            
            if successful_fullpaths:
                exception_db.use_database("exception")
                # Delete successfully processed files from exception table
                with wult.DurationTimer(logg, f"Exception cleanup for table {exception_table}"):
                    cleanup_count = 0
                    batch_delete_size = 100  # Process deletions in batches
                    
                    for i in range(0, len(successful_fullpaths), batch_delete_size):
                        batch = successful_fullpaths[i:i + batch_delete_size]  
                        placeholders = ', '.join(['%s'] * len(batch))
                        delete_query = f"DELETE FROM `{exception_table}` WHERE fullpath IN ({placeholders})"
                        result = exception_db.execute_query(delete_query, batch)
                        if result[0]:
                            cleanup_count += exception_db.cursor.rowcount
                    exception_db.commit_and_refresh_cursor()
                    logg.log_info(f"Cleaned up {cleanup_count} resolved exception records from {exception_table}")
                
        except Exception as cleanup_error:
            logg.log_error(f"Exception cleanup failed: {cleanup_error}")
            try:
                db.use_database(dbname)
            except:
                pass

     # Process all exception data at the end
    if exception_records and exception_db and exception_table:
        logg.log_info(f"Processing {len(exception_records)} exception records")
        try:
            exception_db.use_database("exception")
            for record in exception_records:
                exception_db.insert_if_not_exist(exception_table, record)
            exception_db.commit_and_refresh_cursor()
            logg.log_info(f"Successfully logged {len(exception_records)} exceptions")
        except Exception as e:
            logg.log_error(f"Failed to process exception records: {e}")
    
    if not unlogged_parsed_files and not unlogged_unparsed_files:
        all_logged = True
        
    return all_logged, unlogged_parsed_files, unlogged_unparsed_files

def cleanup_all_resolved_exceptions(
    db: MySQLHelper, 
    exception_db: MySQLHelper, 
    tablename: str, 
    exception_table: str, 
    time_column: str,
    recently_days: int,
    logg: Logger
) -> int:
    """
    Clean up all records that exist in main database but still remain in exception table
    
    :param db: Main database connection
    :param exception_db: Exception database connection  
    :param tablename: Main table name
    :param exception_table: Exception table name
    :param time_column: Time column name for filtering
    :param recently_days: Number of days to check back
    :param logg: Logger instance
    :return: Number of cleaned up records
    """
    try:
        exception_db.use_database("exception")
        success, exception_records = exception_db.execute_query(
            f"SELECT DISTINCT fullpath FROM `{exception_table}`", fetch='all'
        )
        
        if not success or not exception_records:
            return 0
            
        exception_fullpaths = [record[0] for record in exception_records]
        logg.log_info(f"Found {len(exception_fullpaths)} exception records to check for resolution")
        
        # Use extended time range to ensure coverage of all possible exception records
        extended_days = max(recently_days * 2, 30)  # Check at least 30 days back
        existing_fullpaths = db.get_existing_fullpaths(tablename, time_column, extended_days)
        resolved_fullpaths = [fp for fp in exception_fullpaths if fp in existing_fullpaths]
        
        if not resolved_fullpaths:
            return 0
            
        logg.log_info(f"Found {len(resolved_fullpaths)} resolved exception records to cleanup")
        
        # Batch delete resolved exception records
        exception_db.use_database("exception")
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

def mcc_process(
    db: MySQLHelper,
    dbname: str,
    tablename: str,
    mcc_rule_dict: dict,
    time_column: str,
    logg: Logger,
    recently_days: int = 7,
    batch_size: int = 500,
    thread_pool_size: int = 2
) -> bool:
    """
    MCC processing: find mcc files for each cap who has missing columns, parse .mcc files in parallel, update MCC fields by delete and insert in DB in batch.

    :param db: MySQLHelper instance for DB interaction.
    :param dbname: Name of the database being used.
    :param tablename: Table to insert records into.
    :param mcc_rule_dict: JSON rule used for parsing structure.
    :param time_column: Column used for timestamp filtering in DB.
    :param logg: Logger instance for logging info/warnings/errors.
    :param recently_days: Lookback range (in days) to filter files.
    :param batch_size: Number of items to batch insert.
    :param thread_pool_size: Number of worker threads for parsing.
    """
    db_schema = db.get_table_schema(tablename)
    mcc_field_names = [k for k, v in mcc_rule_dict.items() if "dbtype" in v]
    missing_fields = {k: v["dbtype"] for k, v in mcc_rule_dict.items() if k not in db_schema and "dbtype" in v}
    if missing_fields:
        logg.log_info(f"[MCC] Adding missing MCC columns: {list(missing_fields.keys())}")
        warnings, schema_updated = db.sync_table_schema(tablename, missing_fields)
        for w in warnings:
            logg.log_warn(w)
        if schema_updated:
            logg.log_ok("[MCC] MCC columns added to table.")

    # Find records needing MCC processing
    date_threshold = (datetime.now() - timedelta(days=recently_days)).strftime('%Y-%m-%d')
    mcc_null_conditions = [f"(`{field}` IS NULL OR `{field}` = '' OR `{field}` = 'NA')" for field in mcc_field_names]
    where_clause = " AND ".join(mcc_null_conditions)
    sql = f"SELECT fullpath FROM `{tablename}` WHERE ({where_clause} AND `{time_column}` >= %s)"
    success, records = db.execute_query(sql, (date_threshold,), fetch='all')
    if not success or not records:
        logg.log_info("[MCC] No records need MCC processing.")
        return True

    fullpaths = [row[0] for row in records]
    logg.log_info(f"[MCC] Found {len(fullpaths)} records needing MCC processing.")
    start_time = time.time()
    done_cnt = 0

    def parse_mcc_file(fp):
        if not fp or not isinstance(fp, str):
            updated_fields = {}
            for field in mcc_field_names:
                if "time" in field:
                    updated_fields[field] = "00-00-00 00:00:00"
                else:
                    updated_fields[field] = "NA"
            return fp, updated_fields

        mcc_fp = fp.replace('.cap', '.mcc').replace('/PT/', '/PT-MCC/')
        updated_fields = {}
        if os.path.exists(mcc_fp):
            parser = ParseEngineData(selective_fields=mcc_field_names)
            if parser.parse_engine(mcc_fp, mcc_rule_dict):
                parsed = dict(parser.items())
                for field in mcc_field_names:
                    value = parsed.get(field, "NA")
                    value_str = str(value).strip().upper() if value is not None else ""
                    if "time" in field:
                        if value_str in ("NA", "N/A", "", "NONE"):
                            updated_fields[field] = "00-00-00 00:00:00"
                        else:
                            updated_fields[field] = value
                    else:
                        updated_fields[field] = value
            else:
                for field in mcc_field_names:
                    if "time" in field:
                        updated_fields[field] = "00-00-00 00:00:00"
                    else:
                        updated_fields[field] = "NA"
        else:
            for field in mcc_field_names:
                if "time" in field:
                    updated_fields[field] = "00-00-00 00:00:00"
                else:
                    updated_fields[field] = "NA"
        return fp, updated_fields

    # Process in batches
    for i in range(0, len(fullpaths), batch_size):
        batch_fullpaths = fullpaths[i:i+batch_size]

        # Threaded parsing of MCC files
        results = []
        with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
            future_map = {executor.submit(parse_mcc_file, fp): fp for fp in batch_fullpaths}
            for future in as_completed(future_map):
                try:
                    fp, updated_fields = future.result()
                    results.append((fp, updated_fields))
                except Exception as e:
                    logg.log_error(f"[MCC] Exception parsing {future_map[future]}: {e}")

        # Fetch original rows
        placeholders = ','.join(['%s'] * len(batch_fullpaths))
        select_sql = f"SELECT * FROM `{tablename}` WHERE fullpath IN ({placeholders})"
        success, rows = db.execute_query(select_sql, batch_fullpaths, fetch='all')
        if not success:
            logg.log_error(f"[MCC] Failed to fetch rows for batch {i//batch_size+1}")
            continue

        # Build full row data by merging original + parsed MCC fields
        db_schema = db.get_table_schema(tablename)
        col_names = list(db_schema.keys())
        existing_data_dict = {row[col_names.index('fullpath')]: dict(zip(col_names, row)) for row in rows}

        merged_data = []
        for fp, mcc_fields in results:
            base = existing_data_dict.get(fp)
            if not base:
                logg.log_warn(f"[MCC] Original row for {fp} not found, skipping")
                continue

            merged = base.copy()
            merged.update(mcc_fields)
            merged_data.append(merged)

        # Delete old rows
        delete_sql = f"DELETE FROM `{tablename}` WHERE fullpath IN ({placeholders})"
        success, _ = db.execute_query(delete_sql, batch_fullpaths)
        if not success:
            logg.log_error(f"[MCC] Failed to delete rows for batch {i//batch_size+1}")
            continue

        # Insert merged rows
        try:
            success = db.add_data_batch(tablename, merged_data)
            if not success:
                logg.log_error(f"[MCC] Batch insert failed for batch {i//batch_size+1}")
        except Exception as e:
            logg.log_error(f"[MCC] Batch insert exception: {e}")

        db.commit_and_refresh_cursor()
        total_elapsed = time.time() - start_time
        done_cnt += len(batch_fullpaths)
        logg.log_info(f"[MCC] Processed: {done_cnt/len(fullpaths)*100:.1f}%, {done_cnt}/{len(fullpaths)} records in {total_elapsed:.1f} secs.")
    return True

def process_test_station_with_cleanup(
    db: MySQLHelper,
    dbname: str,
    test_station: str,
    ts_info: Dict[str, Any],
    default_recently_days: int,
    default_enable: bool,
    logg: Logger,
    exception_db: MySQLHelper,
    exception_table: str,
    schema_updated: bool = False,
) -> None:
    """
    Process rule-based logic for a given test station with comprehensive exception cleanup.
    
    :param db: MySQLHelper instance
    :param dbname: Name of the database being used
    :param test_station: Name of the test station (e.g., "pt", "pdlp")
    :param ts_info: Test station configuration
    :param default_recently_days: Fallback recently_days if not provided
    :param default_enable: Fallback enable flag
    :param logg: Logger instance
    :param exception_db: MySQLHelper instance for exception database operations
    :param exception_table: Name of the table to insert or delete records in exception database
    :param schema_updated: Flag indicating if the schema was updated
    """
    if not ts_info.get('enable', default_enable):
        logg.log_warn(f"  - <{test_station}> disabled, skipped")
        return
    tablename = ts_info['tablename']
    
    try:
        rule = ts_info['rule']
        tablename = ts_info['tablename']
        folder = ts_info['folder']
        recently_days = ts_info.get('recently_days', default_recently_days)
        time_column = ts_info.get('time_column', 'tbeg')

        logg.log_sub_title(f"Test Station: {test_station}")
        logg.log_info(f"Rule: {rule}")
        logg.log_info(f"Table: {tablename}")
        logg.log_info(f"Folder: {folder}")
        logg.log_info(f"Recently Days: {recently_days}")

        if not os.path.isfile(rule):
            logg.log_warn(f"  - <{rule}> does not exist, skipped")
            return

        with wult.DurationTimer(logg, f"Load rule and create/sync table: {tablename}"):
            with open(rule, 'r', encoding='UTF-8') as f:
                rule_dict = json.load(f)
            schema_fields = wult.generate_schema_from_rule(rule_dict)
            db.create_table_if_nexists(tablename, schema_fields)

            # identify new fields before syncing schema
            db_schema_before = db.get_table_schema(tablename)
            db_schema_cols_before = set(db_schema_before.keys())
            schema_dict = {k: v["dbtype"] for k, v in rule_dict.items() if "dbtype" in v}
            json_defined_cols = set(schema_dict.keys())
            new_fields = {col: rule_dict[col] for col in json_defined_cols if col not in db_schema_cols_before}

            warnings, schema_updated = db.sync_table_schema(tablename, schema_dict)
            for w in warnings: logg.log_warn(w)

            # unused fields
            db_schema_cols = set(db.get_table_schema(tablename).keys())
            unused_in_the_future = db_schema_cols - json_defined_cols
            if unused_in_the_future:
                logg.log_warn(f"Potential unused fields in DB not defined in JSON: {unused_in_the_future}")
            logg.log_ok(f"Table {tablename} created and schema synchronized.")

            if new_fields:
                logg.log_info(f"New fields detected before schema sync: {list(new_fields.keys())}")

        use_wo_grouping = ts_info.get('use_wo_grouping', False)
        # Process files based on configuration
        if use_wo_grouping:
            process_info = process_files_for_global(
                db, dbname, tablename, rule_dict, folder,
                recently_days, time_column, logg
            )
        else:
            process_info = process_files(
                db, dbname, tablename, rule_dict, folder, recently_days,
                time_column, logg, exception_db, exception_table, new_fields, schema_updated
            )
        
        # Handle process results
        if process_info:
            all_logged, unlogged_parsed_files, unlogged_unparsed_files = process_info
            if all_logged:
                logg.log_ok(f"All files for Test Station '{test_station}' logged into Database '{dbname}'")
            else:
                failed_count = len(unlogged_parsed_files) + len(unlogged_unparsed_files)
                logg.log_warn(f"{failed_count} files failed to be logged into Database '{dbname}'")
                logg.log_warn(f"{len(unlogged_parsed_files)} files failed during data insertion: {unlogged_parsed_files}")
                logg.log_warn(f"{len(unlogged_unparsed_files)} files failed during parsing: {unlogged_unparsed_files}")
        
        # Comprehensive exception cleanup after processing
        if exception_db and exception_table:            
            try:             
                cleanup_count = cleanup_all_resolved_exceptions(
                    db, exception_db, tablename, exception_table, 
                    time_column, recently_days, logg
                )
                if cleanup_count > 0:
                    logg.log_ok(f"Exception cleanup completed: {cleanup_count} resolved records removed")
                else:
                    logg.log_info("Exception cleanup completed: no resolved records found")
                    
            except Exception as cleanup_error:
                logg.log_error(f"Exception cleanup failed for {test_station}: {cleanup_error}")
                try:
                    db.use_database(dbname)
                except:
                    pass

        # MCC Processing
        mcc_rule = ts_info.get('mcc_rule')
        has_mcc = mcc_rule is not None
        if has_mcc:
            logg.log_info(f"Starting MCC processing for test station: {test_station}")
            if not os.path.isfile(mcc_rule):
                logg.log_warn(f"MCC rule file does not exist: {mcc_rule}, skipping MCC processing")
            else:
                try:
                    with open(mcc_rule, 'r', encoding='UTF-8') as f:
                        mcc_rule_dict = json.load(f)
                    
                    with wult.DurationTimer(logg, f"MCC processing for {tablename}"):
                        success = mcc_process(
                            db, dbname, tablename, mcc_rule_dict,
                            time_column, logg, recently_days)
                        if success:
                            logg.log_ok(f"MCC processing completed successfully for {tablename}")
                        else:
                            logg.log_error(f"MCC processing failed for {tablename}")
                            
                except Exception as mcc_error:
                    logg.log_error(f"Error during MCC processing for {test_station}: {mcc_error}")
        else:
            logg.log_info("No MCC processing required - mcc_rule")
    finally:
        db.release_table_lock()

def process_sku_with_comprehensive_cleanup(
    db: MySQLHelper,
    sku_title: str,
    sku_info: Dict[str, Any],
    default_recently_days: int,
    default_enable: bool,
    logg: Logger,
    exception_db: MySQLHelper,
    exception_table: str = None
) -> None:
    """
    Loop through and process all test stations of the given SKU with comprehensive exception cleanup.
    
    :param db: MySQLHelper instance
    :param sku_title: Identifier of the SKU (e.g., "K2v5_JRD03_R")
    :param sku_info: Configuration dictionary for that SKU
    :param default_recently_days: Fallback recently_days for test stations
    :param default_enable: Fallback enable flag
    :param logg: Logger instance
    :param exception_db: MySQLHelper instance for exception database operations
    :param exception_table: Name of the table to insert or delete records in exception database
    """
    if not sku_info.get('enable', default_enable):
        logg.log_warn(f"SKU <{sku_title}> disabled, skipped")
        return

    dbname = sku_info['dbname']
    logg.log_title(f"DB_NAME : {dbname}")
    exception_table = dbname

    class TableLockTimeoutException(Exception):
        pass
    class TableLockManager:
        def __init__(self, db_helper: MySQLHelper, db_name: str, table_name: str, lock_timeout: int = 5, logger: Logger = None):
            self.db = db_helper
            self.dbname = db_name
            self.tablename = table_name
            self.lock_timeout = lock_timeout
            self.logg = logger
            self.locked = False
            
        def __enter__(self):
            try:
                # Check whether table is locked
                if not self.db.use_database_lock(self.dbname):
                    self.logg.log_warn(f"Database {self.dbname} is locked")
                    raise TableLockTimeoutException(f"Could not select database {self.dbname}")

                # Set lock timeout and lock table
                self.db.execute_query(f"SET SESSION innodb_lock_wait_timeout = {self.lock_timeout}")
                lock_sql = f"LOCK TABLES `{self.tablename}` WRITE"
                success, _ = self.db.execute_query(lock_sql)
                if success:
                    self.locked = True
                    if self.logg:
                        self.logg.log_info(f"Successfully acquired WRITE lock on table {self.tablename}")
                else:
                    if self.logg:
                        self.logg.log_error(f"Failed to acquire lock on table {self.tablename}")
                    raise TableLockTimeoutException(f"Failed to acquire table lock on {self.tablename}")
            except Exception as e:
                raise TableLockTimeoutException(f"Unexpected error when locking {self.tablename}: {e}")

            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.locked:
                try:
                    self.db.execute_query("UNLOCK TABLES")
                    if self.logg:
                        self.logg.log_info(f"Released table lock on {self.tablename}")
                except Exception as e:
                    if self.logg:
                        self.logg.log_error(f"Error releasing table lock: {e}")

    # Process each test station
    for test_station, ts_info in sku_info['test_station'].items():
        tablename = ts_info['tablename']
        try:
            with TableLockManager(db, dbname, tablename, lock_timeout=5, logger=logg):
                process_test_station_with_cleanup(
                    db, dbname, test_station, ts_info, 
                    default_recently_days, default_enable, 
                    logg, exception_db, exception_table
                )
        except TableLockTimeoutException:
            logg.log_warn(f"Skipped table <{tablename}> due to lock timeout")
            continue
        
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
            dbpath = 'db_setting/db_setting.json'
        dbsetting = load_dbsetting(dbpath)
        default_recently_days = dbsetting.get('default_recently_days', 7)
        default_enable = dbsetting.get('default_enable', True)

        db = MySQLHelper(dbsetting)
        db.connect()

        # Setup exception DB and tables
        exception_db = MySQLHelper(dbsetting)
        exception_db.connect()
        create_exception_db(exception_db, dbsetting)
        
        for sku_title, sku_info in dbsetting['sku_parser_rule'].items():
            process_sku_with_comprehensive_cleanup(
                db, sku_title, sku_info, default_recently_days, 
                default_enable, logg, exception_db
            )

        exception_db.close()
        db.close()
        logg.log_ok("All tasks completed successfully.")

    except Exception as e:
        logg.log_error(f"Fatal error: {e}")


if __name__ == '__main__':
    main()