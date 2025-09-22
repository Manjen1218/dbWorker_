#!/usr/bin/env python3
"""
FTP Log Database Checker - High Performance Version
移除所有緩存，強制實時查詢，大幅提升處理速度
"""

import os
import glob
import json
import pymysql
from datetime import datetime, timedelta
import re
import sys
import subprocess
from typing import List, Dict, Tuple, Optional, Set, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import argparse
from pathlib import Path
import time
import signal
import gc
import psutil

# Global lock for thread-safe output
print_lock = threading.Lock()
shutdown_flag = threading.Event()

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully"""
    print("\nReceived interrupt signal, shutting down gracefully...")
    shutdown_flag.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def safe_print(message):
    """Thread-safe print function with memory monitoring"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    try:
        memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
        with print_lock:
            print(f"[{timestamp}|{memory_mb:.0f}MB] {message}")
            sys.stdout.flush()
    except:
        with print_lock:
            print(f"[{timestamp}] {message}")
            sys.stdout.flush()

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration file"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        safe_print(f"Error: Configuration file not found: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        safe_print(f"Error: Invalid configuration file format: {e}")
        sys.exit(1)

class DatabaseHelper:
    """Database connection helper with high performance optimization"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = {
            'host': db_config['db_host'],
            'port': db_config['db_port'],
            'user': db_config['db_user'],
            'password': db_config['db_pwd'],
            'connect_timeout': 10,  # 減少連接超時
            'read_timeout': 300,    # 增加讀取超時以支持大查詢
            'write_timeout': 300,
            'charset': 'utf8mb4',
            'autocommit': True
        }
        self.connection_lock = threading.Lock()
    
    def create_connection(self) -> pymysql.Connection:
        """Create database connection with aggressive optimization"""
        max_retries = 2  # 減少重試次數
        for attempt in range(max_retries):
            try:
                conn = pymysql.connect(**self.db_config)
                with conn.cursor() as cursor:
                    # 高性能優化設置
                    cursor.execute("SET SESSION wait_timeout = 28800")  # 8小時
                    cursor.execute("SET SESSION interactive_timeout = 28800")
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 60")
                    cursor.execute("SET SESSION query_cache_type = OFF")  # 關閉查詢緩存
                    cursor.execute("SET SESSION sql_mode = ''")  # 寬鬆模式
                    
                    # 檢查並設置 max_execution_time
                    try:
                        cursor.execute("SELECT VERSION()")
                        version = cursor.fetchone()[0]
                        if self._check_mysql_version_supports_max_execution_time(version):
                            cursor.execute("SET SESSION max_execution_time = 600000")  # 10分鐘
                    except Exception:
                        pass
                            
                return conn
            except Exception as e:
                if attempt < max_retries - 1:
                    safe_print(f"Database connection attempt {attempt + 1} failed, retrying...")
                    time.sleep(0.5)  # 減少等待時間
                else:
                    raise e
    
    def _check_mysql_version_supports_max_execution_time(self, version: str) -> bool:
        """Check if MySQL version supports max_execution_time (5.7.8+)"""
        try:
            import re
            version_match = re.match(r'(\d+)\.(\d+)\.(\d+)', version)
            if not version_match:
                return False
            
            major, minor, patch = map(int, version_match.groups())
            
            if major > 5:
                return True
            elif major == 5 and minor > 7:
                return True
            elif major == 5 and minor == 7 and patch >= 8:
                return True
            else:
                return False
        except Exception:
            return False
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            conn = self.create_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.close()
            return True
        except Exception as e:
            safe_print(f"Database connection test failed: {e}")
            return False

class FTPLogChecker:
    """FTP Log Database checker main class - High Performance Version"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db_helper = DatabaseHelper(config)
        
        # 移除所有緩存和臨時文件機制
        self.missing_files = []
        self.exception_files = []
        self.missing_lock = threading.Lock()
        self.exception_lock = threading.Lock()
        
        # 移除表緩存 - 強制每次實時查詢
        # self.table_cache = {}  # 完全移除
        self.null_or_empty_records = []
        self.null_or_empty_lock = threading.Lock()
        
        # Processing statistics
        self.processed_dates = 0
        self.total_dates = 0
        self.stats_lock = threading.Lock()
        
        # Configuration parameters
        self.days_back = config.get('default_recently_days', 7)
        self.enabled = config.get('default_enable', True)
        self.path_replace_org = config.get('info_path_replace_org', '')
        self.path_replace_dst = config.get('info_path_replace_dst', '')
        
        # 大幅提升批次大小和性能設置
        self.batch_size = 5000       # 從 1000 提升到 5000
        self.max_workers = 32        # 大幅增加線程數
        self.memory_check_interval = 1000  # 減少內存檢查頻率
        
    def get_current_date_linux(self) -> str:
        """Get current date from Linux system, formatted as YYYYMMDD"""
        try:
            result = subprocess.run(['date', '+%Y%m%d'], 
                                  capture_output=True, text=True, check=True, timeout=5)
            return result.stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return datetime.now().strftime('%Y%m%d')
    
    def check_table_exists(self, connection, db_name: str, table_name: str) -> bool:
        """強制實時檢查表是否存在 - 完全移除緩存機制"""
        try:
            with connection.cursor() as cursor:
                # 直接查詢，不使用任何緩存
                cursor.execute("SHOW TABLES FROM `{}` LIKE %s".format(db_name), (table_name,))
                exists = cursor.fetchone() is not None
                safe_print(f"Real-time table check: {db_name}.{table_name} = {'EXISTS' if exists else 'NOT EXISTS'}")
                return exists
        except Exception as e:
            safe_print(f"Error checking table existence for {db_name}.{table_name}: {e}")
            return False
    
    def get_sorted_dates(self, path: str) -> List[str]:
        """Get all date folders under specified path and sort them"""
        if not os.path.exists(path):
            return []
        
        current_date = self.get_current_date_linux()
        current_datetime = datetime.strptime(current_date, '%Y%m%d')
        cutoff_date = current_datetime - timedelta(days=self.days_back)
        cutoff_date_str = cutoff_date.strftime('%Y%m%d')
        
        dates = []
        try:
            for item in os.listdir(path):
                if shutdown_flag.is_set():
                    break
                item_path = os.path.join(path, item)
                if os.path.isdir(item_path) and re.match(r'^\d{8}$', item):
                    if item >= cutoff_date_str:
                        dates.append(item)
        except OSError as e:
            safe_print(f"Error reading directory {path}: {e}")
            return []
        
        if not dates:
            return []
        
        dates.sort(reverse=True)
        
        # Skip today's data
        if dates and dates[0] == current_date:
            dates = dates[1:]
        
        return dates
    
    def apply_path_replacement(self, path: str) -> str:
        """Apply path replacement rules"""
        if self.path_replace_org and self.path_replace_dst:
            return path.replace(self.path_replace_org, self.path_replace_dst)
        return path
    
    def check_exception_db(self, connection, exception_table: str, file_paths: List[str]) -> Set[str]:
        """高性能檢查異常數據庫"""
        if not file_paths or shutdown_flag.is_set():
            return set()
        
        # 強制實時檢查異常數據庫表是否存在
        try:
            connection.ping(reconnect=True)
            if not self.check_table_exists(connection, 'exception', exception_table):
                safe_print(f"Exception table exception.{exception_table} does not exist")
                return set()
        except Exception as e:
            safe_print(f"Failed to check exception database: {e}")
            return set()
        
        found_in_exception = set()
        
        try:
            # 使用更大的批次大小處理異常數據庫
            large_batch_size = 2000  # 增加批次大小
            
            for i in range(0, len(file_paths), large_batch_size):
                if shutdown_flag.is_set():
                    break
                    
                batch = file_paths[i:i + large_batch_size]
                
                try:
                    connection.ping(reconnect=True)
                    with connection.cursor() as cursor:
                        batch_replaced = [self.apply_path_replacement(f) for f in batch]
                        placeholders = ','.join(['%s'] * len(batch_replaced))
                        query = f"SELECT fullpath FROM `exception`.`{exception_table}` WHERE fullpath IN ({placeholders})"
                        
                        start_time = time.time()
                        cursor.execute(query, batch_replaced)
                        query_time = time.time() - start_time
                        
                        found_replaced = {row[0] for row in cursor.fetchall()}
                        for orig, replaced in zip(batch, batch_replaced):
                            if replaced in found_replaced:
                                found_in_exception.add(orig)
                        
                        safe_print(f"Exception DB batch {i//large_batch_size + 1}: {len(found_replaced)} found in {query_time:.2f}s")
                        
                except Exception as e:
                    safe_print(f"Exception DB query error (exception.{exception_table}): {e}")
                    # 不要重試，直接繼續下一個批次
                    continue
                            
        except Exception as e:
            safe_print(f"Exception DB query error (exception.{exception_table}): {e}")
        
        return found_in_exception
    
    def process_files_batch(self, connection, db_name: str, table_name: str, 
                           file_batch: List[str]) -> Tuple[Set[str], int]:
        """高性能批次處理文件"""
        found_files = set()
        null_empty_count = 0
        
        try:
            connection.ping(reconnect=True)
            
            with connection.cursor() as cursor:
                batch_replaced = [self.apply_path_replacement(f) for f in file_batch]
                placeholders = ','.join(['%s'] * len(batch_replaced))
                query = f"""
                    SELECT fullpath, wo, jig, sn, tbeg, tend, is_y
                    FROM `{db_name}`.`{table_name}`
                    WHERE fullpath IN ({placeholders})
                """
                
                start_time = time.time()
                cursor.execute(query, batch_replaced)
                query_time = time.time() - start_time
                
                result_rows = cursor.fetchall()
                
                for row in result_rows:
                    fullpath, wo, jig, sn, tbeg, tend, is_y = row
                    found_files.add(fullpath)
                    
                    # Skip NULL/empty check if is_y is False
                    if is_y is False:
                        continue
                    
                    # Check for NULL or empty fields only when is_y is not False
                    if any(x is None or str(x).strip() == '' for x in [wo, jig, sn, tbeg, tend]):
                        missing_fields = []
                        if wo is None or str(wo).strip() == '':
                            missing_fields.append('wo')
                        if jig is None or str(jig).strip() == '':
                            missing_fields.append('jig')
                        if sn is None or str(sn).strip() == '':
                            missing_fields.append('sn')
                        if tbeg is None or str(tbeg).strip() == '':
                            missing_fields.append('tbeg')
                        if tend is None or str(tend).strip() == '':
                            missing_fields.append('tend')
                        
                        if missing_fields:
                            with self.null_or_empty_lock:
                                self.null_or_empty_records.append((fullpath, missing_fields, db_name))
                            null_empty_count += 1
                
                safe_print(f"Batch query {db_name}.{table_name}: {len(result_rows)} found in {query_time:.2f}s")
                
        except Exception as e:
            safe_print(f"Batch query error ({db_name}.{table_name}): {e}")
        
        return found_files, null_empty_count
    
    def process_date_batch(self, date_info: Tuple[str, str, str, str, str]) -> Dict:
        """高性能處理單個日期批次"""
        if shutdown_flag.is_set():
            return {'date': date_info[3], 'total': 0, 'pass': 0, 'fail': 0, 'exception': 0, 
                   'missing': [], 'in_exception': [], 'null_or_empty': 0}
        
        date_path, db_name, table_name, date, sku_filter = date_info
        
        # Progress tracking
        with self.stats_lock:
            self.processed_dates += 1
            progress = f"[{self.processed_dates}/{self.total_dates}]"
        
        safe_print(f"{progress} Processing date: {date}")
        
        connection = None
        try:
            connection = self.db_helper.create_connection()
            
            # 快速收集所有 .cap 文件
            all_cap_files = []
            for status in ['PASS', 'FAIL']:
                if shutdown_flag.is_set():
                    break
                status_path = os.path.join(date_path, status)
                if os.path.exists(status_path):
                    try:
                        cap_files = glob.glob(os.path.join(status_path, '*.cap'))
                        if sku_filter:
                            cap_files = [f for f in cap_files if sku_filter not in f]
                        all_cap_files.extend(cap_files)
                    except OSError as e:
                        safe_print(f"Error reading CAP files from {status_path}: {e}")
                        continue
            
            if not all_cap_files or shutdown_flag.is_set():
                return {'date': date, 'total': 0, 'pass': 0, 'fail': 0, 'exception': 0, 
                       'missing': [], 'in_exception': [], 'null_or_empty': 0}
            
            total_files = len(all_cap_files)
            safe_print(f"{progress} Date {date}: Found {total_files} CAP files")
            
            # 強制實時檢查表是否存在
            if not self.check_table_exists(connection, db_name, table_name):
                safe_print(f"{progress} Table {db_name}.{table_name} does not exist, checking exception DB")
                exception_files = self.check_exception_db(connection, db_name, all_cap_files)
                missing = [f for f in all_cap_files if f not in exception_files]
                
                with self.missing_lock:
                    self.missing_files.extend(missing)
                with self.exception_lock:
                    self.exception_files.extend(list(exception_files))
                
                return {
                    'date': date,
                    'total': total_files,
                    'pass': 0,
                    'fail': len(missing),
                    'exception': len(exception_files),
                    'missing': missing,
                    'in_exception': list(exception_files),
                    'null_or_empty': 0
                }
            
            # 高速處理文件批次
            all_found_files = set()
            total_null_empty = 0
            processed_count = 0
            
            for i in range(0, total_files, self.batch_size):
                if shutdown_flag.is_set():
                    break
                
                batch = all_cap_files[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                total_batches = (total_files + self.batch_size - 1) // self.batch_size
                
                found_files, null_empty_count = self.process_files_batch(
                    connection, db_name, table_name, batch
                )
                
                all_found_files.update(found_files)
                total_null_empty += null_empty_count
                processed_count += len(batch)
                
                # 顯示處理進度
                if total_files > 1000:
                    safe_print(f"{progress} Date {date}: Batch {batch_num}/{total_batches} - Found {len(found_files)}")
            
            # 檢查未在主數據庫中找到的文件
            not_in_main_db = [f for f in all_cap_files if f not in all_found_files]
            exception_files = set()
            
            if not_in_main_db and not shutdown_flag.is_set():
                safe_print(f"{progress} Date {date}: Checking {len(not_in_main_db)} files in exception DB")
                exception_files = self.check_exception_db(connection, db_name, not_in_main_db)
            
            missing = [f for f in not_in_main_db if f not in exception_files]
            
            # 存儲結果
            if missing:
                with self.missing_lock:
                    self.missing_files.extend(missing)
            
            if exception_files:
                with self.exception_lock:
                    self.exception_files.extend(list(exception_files))
            
            result = {
                'date': date,
                'total': total_files,
                'pass': len(all_found_files),
                'fail': len(missing),
                'exception': len(exception_files),
                'missing': missing,
                'in_exception': list(exception_files),
                'null_or_empty': total_null_empty
            }
            
            safe_print(f"{progress} Completed date {date}: Found {result['pass']}, Exception {result['exception']}, Missing {result['fail']}")
            return result
            
        except Exception as e:
            safe_print(f"Error processing date {date}: {e}")
            return {'date': date, 'total': 0, 'pass': 0, 'fail': 0, 'exception': 0, 
                   'missing': [], 'in_exception': [], 'null_or_empty': 0}
        finally:
            if connection:
                try:
                    connection.close()
                except:
                    pass
    
    def process_test_station(self, sku_title: str, sku_info: Dict, 
                           station_name: str, station_info: Dict) -> Tuple[int, int]:
        """高性能處理單個測試站"""
        if shutdown_flag.is_set():
            return 0, 0
            
        db_name = sku_info['dbname']
        table_name = station_info['tablename']
        folder = station_info['folder']
        
        if not os.path.exists(folder):
            safe_print(f"\n  Warning: Test station {station_name} folder does not exist: {folder}")
            return 0, 0
        
        safe_print(f"\n  Checking test station: {station_name} -> Table: {table_name}")
        
        # Get sorted dates
        dates = self.get_sorted_dates(folder)
        
        if not dates:
            safe_print(f"  Warning: No processable date folders found in the last {self.days_back} days")
            return 0, 0
        
        # Set total dates for progress tracking
        with self.stats_lock:
            self.total_dates = len(dates)
            self.processed_dates = 0
        
        safe_print(f"  Found {len(dates)} date folders (last {self.days_back} days, excluding today)")
        
        # Special filtering rules
        sku_filter = self.get_sku_filter(sku_title)
        
        # Prepare date information
        date_infos = [
            (os.path.join(folder, date), db_name, table_name, date, sku_filter)
            for date in dates
        ]
        
        # 使用最大線程數來提升速度
        station_missing = 0
        station_exception = 0
        
        # 根據數據集大小調整線程數，但更激進
        if 'JRD03' in sku_title.upper():
            max_workers = min(self.max_workers, 24)  # JRD03R 使用 24 線程
            safe_print(f"  Using {max_workers} threads for large dataset {sku_title}")
        else:
            max_workers = min(self.max_workers, len(dates), 16)  # 其他使用 16 線程
            safe_print(f"  Using {max_workers} threads for {len(dates)} dates")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {}
            
            # Submit tasks
            for date_info in date_infos:
                if shutdown_flag.is_set():
                    break
                future = executor.submit(self.process_date_batch, date_info)
                future_to_date[future] = date_info[3]
            
            # Process results with extended timeout
            completed = 0
            for future in as_completed(future_to_date, timeout=3600):  # 1小時超時
                if shutdown_flag.is_set():
                    break
                    
                completed += 1
                date = future_to_date[future]
                
                try:
                    result = future.result(timeout=1200)  # 20分鐘每個任務
                    
                    if result['exception'] > 0:
                        safe_print(f"  Date {result['date']}: Found {result['exception']} files in exception DB")
                    
                    if result['fail'] > 0:
                        safe_print(f"  WARNING: Date {result['date']}: {result['fail']} files not recorded in any database!")
                        # 只顯示少數缺失文件示例
                        show_count = min(2, len(result['missing']))
                        for missing_file in result['missing'][:show_count]:
                            filename = os.path.basename(missing_file)
                            safe_print(f"    Missing: {filename}")
                        
                        if len(result['missing']) > show_count:
                            safe_print(f"    ... and {len(result['missing']) - show_count} more missing files")
                    
                    safe_print(f"  Statistics: Found {result['pass']} / Exception {result['exception']} / Missing {result['fail']} / Total {result['total']}")
                    
                    station_missing += result['fail']
                    station_exception += result['exception']
                    
                except Exception as e:
                    safe_print(f"  Error processing date {date}: {e}")
        
        if shutdown_flag.is_set():
            safe_print("  Detected interrupt signal, stopping processing")
            
        safe_print(f"\n  {station_name} summary:")
        safe_print(f"    - Files in exception DB (recorded): {station_exception}")
        safe_print(f"    - Files missing (NOT RECORDED): {station_missing}")
        return station_missing, station_exception
    
    def get_sku_filter(self, sku_title: str) -> Optional[str]:
        """Get special filtering rules based on SKU"""
        if 'JRD03_R' in sku_title:
            return 'JRD03-C'
        return None
    
    def process_sku(self, sku_title: str, sku_info: Dict) -> Tuple[int, int]:
        """Process a single SKU"""
        if shutdown_flag.is_set():
            return 0, 0
            
        safe_print(f"\n{'='*80}")
        safe_print(f"Processing SKU: {sku_title}")
        safe_print(f"Database: {sku_info['dbname']}")
        safe_print(f"Exception table: exception.{sku_info['dbname']}")
        
        total_missing = 0
        total_exception = 0
        
        # Process each test station
        for station_name, station_info in sku_info.get('test_station', {}).items():
            if shutdown_flag.is_set():
                break
            station_missing, station_exception = self.process_test_station(
                sku_title, sku_info, station_name, station_info
            )
            total_missing += station_missing
            total_exception += station_exception
        
        safe_print(f"\n{sku_title} summary across all test stations:")
        safe_print(f"  - Files in exception DB (recorded but failed): {total_exception}")
        safe_print(f"  - Files NOT RECORDED anywhere: {total_missing}")
        return total_missing, total_exception
    
    def run(self, selected_skus: Optional[List[str]] = None):
        """Execute main checking process with maximum performance"""
        safe_print("=== FTP Log Database Checker (HIGH PERFORMANCE VERSION) ===")
        safe_print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        safe_print(f"Check range: Last {self.days_back} days of data (automatically skip today)")
        safe_print(f"Performance settings: Batch={self.batch_size}, MaxWorkers={self.max_workers}")
        safe_print("*** CACHE DISABLED - ALL QUERIES ARE REAL-TIME ***")
        
        try:
            # Test database connection
            if not self.db_helper.test_connection():
                return
            
            safe_print("Successfully connected to database")
            
            # Get list of SKUs to process
            sku_parser_rule = self.config.get('sku_parser_rule', {})
            
            if selected_skus:
                skus_to_process = {
                    k: v for k, v in sku_parser_rule.items() 
                    if k in selected_skus
                }
            else:
                skus_to_process = sku_parser_rule
            
            if not skus_to_process:
                safe_print("Error: No SKUs found to process")
                return
            
            safe_print(f"\nFound {len(skus_to_process)} SKUs to process:")
            for i, sku_title in enumerate(skus_to_process.keys(), 1):
                safe_print(f"  {i:2d}. {sku_title}")
            
            # Process each SKU
            total_skus = len(skus_to_process)
            total_missing_all = 0
            total_exception_all = 0
            
            for i, (sku_title, sku_info) in enumerate(skus_to_process.items(), 1):
                if shutdown_flag.is_set():
                    safe_print("Detected interrupt signal, stopping SKU processing")
                    break
                    
                safe_print(f"\nProcessing SKU [{i}/{total_skus}]: {sku_title}")
                
                missing, exception = self.process_sku(sku_title, sku_info)
                total_missing_all += missing
                total_exception_all += exception
            
            # Output summary
            safe_print("\n" + "="*80)
            if shutdown_flag.is_set():
                safe_print("All checks interrupted!")
            else:
                safe_print("All checks completed!")
            
            safe_print(f"Processed {i} of {total_skus} SKUs")
            safe_print(f"Check range: Last {self.days_back} days")
            safe_print(f"\nSummary:")
            safe_print(f"  - Total files in exception DB (parse/insert failed): {len(self.exception_files)}")
            safe_print(f"  - Total files missing (not recorded anywhere): {len(self.missing_files)}")
            
            # Write reports
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Write missing files report
            if self.missing_files:
                output_file = f"missing_files_{self.days_back}days_{timestamp}.txt"
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write("Missing Files Report (Not recorded in any database)\n")
                    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Check range: Last {self.days_back} days\n")
                    f.write(f"Processed SKUs: {i}\n")
                    if shutdown_flag.is_set():
                        f.write("Note: Check was interrupted, results may be incomplete\n")
                    f.write("="*60 + "\n\n")
                    
                    for file in sorted(self.missing_files):
                        f.write(f"fullpath: {file}\n")
                
                safe_print(f"\nMissing files report saved to: {output_file}")
                safe_print(f"  (Contains {len(self.missing_files)} files not recorded in any database)")
            else:
                safe_print(f"\nNo missing files found - all files are properly recorded!")
            
            # Write NULL/empty fields report with database information
            if self.null_or_empty_records:
                null_report = f"null_or_empty_{self.days_back}days_{timestamp}.txt"
                with open(null_report, 'w', encoding='utf-8') as f:
                    f.write("Files with NULL or empty critical fields\n")
                    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Check range: Last {self.days_back} days\n")
                    f.write(f"Processed SKUs: {i}\n")
                    if shutdown_flag.is_set():
                        f.write("Note: Check was interrupted, results may be incomplete\n")
                    f.write("="*60 + "\n\n")
                    
                    for file, fields, db_location in sorted(self.null_or_empty_records):
                        f.write(f"fullpath: {file}\n")
                        f.write(f"Missing fields: {', '.join(fields)}\n")
                        f.write(f"Database location: {db_location}\n\n") 
                
                safe_print(f"\nNULL or empty fields report saved to: {null_report}")
                safe_print(f"  (Contains {len(self.null_or_empty_records)} files with missing critical fields)")
            else:
                safe_print(f"\nNo files found with NULL or empty critical fields.")

        except Exception as e:
            safe_print(f"\nError occurred during execution: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            safe_print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """Main program entry point"""
    parser = argparse.ArgumentParser(
        description='FTP Log Database Checker - High Performance Version (Cache Disabled)'
    )
    parser.add_argument(
        '-c', '--config',
        default='db_setting/db_setting.json',
        help='Configuration file path (default: db_setting/db_setting.json)'
    )
    parser.add_argument(
        '-d', '--days',
        type=int,
        help='Number of days to check (overrides configuration file setting)'
    )
    parser.add_argument(
        '-s', '--sku',
        nargs='+',
        help='Specify SKUs to process (multiple allowed, e.g.: -s K2v5_JRD01_R K2v5_JRD02_R)'
    )
    parser.add_argument(
        '--list-skus',
        action='store_true',
        help='List all available SKUs'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=5000,
        help='Batch size for database queries (default: 5000, larger = faster)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=32,
        help='Maximum number of worker threads (default: 32, higher = faster)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # If only listing SKUs
        if args.list_skus:
            safe_print("\nAvailable SKUs:")
            for i, sku in enumerate(config.get('sku_parser_rule', {}).keys(), 1):
                safe_print(f"  {i:2d}. {sku}")
            return
        
        # Override days setting
        if args.days:
            config['default_recently_days'] = args.days
            safe_print(f"Using command-line specified days: {args.days}")
        
        # Show high performance settings
        safe_print(f"HIGH PERFORMANCE MODE ENABLED:")
        safe_print(f"  - Batch size: {args.batch_size}")
        safe_print(f"  - Max workers: {args.max_workers}")
        safe_print(f"  - All caching disabled - real-time queries only")
        safe_print(f"  - Aggressive threading enabled")
        safe_print(f"  - Memory optimization reduced for speed")
        
        # Create checker and run
        checker = FTPLogChecker(config)
        checker.batch_size = args.batch_size
        checker.max_workers = args.max_workers
        checker.run(selected_skus=args.sku)
        
    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user")
    except Exception as e:
        print(f"\nProgram execution error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()