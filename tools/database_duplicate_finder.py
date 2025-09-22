#!/usr/bin/env python3
"""
Database Duplicate Finder - Simplified version for fullpath duplicates within tables
Memory efficient design for large datasets
"""

import os
import json
import pymysql
from datetime import datetime
import sys
import time
import signal
import threading
from typing import List, Dict, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import psutil
import gc

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
    """Database connection helper"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = {
            'host': db_config['db_host'],
            'port': db_config['db_port'],
            'user': db_config['db_user'],
            'password': db_config['db_pwd'],
            'connect_timeout': 30,
            'read_timeout': 300,
            'write_timeout': 300,
            'charset': 'utf8mb4',
            'autocommit': True
        }
        self.connection_lock = threading.Lock()
    
    def create_connection(self) -> pymysql.Connection:
        """Create database connection with retry mechanism"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.connection_lock:
                    conn = pymysql.connect(**self.db_config)
                    with conn.cursor() as cursor:
                        # Optimize connection for large queries
                        cursor.execute("SET SESSION wait_timeout = 600")
                        cursor.execute("SET SESSION interactive_timeout = 600")
                        cursor.execute("SET SESSION group_concat_max_len = 100000")
                return conn
            except Exception as e:
                if attempt < max_retries - 1:
                    safe_print(f"Database connection attempt {attempt + 1} failed, retrying...")
                    time.sleep(2 ** attempt)
                else:
                    raise e
    
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

class DuplicateFinder:
    """Simplified duplicate finder - checks fullpath duplicates within each table"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db_helper = DatabaseHelper(config)
        
        # Results storage
        self.results = []
        self.total_duplicates = 0
        self.total_records = 0
        self.stats_lock = threading.Lock()
        
        # Performance settings
        self.batch_size = 5000  # Records per batch for GROUP BY query
        
    def check_memory_usage(self) -> bool:
        """Check if memory usage is within acceptable limits"""
        try:
            process = psutil.Process()
            memory_percent = process.memory_percent()
            if memory_percent > 80:
                safe_print(f"Warning: High memory usage: {memory_percent:.1f}%")
                gc.collect()
                return False
            return True
        except:
            return True
    
    def check_table_exists(self, connection, db_name: str, table_name: str) -> bool:
        """Check if table exists"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SHOW TABLES FROM `{db_name}` LIKE %s", (table_name,))
                return cursor.fetchone() is not None
        except Exception as e:
            safe_print(f"Error checking table existence: {e}")
            return False
    
    def find_duplicates_in_table(self, db_name: str, table_name: str) -> Dict:
        """Find all fullpath duplicates in a single table using GROUP BY"""
        safe_print(f"\nChecking table: {db_name}.{table_name}")
        
        connection = None
        try:
            connection = self.db_helper.create_connection()
            
            # Check if table exists
            if not self.check_table_exists(connection, db_name, table_name):
                safe_print(f"  Table does not exist, skipping")
                return {
                    'db': db_name,
                    'table': table_name,
                    'total_records': 0,
                    'duplicate_groups': 0,
                    'duplicate_records': 0,
                    'duplicates': []
                }
            
            with connection.cursor() as cursor:
                # First get total record count
                cursor.execute(f"SELECT COUNT(*) FROM `{db_name}`.`{table_name}`")
                total_records = cursor.fetchone()[0]
                safe_print(f"  Total records: {total_records:,}")
                
                if total_records == 0:
                    return {
                        'db': db_name,
                        'table': table_name,
                        'total_records': 0,
                        'duplicate_groups': 0,
                        'duplicate_records': 0,
                        'duplicates': []
                    }
                
                # Find duplicates using GROUP BY
                # This query finds all fullpaths that appear more than once
                query = f"""
                    SELECT fullpath, COUNT(*) as count
                    FROM `{db_name}`.`{table_name}`
                    GROUP BY fullpath
                    HAVING count > 1
                    ORDER BY count DESC, fullpath
                """
                
                safe_print(f"  Searching for duplicate fullpaths...")
                start_time = time.time()
                cursor.execute(query)
                query_time = time.time() - start_time
                safe_print(f"  Query completed in {query_time:.2f} seconds")
                
                # Process results
                duplicates = []
                duplicate_groups = 0
                total_duplicate_records = 0
                
                while True:
                    # Fetch in batches to avoid memory issues
                    rows = cursor.fetchmany(self.batch_size)
                    if not rows:
                        break
                    
                    for row in rows:
                        fullpath, count = row
                        duplicate_groups += 1
                        total_duplicate_records += count  # All records in group are duplicates
                        
                        duplicates.append({
                            'fullpath': fullpath,
                            'count': count
                        })
                    
                    # Check memory
                    if not self.check_memory_usage():
                        safe_print(f"  Memory optimization triggered, processed {duplicate_groups} groups so far")
                        time.sleep(1)
                
                # Update global statistics
                with self.stats_lock:
                    self.total_records += total_records
                    self.total_duplicates += total_duplicate_records
                
                safe_print(f"  Found {duplicate_groups} duplicate groups containing {total_duplicate_records} total records")
                
                return {
                    'db': db_name,
                    'table': table_name,
                    'total_records': total_records,
                    'duplicate_groups': duplicate_groups,
                    'duplicate_records': total_duplicate_records,
                    'duplicates': duplicates[:100]  # Limit stored details to prevent memory issues
                }
                
        except Exception as e:
            safe_print(f"Error processing table {db_name}.{table_name}: {e}")
            return {
                'db': db_name,
                'table': table_name,
                'total_records': 0,
                'duplicate_groups': 0,
                'duplicate_records': 0,
                'duplicates': [],
                'error': str(e)
            }
        finally:
            if connection:
                try:
                    connection.close()
                except:
                    pass
    
    def process_sku_tables(self, sku_title: str, sku_info: Dict) -> List[Dict]:
        """Process all tables for a single SKU"""
        safe_print(f"\n{'='*80}")
        safe_print(f"Processing SKU: {sku_title}")
        safe_print(f"Database: {sku_info['dbname']}")
        
        db_name = sku_info['dbname']
        sku_results = []
        
        # Process each test station (table)
        for station_name, station_info in sku_info.get('test_station', {}).items():
            if shutdown_flag.is_set():
                break
            
            table_name = station_info['tablename']
            safe_print(f"\nProcessing station: {station_name}")
            
            result = self.find_duplicates_in_table(db_name, table_name)
            result['sku'] = sku_title
            result['station'] = station_name
            sku_results.append(result)
        
        return sku_results
    
    def generate_report(self, output_file: str):
        """Generate final report"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("Database Fullpath Duplicate Report\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 100 + "\n\n")
            
            f.write(f"Total records checked: {self.total_records:,}\n")
            f.write(f"Total duplicate records found: {self.total_duplicates:,}\n\n")
            
            # Summary table
            f.write("Summary by Table:\n")
            f.write("-" * 100 + "\n")
            f.write(f"{'SKU':<20} {'Station':<20} {'Table':<30} {'Total Records':>15} {'Dup Groups':>12} {'Dup Records':>12}\n")
            f.write("-" * 100 + "\n")
            
            for result in self.results:
                if 'error' not in result:
                    f.write(f"{result['sku']:<20} {result['station']:<20} {result['table']:<30} "
                           f"{result['total_records']:>15,} {result['duplicate_groups']:>12,} "
                           f"{result['duplicate_records']:>12,}\n")
            
            # Detailed duplicates
            f.write("\n\nDetailed Duplicate Information (showing up to 100 per table):\n")
            f.write("=" * 100 + "\n")
            
            for result in self.results:
                if result['duplicate_groups'] > 0:
                    f.write(f"\nTable: {result['db']}.{result['table']} (SKU: {result['sku']}, Station: {result['station']})\n")
                    f.write("-" * 80 + "\n")
                    
                    for i, dup in enumerate(result['duplicates'], 1):
                        f.write(f"{i}. Fullpath: {dup['fullpath']}\n")
                        f.write(f"   Count: {dup['count']} duplicates\n\n")
                    
                    if result['duplicate_groups'] > len(result['duplicates']):
                        f.write(f"   ... and {result['duplicate_groups'] - len(result['duplicates'])} more duplicate groups\n\n")
    
    def run(self, selected_skus: Optional[List[str]] = None):
        """Execute duplicate finding process"""
        safe_print("=== Database Fullpath Duplicate Finder ===")
        safe_print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        safe_print("Checking for duplicate fullpaths within each table")
        
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
            
            safe_print(f"\nFound {len(skus_to_process)} SKUs to process")
            
            # Process each SKU with threading
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                
                for sku_title, sku_info in skus_to_process.items():
                    if shutdown_flag.is_set():
                        break
                    
                    future = executor.submit(self.process_sku_tables, sku_title, sku_info)
                    futures.append((future, sku_title))
                
                # Collect results
                for future, sku_title in futures:
                    if shutdown_flag.is_set():
                        break
                    
                    try:
                        sku_results = future.result(timeout=600)
                        self.results.extend(sku_results)
                    except Exception as e:
                        safe_print(f"Error processing SKU {sku_title}: {e}")
            
            # Generate report
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = f"fullpath_duplicates_{timestamp}.txt"
            self.generate_report(report_file)
            
            safe_print(f"\n{'='*80}")
            safe_print(f"Report saved to: {report_file}")
            safe_print(f"Total tables checked: {len(self.results)}")
            safe_print(f"Total records checked: {self.total_records:,}")
            safe_print(f"Total duplicate records found: {self.total_duplicates:,}")
            
        except Exception as e:
            safe_print(f"\nError occurred during execution: {e}")
            import traceback
            traceback.print_exc()
        finally:
            safe_print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Main program entry point"""
    parser = argparse.ArgumentParser(
        description='Database Fullpath Duplicate Finder - Find duplicate fullpaths within tables'
    )
    parser.add_argument(
        '-c', '--config',
        default='db_setting/db_setting.json',
        help='Configuration file path (default: db_setting/db_setting_one.json)'
    )
    parser.add_argument(
        '-s', '--sku',
        nargs='+',
        help='Specify SKUs to process (multiple allowed)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=5000,
        help='Records per batch when fetching results (default: 5000)'
    )
    parser.add_argument(
        '--list-skus',
        action='store_true',
        help='List all available SKUs'
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
        
        # Create finder and configure
        finder = DuplicateFinder(config)
        finder.batch_size = args.batch_size
        
        # Run duplicate detection
        finder.run(selected_skus=args.sku)
        
    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user")
    except Exception as e:
        print(f"\nProgram execution error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()