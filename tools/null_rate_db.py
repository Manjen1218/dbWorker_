#!/usr/bin/env python3
"""
Database Field Non-NULL Rate Checker - Enhanced Version
Checks non-NULL rates of database fields where fullpath contains "PASS"
Supports time range filtering and time segmentation analysis
"""

import os
import json
import pymysql
from datetime import datetime, timedelta
import sys
import argparse
from typing import Dict, Any, List, Tuple
import time
import threading
import math

# Global lock for thread-safe output
print_lock = threading.Lock()

def safe_print(message):
    """Thread-safe print function"""
    timestamp = datetime.now().strftime('%H:%M:%S')
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
            'port': db_config.get('db_port', 3306),
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
        """Create database connection"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.connection_lock:
                    conn = pymysql.connect(**self.db_config)
                    with conn.cursor() as cursor:
                        # Optimize connection settings
                        cursor.execute("SET SESSION wait_timeout = 1800")
                        cursor.execute("SET SESSION interactive_timeout = 1800")
                        cursor.execute("SET SESSION innodb_lock_wait_timeout = 120")
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

class FieldNullRateChecker:
    """Field non-NULL rate checker"""
    
    def __init__(self, config: Dict[str, Any], cut_days: int = None):
        self.config = config
        self.db_helper = DatabaseHelper(config)
        self.database = config['database']
        self.table = config['table']
        self.date_field = config.get('date_field', 'tbeg')  # Default to tbeg
        self.recent_days = config.get('recent_days', 100)  # Use recent_days from config
        self.cut_days = cut_days  # Time segmentation parameter
    
    def get_table_columns(self, connection) -> List[str]:
        """Get all column names from the table"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DESCRIBE `{self.database}`.`{self.table}`")
                columns = [row[0] for row in cursor.fetchall()]
                return columns
        except Exception as e:
            safe_print(f"Failed to get table structure: {e}")
            return []
    
    def get_time_range(self, connection) -> Tuple[datetime, datetime]:
        """Get the actual time range of data"""
        try:
            with connection.cursor() as cursor:
                # Get the time range for PASS records within recent_days
                now = datetime.now()
                start_date = now - timedelta(days=self.recent_days)
                
                query = f"""
                    SELECT 
                        MIN(`{self.date_field}`) as min_time,
                        MAX(`{self.date_field}`) as max_time,
                        COUNT(*) as total_count
                    FROM `{self.database}`.`{self.table}` 
                    WHERE fullpath LIKE %s 
                    AND `{self.date_field}` >= %s
                """
                
                cursor.execute(query, ('%PASS%', start_date))
                result = cursor.fetchone()
                
                if result and result[0] and result[1]:
                    min_time, max_time, total_count = result
                    safe_print(f"Data time range: {min_time} to {max_time}")
                    safe_print(f"Total qualifying records in recent {self.recent_days} days: {total_count:,}")
                    return min_time, max_time
                else:
                    safe_print("No qualifying records found in the specified time range")
                    return None, None
                    
        except Exception as e:
            safe_print(f"Failed to get time range: {e}")
            return None, None
    
    def generate_time_segments(self, start_time: datetime, end_time: datetime) -> List[Tuple[datetime, datetime, str]]:
        """Generate time segments based on cut_days"""
        if not self.cut_days:
            return [(start_time, end_time, "Full Range")]
        
        segments = []
        current_start = start_time
        segment_num = 1
        
        while current_start < end_time:
            current_end = min(current_start + timedelta(days=self.cut_days), end_time)
            
            # Format segment name
            segment_name = f"Segment {segment_num} ({current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')})"
            segments.append((current_start, current_end, segment_name))
            
            current_start = current_end
            segment_num += 1
        
        safe_print(f"Generated {len(segments)} time segments (每 {self.cut_days} 天)")
        return segments
    
    def show_progress_bar(self, current: int, total: int, prefix: str = "", bar_length: int = 40):
        """Display a progress bar"""
        filled_length = int(bar_length * current / total)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        percentage = 100 * current / total
        
        with print_lock:
            print(f"\r{prefix} [{bar}] {percentage:.1f}% ({current}/{total})", end='', flush=True)
            if current == total:
                print()  # New line when complete

    def check_field_null_rates_for_segment(self, connection, columns: List[str], 
                                         start_time: datetime, end_time: datetime, 
                                         segment_name: str, segment_num: int, total_segments: int) -> Dict[str, Dict]:
        """Check non-NULL rates for each field in a specific time segment"""
        
        safe_print(f"\n[{segment_num}/{total_segments}] Analyzing {segment_name}")
        safe_print(f"Time range: {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        results = {}
        
        # First get total record count for this segment
        try:
            with connection.cursor() as cursor:
                total_query = f"""
                    SELECT COUNT(*) 
                    FROM `{self.database}`.`{self.table}` 
                    WHERE fullpath LIKE %s
                    AND `{self.date_field}` >= %s
                    AND `{self.date_field}` < %s
                """
                cursor.execute(total_query, ('%PASS%', start_time, end_time))
                total_records = cursor.fetchone()[0]
                
                safe_print(f"Records in this segment: {total_records:,}")
                
                if total_records == 0:
                    safe_print("Warning: No qualifying records found in this segment")
                    return results
                
        except Exception as e:
            safe_print(f"Failed to get total record count for segment: {e}")
            return results
        
        # Filter out system fields
        check_columns = [col for col in columns if col not in ['id', 'create_time', 'update_time']]
        total_fields = len(check_columns)
        
        safe_print(f"Checking {total_fields} fields...")
        
        # Check non-NULL rate for each field with progress bar
        field_count = 0
        for column in check_columns:
            field_count += 1
            
            # Show progress bar
            self.show_progress_bar(field_count, total_fields, 
                                 f"  Segment {segment_num}/{total_segments} Fields", 30)
            
            try:
                with connection.cursor() as cursor:
                    # Count non-NULL values for this field in this time segment
                    non_null_query = f"""
                        SELECT 
                            COUNT(*) as total_count,
                            COUNT(`{column}`) as non_null_count,
                            COUNT(*) - COUNT(`{column}`) as null_count
                        FROM `{self.database}`.`{self.table}` 
                        WHERE fullpath LIKE %s
                        AND `{self.date_field}` >= %s
                        AND `{self.date_field}` < %s
                    """
                    
                    cursor.execute(non_null_query, ('%PASS%', start_time, end_time))
                    result = cursor.fetchone()
                    
                    if result:
                        total_count, non_null_count, null_count = result
                        non_null_rate = (non_null_count / total_count * 100) if total_count > 0 else 0
                        null_rate = (null_count / total_count * 100) if total_count > 0 else 0
                        
                        results[column] = {
                            'total_records': total_count,
                            'non_null_count': non_null_count,
                            'null_count': null_count,
                            'non_null_rate': round(non_null_rate, 2),
                            'null_rate': round(null_rate, 2),
                            'segment_name': segment_name,
                            'start_time': start_time,
                            'end_time': end_time
                        }
                    
            except Exception as e:
                with print_lock:
                    print(f"\n  Error: Failed to check field {column} in segment: {e}")
                results[column] = {
                    'error': str(e),
                    'segment_name': segment_name
                }
        
        # Show completion for this segment
        safe_print(f"✓ Segment {segment_num} completed ({total_fields} fields analyzed)")
        
        return results
    
    def generate_report(self, all_results: List[Dict[str, Dict]]) -> str:
        """Generate comprehensive report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f"field_null_rate_report_{timestamp}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("Database Field Non-NULL Rate Report\n")
            f.write("="*80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Database: {self.database}\n")
            f.write(f"Table: {self.table}\n")
            f.write(f"Condition: fullpath contains 'PASS'\n")
            f.write(f"Time field: {self.date_field}\n")
            f.write(f"Recent days limit: {self.recent_days}\n")
            if self.cut_days:
                f.write(f"Time segmentation: Every {self.cut_days} days\n")
            f.write("="*80 + "\n\n")
            
            # Process each segment
            for segment_idx, segment_results in enumerate(all_results):
                if not segment_results:
                    continue
                
                # Get segment info from first field
                first_field = next(iter(segment_results.values()))
                segment_name = first_field.get('segment_name', f'Segment {segment_idx + 1}')
                
                f.write(f"SEGMENT ANALYSIS: {segment_name}\n")
                f.write("-" * 80 + "\n")
                
                if 'start_time' in first_field and 'end_time' in first_field:
                    f.write(f"Time Range: {first_field['start_time'].strftime('%Y-%m-%d %H:%M:%S')} ")
                    f.write(f"to {first_field['end_time'].strftime('%Y-%m-%d %H:%M:%S')}\n")
                
                # Sort by non-NULL rate
                sorted_results = sorted(
                    [(k, v) for k, v in segment_results.items() if 'error' not in v],
                    key=lambda x: x[1]['non_null_rate']
                )
                
                # Statistics for this segment
                total_fields = len(sorted_results)
                perfect_fields = len([r for _, r in sorted_results if r['null_count'] == 0])
                problematic_fields = len([r for _, r in sorted_results if r['non_null_rate'] < 95])
                
                f.write(f"Fields analyzed: {total_fields}\n")
                if total_fields > 0:
                    f.write(f"Perfect fields (100% non-NULL): {perfect_fields}\n")
                    f.write(f"Problematic fields (<95% non-NULL): {problematic_fields}\n")
                    f.write(f"Records in segment: {sorted_results[0][1]['total_records']:,}\n")
                f.write("\n")
                
                # Field details
                f.write("Field Analysis (sorted by non-NULL rate, lowest first):\n")
                f.write("." * 60 + "\n")
                
                for field_name, stats in sorted_results:
                    f.write(f"{field_name:25} | ")
                    f.write(f"Non-NULL: {stats['non_null_rate']:6.2f}% ")
                    f.write(f"({stats['non_null_count']:>8,}/{stats['total_records']:>8,}) | ")
                    f.write(f"NULL: {stats['null_count']:>6,}")
                    
                    if stats['null_count'] == 0:
                        f.write(" ✓")
                    elif stats['non_null_rate'] < 80:
                        f.write(" ✗")
                    elif stats['non_null_rate'] < 95:
                        f.write(" ⚠")
                    
                    f.write("\n")
                
                # Error fields
                error_fields = [(k, v) for k, v in segment_results.items() if 'error' in v]
                if error_fields:
                    f.write(f"\nFields with errors ({len(error_fields)}):\n")
                    for field_name, error_info in error_fields:
                        f.write(f"  {field_name}: {error_info['error']}\n")
                
                f.write("\n" + "="*80 + "\n\n")
            
            # Summary across all segments (if multiple segments)
            if len(all_results) > 1 and self.cut_days:
                f.write("CROSS-SEGMENT SUMMARY\n")
                f.write("="*80 + "\n")
                
                # Collect all field names
                all_fields = set()
                for segment_results in all_results:
                    all_fields.update(k for k, v in segment_results.items() if 'error' not in v)
                
                f.write(f"Analysis across {len(all_results)} time segments:\n\n")
                f.write(f"{'Field Name':25} | {'Avg Non-NULL%':>12} | {'Min Non-NULL%':>12} | {'Max Non-NULL%':>12} | {'Trend':>8}\n")
                f.write("-" * 80 + "\n")
                
                for field in sorted(all_fields):
                    rates = []
                    for segment_results in all_results:
                        if field in segment_results and 'error' not in segment_results[field]:
                            rates.append(segment_results[field]['non_null_rate'])
                    
                    if rates:
                        avg_rate = sum(rates) / len(rates)
                        min_rate = min(rates)
                        max_rate = max(rates)
                        
                        # Simple trend analysis
                        if len(rates) >= 2:
                            if rates[-1] > rates[0] + 1:
                                trend = "↗"
                            elif rates[-1] < rates[0] - 1:
                                trend = "↘"
                            else:
                                trend = "→"
                        else:
                            trend = "-"
                        
                        f.write(f"{field:25} | {avg_rate:>11.2f}% | {min_rate:>11.2f}% | {max_rate:>11.2f}% | {trend:>7}\n")
        
        return report_file
    
    def run(self):
        """Execute main checking process"""
        safe_print("=== Database Field Non-NULL Rate Checker (Enhanced) ===")
        safe_print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        safe_print(f"Recent days limit: {self.recent_days}")
        if self.cut_days:
            safe_print(f"Time segmentation: Every {self.cut_days} days")
        
        try:
            # Test database connection
            if not self.db_helper.test_connection():
                return
            
            safe_print("Database connection successful")
            
            connection = self.db_helper.create_connection()
            
            # Get table structure
            safe_print(f"Getting structure of table {self.database}.{self.table}...")
            columns = self.get_table_columns(connection)
            
            if not columns:
                safe_print("Error: Unable to get table structure")
                return
            
            safe_print(f"Found {len(columns)} fields")
            
            # Get time range
            start_time, end_time = self.get_time_range(connection)
            if not start_time or not end_time:
                safe_print("Error: Unable to determine time range")
                return
            
            # Generate time segments
            time_segments = self.generate_time_segments(start_time, end_time)
            
            # Analyze each segment
            all_results = []
            total_segments = len(time_segments)
            
            safe_print(f"\nStarting analysis of {total_segments} time segments...")
            safe_print("="*80)
            
            for segment_idx, (segment_start, segment_end, segment_name) in enumerate(time_segments, 1):
                segment_results = self.check_field_null_rates_for_segment(
                    connection, columns, segment_start, segment_end, segment_name, 
                    segment_idx, total_segments
                )
                all_results.append(segment_results)
                
                # Show overall progress
                self.show_progress_bar(segment_idx, total_segments, 
                                     "Overall Progress", 50)
                
                if segment_idx < total_segments:
                    safe_print("")  # Add some spacing between segments
            
            connection.close()
            
            if not any(all_results):
                safe_print("No results obtained")
                return
            
            # Generate report
            report_file = self.generate_report(all_results)
            
            # Output summary
            safe_print("\n" + "="*80)
            safe_print("Analysis completed!")
            
            # Show overall problematic fields
            all_problematic = {}
            for segment_results in all_results:
                for field_name, stats in segment_results.items():
                    if 'error' not in stats and stats['non_null_rate'] < 95:
                        if field_name not in all_problematic:
                            all_problematic[field_name] = []
                        all_problematic[field_name].append(stats['non_null_rate'])
            
            if all_problematic:
                safe_print(f"\nFields with issues across segments:")
                for field_name, rates in all_problematic.items():
                    avg_rate = sum(rates) / len(rates)
                    safe_print(f"  {field_name}: Average {avg_rate:.2f}% non-NULL across {len(rates)} segments")
            else:
                safe_print("\n✓ All fields maintain good non-NULL rates (≥95%) across all segments")
            
            safe_print(f"\nDetailed report saved to: {report_file}")
            
        except Exception as e:
            safe_print(f"\nError occurred during execution: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            safe_print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Main program entry point"""
    parser = argparse.ArgumentParser(
        description='Database Field Non-NULL Rate Checker - Enhanced with time filtering and segmentation'
    )
    parser.add_argument(
        '-c', '--config',
        default='database_config.json',
        help='Configuration file path (default: database_config.json)'
    )
    parser.add_argument(
        '--database',
        help='Specify database name (overrides config file setting)'
    )
    parser.add_argument(
        '--table',
        help='Specify table name (overrides config file setting)'
    )
    parser.add_argument(
        '--cut_day', '--cut_days',
        type=int,
        help='Cut analysis into segments of N days (e.g., --cut_day 7 for weekly analysis)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Command line argument overrides
        if args.database:
            config['database'] = args.database
            safe_print(f"Using command line specified database: {args.database}")
        
        if args.table:
            config['table'] = args.table
            safe_print(f"Using command line specified table: {args.table}")
        
        # Validate required configuration
        required_keys = ['db_host', 'db_user', 'db_pwd', 'database', 'table']
        missing_keys = [key for key in required_keys if not config.get(key)]
        
        if missing_keys:
            safe_print(f"Error: Configuration file missing required keys: {', '.join(missing_keys)}")
            return
        
        # Create checker and run
        checker = FieldNullRateChecker(config, args.cut_day)
        checker.run()
        
    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user")
    except Exception as e:
        print(f"\nProgram execution error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()