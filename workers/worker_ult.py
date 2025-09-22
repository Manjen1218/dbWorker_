#!/usr/bin/python3
import os, datetime, time, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Any, Dict, List

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.Logger import Logger

class DurationTimer:
    """
    Context manager for measuring execution duration of a code block and logging the result.
    """

    def __init__(self, logg: Logger, label: str) -> None:
        """
        Initialize the timer with logger and label.

        :param logg: Logger instance used to print and store duration message
        :param label: Descriptive label of the operation being timed
        """
        self.logg: Logger = logg
        self.label: str = label
        self.start_time: Optional[float] = None

    def __enter__(self) -> "DurationTimer":
        """
        Start timing upon entering the context.

        :return: self reference
        """
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        """
        End timing and log the duration upon exiting the context.

        :param exc_type: Exception type, if raised inside the block
        :param exc_val: Exception value
        :param exc_tb: Traceback object
        """
        end_time = time.time()
        if self.start_time is not None:
            duration = end_time - self.start_time
            self.logg.log_info(f"{self.label} , duration: {duration:.2f} sec")


def generate_schema_from_rule(rule_dict: Dict[str, Any]) -> str:
        """
        Generate MySQL CREATE TABLE schema field definition string from rule JSON.

        Args:
            rule_dict (Dict[str, Any]): Dictionary containing rule info per field.

        Returns:
            str: SQL schema definition string (to be placed inside CREATE TABLE).
        """
        schema_parts = []

        for field, field_info in rule_dict.items():
            dbtype = field_info.get("dbtype", "TEXT")
            schema_parts.append(f"`{field}` {dbtype}")
               
        schema_sql = ",\n  ".join(schema_parts)

        return schema_sql

def get_local_filelist(test_station_path: str, recently_days: int, nthread: int = 4) -> list:
    def list_files_for_day(day: datetime.date) -> tuple:
        """Return (date, file_list) tuple for sorting purposes"""
        filelist = []
        dstr = day.strftime('%Y%m%d')
        for spath in ['PASS', 'FAIL']:
            target_path = os.path.join(test_station_path, dstr, spath)
            if os.path.isdir(target_path):
                filelist.extend([
                    os.path.join(target_path, f)
                    for f in os.listdir(target_path)
                    if os.path.isfile(os.path.join(target_path, f))
                ])
        return (day, filelist)

    tday = datetime.date.today()
    days_range = [tday - datetime.timedelta(days=i) for i in range(recently_days + 1)]

    # Collect results with date information
    day_file_pairs = []
    with ThreadPoolExecutor(max_workers=nthread) as executor:
        futures = [executor.submit(list_files_for_day, day) for day in days_range]
        for future in as_completed(futures):
            day, files = future.result()
            day_file_pairs.append((day, files))
    
    # Sort by date in descending order (newest first)
    day_file_pairs.sort(key=lambda x: x[0], reverse=True)
    
    # Extract sorted file list
    filelist = []
    for day, files in day_file_pairs:
        filelist.extend(files)
    
    return filelist

def get_local_filelist_single_thread(test_station_path: str, recently_days: int) -> list:
    filelist = []
    tday = datetime.date.today()
    start_day = tday - datetime.timedelta(days=recently_days)

    while start_day <= tday:
        dstr = start_day.strftime('%Y%m%d')
        for spath in ['PASS', 'FAIL']:
            target_path = os.path.join(test_station_path, dstr, spath)
            if os.path.isdir(target_path):
                filelist.extend([
                    os.path.join(target_path, f)
                    for f in os.listdir(target_path)
                    if os.path.isfile(os.path.join(target_path, f))
                ])
        start_day += datetime.timedelta(days=1)
    return filelist

def is_win32() -> bool:
    """Check the operating system.
    
    Returns:
        bool: True if the OS is windows, False otherwise
    """
    if sys.platform == "win32":
        return True
    else:
        return False

def get_schema(json_data: dict) -> dict:
    """Get schema from JSON file.
    
    Args:
        json_path (str): Path to the JSON file
        
    Returns:
        dict: Schema dictionary
    """
    return {k: v["dbtype"] for k, v in json_data.items() if "dbtype" in v}

if __name__ == '__main__':
    pass