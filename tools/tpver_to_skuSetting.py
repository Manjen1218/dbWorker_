#!/usr/bin/python3
import json
from typing import Dict, List
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from core.MySQLHelper import MySQLHelper
from core.Logger import Logger

logger = Logger(log_dir="logs", log_prefix="tpver_mapper")

class TpverMapper:
    """Maps tpver values to machine-test station combinations (db name and table name)."""
    
    def __init__(self, db_setting_path: str):
        """Initialize the TpverMapper with the database settings.
        
        Args:
            db_setting_path: Path to the db_setting.json file.
        """
        self.db_setting_path = db_setting_path
        self.db_helper = MySQLHelper(db_setting_path)
        self.db_settings = self._load_db_settings()
        self.tpver_map = {}
        
    def _load_db_settings(self) -> Dict:
        """Load database settings from the JSON file.
        
        Returns:
            Dict containing the database settings.
        """
        try:
            with open(self.db_setting_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.log_error(f"Failed to load database settings: {e}")
            return {}
    
    def connect(self) -> bool:
        """Connect to the database.
        
        Returns:
            True if connection was successful, False otherwise.
        """
        return self.db_helper.connect()
    
    def close(self) -> None:
        """Close the database connection."""
        self.db_helper.close()
        
    def map_tpver_to_machines(self) -> Dict[str, List[Dict[str, str]]]:
        """Map tpver values to machine-test station combinations.
        
        Scans through all databases and tables defined in the settings,
        finds all tpver values, and maps them to the corresponding
        machine-test station combinations.
        
        Returns:
            Dictionary with tpver as keys and lists of machine-test station
            combinations as values.
        """
        if not self.connect():
            logger.log_error("Failed to connect to database")
            return {}
            
        try:
            # Process each machine (database)
            for sku, sku_info in self.db_settings.get('sku_parser_rule', {}).items():
                dbname = sku_info.get('dbname')
                if not dbname:
                    logger.log_warn(f"No database name found for SKU {sku}")
                    continue
                    
                # Use the database
                if not self.db_helper.use_database(dbname):
                    logger.log_warn(f"Failed to use database {dbname}")
                    continue
                    
                # Process each test station (table)
                for station_name, station_info in sku_info.get('test_station', {}).items():
                    tablename = station_info.get('tablename')
                    if not tablename:
                        logger.log_warn(f"No table name found for station {station_name} in {dbname}")
                        continue
                        
                    # Check if table exists
                    table_exists_query = f"SHOW TABLES LIKE '{tablename}'"
                    result = self.db_helper.execute_query(table_exists_query, fetch='one')
                    if not result:
                        logger.log_warn(f"Table {tablename} does not exist in database {dbname}")
                        continue
                        
                    # Check if tpver column exists
                    columns = self.db_helper.get_columns(tablename)
                    if 'tpver' not in columns:
                        logger.log_warn(f"No 'tpver' column in table {tablename} in database {dbname}")
                        continue
                        
                    # Get distinct tpver values
                    query = f"SELECT DISTINCT tpver FROM `{tablename}` WHERE tpver IS NOT NULL AND tpver != ''"
                    tpvers = self.db_helper.execute_query(query, fetch='all')
                    if not tpvers:
                        logger.log_info(f"No tpver values found in table {tablename} in database {dbname}")
                        continue
                        
                    # Map each tpver to this machine-test station combination
                    for tpver_tuple in tpvers:
                        tpver = tpver_tuple[0]
                        if tpver not in self.tpver_map:
                            self.tpver_map[tpver] = []
                            
                        # Add machine-test station combination
                        self.tpver_map[tpver].append({
                            'machine': dbname,
                            'station': tablename,
                        })
                        
            logger.log_info(f"Mapped {len(self.tpver_map)} tpver values to machine-test station combinations")
            return self.tpver_map
                    
        except Exception as e:
            logger.log_error(f"Error mapping tpver values: {e}")
            return {}
        finally:
            self.close()
    
    def save_mapping_to_file(self, output_path: str) -> bool:
        """Save the tpver mapping to a JSON file.
        
        Args:
            output_path: Path to save the mapping file.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            with open(output_path, 'w') as f:
                json.dump(self.tpver_map, f, indent=4)
            logger.log_info(f"Saved tpver mapping to {output_path}")
            return True
        except Exception as e:
            logger.log_error(f"Failed to save tpver mapping: {e}")
            return False

    def sku_setting_selector(self, tpver:str, priciple:str = "large") -> str:
        """
        Select a sku_setting file path based on the specified principle.
        
        Args:
            tpver: The tpver value to look up
            priciple: Selection principle, currently supports "large" which returns the path 
                     with the most elements (dimension 0) in the file

        Returns:
            The selected file path, or empty string if no path is found or an error occurs
        """
        map_tpver = self.map_tpver_to_machines()
        if tpver not in map_tpver:
            logger.log_error(f"No matching machines/stations found for tpver: {tpver}")
            return ""
            
        if priciple == "large" or priciple == "small":
            paths = map_tpver[tpver]
            if not paths:
                logger.log_error(f"No paths found for tpver: {tpver}")
                return ""
                
            max_elements = 0
            selected_path = ""
            prefix = 'ParseEngine/sku_setting/'
            for path_component in paths:
                path = prefix + path_component['machine'].lower() + '/' + path_component['machine'].lower() + '_' + path_component['station'] + '.json'
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                    
                    element_count = len(data)
                    
                    if priciple == "large":
                        if element_count > max_elements:
                            max_elements = element_count
                            selected_path = path
                    elif priciple == "small":
                        if element_count < max_elements:
                            max_elements = element_count
                            selected_path = path
                        
                except Exception as e:
                    logger.log_warn(f"Error reading file {path}: {e}")
                    continue
            
            if selected_path:
                logger.log_info(f"Selected path for tpver {tpver}: {selected_path} with {max_elements} elements")
                return selected_path
            else:
                logger.log_error(f"Could not find a valid path for tpver: {tpver}")
                return ""
        else:
            logger.log_error(f"Unknown selection principle: {priciple}")
            return ""

def get_sku_setting_filePath_by_tpver(tpver:str, db_setting_path:str = "db_setting/db_setting.json") -> List[str]:
    """
    Select a sku_setting file path based on the specified tpver.
    
    Args:
        tpver: The tpver value to look up
        db_setting_path: Path to the db_setting.json file
        
    Returns:
        The selected file path, or empty string if no path is found or an error occurs
    """
    logger.log_title(f"Searching sku_setting with tpver: {tpver}")
    
    mapper = TpverMapper(db_setting_path)
    
    path = mapper.sku_setting_selector(tpver)
    
    if not path:
        logger.log_error(f"No matching machines/stations found for tpver: {tpver}")
        return ""
    
    logger.log_info(f"Found {path} matching machines/stations for tpver: {tpver}")
    
    return path

def main():
    """Main function to run the tpver mapper."""
    # Default paths
    db_setting_path = "db_setting/db_setting.json"
    output_path = "tpver_mapping.json"
    
    if len(sys.argv) > 1:
        db_setting_path = sys.argv[1]
    
    print(get_sku_setting_filePath_by_tpver('V3.78.1.1E4.41'))

if __name__ == "__main__":
    main()