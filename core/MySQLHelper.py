#!/usr/bin/python3
import pymysql
import pandas as pd
import logging
from typing import Dict, Optional, Set, List, Tuple, Any, Union
from datetime import datetime, timedelta
import json

class MySQLHelper:
    def __init__(self, dbsetting: Union[Dict[str, str], str]):
        """
        Initialize the database helper.

        :param dbsetting: Dictionary containing database settings.
                          Required keys: db_host, db_port, db_user, db_pwd
        """
        if isinstance(dbsetting, str):
            self.dbsetting = json.load(open(dbsetting, "r", encoding="UTF-8"))
        else:
            self.dbsetting = dbsetting
        self.conn: Optional[pymysql.connections.Connection] = None
        self.cursor: Optional[pymysql.cursors.Cursor] = None
        self.stats = {"success_num": 0, "failed_num": 0}
        self.logger = logging.getLogger(__name__)

    def connect(self) -> bool:
        """
        Establish a connection to the database.
        
        :return: True if connection was successful, False otherwise
        """
        try:
            self.conn = pymysql.connect(
                host=self.dbsetting['db_host'],
                port=int(self.dbsetting['db_port']),
                user=self.dbsetting['db_user'],
                password=self.dbsetting['db_pwd']
            )
            self.cursor = self.conn.cursor()
            self.logger.info(f"Connected to database at {self.dbsetting['db_host']}:{self.dbsetting['db_port']}")
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            return False

    def close(self) -> None:
        """
        Commit any pending transactions and close the cursor and connection.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def use_database(self, dbname: str) -> bool:
        """
        Create the specified database if it does not exist, and switch to it.

        :param dbname: Name of the database to use.
        :return: True if successful, False otherwise
        """
        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{dbname}`")
            self.cursor.execute(f"USE `{dbname}`")
            self.logger.info(f"Using database: {dbname}")
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error using database {dbname}: {e}")
            return False

    def use_database_lock(self, dbname: str) -> bool:
        """
        Create the specified database if it does not exist, and switch to it.
        Uses timeout to avoid hanging on metadata locks.

        :param dbname: Name of the database to use.
        :return: True if successful, False otherwise
        """
        try:
            # Set only lock_wait_timeout (metadata_lock_wait_timeout doesn't exist in some MySQL versions)
            self.cursor.execute("SET SESSION lock_wait_timeout = 1")
            
            # Try to create database with timeout
            create_sql = f"CREATE DATABASE IF NOT EXISTS `{dbname}`"
            success, _ = self.execute_query(create_sql)
            if not success:
                return False
                
            # Try to use the database
            use_sql = f"USE `{dbname}`"
            success, _ = self.execute_query(use_sql)
            if not success:
                return False
            return True
            
        except pymysql.Error as e:
            self.logger.warning(f"Database {dbname} is locked or timeout occurred: {e}")
            return False

    def get_table_lock(self, tablename: str, lock_type: str = 'WRITE') -> bool:
        """
        Acquire a lock on a table.
        
        :param tablename: Name of the table to lock
        :param lock_type: Type of lock ('READ' or 'WRITE')
        :return: True if lock acquired, False otherwise
        """
        try:
            # First ensure we have a database selected
            if not self.cursor.connection.db:
                self.logger.error("No database selected before attempting table lock")
                return False
                
            lock_sql = f"LOCK TABLES `{tablename}` {lock_type}"
            self.logger.info(f"Attempting to acquire {lock_type} lock on table {tablename}")
            
            success, result = self.execute_query(lock_sql)
            if success:
                self.logger.info(f"Successfully acquired {lock_type} lock on table {tablename}")
                return True
            else:
                self.logger.error(f"Failed to acquire lock on table {tablename}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error acquiring table lock: {e}")
            return False

    def release_table_lock(self) -> bool:
        """
        Release all table locks held by current session.
        
        :return: True if locks released, False otherwise
        """
        try:
            success, _ = self.execute_query("UNLOCK TABLES")
            if success:
                self.logger.info("Released all table locks")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error releasing table locks: {e}")
            return False

    def drop_database(self, dbname: str) -> bool:
        """
        Drop the specified database if it exists.

        :param dbname: Name of the database to drop.
        :return: True if successful, False otherwise
        """
        try:
            self.cursor.execute(f"DROP DATABASE IF EXISTS `{dbname}`")
            self.logger.info(f"Dropped database: {dbname}")
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error dropping database {dbname}: {e}")
            return False

    def get_table_schema(self, tablename: str) -> Dict[str, str]:
        """
        Retrieve the current schema of a table in the form of column → type mapping.

        Args:
            tablename (str): Name of the table to retrieve schema from.

        Returns:
            Dict[str, str]: Dictionary where keys are column names and values are 
                            their corresponding SQL types (e.g., 'varchar(100)', 'int').
                            All types are returned in lowercase.
                            Returns an empty dict if table does not exist or error occurs.
        """
        try:
            success, results = self.execute_query(f"SHOW COLUMNS FROM `{tablename}`", fetch='all')
            if success and results:
                return {col[0]: col[1].lower() for col in results}
            else:
                return {}
        except Exception as e:
            self.logger.error(f"Failed to get schema for table '{tablename}': {e}")
            return {}

    def create_table_if_nexists(self, tablename: str, schema_fields: str) -> bool:
        """
        Create a table with the given schema if it does not exist.

        :param tablename: Name of the table to create.
        :param schema_fields: Table column definitions (raw SQL string).
        :return: True if successful, False otherwise
        """
        try:
            create_table_cmd = f"""CREATE TABLE IF NOT EXISTS `{tablename}` (
                {schema_fields}
            );"""
            self.cursor.execute(create_table_cmd)
            self.logger.info(f"Created table: {tablename}")
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error creating table {tablename}: {e}")
            return False

    def drop_table(self, tablename: str) -> bool:
        """
        Drop existing table.

        :param tablename: Name of table to drop.
        :return: True if successful, False otherwise
        """
        try:
            cmd = f"DROP TABLE IF EXISTS `{tablename}`"
            self.cursor.execute(cmd)
            self.logger.info(f"Dropped table: {tablename}")
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error dropping table {tablename}: {e}")
            return False

    def get_columns(self, tablename: str) -> list:
        """
        Retrieve all column names in table.

        :param tablename: Name of table with columns of interest.
        :return: List of column name strings.
        """
        columns_query = f"""SHOW COLUMNS FROM {tablename}"""
        success, results = self.execute_query(columns_query, fetch='all')
        if success and results:
            return [col[0] for col in results]
        else:
            return []

    def sync_table_schema(self, tablename: str, json_schema: Dict[str, str]) -> Tuple[List[str], bool]:
        """
        Synchronize the schema of a given MySQL table with a JSON-defined schema.

        For each column defined in `json_schema`, this method checks if:
        - It does not exist in the database → `ALTER TABLE ADD COLUMN`
        - It exists but type differs → `ALTER TABLE MODIFY COLUMN` (with warning)

        Note:
        - No column deletion is performed.
        - Comparison is case-insensitive for types.

        Args:
            tablename (str): The name of the table to sync.
            json_schema (Dict[str, str]): A dictionary where keys are column names and
                values are SQL type strings (e.g., "varchar(100)", "int", etc.).

        Returns:
            List[str]: A list of warning messages if any type mismatches were found and updated.
            Bool: True if schema is updated, otherwise false.
        """
        warnings = []
        schema_updated = False
        try:
            success, results = self.execute_query(f"SHOW COLUMNS FROM `{tablename}`", fetch='all')
            if success is False:
                self.logger.error(f"[sync_table_schema] Failed to retrieve schema for table '{tablename}'.")
                return warnings

            db_schema = {col[0]: col[1].lower() for col in results}

            for col, dbtype in json_schema.items():
                dbtype_lower = dbtype.lower()
                if col not in db_schema:
                    schema_updated = True
                    self.logger.info(f"[Schema] Adding column: {col} {dbtype}")
                    self.cursor.execute(f"ALTER TABLE `{tablename}` ADD COLUMN `{col}` {dbtype}")
                elif db_schema[col] != dbtype_lower:
                    warnings.append(f"[Warn] Column '{col}' type mismatch: DB({db_schema[col]}) vs JSON({dbtype})")
                    self.logger.info(f"[Schema] Modifying column: {col} to {dbtype}")
                    self.cursor.execute(f"ALTER TABLE `{tablename}` MODIFY COLUMN `{col}` {dbtype}")
            self.conn.commit()
            return warnings, schema_updated

        except pymysql.Error as e:
            self.logger.error(f"[sync_table_schema] MySQL error on table '{tablename}': {e}")
            return warnings, schema_updated

    def get_existing_fullpaths(self, tablename: str, time_column: str, recently_days: int) -> Set[str]:
        """
        Retrieve a set of fullpath values from a given table where the time column is within the last X days.

        :param tablename: Table to query.
        :param time_column: Time-related column name for filtering.
        :param recently_days: Number of days to look back from today.
        :return: Set of fullpath strings.
        """
        days_ago = datetime.now() - timedelta(days=recently_days)
        time_str = days_ago.strftime('%Y-%m-%d 00:00:00')
        query = f"""SELECT fullpath FROM `{tablename}` WHERE `{time_column}` >= %s"""    
        success, results = self.execute_query(query, params=(time_str,), fetch='all')
        return set(row[0] for row in results) if success and results else set()

    def insert_data(self, tablename: str, data: Dict[str, any]) -> bool:
        """
        Insert a dictionary of data into the specified table.

        :param tablename: Name of the table.
        :param data: Dictionary where keys are column names and values are the data to insert.
        :return: True if successful, False otherwise
        """
        try:
            columns = ', '.join([f"`{k}`" for k in data.keys()])
            placeholders = ', '.join(['%s'] * len(data))
            values = list(data.values())
            insert_sql = f"INSERT INTO `{tablename}` ({columns}) VALUES ({placeholders})"
            self.execute_query(insert_sql, params=values)
            self.conn.commit()
            self.stats["success_num"] += 1
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error inserting data into {tablename}: {e}")
            self.stats["failed_num"] += 1
            return False

    def insert_if_not_exist(self, tablename: str, data: Dict[str, any]) -> bool:
        """
        Insert data into the table only if a row with the same 'fullpath' does not already exist.

        :param tablename: Name of the table.
        :param data: Data dictionary with a 'fullpath' key used as the condition.
        :return: True if successful, False otherwise
        """
        try:
            columns = ', '.join([f"`{k}`" for k in data.keys()])
            placeholders = ', '.join(['%s'] * len(data))
            values = list(data.values())

            condition_column = 'fullpath'
            condition_value = data.get(condition_column)
            if condition_value is None:
                self.logger.error("The data must contain a 'fullpath' key for conditional insert.")
                return False

            insert_sql = f"""
                INSERT INTO `{tablename}` ({columns})
                SELECT {placeholders}
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{tablename}` WHERE `{condition_column}` = %s
                )
            """
            values.append(condition_value)
            self.execute_query(insert_sql, params=values)
            self.conn.commit()
            self.stats["success_num"] += 1
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error in conditional insert into {tablename}: {e}")
            self.stats["failed_num"] += 1
            return False

    def insert_or_update(self, tablename: str, data: Dict[str, Any], key_column: str = 'fullpath') -> Union[bool, str]:
        """
        Insert a row into the table if the primary/unique key value does not exist,
        otherwise update all columns except the key column.
        This method use SEARCH then UPDATE if exists, otherwise INSERT.

        Requirements:
            - The key column (e.g., 'fullpath') must have a UNIQUE constraint in the database.

        Args:
            tablename (str): The name of the table to insert/update.
            data (Dict[str, Any]): The row data, with keys as column names.
            key_column (str, optional): The column name to use as unique key. Defaults to 'fullpath'.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        if key_column not in data:
            self.logger.error(f"Missing key column '{key_column}' in data.")
            return False

        try:
            columns = list(data.keys())
            placeholders = ", ".join(["%s"] * len(columns))
            col_str = ", ".join([f"`{col}`" for col in columns])

            # Check if the key_column value exists
            check_sql = f"SELECT 1 FROM `{tablename}` WHERE `{key_column}` = %s LIMIT 1"
            success, exists = self.execute_query(check_sql, params=(data[key_column],), fetch='one')
            if not success:
                # return error message
                return exists
            
            if exists:
                # Update existing row
                update_columns = [col for col in columns if col != key_column]
                update_clause = ", ".join([f"`{col}` = %s" for col in update_columns])
                update_sql = f"UPDATE `{tablename}` SET {update_clause} WHERE `{key_column}` = %s"
                update_values = [data[col] for col in update_columns] + [data[key_column]]
                sql = update_sql
                values = update_values
            else:
                # Insert new row
                sql = f"INSERT INTO `{tablename}` ({col_str}) VALUES ({placeholders})"
                values = list(data.values())

            success, result = self.execute_query(sql, params=values)
            if not success:
                # return error message
                return result
            else:
                self.conn.commit()
                self.stats["success_num"] += 1
                return True
        except Exception as e:
            self.logger.error(f"Insert/update failed on {tablename}: {e}")
            self.stats["failed_num"] += 1
            return str(e)

    def commit_and_refresh_cursor(self) -> None:
        """
        Commit the current transaction and refresh the cursor.
        """
        self.conn.commit()
        self.cursor = self.conn.cursor()

    def execute_query(self, query: str, params: Optional[Union[tuple, list, dict]] = None, fetch: Optional[str] = None) -> Optional[Tuple[bool, Any]]:
        """
        Execute a SQL query with parameters.

        :param query: SQL query string
        :param params: Parameters for the SQL query
        :param fetch: Fetch mode, 'one' or 'all'
        :return: Query results or None if an error occurs
        """
        try:
            if params is not None:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            if fetch == 'one':
                return True, self.cursor.fetchone()
            elif fetch == 'all':
                return True, self.cursor.fetchall()
            else:
                return True, None
        except pymysql.Error as e:
            if e.args and e.args[0] == 1205:
                return False, str(e)
            self.logger.error(f"Error executing query: {e}\nQuery: {query}")
            return False, str(e)

    def count_distinct(self, dbname: str, tablename: str, column: str, condition_column: str, condition_value: Any, additional_conditions: Optional[Dict[str, Any]] = None, null_conditions: Optional[List[str]] = None) -> int:
        """
        Count distinct values in a column with a simple condition and optional additional conditions.

        :param dbname: Database name (can be empty to use current database)
        :param tablename: Table name
        :param column: Column to count distinct values from
        :param condition_column: Column to use in WHERE clause
        :param condition_value: Value for the condition
        :param additional_conditions: Optional dictionary of additional conditions {column: value}
        :return: Count of distinct values
        """
        table_path = f"`{dbname}`.`{tablename}`" if dbname else f"`{tablename}`"
        sql = f"SELECT COUNT(DISTINCT {column}) FROM {table_path} WHERE `{condition_column}`=%s"
        params = [condition_value]
        if additional_conditions:
            for col, val in additional_conditions.items():
                sql += f" AND `{col}`=%s"
                params.append(val)
        if null_conditions:
            for col in null_conditions:
                sql += f" AND `{col}` IS NULL"
        success, result = self.execute_query(sql, params=tuple(params), fetch='one')
        return result[0] if success and result else 0


    def search_by_like(self, 
                      dbname: str, 
                      tablename: str, 
                      columns: List[str], 
                      search_column: str, 
                      search_value: str, 
                      additional_conditions: Optional[Dict[str, Any]] = None,
                      order_by: Optional[List[str]] = None,
                      fetch: Optional[str] = 'all') -> Optional[Union[List[Tuple], Tuple]]:
        """
        Search for records where a column matches a pattern with LIKE.

        :param dbname: Database name (can be empty to use current database)
        :param tablename: Table name
        :param columns: List of columns to retrieve
        :param search_column: Column to search with LIKE
        :param search_value: Value to search for (will be wrapped with %)
        :param additional_conditions: Dictionary of additional conditions {column: value}
        :param order_by: List of columns to order by
        :param fetch: Fetch mode, 'one' or 'all'
        :return: Query results
        """
        table_path = f"`{dbname}`.`{tablename}`" if dbname else f"`{tablename}`"
        columns_str = ", ".join([f"`{col}`" for col in columns])
        sql = f"SELECT {columns_str} FROM {table_path} WHERE `{search_column}` LIKE %s"
        params = [f"%{search_value}%"]

        if additional_conditions:
            for col, val in additional_conditions.items():
                sql += f" AND `{col}` = %s"
                params.append(val)

        if order_by:
            sql += f" ORDER BY {', '.join(order_by)}"

        success, results = self.execute_query(sql, params=params, fetch=fetch)
        return results if success else None

    def get_min_max_dates(self, 
                          dbname: str, 
                          tablename: str, 
                          date_columns: List[str], 
                          group_column: str, 
                          condition_column: str, 
                          condition_value: str,
                          additional_conditions: Optional[Dict[str, Any]] = None) -> List[Tuple]:
        """
        Get minimum and maximum dates for specific columns grouped by another column.
        
        :param dbname: Database name (can be empty to use current database)
        :param tablename: Table name
        :param date_columns: List of date columns to get min/max from
        :param group_column: Column to group by
        :param condition_column: Column to use in WHERE clause
        :param condition_value: Value for the condition
        :param additional_conditions: Dictionary of additional conditions {column: value}
        :return: List of tuples with query results
        """
        table_path = f"`{dbname}`.`{tablename}`" if dbname else f"`{tablename}`"
        
        # Build the date functions part of the query
        date_functions = []
        for col in date_columns:
            date_functions.append(f"MIN(`{col}`) as min_{col}")
            date_functions.append(f"MAX(`{col}`) as max_{col}")
            
        date_functions_str = ", ".join(date_functions)
        
        sql = f"SELECT `{group_column}`, {date_functions_str} FROM {table_path} WHERE `{condition_column}` LIKE %s"
        params = [f"%{condition_value}%"]
        
        if additional_conditions:
            for col, val in additional_conditions.items():
                sql += f" AND `{col}` = %s"
                params.append(val)
                
        sql += f" GROUP BY `{group_column}` ORDER BY `{group_column}`"
        
        success, results = self.execute_query(sql, params=params, fetch='all')
        return results if success else None
    
    def count_records_with_condition(self, 
                                    dbname: str, 
                                    tablename: str, 
                                    conditions: Dict[str, Any],
                                    group_by: Optional[List[str]] = None) -> Union[int, List[Tuple]]:
        """
        Count records matching certain conditions, optionally grouped.
        
        :param dbname: Database name (can be empty to use current database)
        :param tablename: Table name
        :param conditions: Dictionary of conditions {column: value}
        :param group_by: Optional list of columns to group by
        :return: Count as int, or list of (group, count) tuples if group_by is provided
        """
        table_path = f"`{dbname}`.`{tablename}`" if dbname else f"`{tablename}`"
        
        if group_by:
            group_columns_str = ", ".join([f"`{col}`" for col in group_by])
            sql = f"SELECT {group_columns_str}, COUNT(*) FROM {table_path} WHERE "
        else:
            sql = f"SELECT COUNT(*) FROM {table_path} WHERE "
            
        where_clauses = []
        params = []
        
        for col, val in conditions.items():
            where_clauses.append(f"`{col}` = %s")
            params.append(val)
            
        sql += " AND ".join(where_clauses)
        
        if group_by:
            sql += f" GROUP BY {group_columns_str}"
            
        if group_by:
            success, results = self.execute_query(sql, params=params, fetch='all')
            return results if success and results else 0
        else:
            success, results = self.execute_query(sql, params=params, fetch='one')
            return results[0] if success and results else 0
            
    def get_table_names(self, dbname: str) -> List[str]:
        """
        Get a list of all tables in a database.
        
        :param dbname: Database name
        :return: List of table names
        """
        success, results = self.execute_query(f"SHOW TABLES FROM `{dbname}`", fetch='all')
        return [row[0] for row in results] if success and results else []
        
    def get_fail_distribution(self, 
                             dbname: str, 
                             tablename: str, 
                             error_column: str, 
                             condition_column: str, 
                             condition_value: Any) -> List[Tuple]:
        """
        Get distribution of error messages and their counts.
        
        :param dbname: Database name (can be empty to use current database)
        :param tablename: Table name
        :param error_column: Column containing error messages
        :param condition_column: Column to use in WHERE clause
        :param condition_value: Value for the condition
        :return: List of tuples with (error_message, count)
        """
        table_path = f"`{dbname}`.`{tablename}`" if dbname else f"`{tablename}`"
        
        sql = f"""
            SELECT 
                {error_column}, 
                COUNT(*) as count 
            FROM {table_path} 
            WHERE {condition_column} = %s 
                AND {error_column} IS NOT NULL 
            GROUP BY {error_column} 
            ORDER BY count DESC
        """
        
        success, results = self.execute_query(sql, params=(condition_value,), fetch='all')
        return results if success else None
        
    def add_data_batch(self, tablename: str, data_list: List[Dict[str, any]]) -> bool:
        """
        Add multiple data records to the database in a batch.
        
        :param tablename: Name of the table to insert into
        :param data_list: List of dictionaries containing data to insert
        :return: True if successful, False otherwise
        """
        if not data_list:
            return False
            
        # Ensure all dictionaries have the same keys
        columns = data_list[0].keys()
        for data in data_list:
            if set(data.keys()) != set(columns):
                self.logger.error("All data dictionaries must have the same keys for batch insertion")
                return False
                
        columns_str = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        
        sql = f"INSERT INTO `{tablename}` ({columns_str}) VALUES ({placeholders})"
        
        # Prepare values for batch insertion
        values = []
        for data in data_list:
            # Ensure values are in the same order as columns
            row_values = [data[col] for col in columns]
            values.append(row_values)
            
        try:
            # Execute batch insert
            self.cursor.executemany(sql, values)
            self.conn.commit()
            self.stats["success_num"] += len(data_list)
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error in batch insert: {e}")
            self.stats["failed_num"] += len(data_list)
            return False
            
    def save_failed_data(self, failed_table: str, failed_record: Dict[str, any] = None, reason: str = "db_failed") -> bool:
        """
        Save failed data records to a specified table.
        
        :param failed_table: Name of the table to save failed records to
        :param failed_record: The record that failed to be processed
        :param reason: Reason for the failure
        :return: True if saved successfully, False otherwise
        """
        if not failed_record:
            return False
            
        # Add reason and timestamp to the failed record
        failed_data = failed_record.copy()
        failed_data["fail_reason"] = reason
        failed_data["fail_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return self.insert_data(failed_table, failed_data)
        
    def get_data(self, sql: str, params: Optional[Union[tuple, list, dict]] = None) -> Optional[pd.DataFrame]:
        """
        Get data from the database using a SQL query and return as a pandas DataFrame.
        
        :param sql: SQL query to execute
        :param params: Parameters for the SQL query
        :return: DataFrame containing the query results or None if the query fails
        """
        try:
            success, results = self.execute_query(sql, params=params, fetch='all')
            if not success or not results:
                return None
                
            # Get column names from the cursor description
            columns = [desc[0] for desc in self.cursor.description]
            
            # Create DataFrame from results
            return pd.DataFrame(results, columns=columns)
        except pymysql.Error as e:
            self.logger.error(f"Error executing query: {e}")
            return None
            
    def get_all_data(self, tablename: str) -> Optional[pd.DataFrame]:
        """
        Get all data from the specified table.
        
        :param tablename: Name of the table to retrieve data from
        :return: DataFrame containing all records from the table or None if the query fails
        """
        sql = f"SELECT * FROM `{tablename}`"
        return self.get_data(sql)
        
    def truncate_table(self, tablename: str) -> bool:
        """
        Truncate a table (remove all rows while keeping the structure).
        
        :param tablename: Name of the table to truncate
        :return: True if successful, False otherwise
        """
        try:
            self.cursor.execute(f"TRUNCATE TABLE `{tablename}`")
            self.conn.commit()
            self.logger.info(f"Table '{tablename}' truncated successfully")
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error truncating table {tablename}: {e}")
            return False
            
    def update_data(self, tablename: str, data: Dict[str, any], condition_column: str, condition_value: any) -> bool:
        """
        Update data in the database where a column matches a specific value.
        
        :param tablename: Name of the table to update
        :param data: Dictionary of column-value pairs to update
        :param condition_column: Column to use in the WHERE clause
        :param condition_value: Value to match in the condition column
        :return: True if successful, False otherwise
        """
        if not data:
            return False
            
        set_clause = ", ".join([f"`{k}` = %s" for k in data.keys()])
        sql = f"UPDATE `{tablename}` SET {set_clause} WHERE `{condition_column}` = %s"
        
        try:
            # Add condition value to the values tuple
            values = list(data.values())
            values.append(condition_value)
            
            self.execute_query(sql, params=values)
            self.conn.commit()
            self.stats["success_num"] += 1
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error updating data: {e}")
            self.stats["failed_num"] += 1
            return False
            
    def reset_stats(self) -> None:
        """
        Reset the statistics for success and failure counts.
        """
        self.stats = {"success_num": 0, "failed_num": 0}
        
    def drop_all_data(self, dbname: str) -> bool:
        """
        Drop all tables in the specified database.
        
        :param dbname: Name of the database to clear
        :return: True if successful, False otherwise
        """
        try:
            # Use the specified database
            self.use_database(dbname)
            
            # Get all tables in the database
            tables = self.get_table_names(dbname)
            
            # Drop each table
            for table in tables:
                self.drop_table(table)
                self.logger.info(f"Dropped table: {table}")
                
            self.conn.commit()
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error dropping all data from {dbname}: {e}")
            return False

    def delete_if_exists(self, tablename: str, condition_column: str, condition_value: Any) -> bool:
        """
        Delete a row from the table if it exists based on a condition.

        :param tablename: Name of the table.
        :param condition_column: Column to use in WHERE clause.
        :param condition_value: Value for the condition.
        :return: True if successful, False otherwise
        """
        try:
            sql = f"DELETE FROM `{tablename}` WHERE `{condition_column}` = %s"
            self.execute_query(sql, params=(condition_value,))
            self.conn.commit()     
            return True
        except pymysql.Error as e:
            self.logger.error(f"Error deleting from {tablename}: {e}")
            return False

    def delete_by_fullpaths_batch(self, tablename: str, fullpaths: List[str]) -> bool:
        """
        Delete multiple rows by fullpath values in batch.
        
        :param tablename: Name of the table.
        :param fullpaths: List of fullpath values to delete.
        :return: True if successful, False otherwise
        """
        if not fullpaths:
            return True
            
        try:
            placeholders = ','.join(['%s'] * len(fullpaths))
            sql = f"DELETE FROM `{tablename}` WHERE `fullpath` IN ({placeholders})"
            
            success, result = self.execute_query(sql, params=tuple(fullpaths))
            if not success:
                self.logger.error(f"Error batch deleting from {tablename}: {result}")
                return False
                
            self.conn.commit()
            self.logger.info(f"Batch deleted {len(fullpaths)} records from {tablename}")
            return True
            
        except pymysql.Error as e:
            self.logger.error(f"Error batch deleting from {tablename}: {e}")
            return False

    def check_fullpaths_exist_batch(self, 
                                dbname: str, 
                                tablename: str, 
                                fullpaths: List[str], 
                                batch_size: int = 1000) -> Set[str]:
        """
        Check which fullpaths exist in the specified table in batches.
        
        :param dbname: Database name to check
        :param tablename: Table name to check
        :param fullpaths: List of fullpath values to check
        :param batch_size: Number of fullpaths to check in each batch (default: 1000)
        :return: Set of fullpaths that exist in the table
        """
        if not fullpaths:
            return set()
        
        existing_fullpaths = set()
        
        try:
            # Process in batches to avoid SQL query length limitations
            for i in range(0, len(fullpaths), batch_size):
                batch = fullpaths[i:i + batch_size]
                placeholders = ','.join(['%s'] * len(batch))
                
                table_path = f"`{dbname}`.`{tablename}`" if dbname else f"`{tablename}`"
                sql = f"SELECT `fullpath` FROM {table_path} WHERE `fullpath` IN ({placeholders})"
                
                success, results = self.execute_query(sql, params=tuple(batch), fetch='all')
                if success and results:
                    batch_existing = {row[0] for row in results}
                    existing_fullpaths.update(batch_existing)
                elif not success:
                    self.logger.error(f"Failed to check fullpaths in {dbname}.{tablename}: {results}")
                    
            self.logger.info(f"Found {len(existing_fullpaths)} existing fullpaths out of {len(fullpaths)} checked")
            return existing_fullpaths
            
        except Exception as e:
            self.logger.error(f"Error checking fullpaths existence: {e}")
            return set()

    def cleanup_resolved_exceptions(self, 
                               exception_table: str,
                               main_db_configs: Dict[str, Dict[str, Any]]) -> int:
        """
        Clean up exception records that have been resolved in main databases.
        
        :param exception_table: Exception table name in current database
        :param main_db_configs: Dictionary mapping models to their database configs
                            Format: {model: {'db_name': str, 'tables': List[str]}}
        :return: Number of cleaned up records
        """
        try:
            # Get all exception records grouped by model/table pattern
            sql = f"""
                SELECT `fullpath`, `reason`, `exception_context` 
                FROM `{exception_table}` 
                ORDER BY `fullpath`
            """
            success, exception_records = self.execute_query(sql, fetch='all')
            
            if not success or not exception_records:
                self.logger.info("No exception records found to cleanup")
                return 0
                
            self.logger.info(f"Found {len(exception_records)} exception records to check")
            
            # Group exception records by model (extracted from fullpath)
            model_groups = {}
            for record in exception_records:
                fullpath = record[0]
                # Extract model from fullpath pattern (assuming format contains model info)
                # You may need to adjust this based on your actual fullpath pattern
                try:
                    # Assuming fullpath pattern like: /path/to/model/table/file.ext
                    path_parts = fullpath.split('/')
                    if len(path_parts) >= 7:  # Based on your sort key x.split('/')[6]
                        model = path_parts[5]  # Adjust index based on your path structure
                        if model not in model_groups:
                            model_groups[model] = []
                        model_groups[model].append(fullpath)
                except Exception as e:
                    self.logger.warning(f"Could not extract model from fullpath {fullpath}: {e}")
                    continue
            
            resolved_fullpaths = set()
            
            # Check each model group against corresponding main databases
            for model, fullpaths in model_groups.items():
                if model not in main_db_configs:
                    self.logger.warning(f"No main database config found for model: {model}")
                    continue
                    
                config = main_db_configs[model]
                db_name = config.get('db_name')
                tables = config.get('tables', [])
                
                if not db_name or not tables:
                    self.logger.warning(f"Invalid config for model {model}: {config}")
                    continue
                    
                # Check each table in this model's database
                for table in tables:
                    try:
                        existing_fullpaths = self.check_fullpaths_exist_batch(
                            db_name, table, fullpaths
                        )
                        resolved_fullpaths.update(existing_fullpaths)
                        
                        if existing_fullpaths:
                            self.logger.info(f"Found {len(existing_fullpaths)} resolved records in {db_name}.{table}")
                            
                    except Exception as e:
                        self.logger.error(f"Error checking table {db_name}.{table}: {e}")
                        continue
            
            # Delete resolved exception records
            cleanup_count = 0
            if resolved_fullpaths:
                resolved_list = list(resolved_fullpaths)
                success = self.delete_by_fullpaths_batch(exception_table, resolved_list)
                
                if success:
                    cleanup_count = len(resolved_list)
                    self.logger.info(f"Successfully cleaned up {cleanup_count} resolved exception records")
                else:
                    self.logger.error("Failed to delete resolved exception records")
            else:
                self.logger.info("No resolved exception records found to cleanup")
                
            return cleanup_count
            
        except Exception as e:
            self.logger.error(f"Error during exception cleanup: {e}")
            return 0

    def get_records_by_wo_list(self, tablename: str, wo_list: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Batch query existing records by multiple wo values.
        
        :param tablename: Name of the table to query
        :param wo_list: List of wo values to search for
        :return: Dictionary mapping wo to record data {wo: record_dict}
        """
        if not wo_list:
            return {}
        
        try:
            # Build query with placeholders
            placeholders = ', '.join(['%s'] * len(wo_list))
            sql = f"SELECT * FROM `{tablename}` WHERE wo IN ({placeholders})"
            
            success, results = self.execute_query(sql, params=tuple(wo_list), fetch='all')
            
            if not success or not results:
                return {}
            
            # Get column names
            columns = [desc[0] for desc in self.cursor.description]
            
            # Convert to dictionary format
            records = {}
            for row in results:
                record = dict(zip(columns, row))
                wo = record.get('wo')
                if wo:
                    records[wo] = record
            
            return records
            
        except Exception as e:
            self.logger.error(f"Error querying records by wo list: {e}")
            return {}

    def update_records_by_wo_batch(self, tablename: str, records: List[Dict[str, Any]]) -> bool:
        """
        Batch update multiple records using wo as the key.
        
        :param tablename: Name of the table to update
        :param records: List of records to update, each must contain 'wo' field
        :return: True if all updates successful, False otherwise
        """
        if not records:
            return True
        
        try:
            success_count = 0
            
            for record in records:
                if 'wo' not in record:
                    self.logger.error("Record missing 'wo' field for update")
                    continue
                
                wo = record['wo']
                
                # Prepare update fields (exclude wo)
                update_fields = {k: v for k, v in record.items() if k != 'wo'}
                if not update_fields:
                    continue
                    
                # Build UPDATE statement
                set_clause = ", ".join([f"`{k}` = %s" for k in update_fields.keys()])
                sql = f"UPDATE `{tablename}` SET {set_clause} WHERE `wo` = %s"
                
                # Prepare values (update fields + wo for WHERE clause)
                values = list(update_fields.values()) + [wo]
                
                success, result = self.execute_query(sql, params=values)
                if success:
                    success_count += 1
                else:
                    self.logger.error(f"Failed to update record with wo={wo}: {result}")
            
            self.conn.commit()
            self.logger.info(f"Batch updated {success_count}/{len(records)} records")
            
            return success_count == len(records)
            
        except Exception as e:
            self.logger.error(f"Error in batch update by wo: {e}")
            return False

    def insert_records_batch_ignore_duplicates(self, tablename: str, records: List[Dict[str, Any]]) -> bool:
        """
        Batch insert records, ignoring duplicate wo values using INSERT IGNORE.
        
        :param tablename: Name of the table to insert into
        :param records: List of records to insert
        :return: True if successful, False otherwise
        """
        if not records:
            return True
            
        try:
            # Ensure all records have the same fields
            columns = list(records[0].keys())
            for record in records:
                if set(record.keys()) != set(columns):
                    self.logger.error("All records must have the same fields for batch insert")
                    return False
            
            # Build INSERT IGNORE statement
            columns_str = ", ".join([f"`{col}`" for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT IGNORE INTO `{tablename}` ({columns_str}) VALUES ({placeholders})"
            
            # Prepare values for batch insertion
            values = []
            for record in records:
                row_values = [record[col] for col in columns]
                values.append(row_values)
            
            # Execute batch insert
            self.cursor.executemany(sql, values)
            inserted_count = self.cursor.rowcount
            self.conn.commit()
            
            self.logger.info(f"Batch inserted {inserted_count} new records (duplicates ignored)")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in batch insert with ignore: {e}")
            return False


    def deduplicate_table_records(self, tablename: str, unique_columns: List[str] = None, keep: str = 'first') -> Tuple[bool, int]:
        """
        Remove duplicate records from a table based on specified columns.
        
        :param tablename: Name of the table to deduplicate
        :param unique_columns: List of column names to determine uniqueness. If None, uses all columns except id/auto-increment
        :param keep: Which duplicate to keep ('first', 'last', 'min_id', 'max_id')
        :return: Tuple of (success: bool, removed_count: int)
        """
        try:
            # Step 1: Get table schema to identify columns
            schema = self.get_table_schema(tablename)
            if not schema:
                self.logger.error(f"Failed to get schema for table {tablename}")
                return False, 0
            
            all_columns = list(schema.keys())
            
            # Step 2: Determine unique columns if not specified
            if unique_columns is None:
                # Use all columns except common auto-increment/id columns
                exclude_columns = {'id', 'auto_id', 'record_id', 'primary_id'}
                unique_columns = [col for col in all_columns if col.lower() not in exclude_columns]
            
            if not unique_columns:
                self.logger.error(f"No unique columns specified for deduplication of {tablename}")
                return False, 0
            
            self.logger.info(f"Deduplicating table {tablename} based on columns: {unique_columns}")
            
            # Step 3: Create a temporary table with unique records
            temp_table = f"{tablename}_temp_dedup"
            
            # Drop temp table if exists
            self.execute_query(f"DROP TABLE IF EXISTS `{temp_table}`")
            
            # Create temp table with same structure
            success, _ = self.execute_query(f"CREATE TABLE `{temp_table}` LIKE `{tablename}`")
            if not success:
                self.logger.error(f"Failed to create temporary table {temp_table}")
                return False, 0
            
            # Step 4: Determine which records to keep
            unique_columns_str = ", ".join([f"`{col}`" for col in unique_columns])
            all_columns_str = ", ".join([f"`{col}`" for col in all_columns])
            
            if keep == 'first':
                # Keep the record with minimum primary key/first occurrence
                if 'id' in all_columns:
                    order_by = "MIN(`id`)"
                    group_by_select = f"MIN(`id`) as id"
                else:
                    # If no id column, keep first by natural order
                    order_by = "1"
                    group_by_select = "1"
            elif keep == 'last':
                # Keep the record with maximum primary key/last occurrence
                if 'id' in all_columns:
                    order_by = "MAX(`id`)"
                    group_by_select = f"MAX(`id`) as id"
                else:
                    order_by = "1"
                    group_by_select = "1"
            else:
                # Default to first
                if 'id' in all_columns:
                    order_by = "MIN(`id`)"
                    group_by_select = f"MIN(`id`) as id"
                else:
                    order_by = "1"
                    group_by_select = "1"
            
            # Step 5: Get count before deduplication
            success, count_before = self.execute_query(f"SELECT COUNT(*) FROM `{tablename}`", fetch='one')
            count_before = count_before[0] if success and count_before else 0
            
            # Step 6: Insert unique records into temp table
            if 'id' in all_columns:
                # If there's an id column, use it to identify unique records
                insert_sql = f"""
                    INSERT INTO `{temp_table}` ({all_columns_str})
                    SELECT t1.* FROM `{tablename}` t1
                    INNER JOIN (
                        SELECT {unique_columns_str}, {group_by_select.replace('MIN(`id`)', 'MIN(id)').replace('MAX(`id`)', 'MAX(id)')}
                        FROM `{tablename}`
                        GROUP BY {unique_columns_str}
                    ) t2 ON {' AND '.join([f't1.`{col}` = t2.`{col}`' for col in unique_columns])}
                    AND t1.`id` = t2.id
                """
            else:
                # If no id column, use ROW_NUMBER() approach (MySQL 8.0+) or fallback method
                try:
                    insert_sql = f"""
                        INSERT INTO `{temp_table}` ({all_columns_str})
                        SELECT {all_columns_str} FROM (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY {unique_columns_str} ORDER BY {all_columns_str}) as rn
                            FROM `{tablename}`
                        ) ranked WHERE rn = 1
                    """
                except:
                    # Fallback for older MySQL versions
                    insert_sql = f"""
                        INSERT INTO `{temp_table}` ({all_columns_str})
                        SELECT DISTINCT {all_columns_str}
                        FROM `{tablename}`
                        GROUP BY {unique_columns_str}
                    """
            
            success, _ = self.execute_query(insert_sql)
            if not success:
                self.logger.error(f"Failed to insert unique records into {temp_table}")
                self.execute_query(f"DROP TABLE IF EXISTS `{temp_table}`")
                return False, 0
            
            # Step 7: Get count after deduplication
            success, count_after = self.execute_query(f"SELECT COUNT(*) FROM `{temp_table}`", fetch='one')
            count_after = count_after[0] if success and count_after else 0
            
            removed_count = count_before - count_after
            
            if removed_count <= 0:
                self.logger.info(f"No duplicates found in table {tablename}")
                self.execute_query(f"DROP TABLE IF EXISTS `{temp_table}`")
                return True, 0
            
            # Step 8: Replace original table with deduplicated data
            # Rename original table
            backup_table = f"{tablename}_backup_{int(datetime.now().timestamp())}"
            success, _ = self.execute_query(f"RENAME TABLE `{tablename}` TO `{backup_table}`")
            if not success:
                self.logger.error(f"Failed to backup original table {tablename}")
                self.execute_query(f"DROP TABLE IF EXISTS `{temp_table}`")
                return False, 0
            
            # Rename temp table to original name
            success, _ = self.execute_query(f"RENAME TABLE `{temp_table}` TO `{tablename}`")
            if not success:
                self.logger.error(f"Failed to rename temp table to {tablename}")
                # Restore backup
                self.execute_query(f"RENAME TABLE `{backup_table}` TO `{tablename}`")
                return False, 0
            
            # Drop backup table
            self.execute_query(f"DROP TABLE `{backup_table}`")
            
            self.conn.commit()
            self.logger.info(f"Successfully deduplicated table {tablename}: removed {removed_count} duplicate records")
            
            return True, removed_count
            
        except Exception as e:
            self.logger.error(f"Error during deduplication of table {tablename}: {e}")
            # Cleanup temp table if exists
            try:
                self.execute_query(f"DROP TABLE IF EXISTS `{temp_table}`")
            except:
                pass
            return False, 0
    
    def get_duplicate_count(self, tablename: str, unique_columns: List[str] = None) -> int:
        """
        Get the count of duplicate records without removing them.
        
        :param tablename: Name of the table to check
        :param unique_columns: List of column names to determine uniqueness
        :return: Number of duplicate records
        """
        try:
            if unique_columns is None:
                schema = self.get_table_schema(tablename)
                exclude_columns = {'id', 'auto_id', 'record_id', 'primary_id'}
                unique_columns = [col for col in schema.keys() if col.lower() not in exclude_columns]
            
            if not unique_columns:
                return 0
            
            unique_columns_str = ", ".join([f"`{col}`" for col in unique_columns])
            
            # Count total records
            success, total_count = self.execute_query(f"SELECT COUNT(*) FROM `{tablename}`", fetch='one')
            total_count = total_count[0] if success and total_count else 0
            
            # Count unique combinations
            success, unique_count = self.execute_query(
                f"SELECT COUNT(*) FROM (SELECT DISTINCT {unique_columns_str} FROM `{tablename}`) AS unique_records",
                fetch='one'
            )
            unique_count = unique_count[0] if success and unique_count else 0
            
            return total_count - unique_count
            
        except Exception as e:
            self.logger.error(f"Error counting duplicates in table {tablename}: {e}")
            return 0

    def update_records_by_fullpath_batch(self, tablename: str, records: List[Dict[str, Any]]) -> bool:
        """
        Batch update records using fullpath as the key.
        
        :param tablename: Name of the table to update
        :param records: List of records to update, each must contain 'fullpath' field
        :return: True if all updates successful, False otherwise
        """
        if not records:
            return True
        
        try:
            success_count = 0
            
            for record in records:
                if 'fullpath' not in record:
                    self.logger.error("Record missing 'fullpath' field for update")
                    continue
                
                fullpath = record['fullpath']
                
                # Prepare update fields (exclude fullpath since it's the key)
                update_fields = {k: v for k, v in record.items() if k != 'fullpath'}
                if not update_fields:
                    continue
                
                # Build UPDATE statement
                set_clause = ", ".join([f"`{k}` = %s" for k in update_fields.keys()])
                sql = f"UPDATE `{tablename}` SET {set_clause} WHERE `fullpath` = %s"
                
                # Prepare values (update fields + fullpath for WHERE clause)
                values = list(update_fields.values()) + [fullpath]
                
                success, result = self.execute_query(sql, params=values)
                if success:
                    success_count += 1
                else:
                    self.logger.error(f"Failed to update record with fullpath={fullpath}: {result}")
            
            self.conn.commit()
            self.logger.info(f"Batch updated {success_count}/{len(records)} records by fullpath")
            
            return success_count == len(records)
            
        except Exception as e:
            self.logger.error(f"Error in batch update by fullpath: {e}")
            return False

    def get_records_needing_update(self, tablename: str, new_fields: List[str], 
                                time_column: str, recently_days: int, 
                                batch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Get records that have NULL values in new fields and need updating.
        
        :param tablename: Table name
        :param new_fields: List of new field names that may need values
        :param time_column: Time column for filtering
        :param recently_days: Days to look back
        :param batch_size: Number of records to fetch at a time
        :return: List of records as dictionaries
        """
        try:
            # Build WHERE clause for NULL checks
            null_conditions = " OR ".join([f"`{field}` IS NULL OR `{field}` = ''" for field in new_fields])
            
            # Build query with time filter
            days_ago = datetime.now() - timedelta(days=recently_days)
            time_str = days_ago.strftime('%Y-%m-%d 00:00:00')
            
            sql = f"""
                SELECT * FROM `{tablename}` 
                WHERE ({null_conditions})
                AND `{time_column}` >= %s
                AND `fullpath` IS NOT NULL
                LIMIT %s
            """
            
            all_records = []
            offset = 0
            
            while True:
                # Use LIMIT and OFFSET for pagination
                paginated_sql = sql.replace("LIMIT %s", f"LIMIT {batch_size} OFFSET {offset}")
                success, results = self.execute_query(
                    paginated_sql.replace("OFFSET " + str(offset), ""), 
                    params=(time_str, batch_size), 
                    fetch='all'
                )
                
                if not success or not results:
                    break
                
                # Get column names
                columns = [desc[0] for desc in self.cursor.description]
                
                # Convert to list of dictionaries
                for row in results:
                    record = dict(zip(columns, row))
                    all_records.append(record)
                
                if len(results) < batch_size:
                    break
                
                offset += batch_size
            
            self.logger.info(f"Found {len(all_records)} records needing update in {tablename}")
            return all_records
            
        except Exception as e:
            self.logger.error(f"Error getting records needing update: {e}")
            return []

    def check_fullpaths_exist_batch(self, dbname: str, tablename: str, 
                                    fullpaths: List[str], batch_size: int = 1000) -> Set[str]:
        """
        Check which fullpaths exist in the specified table in batches.
        
        :param dbname: Database name to check
        :param tablename: Table name to check
        :param fullpaths: List of fullpath values to check
        :param batch_size: Number of fullpaths to check in each batch
        :return: Set of fullpaths that exist in the table
        """
        if not fullpaths:
            return set()
        
        existing_fullpaths = set()
        
        try:
            # Process in batches to avoid SQL query length limitations
            for i in range(0, len(fullpaths), batch_size):
                batch = fullpaths[i:i + batch_size]
                placeholders = ','.join(['%s'] * len(batch))
                
                table_path = f"`{dbname}`.`{tablename}`" if dbname else f"`{tablename}`"
                sql = f"SELECT `fullpath` FROM {table_path} WHERE `fullpath` IN ({placeholders})"
                
                success, results = self.execute_query(sql, params=tuple(batch), fetch='all')
                if success and results:
                    batch_existing = {row[0] for row in results}
                    existing_fullpaths.update(batch_existing)
                elif not success:
                    self.logger.error(f"Failed to check fullpaths in {dbname}.{tablename}: {results}")
            
            self.logger.info(f"Found {len(existing_fullpaths)} existing fullpaths out of {len(fullpaths)} checked")
            return existing_fullpaths
            
        except Exception as e:
            self.logger.error(f"Error checking fullpaths existence: {e}")
            return set()

    def get_new_schema_fields(self, tablename: str, rule_dict: Dict[str, Any]) -> List[str]:
        """
        Compare rule dictionary with current table schema to find new fields.
        
        :param tablename: Table name
        :param rule_dict: Parsing rules dictionary
        :return: List of new field names not in current schema
        """
        try:
            current_schema = self.get_table_schema(tablename)
            current_fields = set(current_schema.keys())
            
            # Extract fields from rule_dict that have dbtype defined
            rule_fields = set()
            for field, rules in rule_dict.items():
                if 'dbtype' in rules:
                    rule_fields.add(field)
            
            # Find new fields
            new_fields = list(rule_fields - current_fields)
            
            if new_fields:
                self.logger.info(f"Found new fields for {tablename}: {new_fields}")
            
            return new_fields
            
        except Exception as e:
            self.logger.error(f"Error finding new schema fields: {e}")
            return []
    

    def create_lock_table(self) -> bool:
        """
        Create table_locks table if not exists for managing table-level locks.
        """
        try:
            lock_schema = """
                `database_name` VARCHAR(64) NOT NULL,
                `table_name` VARCHAR(64) NOT NULL,
                `lock_holder` VARCHAR(128) NOT NULL,
                `lock_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `operation_type` VARCHAR(32) NOT NULL,
                PRIMARY KEY (`database_name`, `table_name`),
                INDEX `idx_lock_time` (`lock_time`)
            """
            
            success = self.create_table_if_nexists("table_locks", lock_schema)
            if success:
                self.logger.info("Table locks management table created successfully")
            return success
        except Exception as e:
            self.logger.error(f"Failed to create table_locks table: {e}")
            return False

    def cleanup_stale_locks(self) -> None:
        """
        Clean up locks older than 2 hours (assumed to be stale).
        """
        try:
            cleanup_sql = "DELETE FROM `table_locks` WHERE `lock_time` < NOW() - INTERVAL 2 HOUR"
            success, result = self.execute_query(cleanup_sql)
            if success:
                self.conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error cleaning up stale locks: {e}")

    def is_table_locked(self, database_name: str, table_name: str) -> bool:
        """
        Check if a table is currently locked.
        
        :param database_name: Name of the database
        :param table_name: Name of the table to check
        :return: True if table is locked, False otherwise
        """
        try:
            self.clear_table_locks_cache()
            check_sql = """
                SELECT `lock_holder`, `lock_time`, `operation_type` 
                FROM `table_locks` 
                WHERE `database_name` = %s AND `table_name` = %s
            """
            
            success, result = self.execute_query(check_sql, params=(database_name, table_name), fetch='one')
            if success and result:
                lock_holder, lock_time, operation_type = result
                self.logger.info(f"Table {database_name}.{table_name} is locked by {lock_holder} since {lock_time} for {operation_type}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking table lock status: {e}")
            return False

    def refresh_connection(self) -> None:
        """
        Force auto re-new connection
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            
            self.conn = pymysql.connect(
                host=self.dbsetting['db_host'],
                port=int(self.dbsetting['db_port']),
                user=self.dbsetting['db_user'],
                password=self.dbsetting['db_pwd'],
                autocommit=True
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            self.logger.error(f"Error refreshing connection: {e}")

    def clear_table_locks_cache(self) -> None:
        """
        clean query cache
        """
        try:
            if self.conn:
                self.conn.commit()
            
            self.execute_query("SELECT 1")  # Dummy query to reset connection state
            
        except Exception as e:
            self.logger.error(f"Error clearing cache: {e}")