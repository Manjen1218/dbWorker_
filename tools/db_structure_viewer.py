#!/usr/bin/python3

import json
import os
import sys

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.MySQLHelper import MySQLHelper  # Helper class for MariaDB operations
from core.Logger import Logger           # Logger for console and file output


def print_schema(helper: MySQLHelper, logger: Logger):
    """
    List all user-defined databases and their table schemas.

    :param helper: MySQLHelper instance to interact with the database.
    :param logger: Logger instance for formatted output and logging.
    """
    # Retrieve all databases, excluding system databases
    helper.cursor.execute("SHOW DATABASES")
    databases = [row[0] for row in helper.cursor.fetchall()
                 if row[0] not in ("information_schema", "performance_schema", "mysql", "sys")]

    for db in databases:
        logger.log_title(f"Database: {db}")
        helper.cursor.execute(f"USE `{db}`")

        # Retrieve all tables in the current database
        helper.cursor.execute("SHOW TABLES")
        tables = [row[0] for row in helper.cursor.fetchall()]
        if not tables:
            logger.log_warn("  No Tables")
            continue

        for table in tables:
            logger.log_sub_title(f"  Table: {table}")
            helper.cursor.execute(f"DESCRIBE `{table}`")
            schema = helper.cursor.fetchall()

            logger.log_info("    Fields:")
            for col in schema:
                field, dtype, null, key, default, extra = col
                logger.log_info(
                    f"    - {field:20} , {dtype:15} , Null: {null:3} , Key: {key:3} , Default: {default} , Extra: {extra}"
                )


if __name__ == "__main__":
    # Initialize logger with a specific log prefix
    logger = Logger(log_prefix="db_struct_viewer")
    logger.log_title("MariaDB Structure Listing")

    try:
        # Load database configuration from file
        with open("db_setting/db_setting.json", "r", encoding="UTF-8") as f:
            dbsetting = json.load(f)

        # Initialize DB helper and connect
        helper = MySQLHelper(dbsetting)
        helper.connect()

        # Execute schema printing
        print_schema(helper, logger)

        # Close the connection properly
        helper.close()
        logger.log_ok("Database schema listing completed.")

    except Exception as e:
        logger.log_error(f"Unexpected error occurred: {e}")
