#!/usr/bin/python3

import os, shutil, time, json, sys, argparse
from typing import Dict, Any

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.MySQLHelper import MySQLHelper
from core.Logger import Logger
from ParseEngine.engine.ParseEngineData import ParseEngineData

# define the process result / error code
from enum import Enum
from dataclasses import dataclass
from typing import Optional

class ErrorCode(Enum):
    NONE = "none"
    FOLDER_NOT_DEFINED = "folder_not_define"
    PARSING_ERROR = "parsing_error"
    DB_INSERT_FAIL = "db_insert_fail"
    PARSE_ENGINE_FAIL = "parser_engine_fail"
    UNKNOWN = "unknown"

@dataclass
class ProcessResult:
    success: bool
    message: str
    dbname: str
    sku:str
    test_station:str
    full_path:str
    error_code: ErrorCode = ErrorCode.NONE
    exception: Optional[Exception] = None


def load_dbsetting(path: str = 'db_setting.json') -> Dict[str, Any]:
    """
    Load database configuration from a JSON file.

    :param path: Path to db_setting.json file.
    :return: Parsed database configuration dictionary.
    """
    with open(path, 'r', encoding='UTF-8') as f:
        return json.load(f)

def build_path_rule_map(db_setting: dict, logg: Logger) -> dict:
    """
    Construct a folder-to-parsing-rule mapping from the db_setting configuration.

    This function traverses the 'sku_parser_rule' section in the db_setting dictionary and 
    extracts folder paths as keys. Each key maps to a dictionary containing:
    - the rule file path
    - the associated database name
    - the corresponding table name

    This mapping allows fast lookup of parsing rules and DB target information 
    based on the full path (usually from a .cap metadata file).

    :param db_setting: Dictionary loaded from db_setting.json, containing SKU/test_station configurations.
    :return: A dictionary mapping folder paths to their rule/dbname/tablename configuration.
             Example:
             {
                 "/mnt/FTP_log/.../PT-S": {
                     "rule": "sku_setting/k2x_n_respin_pts.json",
                     "dbname": "k2x_n_respin",
                     "tablename": "pts"
                 },
                 ...
             }
    """
    result = {}
    sku_rules = db_setting.get("sku_parser_rule", {})

    for sku_name, sku_info in sku_rules.items():
        dbname = sku_info.get("dbname")
        for test_station, station_info in sku_info.get("test_station", {}).items():
            folder = station_info.get("folder")
            if folder:
                if not folder.endswith('/'):
                    folder+='/'

                rule_filename = station_info.get("rule")
                with open(rule_filename, 'r', encoding='UTF-8') as f:
                    json_rules = json.load(f)

                result[folder] = {
                    "rule": json_rules,
                    "dbname": dbname,
                    "tablename": station_info.get("tablename")
                }
            else:
                logg.log_error(f'{sku_name} - {test_station} no folder the folder setting')                
    return result

def process_file(
    db: MySQLHelper,
    path_to_rule_dict: dict,
    logg: Logger,
    info_filename: str,
    info_org_path: str, 
    info_replace_path:str,
) -> ProcessResult:
    """
    Process a single metadata file (.txt) and insert parsed data into the database based on rule matching.

    This function will:
    1. Read the .txt file and extract SKU, test station, and original .cap filepath.
    2. Replace original root path with replacement path in filepath.
    3. Match the filepath against known rule folders in path_to_rule_dict.
    4. If a matching folder is found:
        - Use the configured rule to parse the file using ParseEngineData.
        - Insert parsed data into the corresponding database table.
    5. Log and return structured result for success/failure.

    :param db: MySQLHelper instance for performing DB operations.
    :param path_to_rule_dict: Dict mapping folder paths to rule/db/table config.
    :param logg: Logger instance for recording actions and errors.
    :param info_filename: Path to the .txt file containing SKU, test_station, and filepath.
    :param info_org_path: The original filepath root (to be replaced).
    :param info_replace_path: The new filepath root (to replace info_org_path).
    :return: ProcessResult(success: bool, message: str, dbname: str, sku: str, test_station: str, full_path: str, error_code: ErrorCode, exception: Optional[Exception])
    """
    try:
        with open(info_filename, 'r', encoding='UTF-8') as f:
            sku, test_station, filepath = [line.strip() for line in f.readlines()]
            filepath = filepath.strip().replace(info_org_path, info_replace_path)

            for folder, v in path_to_rule_dict.items():
                if folder in filepath:
                    db.use_database(v['dbname'])
                    # sync schema
                    schema_dict = {k: v2["dbtype"] for k, v2 in v['rule'].items() if "dbtype" in v2}
                    warnings, schema_updated = db.sync_table_schema(v['tablename'], schema_dict)
                    
                    parser = ParseEngineData()
                    if parser.parse_engine(filepath, v['rule']):
                        data = dict(parser.items())
                        try:
                            result = db.insert_or_update(v['tablename'], data)
                            if result is True:
                                return ProcessResult(True, "Insert success", v['dbname'], sku,test_station,filepath,ErrorCode.NONE)
                            else:
                                return ProcessResult(False, "DB insert failed", v['dbname'], sku,test_station,filepath,ErrorCode.DB_INSERT_FAIL, result)
                        except Exception as insert_except:
                            logg.log_insert_exception(filepath, "insert_data", data, insert_except)
                            return ProcessResult(False, "DB insert failed", v['dbname'], sku,test_station,filepath,ErrorCode.DB_INSERT_FAIL, insert_except)
                    else:
                        logg.log_parse_fail(filepath, reason="parser.parse_engine returned False")
                        return ProcessResult(False, "Parse engine failed", v['dbname'], sku,test_station,filepath,ErrorCode.PARSE_ENGINE_FAIL, None)
            return ProcessResult(False, "Folder not defined", v['dbname'], sku,test_station,filepath,ErrorCode.FOLDER_NOT_DEFINED, None)
    except KeyError as e:
        logg.log_error(f"Missing key during parsing: {e}")
        return ProcessResult(
            False,
            f"Missing key: {e}",
            v['dbname'] if 'v' in locals() and 'dbname' in v else None,
            sku if 'sku' in locals() else None,
            test_station if 'test_station' in locals() else None,
            filepath if 'filepath' in locals() else None,
            ErrorCode.PARSING_ERROR,
            str(e)
        )
    except Exception as e:
        logg.log_error(f'process_file exception: {e}')
        return ProcessResult(False, "Unhandled exception during file processing", v['dbname'] if 'v' in locals() and 'dbname' in v else None, sku if 'sku' in locals() else None, test_station if 'test_station' in locals() else None, filepath if 'filepath' in locals() else None, ErrorCode.UNKNOWN, str(e))

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

def insert_exception_row(exception_db: MySQLHelper, exception_table: str, proc_result: ProcessResult):
    """
    Insert data into the table only if a row with the same 'fullpath' does not already exist.

    :param exception_db: MySQLHelper instance for performing DB operations, specific in exception database.
    :param exception_table: Name of the table to insert records.
    :param proc_result: Data to insert, performed in class ProcessResult.
    """
    try:
        exception_db.use_database("exception")
        exception_db.insert_if_not_exist(
            exception_table,
            {
                "fullpath": proc_result.full_path,
                "reason": proc_result.error_code.value,
                "exception_context": proc_result.exception if proc_result.exception else ""
           }
        )
    except Exception as exc:
        logg.log_warn(f"Failed to add exception row for {proc_result.full_path}: {exc}")

def main() -> None:
    """
    Entry point: load configuration and process all SKUs and test stations.
    """
    logg = Logger()

    try:
        setting_file = 'db_setting/db_setting.json'
        dbsetting = load_dbsetting(setting_file)
        info_folder = dbsetting['info_folder']
        default_enable = dbsetting.get('default_enable', True)

        insert_error_folder = dbsetting['insert_error_folder']
        parse_failure_folder = dbsetting['parse_failure_folder']
        sku_undefined_folder = dbsetting['sku_undefined_folder']
        unknown_failure_folder = dbsetting['unknown_failure_folder']

        os.makedirs(insert_error_folder, mode=0o777, exist_ok=True)
        os.chmod(insert_error_folder, 0o777)
        os.makedirs(parse_failure_folder, mode=0o777, exist_ok=True)
        os.chmod(parse_failure_folder, 0o777)
        os.makedirs(sku_undefined_folder, mode=0o777, exist_ok=True)
        os.chmod(sku_undefined_folder, 0o777)
        os.makedirs(unknown_failure_folder, mode=0o777, exist_ok=True)
        os.chmod(unknown_failure_folder, 0o777)

        info_org_path = dbsetting['info_path_replace_org'] 
        info_replace_path = dbsetting['info_path_replace_dst']
        polling_sleep_secs = dbsetting['polling_sleep_secs']

        path_to_rule_dict = build_path_rule_map(dbsetting, logg)
        redo_fail_folder = None

        while True and redo_fail_folder is None:
            # /mnt/info/C519163214_PASS_Y_2_221310.cap.sync , writing file
            # /mnt/info/C519163214_PASS_Y_2_221310.cap.txt , finish the writing
            # <sku>\n<test_station>\n<filepath>\n
            
            """
            Default: process data in info_folder.
            If argument --redo [folderpath(s)] is provided, process fail files from the specified failure folder(s) instead of info_folder.
            In addition to fail_folder, any other folder can also be done, and could parse several folders all at once.
            For these data in redo folders, only parse once.
            e.g. --redo [/mnt/info/sku_undefined /mnt/info/insert_error]
            """
            parser = argparse.ArgumentParser(description="Monitor and process info files including parse and insert into database.")
            parser.add_argument(
                "--redo",
                nargs="*",
                help="Deal with failed files from specified folder(s). If no folder is specified, process all failure folders."
            )
            args = parser.parse_args()

            if args.redo is not None:
                if len(args.redo) > 0:
                    redo_fail_folder = args.redo
                else:
                    redo_fail_folder = "all"

            if redo_fail_folder:
                folders_to_scan = []
                if redo_fail_folder == "all":
                    # process files in failure folder
                    folders_to_scan = [
                        insert_error_folder,
                        parse_failure_folder,
                        sku_undefined_folder,
                        unknown_failure_folder
                    ]
                else:
                    # process files in specific folder
                    folders_to_scan = list(redo_fail_folder)
                files_in_info = []
                for folder in folders_to_scan:
                    if os.path.exists(folder):
                        files_in_info.extend([
                            os.path.abspath(os.path.join(folder, f))
                            for f in os.listdir(folder)
                            if f.endswith('.txt')
                        ])
            else:
                # process files in info_folder
                files_in_info = [
                    os.path.abspath(os.path.join(info_folder, f)) 
                    for f in os.listdir(dbsetting['info_folder'])
                    if f.endswith('.txt')
                ]

            if not files_in_info : 
                time.sleep(polling_sleep_secs)
                continue

            db = MySQLHelper(dbsetting)
            db.connect()
            # Setup exception DB and tables
            exception_db = MySQLHelper(dbsetting)
            exception_db.connect()
            create_exception_db(exception_db, dbsetting)

            for info_filename in files_in_info:
                proc_result = process_file(db, path_to_rule_dict,logg, info_filename,info_org_path, info_replace_path)
                if proc_result.success :
                    # logg.log_ok(f"{info_filename} Process PASS")
                    # Remove info_filename if process succeeded
                    os.remove(info_filename)
                    # Remove row from exception database if exists
                    try:
                        exception_db.use_database("exception")
                        if exception_db and proc_result.dbname:
                            exception_db.delete_if_exists(proc_result.dbname , "fullpath", proc_result.full_path)
                        exception_db.delete_if_exists("undefined_sku", "fullpath", proc_result.full_path)              
                    except Exception as exc:
                        logg.log_warn(f"Failed to remove exception row for {proc_result.full_path}: {exc}")
                elif proc_result.error_code == ErrorCode.FOLDER_NOT_DEFINED:
                    # move to sku_undefined_folder folder
                    dst = os.path.join(sku_undefined_folder, os.path.basename(info_filename))
                    shutil.move(info_filename, dst)
                    logg.log_warn(f"{info_filename} skipped: folder not defined in rule map")
                    logg.log_warn(f"  - {proc_result.sku} , {proc_result.test_station} , {proc_result.full_path}")
                    insert_exception_row(exception_db, "undefined_sku", proc_result)
                elif proc_result.error_code == ErrorCode.PARSING_ERROR:
                    dst = os.path.join(parse_failure_folder, os.path.basename(info_filename))
                    # check file exists
                    if not os.path.exists(dst):
                        logg.log_error(f"File move failed: {dst} does not exist after move.")
                        try:
                            exception_db.use_database("exception")
                            exception_table = "undefined_sku"
                            exception_db.insert_if_not_exist(
                                exception_table,
                                {
                                    "fullpath": proc_result.full_path,
                                    "reason": "file not exists",
                                    "exception_context": proc_result.exception if proc_result.exception else ""
                                }
                            )
                        except Exception as exc:
                            logg.log_warn(f"Failed to add exception row for {proc_result.full_path}: {exc}")
                    else:
                        # move to parse_failure_folder folder
                        shutil.move(info_filename, dst)
                        logg.log_error(f"{info_filename} failed due to unexpected exception: {proc_result.exception}")
                        logg.log_error(f"  - {proc_result.sku} , {proc_result.test_station} , {proc_result.full_path}")
                        insert_exception_row(exception_db, proc_result.dbname  if proc_result.dbname else "undefined_sku", proc_result)
                elif proc_result.error_code == ErrorCode.DB_INSERT_FAIL:
                    # move to insert_error_folder folder
                    dst = os.path.join(insert_error_folder, os.path.basename(info_filename))
                    shutil.move(info_filename, dst)
                    logg.log_error(f"{info_filename} failed: DB insert error")
                    logg.log_error(f"  - {proc_result.sku} , {proc_result.test_station} , {proc_result.full_path}")
                    insert_exception_row(exception_db, proc_result.dbname  if proc_result.dbname else "undefined_sku", proc_result)
                elif proc_result.error_code == ErrorCode.PARSE_ENGINE_FAIL:
                    # move to parse_failure_folder folder
                    dst = os.path.join(insert_error_folder, os.path.basename(info_filename))
                    shutil.move(info_filename, dst)
                    logg.log_warn(f"{info_filename} skipped: parse_engine returned False")
                    logg.log_warn(f"  - {proc_result.sku} , {proc_result.test_station} , {proc_result.full_path}")
                    insert_exception_row(exception_db, proc_result.dbname  if proc_result.dbname else "undefined_sku", proc_result)
                else:
                    # move to unknown_failure_folder
                    dst = os.path.join(unknown_failure_folder, os.path.basename(info_filename))
                    shutil.move(info_filename, dst)
                    logg.log_error(f"{info_filename} failed with unknown error: {proc_result.message}")
                    logg.log_error(f"  - {proc_result.sku} , {proc_result.test_station} , {proc_result.full_path}")
                    insert_exception_row(exception_db, proc_result.dbname  if proc_result.dbname else "undefined_sku", proc_result)
            db.close()
    except Exception as e:
        logg.log_error(f"Fatal error: {e}")


if __name__ == '__main__':
    main()
