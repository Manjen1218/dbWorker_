#!/usr/bin/python3
import argparse
import json
import sys
import os

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.MySQLHelper import MySQLHelper
from core.Logger import Logger

class Reporter:
    def __init__(self, helper, logger):
        """
        Initialize the database reporter.

        Parameters:
            helper (MySQLHelper): Instance for interacting with the MySQL database.
            logger (Logger): Logger instance for structured output.
        """
        self.helper = helper
        self.logger = logger

    def output_yield_rate(self, dbname, tablename, wo_val, output, columns):
        """
        Calculate and show SN count/pass/fail/FPY statistics of the specific WO.

        Parameters:
            dbname (str): Name of database to look up.
            tablename (str):Name of table to look up.
            wo_val (str): Target work order.
            output (str): Information of the WO, including sku, earliest, latest time
            columns (str): Columns of interest in table.
        """
        if all(col in columns for col in ("sn", "is_y", "bname")):
            # Total SN Number
            total_sn = self.helper.count_distinct(
                dbname=dbname,
                tablename=tablename,
                column="sn",
                condition_column="wo",
                condition_value=wo_val,
                additional_conditions={"is_y": True}
            )

            # Final Pass SN Number
            pass_sn = self.helper.count_distinct(
                dbname=dbname,
                tablename=tablename,
                column="sn",
                condition_column="wo",
                condition_value=wo_val,
                additional_conditions={"is_y": True},
                null_conditions=["err_msg"]
            )
            # Final Fail SN Number
            fail_sn = total_sn - pass_sn

            # First Pass Yield Rate
            sql = f"""
                SELECT 
                    COUNT(DISTINCT sn) 
                    FROM `{dbname}`.`{tablename}` 
                    WHERE sn IN 
                        (SELECT 
                            sn 
                        FROM `{dbname}`.`{tablename}` 
                        WHERE wo=%s 
                        GROUP BY sn HAVING COUNT(sn) > 1) 
                    AND is_y=1 AND err_msg IS NULL 
                    ORDER BY sn, tbeg
                """
            self.helper.cursor.execute(sql, (wo_val,))
            second_pass = self.helper.cursor.fetchone()[0]
            fpy = pass_sn - second_pass

            output += f"\nSN: {total_sn} \tPASS: {pass_sn} \tFAIL: {fail_sn} \tFPY: {fpy}" 
            output += f"\nSN: {round(total_sn/total_sn*100, 2)}% \tPASS: {round(pass_sn/total_sn*100, 2)}% \tFAIL: {round((fail_sn)/total_sn*100, 2)}% \tFPY: {round(fpy/total_sn*100, 2)}%"
        self.logger.log_sub_title("Yield Rate")
        self.logger.log_info(output)

    def output_average_yield_rate(self, dbname, tablenames, wo_val, output):
        """
        Calculate and display the average yield rate for two tables in the same database for a given WO.

        Parameters:
            dbname (str): Database name.
            tablenames (list): List of two table names.
            wo_val (str): Work order value.
            output (str): Information of the WO, including sku, earliest, latest time.
        """
        total_sns = []
        pass_sns = []
        fail_sns = []
        fpy_sns = []

        output += f"\n{'Table':<10}{'Total SN':>10}{'Pass SN':>10}{'Pass Rate':>15}{'Fail SN':>10}{'Fail Rate':>14}{'FPY SN':>10}{'FPY Rate':>14}\n"
        for tablename in tablenames:
            # Total SN Number
            total_sn = self.helper.count_distinct(
                dbname=dbname,
                tablename=tablename,
                column="sn",
                condition_column="wo",
                condition_value=wo_val,
                additional_conditions={"is_y": True}
            )
            total_sns.append(total_sn)

            # Final Pass SN Number
            pass_sn = self.helper.count_distinct(
                dbname=dbname,
                tablename=tablename,
                column="sn",
                condition_column="wo",
                condition_value=wo_val,
                additional_conditions={"is_y": True},
                null_conditions=["err_msg"]
            )
            pass_sns.append(pass_sn)
            pass_rate = round(pass_sn / total_sn * 100, 2) if total_sn else 0

            # Final Fail Sn Number
            fail_sn = total_sn - pass_sn
            fail_sns.append(fail_sn)
            fail_rate = round(fail_sn / total_sn * 100, 2) if total_sn else 0

            # First Pass Yield Rate
            sql = f"""
                SELECT 
                    COUNT(DISTINCT sn) 
                    FROM `{dbname}`.`{tablename}` 
                    WHERE sn IN 
                        (SELECT 
                            sn 
                        FROM `{dbname}`.`{tablename}` 
                        WHERE wo=%s 
                        GROUP BY sn HAVING COUNT(sn) > 1) 
                    AND is_y=1 AND err_msg IS NULL 
                    ORDER BY sn, tbeg
                """
            self.helper.cursor.execute(sql, (wo_val,))
            second_pass = self.helper.cursor.fetchone()[0]
            fpy = pass_sn - second_pass
            fpy_sns.append(fpy)
            fpy_rate = round(fpy/total_sn*100, 2) if total_sn else 0

            output += f"{tablename:<10}{total_sn:>10}{pass_sn:>10}{pass_rate:>14.2f}%{fail_sn:>10}{fail_rate:>14.2f}%{fpy:>10}{fpy_rate:>14.2f}%\n"
        if len(total_sns) >= 2:
            avg_total = sum(total_sns)/len(total_sns)
            avg_pass = sum(pass_sns)/len(total_sns)
            avg_pass_rate = round((sum(pass_sns) / sum(total_sns))*100, 2)
            avg_fail = sum(fail_sns)/len(total_sns)
            avg_fail_rate = round((sum(fail_sns) / sum(total_sns))*100, 2)
            avg_fpy = sum(fpy_sns)/len(total_sns)
            avg_fpy_rate = round((sum(fpy_sns) / sum(total_sns))*100, 2)
            output += f"{'Average':<10}{avg_total:>10}{avg_pass:>10}{avg_pass_rate:>14.2f}%{avg_fail:>10}{avg_fail_rate:>14.2f}%{avg_fpy:>10}{avg_fpy_rate:>14.2f}%"
        else:
            output += "Warning: Less than two tables found for this WO."

        self.logger.log_sub_title("Average Yield Rate")
        self.logger.log_info(output)

    
    def output_fail_distribution(self, dbname, tablename, wo_val, total_sn):
        """
        Aggregates and logs fail distribution by error message for a given WO. 
        Shows fail count and percentage for each error message.

        Parameters:
            dbname (str): Name of database to look up.
            tablename (str):Name of table to look up.
            wo_val (str): Target work order.
            total_sn (int): Total SN number.
        """
        sql = f"""
            SELECT 
                err_msg, 
                COUNT(*) AS err_cnt 
            FROM (
                SELECT 
                    DISTINCT sn, 
                    err_msg, 
                    ROW_NUMBER() OVER (PARTITION BY sn ORDER BY tbeg) 
                AS row
                FROM `{dbname}`.`{tablename}` 
                WHERE wo=%s AND err_msg IS NOT NULL 
                ORDER BY sn, tbeg
            ) AS sub 
            WHERE row=1 
            GROUP BY err_msg
        """
        self.helper.cursor.execute(sql, (wo_val,))
        result = self.helper.cursor.fetchall()
        output_fail = f"{dbname}.{tablename}\n{'Error Msg':<70}{'Fail':<10}{'Percentage':>5}"
        for err_msg, err_cnt in result:
            output_fail += f"\n{err_msg:<70}{err_cnt:<10}{round(err_cnt/total_sn*100, 2):>5.2f}%"
        self.logger.log_sub_title("Fail Disrtibution")
        self.logger.log_info(output_fail)

    def output_jig_analysis(self, dbname, tablename, wo_val):
        """
        Calculates and logs data count, fail rate (first fail), max, and average SoC temperature for each JIG.

        Parameters:
            dbname (str): Name of database to look up.
            tablename (str):Name of table to look up.
            wo_val (str): Target work order.
        """
        sql = f"""
            SELECT 
                COUNT(*) AS total_cnt 
            FROM `{dbname}`.`{tablename}` 
            WHERE wo=%s
        """
        self.helper.cursor.execute(sql, (wo_val,))
        total_cnt = self.helper.cursor.fetchone()[0]

        sql = f"""
            SELECT 
                jig, 
                COUNT(*) AS jig_cnt,
                SUM(CASE WHEN err_msg IS NOT NULL THEN 1 ELSE 0 END) AS fail_cnt,
                MAX(soc_temp_max) AS max_temp,
                AVG(soc_temp_max) AS avg_temp
            FROM `{dbname}`.`{tablename}` 
            WHERE wo=%s 
            GROUP BY jig
        """
        self.helper.cursor.execute(sql, (wo_val,))
        jig_results = self.helper.cursor.fetchall()

        output_jig = f"{dbname}.{tablename}"
        output_jig += f"\n{'WO':<20}{'Stage':10}{'JIG':<20}{'Data Count':>8}{'Fail':>8}{'Fail Rate':>12}{'Max Temp':>12}{'Avg Temp':>12}"
        for jig, jig_cnt, fail_cnt, max_temp, avg_temp in jig_results:
            max_temp_fmt = max_temp if max_temp is not None else 0
            avg_temp_fmt = avg_temp if avg_temp is not None else 0
            output_jig += f"\n{str(wo_val):<20}{tablename:<10}{str(jig):<20}{jig_cnt:>8}{fail_cnt:>8}{round(fail_cnt / total_cnt * 100, 3):>11.3f}%{max_temp_fmt:>12.0f}{avg_temp_fmt:>12.2f}"
        self.logger.log_sub_title("JIG Analysis")
        self.logger.log_info(output_jig)

    def output_sn_history(self, sn_target, databases):
        """
        Searches all databases and tables for SN history, and logs formatted SN, WO, stage, test begin/end, and error message for each record.
        Handles and logs errors for missing tables or databases and missing target SN.

        Parameters:
            dbname (str): Name of database to look up.
            tablename (str):Name of table to look up.
            wo_val (str): Target work order.
        """
        output_sn = f"\n{'SN':<15}{'WO':<20}{'Stage':<10}{'Test Begin':<25}{'Test End':<25}{'Error Msg':<10}"
        sn_found = False
        for dbname in databases:
            if dbname in ["information_schema", "mysql", "performance_schema", "sys"]:
                continue
            try:
                self.helper.use_database(dbname)
                self.helper.cursor.execute("SHOW TABLES")
                tables = [row[0] for row in self.helper.cursor.fetchall()]
                for tablename in tables:
                    try:
                        sql = f"""
                            SELECT 
                                sn, 
                                wo, 
                                tbeg, 
                                tend, 
                                err_msg
                            FROM `{dbname}`.`{tablename}`
                            WHERE sn LIKE %s
                            ORDER BY sn, tbeg
                        """
                        like_value = f"%{sn_target}%"
                        self.helper.cursor.execute(sql, (like_value,))
                        results = self.helper.cursor.fetchall()
                        if results:
                            sn_found =True  
                            for sn, wo, tbeg, tend, err_msg in results:
                                err_msg_fmt = err_msg if err_msg is not None else "PASS"
                                output_sn += f"\n{str(sn):<15}{wo:<20}{tablename:<10}{str(tbeg):<25}{str(tend):<25}{err_msg_fmt:<10}"
                            sku = dbname
                    except Exception as e_table:
                        self.logger.log_warn(f"Error in table `{tablename}` of DB `{dbname}`: {e_table}")
            except Exception as e_db:
                self.logger.log_error(f"Error switching to database `{dbname}`: {e_db}")    
        if sn_found:
            self.logger.log_sub_title("SN History")
            self.logger.log_info(f"SN: {sn_target} -> SKU: {sku}"+output_sn)
        else:
            self.logger.log_warn(f"Cannot find the target SN: {sn_target}")


def search_in_db(helper: MySQLHelper, logger: Logger, args: argparse.Namespace) -> None:
    """
    Search for a specific Work Order (WO) across all databases and tables, also show SN counts and yield rate, FPY analysis.
        If `--fail` is enabled, show fail distribution of err_msg and percentage.
        If `--jig` is enabled, show JIg analysis of fail rate and show max and average of SoC_temp_max.
        IF `--avg` is eneabled, show average SN total/pass/fail/fpy rate for several tables.
    Search for a specific SN across all databases and tables, also show SKU and the information of SN's each log.
    Handles and logs errors for missing tables or databases and missing target.

    Parameters:
        helper (MySQLHelper): Instance for interacting with the MySQL database.
        logger (Logger): Logger instance for structured output.
        args (argparse): arguments for target information.
            - wo (str): Target Work Order string to search (partial match allowed), then display the detail of SN count/pass/fail/FPY statistics.
            - fail (bool): If True, show the detail of fail distribution statistics such as error msg and its percentage of target WO.
            - jig (bool): If True, show the detail of JIG anlaysis fail rate and max/avg SoC temperature of target WO.
            - sn (str): Target SN string to search (partial match allowed), then display the detail of all its history such as Sku/stage/test_begin/test_end/error_msg. 
    """
    wo_target = args.wo
    fail_distribution = args.fail
    jig_analysis = args.jig
    sn_target = args.sn
    avg_detail = args.avg

    reporter = Reporter(helper, logger)

    helper.cursor.execute("SHOW DATABASES")
    databases = [row[0] for row in helper.cursor.fetchall()]
    results = None

    if wo_target:
        logger.log_title("WO Lookup Started")
        for dbname in databases:
            if dbname in ["information_schema", "mysql", "performance_schema", "sys"]:
                continue

            try:
                helper.use_database(dbname)
                helper.cursor.execute("SHOW TABLES")
                tables = [row[0] for row in helper.cursor.fetchall()]
                tables_with_wo = []
                for tablename in tables:
                    try:
                        helper.cursor.execute(f"SHOW COLUMNS FROM `{tablename}`")
                        columns = [row[0] for row in helper.cursor.fetchall()]

                        if wo_target and "wo" in columns and "tbeg" in columns and "tend" in columns:
                            # Count tables that exist target WO
                            sql = f"SELECT 1 FROM `{tablename}` WHERE wo=%s LIMIT 1"
                            helper.cursor.execute(sql, (wo_target,))
                            if helper.cursor.fetchone():
                                tables_with_wo.append(tablename)
                            
                            sql = f"""
                                SELECT 
                                    wo, 
                                    MIN(tbeg) as earliest, 
                                    MAX(tend) as latest FROM `{tablename}` 
                                WHERE wo LIKE %s AND is_y = TRUE
                                GROUP BY wo
                                ORDER BY wo, sn, tbeg
                            """
                            like_value = f"%{wo_target}%"
                            helper.cursor.execute(sql, (like_value,))
                            results = helper.cursor.fetchall()

                            for wo_val, tbeg, tend in results:
                                output = f"{dbname}.{tablename} -> WO: {wo_val}, Earliest: {tbeg}, Latest: {tend}"
                                reporter.output_yield_rate(dbname, tablename, wo_val, output, columns)
                                
                                # Show fail distribution of target WO
                                if fail_distribution and results:
                                    # Total SN Number
                                    total_sn = helper.count_distinct(
                                        dbname=dbname,
                                        tablename=tablename,
                                        column="sn",
                                        condition_column="wo",
                                        condition_value=wo_val,
                                        additional_conditions={"is_y": True}
                                    )
                                    reporter.output_fail_distribution(dbname, tablename, wo_val, total_sn)
                                
                                # Show jig analysis of target WO
                                if jig_analysis and results:
                                    reporter.output_jig_analysis(dbname, tablename, wo_val)
                    except Exception as e_table:
                        logger.log_warn(f"Error in table `{tablename}` of DB `{dbname}`: {e_table}")
                
                # Show average yield rate of target WO
                if avg_detail and len(tables_with_wo) >= 2:
                    reporter.output_average_yield_rate(dbname, tables_with_wo, wo_target, output)
            except Exception as e_db:
                logger.log_error(f"Error switching to database `{dbname}`: {e_db}")
        
        if results is None:
            logger.log_warn(f"Cannot find the target WO: {wo_target}")

    # Show data of target SN
    if sn_target:
        logger.log_title("SN Lookup Started")
        reporter.output_sn_history(sn_target, databases)

def search_tmp(helper: MySQLHelper, logger: Logger, args: argparse.Namespace) -> None:
    reporter = Reporter(helper, logger)

    helper.cursor.execute("SHOW DATABASES LIKE 'k%'")
    databases = [row[0] for row in helper.cursor.fetchall()]
    databases.append("global_db")
    
    for dbname in databases:
        helper.use_database(dbname)
        helper.cursor.execute("SHOW TABLES")
        tables = [row[0] for row in helper.cursor.fetchall()]
        for tablename in tables:

            #sql = f"""ALTER TABLE `{tablename}` MODIFY tbeg TIMESTAMP DEFAULT '0000-00-00 00:00:00';"""
            #helper.cursor.execute(sql)
            helper.cursor.execute(f"SHOW COLUMNS FROM `{tablename}`")
            columns = [row[0] for row in helper.cursor.fetchall()]
            if "fullpath" in columns:
                sql = f"""
                            SELECT fullpath, tbeg, tend
                            FROM `{tablename}`
                            WHERE tend <= tbeg OR tbeg="0000-00-00 00:00:00" OR tend="0000-00-00 00:00:00" OR total_td is Null
                        """
            else:
                sql = f"""
                            SELECT sku_id, tbeg, tend
                            FROM `{tablename}`
                            WHERE tend <= tbeg OR tbeg="0000-00-00 00:00:00" OR tend="0000-00-00 00:00:00"
                        """
            helper.cursor.execute(sql)
            results = helper.cursor.fetchall()
            if results:
                for fullpath, tbeg, tend in results:
                    print(fullpath,',\t',tbeg,',\t',tend)
            if dbname != "global_db":
                sql = f"""SELECT fullpath, sn, COUNT(fullpath) AS cnt
                        FROM `{tablename}`
                        GROUP BY fullpath, sn
                        HAVING COUNT(fullpath) > 1
                        ORDER BY cnt DESC;"""
                helper.cursor.execute(sql)
                results = helper.cursor.fetchall()
                if results:
                    for fullpath, sn, cnt in results:
                        print(fullpath,',\t',sn,',\t',cnt)
            if 'mcc_check' in columns:
                sql = f"""SELECT count(*) FROM `{tablename}` where (mcc_check = 'PASS' and mcc_err_check != 'NA') or (mcc_check != 'NA' and mcc_check_time = '' and mcc_upload_time = '')"""
                helper.cursor.execute(sql)
                results = helper.cursor.fetchone()
                if results:
                    print(dbname, tablename, results[0])
    print("-------------------------------------------------------------------------------------------------------------------------------------------------")
    # exception
    helper.use_database('exception')
    helper.cursor.execute("SHOW TABLES")
    tables = [row[0] for row in helper.cursor.fetchall()]
    for tablename in tables:
        helper.cursor.execute(f"SHOW COLUMNS FROM `{tablename}`")
        columns = [row[0] for row in helper.cursor.fetchall()]
        sql = f"""
                    SELECT *
                    FROM `{tablename}`
                """
        helper.cursor.execute(sql)
        results += helper.cursor.fetchall() if not None else None
    print("total count:",len(results))
    for fullpath, reason, exception_context in results:
        print(fullpath,',\t',reason,',\t',exception_context)
    

def main():
    parser = argparse.ArgumentParser(description="Search WO or SN in all databases and optionally show details. (Two functionality is separate) " \
        "Function 1: Look up specific WO, including its yield rate, fail distributon(`--fail`), and JIG analysis(`--JIG`). " \
        "Function 2: Look up specific SN about its history log parse. ")
    parser.add_argument("--wo", "-wo", help="Work Order to search for")
    parser.add_argument("--fail", "-f", action="store_true", help="Show detail of fail distribution statistics of the WO")
    parser.add_argument("--jig", "-jig", action="store_true", help="Show detail of JIG anlaysis fail rate and max/avg SoC temperature of the WO")
    parser.add_argument("--avg", "-avg", action="store_true", help="Show detail of average yield rate of target WO")
    parser.add_argument("--sn", "-sn", help="Target SN to search for its log parse history")
    args = parser.parse_args()

    # Check requirement one of WO/SN argument
    '''
     if not args.wo and not args.sn:
        print("### Error: At least one of --wo or --sn is required. ###")
        parser.print_help()
        exit(1)
    '''
   

    # Intialize logger
    logger = Logger()

    try:
        # Load database configuration from file
        with open("db_setting/db_setting.json", "r", encoding="UTF-8") as f:
            dbsetting = json.load(f)

        # Initialize DB helper and connect
        db = MySQLHelper(dbsetting)
        db.connect()

        # Execute database searching
        #search_in_db(db, logger, args)
        search_tmp(db, logger, args)
        db.close()

         # Close the connection properly
        logger.log_ok("WO Lookup completed.")

    except Exception as e:
        logger.log_error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()