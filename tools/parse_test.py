#!/usr/bin/python3
"""
Log Parser CLI Tool

This script allows parsing of a single `.cap` log file or batch parsing of a folder
of `.cap` files using JSON-defined rules. The parsed data can be printed or saved
to a CSV file.

Usage:
    - Parse a single file:
        python your_script.py --json_rule rules.json --cap file.cap

    - Parse a folder and output to CSV:
        python your_script.py --json_rule rules.json --folder logs/ --csv output.csv
"""

import os
import json
import sys
import argparse

# Add parent directory to sys.path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ParseEngine.engine.ParseEngineData import ParseEngineData
from ParseEngine.engine.ParseEngine import ParseEngine
from core.Logger import Logger


def single_file_test(capfilename: str, rules: dict, logger: Logger) -> None:
    """
    Parse a single .cap log file and print the parsed result.

    Args:
        capfilename (str): Path to the .cap file to parse.
        rules (dict): JSON parsing rules already loaded from file.
        logger (Logger): Logger instance for output.

    Returns:
        None
    """
    if not os.path.exists(capfilename):
        logger.log_error(f"Log file not found: {capfilename}")
        return

    logger.log_info(f"Parsing single file: {capfilename}")
    parser = ParseEngineData()
    success = parser.parse_engine(capfilename, rules)

    if not success:
        logger.log_error("Parse failed.")
    else:
        logger.log_ok("Parse succeeded. Parsed variables:")
        for k, v in parser.items():
            logger.log_info(f"  {k} = \"{v}\"")


def folder_parser_to_csv(sfolder: str, rules: dict, csvfilename: str, logger: Logger) -> None:
    """
    Parse all .cap files in a folder (recursively) and save results to a CSV.

    Args:
        sfolder (str): Root folder containing .cap files.
        rules (dict): JSON parsing rules already loaded from file.
        csvfilename (str): Path to the output CSV file.
        logger (Logger): Logger instance for output.

    Returns:
        None
    """
    logger.log_info(f"Parsing folder: {sfolder}")
    logger.log_info(f"Output CSV: {csvfilename}")
    folder_parser = ParseEngine()
    stats = folder_parser.parse_folder_to_csv(sfolder, rules, csvfilename)
    logger.log_ok(f"Folder parse complete: Total={stats['total_files']}, "
                  f"Success={stats['successful_parses']}, Fail={stats['failed_parses']}")

def parse_golden_files(golden_files: list, rule_path: str, logger: Logger):
    if not os.path.exists(rule_path):
        logger.log_error(f"Rule file not found: {rule_path}")
        return
    with open(rule_path, 'r', encoding='utf-8') as f:
        rules = json.load(f)
    # Only parse these fields:
    selective_fields = ["fullpath", "tbeg", "tend", "test_pc_ip"]
    for capfilename in golden_files:
        logger.log_info(f"Parsing golden file: {capfilename}")
        parser = ParseEngineData(selective_fields=selective_fields)
        success = parser.parse_engine(capfilename, rules)
        if not success:
            logger.log_error(f"Parse failed: {capfilename}")
        else:
            logger.log_ok(f"Parse succeeded: {capfilename}")
            for k, v in parser.items():
                logger.log_info(f"  {k} = \"{v}\"")

def main() -> None:
    """
    Main entry point for the script.

    Parses CLI arguments and invokes appropriate parsing function:
        - If --cap is provided: parse single file and print result.
        - If --folder and --csv are provided: parse folder and output to CSV.

    Constraints:
        - --json_rule is always required.
        - Either --cap OR (--folder AND --csv) must be specified.

    Returns:
        None
    """
    parser = argparse.ArgumentParser(description="Log Parser with JSON Rules")
    parser.add_argument('--json_rule', required=True, help="Path to JSON rule file")
    parser.add_argument('--cap', help="Path to a single .cap file")
    parser.add_argument('--folder', help="Folder path to recursively parse .cap files")
    parser.add_argument('--csv', help="Output CSV file path")

    args = parser.parse_args()
    logger = Logger(log_prefix="parse_engine")

    if not os.path.exists(args.json_rule):
        logger.log_error(f"JSON rule file not found: {args.json_rule}")
        return

    with open(args.json_rule, 'r', encoding='utf-8') as f:
        rules = json.load(f)

    if args.folder and args.csv:
        folder_parser_to_csv(args.folder, rules, args.csv, logger)
    elif args.cap and not args.folder and not args.csv:
        single_file_test(args.cap, rules, logger)
    else:
        logger.log_error("Invalid argument combination.")
        logger.log_info("Must provide either:")
        logger.log_info("  --cap [file] OR (--folder [dir] AND --csv [file])")
        return

if __name__ == "__main__":
    
    # ./ParseTest.py --json_rule ./sku_setting/k2v5_yrk10_pt.json --folder /mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-YRK10/PT --csv ./yrk10.csv
    # ./ParseTest.py --json_rule ./sku_setting/k2v5_yrk10_pt.json --cap /mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-YRK10/PT/20250305/FAIL/C509170441_FAIL_N_1_134322.cap
    # ./ParseTest.py --json_rule ./sku_setting/k2v5_yrk10_pt.json --cap /mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-YRK10/PT/20250507/PASS/C509170414_PASS_N_4_140030.cap
    
    
    gold_files = [
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-YRK10/PT/20250516/PASS/C514074367_PASS_Y_2_132707.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-JRD03-R/PT/20250520/PASS/C506089026_PASS_Y_1_063822.cap'
        '/mnt/FTP_log/-APL/EN1QQNSN-0723-APL_K2V6-AIC10/PT/20250520/PASS/C519008203_PASS_Y_5_015526.cap',
        '/mnt/FTP_log/-APL/EN1QQNSN-0723-APL_K2V6-AIC20/PT/20250520/PASS/C519186959_PASS_Y_1_055834.cap',
        '/mnt/FTP_log/-APL/EN1QQNSN-0723-APL_K2V6-JRD10/PT/20250520/PASS/C518122716_PASS_Y_1_043303.cap',
        '/mnt/FTP_log/-APL/EN1QQNSN-0723-APL_K2V6-JRD10/PT-S/20250520/PASS/C518122716_PASS_Y_3_PTS_063826.cap',
        '/mnt/FTP_log/-APL/EN4VFNSN1-0217-APL_K2X-N_respin/PT/20250520/PASS/C519004311_PASS_Y_6_081435.cap',
        '/mnt/FTP_log/-APL/EN4VFNSN1-0217-APL_K2X-N_respin/PT-S/20250520/PASS/C519004311_PASS_Y_1_091524.cap',
        '/mnt/FTP_log/-APL/EN4VFNSN1-0217-APL_K2X-N2/PT/20250519/PASS/C517289918_PASS_Y_2_104231.cap',
        '/mnt/FTP_log/-APL/EN4VFNSN1-0217-APL_K2X-N2/PT-S/20250519/PASS/C517289918_PASS_Y_1_165322.cap',
        '/mnt/FTP_log/-APL/EN2CFNSN1-0519-APL_K2V4-N4/PT-S/20240306/FAIL/C348292455_FAIL_N_1_PTS_155536.cap',
        '/mnt/FTP_log/-APL/EN2CFNSN1-0519-APL_K2V4-NA2/PT/20250323/PASS/C510186869_PASS_Y_1_041411.cap',
        '/mnt/FTP_log/-APL/EN2CFNSN1-0519-APL_K2V4-NA2/PT-S/20250323/PASS/C510186869_PASS_Y_2_053148.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-YRK10/PDL-P/20250505/PASS/C509170283_PASS_Y_6_112332.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-JRD03-R/PDL-P/20250218/PASS/C453000985_PASS_N_4_162210.cap',
        '/mnt/FTP_log/-APL/EN1QQNSN-0723-APL_K2V6-AIC10/PDL-P/20250603/PASS/C519128350_PASS_Y_2_202558.cap',
        '/mnt/FTP_log/-APL/EN1QQNSN-0723-APL_K2V6-AIC20/PDL-P/20250310/PASS/C502086534_PASS_Y_2_174936.cap',
        '/mnt/FTP_log/-APL/EN1QQNSN-0723-APL_K2V6-JRD10/PDL-P/20250605/PASS/C522122024_PASS_Y_6_073749.cap',
        '/mnt/FTP_log/-APL/EN4VFNSN1-0217-APL_K2X-A-32G-T2/PT/20250521/PASS/A139106737_PASS_N_1_133136.cap',
        '/mnt/FTP_log/-APL/EN4VFNSN1-0217-APL_K2X-A-32G-T2/PT-S/20250514/PASS/A130128091_PASS_N_1_163536.cap',
        '/mnt/FTP_log/-APL/EN4VFNSN1-0217-APL_K2X-A-32G-T2A/PT/20250410/PASS/C509171910_PASS_Y_1_105411.cap',
        '/mnt/FTP_log/-APL/EN4VFNSN1-0217-APL_K2X-A-32G-T2A/PT-S/20250408/PASS/C509172034_PASS_Y_1_184221.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-JRD01-R/PT/20250219/PASS/C507086398_PASS_Y_7_170344.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-JRD03-R/PT-S/20250609/PASS/C523200203_PASS_Y_3_PTS_133258.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-JRD01-R/PT-S/20250315/PASS/C508077054_PASS_Y_2_PTS_045514.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-JRD02-R/PT/20250608/PASS/C521210860_PASS_Y_2_055652.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-JRD02-R/PT-S/20250608/PASS/C521210860_PASS_Y_4_PTS_113429.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-JRD03-R/PT-S/20250514/FAIL/C518163755_FAIL_Y_2_PTS_042457.cap',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-JRD03-R/BAKING/20240522/PASS/C420172994_PASS_Y_7_182106.txt',
        '/mnt/FTP_log/-APL/EN1DQNSN1-0821-APL_K2V5-YRK10/PT/20250703/PASS/C525055198_PASS_Y_8_165137.cap'
    ]
    
    logger = Logger(log_prefix="parse_engine")
    with open("./ParseEngine/sku_setting/mcc.json", 'r', encoding='UTF-8') as f:
        rule = json.load (f)
    single_file_test('/home/elisa/new/dbWorker/ParseEngine/PT_Y (1).mcc', rule, logger)

    # rule_path = "./ParseEngine/sku_setting/k2v4_n3/k2v4_n3_pt.json"
    # parse_golden_files(gold_files, rule_path, logger)
    # main()
