# dbWorker — Database & Log-Parsing Toolkit

A **clean-room**, generic example service, reimplementation inspired by Accton Technology project, with no confidential content.
This project is **not derived from any company code or proprietary software**.
It is designed purely as an open-source educational example.

`dbWorker` is a collection of Python 3 utilities that **parse production‐line log files**, load them into a MariaDB / MySQL database and provide rich analytics & reporting commands.

It was designed for factory environments where large volumes of *`.cap`* (capture) files are continuously produced by multiple SKUs / test-stations and must be converted into structured data in near-real-time.

---

## 1.  Repository Layout

```text
┬ dbWorker/                 ← project root
├─ core/                    ← reusable utilities
│  ├─ Logger.py             · unified colourful logger (console + file)
│  └─ MySQLHelper.py        · thin wrapper around pymysql cursor/connection
│
├─ ParseEngine/             ← generic rule-based parser
│  ├─ engine/               · ParseEngine & ParseEngineData classes
│  └─ sku_setting/          · JSON rule files grouped by SKU / station
│
├─ workers/                 ← long-running or batch ingestion services
│  ├─ worker_monitor.py     · daemon that polls *info* drop-folder & ingest
│  ├─ worker_ult.py         · helper functions (timers, schema utils …)
│  ├─ worker_once_stable.py · single process original dbworker (multi-threads version)       
|  ├─ worker_once_multiprocess.py · one-shot folder synchronisation into DB
│  └─ processors/             # for worker_once_multiprocess functions
│      ├── __init__.py
│      ├── file_processor.py   # process_files relative 
│      ├── reschema_processor.py # re-schema relative 
│      └── mcc_processor.py    # mcc_process relative  
│
├─ tools/                   ← ad-hoc CLI helpers
│  ├─ parse_test.py         · quick parse of a file / folder with rules
│  ├─ db_structure_viewer.py· print DB → tables → columns tree
│  ├─ reporter.py           · yield-rate, FPY, jig & fail statistics
│  └─ tpver_to_skuSetting.py· map firmware `tpver` → best rule file
│
├─ database_checker.py      ← Confirm database integrity
│
├─ db_setting/              ← per-site JSON configs; credentials & topology
│  └─ db_setting*.json      · see example below
└─ README.md (this file)
```

## 2.  Quick Start

```bash
# Clone & enter
git clone <repo-url> && cd dbWorker

# Optional: create virtual environment
python3 -m venv .venv && source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Prepare database config
cp db_setting/db_setting_example.json db_setting/db_setting.json
vim db_setting/db_setting.json   # fill in host/user/password

# Run one-time ingestion
python3 workers/worker_once_stable.py --config db_setting/db_setting.json
```

> **Tip** Every script can also be launched directly, e.g. `python3 tools/parse_test.py …`. Internally each script appends the project root to `sys.path`, so absolute location does not matter.

---

## 3.  Configuration (db_setting.json)
Defines database connection, SKU rules, folder paths, and polling parameters.
See the included db_setting_example.json for reference.

---

## 4. Architecture & Performance

| Script | Purpose | Example |
| ------ | ------- | ------- |
| `tools/parse_test.py` | Parse single `.cap` or whole folder & print or save to CSV | `python3 tools/parse_test.py --json_rule sku.json --cap ....cap` |
| `workers/worker_once_stable.py` | Batch insert *new* files under each configured folder into its DB table (no config parameter is default test_db json avoid wrong operation) | `python3 workers/worker_once.py --config db_setting/db_setting.json` |
| `workers/worker_monitor.py` | Watch an *info* drop-folder for new `*.txt` manifests, parse corresponding `.cap`, insert and route successes / errors to sub-folders | `python3 -m workers.worker_monitor` |
| `tools/reporter.py` | Analyse WO yield, fail distribution, JIG stats, SN history | `python3 tools/reporter.py --wo WO12345 --fail --jig` |
| `tools/db_structure_viewer.py` | Pretty-print tables & schemas of every non-system DB | `python3 tools/db_structure_viewer.py` |
| `tools/tpver_to_skuSetting.py` | Given firmware `tpver`, suggest best matching rule file (largest or smallest JSON) | `python3 tools/tpver_to_skuSetting.py` |
| `tools/database_checker.py` | Check the differece between local files and db files and generate a report | `python3 tools/database_checker.py` |
| `tools/database_duplicate_finder.py` | Check the duplicate files in DB | `python3 tools/database_duplicate_finder.py` |
| `tools/null_rate_db.py` | Check NULL rate in db different columes with database_config.json to work | `python3 tools/null_rate_db.py` |


### Worker_once Architecture

The `worker_once_stable.py` script uses a sophisticated multi-threading architecture to achieve high-performance batch processing:

![Worker_once_stable.py Multi-threading Architecture](pics/dbWorker_worker_once_workflow.svg)

Highlights:  
- Thread-safe writers (MCC/File/Re-schema)  
- Batch insertion: up to 100–1000 records/sec (depending on complexity)  
- Auto table creation & schema sync 
- Independent error logging  
- Designed to handle millions of records efficiently

---

## 5. Database/Table Lock Handling

To ensure safe concurrent operation, `dbWorker` uses explicit database and table locking with short timeouts:

To avoid deadlocks, dbWorker uses short-timeout database and table locks:
- If a database or table is locked by another process, the current operation **skips** that table or database and moves on, avoiding long waits or deadlocks.
- Locked resources are skipped with warnings instead of blocking.
- Safe for multi-process and multi-user operation.

---

## 6. Logging

All logs are colourised and timestamped via core.Logger.  
Daily log files are stored under ./logs/YYYYMMDD.log.  
Log levels: *INFO*, *WARN*, *ERROR*, *OK*, *TITLE*, *SUB_TITLE*.

---

## 7. Extending the Parser

### Important: ParseEngine Setup
Rules are defined in JSON under ParseEngine/sku_setting/.  
Each rule file describes how to extract structured data from raw logs.  
 
You can clone or replace the ParseEngine module with your own parser implementation.  

---

## 8. Dependencies

The project requires the following dependencies, which are specified in the provided `requirements.txt` file:
```text
pymysql>=1.1.0
pandas>=2.0.0
```

Install all dependencies with:
```bash
pip install -r requirements.txt
```

---

## 9. Troubleshooting

| Symptom | Cause / Fix |
| ------- | ----------- |
| `ModuleNotFoundError` for *core.Logger* | Make sure you are **inside** project root or ran module with `python3 -m …`. Each script dynamically inserts the parent dir into `sys.path`. |
| `Table … does not exist` warnings | The rule refers to a table that hasn’t been created yet → run `worker_once_stable.py` to auto-create via `MySQLHelper.create_table()` |
| Cannot connect to DB | Check `host`, `port`, firewalls, credentials inside *db_setting.json* |
| Skipped table due to lock timeout | Another process is using the table/database. This is normal in concurrent environments. |

---

## 10. License & Contributing
It is a clean-room reimplementation inspired by industrial data-processing concepts — no proprietary code or confidential content included.

Contributions are welcome!
Follow PEP-8, use type hints, and include unit tests under tests/.
