# taldbt_rsp

**Talend-to-dbt Migration Engine for WU RSP (Reconciliation Settlement Platform)**

Converts Western Union's Talend ETL pipeline into a dbt project running on DuckDB, with Parquet export and Snowflake push. Replaces Talend Studio + TMC with a Python-native stack: parse XML → generate dbt models + execution plans → orchestrate runs across 20 parallel groups.

---

## Architecture

```
Talend .item XML files
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  GENERATOR  (generator/)                            │
│                                                     │
│  xml_parser.py ──► generate.py ──► dbt models/      │
│  31 component      13 SQL model    execution plans/ │
│  type handlers     generators      sources.yml      │
│                    15 plan actions  macros/          │
│                                    DDLs             │
│  expression_translator.py                           │
│  Java→SQL, BigDecimal, ternaries, routines          │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  dbt PROJECT  (dbt_project/)                        │
│                                                     │
│  models/staging/     ← file parsing + joblet SQL    │
│  models/post_sync/   ← dedup, upsert, merge        │
│  models/final_load/  ← MERGE + sign-flip + INSERT   │
│  models/post_job/    ← STATUS updates + TRUNCATE    │
│  models/sources/     ← tDBInput queries + sources   │
│  macros/             ← Talend function equivalents  │
│  execution_plans/    ← JSON plans per job           │
│  sources.yml         ← shared Snowflake tables      │
│  profiles.yml        ← DuckDB with env_var()        │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  ORCHESTRATOR  (engine/)                            │
│                                                     │
│  Plan-first execution:                              │
│    1. Try execution_plan.json → execute_from_plan() │
│    2. Fallback → hardcoded Step 1 + Step 2 flow     │
│                                                     │
│  _dispatch_action() handles ALL 15 action types:    │
│    DBT_MODEL, LOOP, PARALLELIZE, FILE_LIST,         │
│    FILE_COPY, FILE_DELETE, FILE_ARCHIVE,             │
│    RUN_CHILD_JOB, SEND_MAIL, LOG_CATCHER,           │
│    CHRONOMETER, PREJOB, POSTJOB, DB_CONNECTION,      │
│    RUN_IF conditions                                │
│                                                     │
│  Real operations:                                   │
│    dbt subprocess, DuckDB, Parquet, Snowflake,      │
│    SMTP email, file archival (ZIP + AES),            │
│    group-aware file routing via GRPS_TABLE           │
└─────────────────────────────────────────────────────┘
        │
        ▼
  DuckDB (.duckdb)  →  Parquet export  →  Snowflake PUT + COPY INTO
```

---

## 20-Group Scaling

The RSP pipeline processes 20 groups in parallel. The dbt project is generated **once** and parameterized via `--vars` at runtime.

**Table classification:**

| Type | Example | In dbt | How it scales |
|------|---------|--------|---------------|
| **Shared** | `TXNSHAREDCOST`, `EMAIL_ALERTS` | `{{ source('connection', 'table') }}` | Same table for all groups |
| **Group-specific** | `TXNENTRY_GRP{N}_SATEST` | `{{ var('entry_tablename') }}` | Different per group via `--vars` |

**Runtime:**

```
python main.py run-all --prod

  GRP1:  --vars {"entry_tablename": "TXNENTRY_GRP1_SATEST", ...}
  GRP2:  --vars {"entry_tablename": "TXNENTRY_GRP2_SATEST", ...}
  ...
  GRP20: --vars {"entry_tablename": "TXNENTRY_GRP20_SATEST", ...}

Each group gets its own:
  - Ephemeral DuckDB (runs/grp{N}_timestamp.duckdb)
  - File paths (ST_File_grp{N}/, AC_File_grp{N}/)
  - Config from load_config(grp_number)
  - Parquet export + Snowflake push
```

---

## File Movement Flow

```
Source dirs (ST_File/, AC_Target/)
  │  _sort_files_copy(): filter by NetworkID via GRPS_TABLE, cap 15, seq prefix
  ▼
Processing dirs (ST_File_grp{N}/, AC_File_grp{N}/)
  │  per-iteration: copy to ST_Inprocess/ (strip prefix)
  ▼
ST_Inprocess/ → dbt run (staging models)
  │  SUCCESS: delete from ST_Inprocess/
  │  ERROR:   move to ST_Error/
  ▼
Post-sync chain → Final load
  │
  ▼  _postjob_file_movement() — 4 parallel branches:
  ├── ST_File_grp{N}/*.ST  → Archive/ST_Files/ (zip, strip prefix)
  ├── ST_File_grp{N}/*.txt → Archive/ST_Files/ (zip, strip prefix)
  ├── AC_File_grp{N}/*     → Archive/AC_Files/ (zip, strip prefix)
  └── Cleanup: ST_Inprocess/ + ST_Parse_grp{N}/ (remove leftovers)
```

---

## Installation

### Prerequisites

- Python 3.10+
- Access to Talend `.item` XML files (on WU AWS Workspace)

### Setup

```powershell
git clone https://github.com/SouravRoy-ETL/taldbt_rsp.git
cd taldbt_rsp
pip install -r requirements.txt
```

### Dependencies

```
duckdb>=0.10.0
snowflake-connector-python>=3.6.0
streamlit>=1.30.0
pyyaml>=6.0
pandas>=2.0.0
pyarrow>=14.0.0
apscheduler>=3.10.0
dbt-core>=1.7.0
dbt-duckdb>=1.7.0
```

Optional: `pyzipper` for AES-encrypted ZIP archives.

---

## Usage

### Step 1: Generate dbt project from Talend XML

```powershell
python main.py generate --input "D:\path\to\UNIFIEDPORTAL" --output dbt_project
```

**Output:**
- `dbt_project/models/` — SQL models for every data-transforming component
- `dbt_project/execution_plans/` — JSON plans for every orchestration component
- `dbt_project/models/sources/sources.yml` — shared Snowflake table definitions
- `dbt_project/macros/talend_functions.sql` — auto-generated from discovered Java patterns
- `dbt_project/analyses/create_staging_tables.sql` — DDLs from all schemas
- `dbt_project/UNTRANSLATED_REPORT.json` — expressions that need manual review
- `dbt_project/generation_manifest.json` — full generation metadata

### Step 2: Compile and validate

```powershell
cd dbt_project
dbt debug
dbt compile
dbt run --select tag:staging
```

### Step 3: Run a single group

```powershell
# Dev mode (permissive file routing)
python main.py run --grp 16

# Production mode (requires GRPS_TABLE for file routing)
python main.py run --grp 16 --prod

# With Snowflake push
python main.py run --grp 16 --prod --push
```

### Step 4: Run all 20 groups

```powershell
# 4 parallel groups
python main.py run-all --parallel 4 --prod

# With Snowflake push
python main.py run-all --parallel 4 --prod --push
```

### Step 5: Start the 30-minute scheduler (replaces TMC)

```powershell
# Defaults to --prod (production mode)
python main.py scheduler --interval 30 --parallel 4
```

### Other commands

```powershell
# Export DuckDB tables to Parquet
python main.py export --grp 16 --duckdb runs/grp16_20260325.duckdb

# Push Parquet to Snowflake
python main.py push --grp 16 --duckdb runs/grp16_20260325.duckdb

# Launch Streamlit TMC dashboard
python main.py ui
```

---

## Configuration

### `config/context_template.yaml`

Contains all 110 Talend context variables. Group-specific values are resolved at runtime by `config_loader.py`.

**Pattern A** — table names with grp appended: `Entry_stgstfile_grp` → `TALENDSTAGTXNENTRYST_GRP16`

**Pattern B** — table names fully replaced: `Entry_tablename` → `TXNENTRY_GRP16_SATEST`

### Snowflake credentials

Set the `password` field in `config/context_template.yaml` under `snowflake:`, or use environment variables.

### `--prod` vs dev mode

| Mode | GRPS_TABLE missing | File routing | Use case |
|------|--------------------|--------------|----------|
| Dev (default) | Warning, processes all files | No NetworkID filtering | Local testing |
| `--prod` | **HARD FAILURE** — aborts | Enforces NetworkID → group mapping | Production |

---

## Project Structure

```
taldbt_rsp/
├── main.py                      6KB    CLI entry point (7 commands)
├── requirements.txt             189B   Python dependencies
├── .gitignore                   136B
├── README.md                           This file
│
├── generator/                          Talend XML → dbt project
│   ├── xml_parser.py            41KB   31 component type handlers
│   ├── expression_translator.py 24KB   Java→SQL translator + routine discovery
│   └── generate.py              69KB   Connection graph, 13 model generators,
│                                       execution plan builder, sources.yml
│
├── engine/                             Runtime orchestration
│   └── orchestrator.py          54KB   Plan-first executor, dbt subprocess,
│                                       DuckDB, Parquet, Snowflake, scheduler
│
├── config/                             Context variable management
│   ├── config_loader.py         4KB    110 vars, Pattern A/B, group resolution
│   └── context_template.yaml    3KB    All Talend context variables
│
├── ui/                                 Streamlit TMC replacement
│   └── app.py                   7KB    Dashboard for monitoring runs
│
└── dbt_project/                        Generated output (not committed)
    └── README.md
```

---

## Component Coverage

### Data-transforming components (→ dbt models)

| Component | Generator | Semantics |
|-----------|-----------|-----------|
| `tMap` | CTE vars + CROSS JOIN, lookup JOINs with parsed jk_expr, per-output models, filters | Full |
| `tJavaRow` | Java→SQL translation, var CTE + CROSS JOIN | Full |
| `tJava` | Context var extraction, logging skip, REVIEW flag | Full |
| `tDBRow` / `tSnowflakeRow` | Translated SQL model | Full |
| `tDBOutput` / `tSnowflakeOutput` | Incremental merge for UPDATE/UPSERT, table for INSERT | Full |
| `tDBInput` / `tSnowflakeInput` | `{{ source() }}` for shared, `{{ var() }}` for group-specific | Full |
| `tUniqRow` | `ROW_NUMBER() OVER (PARTITION BY ...)` first-row-wins | Full |
| `tFileInputDelimited` | DuckDB `read_csv_auto()` with parsed params | Full |
| `tFileOutputDelimited` | Model + COPY TO post-hook metadata | Full |
| `tExtractDelimitedFields` | `string_split()` with positional extraction | Full |
| `tFixedFlowInput` | Static row model from schema defaults | Full |
| `tFlowToIterate` / `tIterateToFlow` | Passthrough + variable mapping | Full |
| `tBufferOutput` | Passthrough model | Full |

### Orchestration components (→ execution_plan.json)

| Component | Executor | Behavior |
|-----------|----------|----------|
| `tLoop` | File-driven iteration (from FILE_LIST) or numeric range | Updates CURRENT_FILEPATH per iteration |
| `tParallelize` | `ThreadPoolExecutor` with targets | All action types work in parallel |
| `tFileList` | `os.listdir` + fnmatch + sort | Stores enumerated files in config |
| `tFileCopy` | `shutil.copy2` + optional remove | Context-resolved paths |
| `tFileDelete` | `os.remove` | Context-resolved paths |
| `tFileArchive` | ZIP + optional AES via pyzipper | Encrypted archives supported |
| `tRunJob` | Recursive child plan execution | Context overrides save/apply/restore |
| `tSendMail` | Real SMTP with CC, auth, X-Priority | Error context injection from LogCatcher |
| `tLogCatcher` | Active flag + catch type registration | Feeds error state to tSendMail |
| `tChronometerStop` | Real elapsed time calculation | Stores in config as ms (Talend format) |
| `tPrejob` | DuckDB init + Snowflake env/email load | Idempotent (safe to call twice) |
| `tPostjob` | 4-branch archive + cleanup | Idempotent (safe to call twice) |
| `tDBConnection` | Real Snowflake connect with plan metadata | Named connection refs, fatal on failure |
| `RUN_IF` | Safe pattern matching (no eval) | Handles &&, ||, !=null, ==, >, <, .equals() |

---

## Safety Features

| Feature | Behavior |
|---------|----------|
| Missing dbt | **HARD FAILURE** — `RuntimeError`, not silent skip |
| Missing GRPS_TABLE in `--prod` | **HARD FAILURE** — prevents cross-group contamination |
| Missing GRPS_TABLE in dev | Warning + pass-all (logged as contamination risk) |
| ST file processing error | File moved to `ST_Error/`, not left in inprocess |
| Prejob/Postjob duplication | Idempotent guards (`_prejob_done`, `_postjob_done`) |
| Untranslated expressions | Flagged with `UNTRANSLATED()` wrapper, reported in JSON |
| Unknown component types | Flagged as UNKNOWN in execution plan with warning |
| Condition evaluation | Safe regex pattern matching, no `eval()` |
| DB connection failure | Fatal (matches Talend behavior) |
| Email failure | Non-fatal (matches Talend tSendMail behavior) |

---

## Expression Translation

The `expression_translator.py` handles:

- **Talend function families:** `StringHandling.*`, `Mathematical.*`, `TalendDate.*`, `Numeric.sequence`
- **BigDecimal chains:** `.subtract().multiply().divide().setScale().compareTo()`
- **Nested ternaries:** Up to 5 levels deep
- **Row references:** `input_row.COLUMN` → `COLUMN`, `row1.COLUMN` → `row1.COLUMN`
- **Type casts:** `Double.parseDouble()`, `Integer.parseInt()`, `String.valueOf()`
- **Snowflake→DuckDB SQL:** `CONVERT_TIMEZONE`, `TO_CHAR`, `TRY_TO_TIMESTAMP`, `NVL`
- **Routine discovery:** Scans `.java` files, flags custom routine calls as UNTRANSLATED
- **Context references:** `context.varname` → `{{ var('varname') }}` in SQL

---

## TMC Plan Equivalent

The Talend TMC schedule for RSP-Daily-GRP{N}:

| Step | Talend | taldbt_rsp |
|------|--------|------------|
| Trigger | TMC 30-min cron | `python main.py scheduler --interval 30` |
| Step 1 | AC_ST_ALL_LOAD_MAIN_GRP_AG25V2 | `execute_from_plan(step1_plan.json)` or `_step1_all_load()` |
| Step 2 | AC_ST_POST_MAIN_GRP_LOAD_AG25v3 | `execute_from_plan(step2_plan.json)` or `_step2_post_load()` |
| Parallel | TMC parallel tasks | `run_all_groups(max_parallel=4)` |
| Monitoring | TMC dashboard | `python main.py ui` (Streamlit) |

---

## License

Internal use — Western Union RSP Pipeline Migration.
