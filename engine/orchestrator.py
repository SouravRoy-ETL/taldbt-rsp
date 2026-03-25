"""
Master Orchestrator — REAL implementation.
Replicates AC_ST_ALL_LOAD + AC_ST_POST flow.
Calls dbt subprocess, manages DuckDB, exports Parquet, pushes to Snowflake.

TMC Plan per group (RSP-Daily-GRP{N}):
  Trigger: Daily (every 30 min)
  Step 1: AC_ST_ALL_LOAD_MAIN_GRP_AG25V2 (V7)
  Step 2: AC_ST_POST_MAIN_GRP_LOAD_AG25v3 (V2)
"""
import os
import json
import sys
import time
import shutil
import zipfile
import logging
import subprocess
import traceback
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config_loader import load_config, resolve_table, get_dbt_vars

logger = logging.getLogger('rsp_pipeline')


class PipelineRun:
    """Tracks state for a single pipeline run."""

    def __init__(self, grp_number: str, config: dict):
        self.grp_number = grp_number
        self.config = config
        self.run_id = datetime.now().strftime('%Y%m%d_%H%M%S') + f'_grp{grp_number}'
        self.status = 'PENDING'
        self.current_step = ''
        self.steps = []
        self.st_files_processed = 0
        self.ac_files_processed = 0
        self.st_cldrn_code = None
        self.act_cldrn_code = None
        self.errors = []
        self.start_time = None
        self.end_time = None

    def log_step(self, step_name: str, status: str, rows_in: int = 0, rows_out: int = 0,
                 error: str = '', detail: str = ''):
        self.steps.append({
            'step': step_name, 'status': status,
            'rows_in': rows_in, 'rows_out': rows_out,
            'error': error, 'detail': detail,
            'timestamp': datetime.now().isoformat(),
        })
        self.current_step = step_name
        if status == 'ERROR':
            self.errors.append(f'{step_name}: {error}')
        logger.info(f"GRP{self.grp_number} [{status}] {step_name}"
                     + (f" — {detail}" if detail else "")
                     + (f" — ERROR: {error}" if error else ""))

    def to_dict(self) -> dict:
        return {
            'run_id': self.run_id, 'grp_number': self.grp_number,
            'status': self.status, 'steps': self.steps,
            'st_files': self.st_files_processed, 'ac_files': self.ac_files_processed,
            'errors': self.errors,
            'start': self.start_time.isoformat() if self.start_time else None,
            'end': self.end_time.isoformat() if self.end_time else None,
            'duration_sec': (self.end_time - self.start_time).total_seconds()
                            if self.start_time and self.end_time else None,
        }


class MasterOrchestrator:
    """Replicates AC_ST_ALL_LOAD + AC_ST_POST execution flow with real operations."""

    def __init__(self, grp_number: str, dbt_project_dir: str,
                 config_path: str = None, dev_mode: bool = True):
        self.grp_number = str(grp_number)
        self.dev_mode = dev_mode
        self.dbt_project_dir = os.path.abspath(dbt_project_dir)
        self.config = load_config(self.grp_number, config_path, dev_mode)
        self.run = PipelineRun(self.grp_number, self.config)
        self.duckdb_path = None
        self.snowflake_conn = None
        self._prejob_done = False
        self._postjob_done = False

    # =========================================================================
    # MAIN ENTRY: Step 1 + Step 2
    # =========================================================================
    def execute_full_plan(self) -> PipelineRun:
        """TMC Plan: Step 1 (AC_ST_ALL_LOAD) → Step 2 (AC_ST_POST)."""
        self.run.start_time = datetime.now()
        self.run.status = 'RUNNING'

        try:
            # === STEP 1: AC_ST_ALL_LOAD ===
            self._step1_all_load()

            # === STEP 2: AC_ST_POST (only if Step 1 processed files) ===
            if self.run.st_cldrn_code is not None or self.run.act_cldrn_code is not None:
                self._step2_post_load()

            self.run.status = 'COMPLETED'

        except Exception as e:
            self.run.status = 'FAILED'
            self.run.log_step('fatal_error', 'ERROR', error=str(e))
            logger.error(f"GRP{self.grp_number} FATAL:\n{traceback.format_exc()}")
            self._send_error_email(str(e))

        finally:
            # POSTJOB always runs
            try:
                self._postjob_file_movement()
            except Exception as e:
                logger.error(f"GRP{self.grp_number} Postjob error: {e}")

            self.run.end_time = datetime.now()
            self._cleanup()

        return self.run

    # =========================================================================
    # STEP 1: AC_ST_ALL_LOAD_MAIN_GRP_AG25V2
    # =========================================================================
    def _step1_all_load(self):
        # --- PREJOB ---
        self._prejob()

        # --- SORT FILES COPY ---
        self._sort_files_copy()

        # --- MAIN LOOP (tLoop_1: 1 to 15) ---
        max_loop = self.config.get('maxloopNo', 15)
        for iteration in range(1, max_loop + 1):
            # tJava_1: reset sequence gates (DIFFERENT date formats per Talend)
            now = datetime.now()
            self.config['SEQ_RESET_GRP_AC'] = now.strftime('%Y%m%d%H%M%S')
            self.config['SEQ_RESET_GRP_ST'] = now.strftime('%m%d%Y%H%M%S')

            # tParallelize_2: ST + AC in parallel, one file each (sequence gate)
            with ThreadPoolExecutor(max_workers=2) as ex:
                st_f = ex.submit(self._process_st_file, iteration)
                ac_f = ex.submit(self._process_ac_file, iteration)
                st_ok = st_f.result()
                ac_ok = ac_f.result()

            if st_ok:
                self.run.st_files_processed += 1
                self.run.st_cldrn_code = self.run.st_files_processed
            if ac_ok:
                self.run.ac_files_processed += 1
                self.run.act_cldrn_code = self.run.ac_files_processed

        # --- POST-SYNC CHAIN ---
        if self.run.st_cldrn_code is not None or self.run.act_cldrn_code is not None:
            self._post_sync_chain()
        else:
            self.run.log_step('early_exit', 'COMPLETED', detail='No files to process (tJava_7 path)')

    # =========================================================================
    # STEP 2: AC_ST_POST_MAIN_GRP_LOAD_AG25v3
    # =========================================================================
    def _step2_post_load(self):
        self.run.log_step('step2_start', 'RUNNING', detail='AC_ST_POST_MAIN_GRP_LOAD')

        # Child 1: STS_TXNENTRY_UPDATE_MAINOUTPUT
        self._run_dbt_tag('post_sts_update', 'STS_TXNENTRY_UPDATE: STATUS from Recordtype4')

        # Child 2: PAYDATE_TXNENTRY_UPDATE + TRUNCATE Recordtype4
        self._run_dbt_tag('post_paydate_update', 'PAYDATE_TXNENTRY_UPDATE + TRUNCATE Recordtype4')

        # Child 3: AC_STATUS_UPDATE + TRUNCATE entry_dup
        self._run_dbt_tag('post_ac_status', 'AC_STATUS_UPDATE: dedup + STATUS=C + TRUNCATE entry_dup')

        self.run.log_step('step2_complete', 'COMPLETED')

    # =========================================================================
    # PREJOB: Snowflake connection + env/email config
    # =========================================================================
    def _prejob(self):
        if self._prejob_done:
            logger.info(f"GRP{self.grp_number} Prejob already executed, skipping")
            return
        self._prejob_done = True
        self.run.log_step('prejob', 'RUNNING')

        # Create ephemeral DuckDB for this run
        runs_dir = os.path.join(self.dbt_project_dir, 'runs')
        os.makedirs(runs_dir, exist_ok=True)
        self.duckdb_path = os.path.join(runs_dir,
            f'grp{self.grp_number}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.duckdb')

        # Set DUCKDB_PATH env var so dbt profiles.yml picks it up
        os.environ['DUCKDB_PATH'] = self.duckdb_path

        # Connect to Snowflake for env + email config
        try:
            sf_cfg = self.config.get('snowflake', {})
            if sf_cfg.get('host') and sf_cfg.get('password'):
                import snowflake.connector
                self.snowflake_conn = snowflake.connector.connect(
                    account=sf_cfg['account'],
                    host=sf_cfg['host'],
                    user=sf_cfg['user'],
                    password=sf_cfg['password'],
                    database=sf_cfg['database'],
                    schema=sf_cfg['schema'],
                    warehouse=sf_cfg['warehouse'],
                    role=sf_cfg.get('role', ''),
                    login_timeout=sf_cfg.get('loginTimeout', 15),
                )
                # tDBRow_2: SELECT * FROM ALERT_ENVIRONMENT
                cur = self.snowflake_conn.cursor()
                cur.execute("SELECT * FROM ALERT_ENVIRONMENT")
                row = cur.fetchone()
                if row:
                    self.config['environment'] = row[0] if row else 'UNKNOWN'

                # tDBRow_3: SELECT * FROM EMAIL_ALERTS
                cur.execute("SELECT * FROM EMAIL_ALERTS")
                row = cur.fetchone()
                if row:
                    self.config['email_to'] = str(row[0]) if len(row) > 0 else ''
                    self.config['email_to_talend_dev'] = str(row[1]) if len(row) > 1 else ''
                    self.config['email_from'] = str(row[2]) if len(row) > 2 else ''
                cur.close()

                self.run.log_step('prejob', 'COMPLETED', detail='Snowflake connected, env+email loaded')
            else:
                self.config['environment'] = 'DEV'
                self.run.log_step('prejob', 'COMPLETED', detail='No Snowflake creds — DEV mode')

        except Exception as e:
            self.config['environment'] = 'DEV'
            self.run.log_step('prejob', 'COMPLETED',
                              detail=f'Snowflake connection failed ({e}), running DEV mode')

    # =========================================================================
    # SORT FILES COPY: scan flat dirs, route by NetworkID, cap 15, seq prefix
    # =========================================================================
    def _sort_files_copy(self):
        self.run.log_step('sort_files', 'RUNNING')

        st_src = self.config.get('source_st_dir', '')
        ac_src = self.config.get('source_ac_dir', '')
        st_dst = self.config.get('processing_st_grp', '') + self.grp_number + os.sep
        ac_dst = self.config.get('processing_ac_grp', '') + self.grp_number + os.sep
        max_files = self.config.get('maxloopNo', 15)

        os.makedirs(st_dst, exist_ok=True)
        os.makedirs(ac_dst, exist_ok=True)

        # Group-aware file filtering via GRPS_TABLE
        # In production: query Snowflake for NetworkID→grp mapping
        # Files without a matching NetworkID for this group are SKIPPED (not processed)
        grp_filter = self._load_group_filter()
        st_count = self._route_files(st_src, st_dst, max_files,
                                      lambda f: (f.endswith('.txt') or f.endswith('.ST'))
                                                and self._file_belongs_to_group(f, grp_filter))
        ac_count = self._route_files(ac_src, ac_dst, max_files,
                                      lambda f: 'ACTIV' in f.upper()
                                                and self._file_belongs_to_group(f, grp_filter))

        self.run.log_step('sort_files', 'COMPLETED',
                          rows_in=st_count + ac_count,
                          detail=f'ST:{st_count} AC:{ac_count} files routed')

    def _route_files(self, src_dir: str, dst_dir: str, max_files: int,
                     file_filter) -> int:
        """Copy matching files with sequence prefix, sorted by date ASC."""
        if not os.path.isdir(src_dir):
            return 0

        files = sorted([f for f in os.listdir(src_dir) if file_filter(f)])
        count = 0
        for f in files:
            if count >= max_files:
                break
            count += 1
            src = os.path.join(src_dir, f)
            dst = os.path.join(dst_dir, f"{count}.{f}")
            if not os.path.exists(dst):
                shutil.copy2(src, dst)
        return count

    def _load_group_filter(self) -> dict:
        """Load NetworkID → grp_number mapping from Snowflake or config.
        Returns dict of {network_id: grp_number}. If unavailable, returns None
        which means ALL files pass (dev mode, but logs a warning)."""
        if self.snowflake_conn:
            try:
                cur = self.snowflake_conn.cursor()
                cur.execute('SELECT NETWORK_ID, GRP_NUMBER FROM GRPS_TABLE')
                mapping = {str(row[0]): str(row[1]) for row in cur.fetchall()}
                cur.close()
                if mapping:
                    logger.info(f'GRP{self.grp_number} Loaded {len(mapping)} NetworkID mappings')
                    return mapping
            except Exception as e:
                logger.warning(f'GRP{self.grp_number} Could not load GRPS_TABLE: {e}')

        if not self.dev_mode:
            error_msg = (f'GRP{self.grp_number} GRPS_TABLE unavailable and dev_mode=False. '
                         f'Cannot route files without group filter. ABORTING to prevent '
                         f'cross-group contamination.')
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        logger.warning(f'GRP{self.grp_number} No GRPS_TABLE — dev_mode=True so ALL files pass. '
                       f'Set dev_mode=False for production to enforce group filtering.')
        return None

    def _file_belongs_to_group(self, filename: str, grp_filter: dict) -> bool:
        """Check if a file belongs to this group based on its NetworkID.
        If grp_filter is None (dev mode), all files pass with a warning."""
        if grp_filter is None:
            return True  # dev mode — no filtering

        # Extract NetworkID from filename (pattern: STLMTP_NNNN_... or ACTIV_NNNN_...)
        import re as _re
        match = _re.search(r'[_](\d{3,6})[_.]', filename)
        if not match:
            logger.warning(f'GRP{self.grp_number} Could not extract NetworkID from {filename}, skipping')
            return False

        network_id = match.group(1)
        mapped_grp = grp_filter.get(network_id)
        if mapped_grp == self.grp_number:
            return True
        elif mapped_grp:
            return False  # belongs to a different group
        else:
            logger.warning(f'GRP{self.grp_number} NetworkID {network_id} from {filename} not in GRPS_TABLE')
            return False

    # =========================================================================
    # PER-FILE PROCESSING: one ST + one AC per loop iteration
    # =========================================================================
    def _process_st_file(self, iteration: int) -> bool:
        """Process one ST file: copy to inprocess → dbt run → cleanup.
        On error: move to ST_Error/. On success: remove from inprocess."""
        proc_dir = self.config.get('processing_st_grp', '') + self.grp_number + os.sep
        filepath = self._find_iteration_file(proc_dir, iteration)
        if not filepath:
            return False

        actual_name = os.path.basename(filepath).split('.', 1)[1]
        self.run.log_step(f'st_{iteration}', 'RUNNING', detail=actual_name)
        inprocess_path = None

        try:
            # Step 1: Copy to ST_Inprocess (strip sequence prefix)
            inprocess_dir = self.config.get('st_inprocess', '')
            if inprocess_dir:
                os.makedirs(inprocess_dir, exist_ok=True)
                inprocess_path = os.path.join(inprocess_dir, actual_name)
                shutil.copy2(filepath, inprocess_path)
                logger.info(f"GRP{self.grp_number} ST {iteration}: {actual_name} → ST_Inprocess/")

            # Step 2: Run dbt staging + joblet models
            self._run_dbt_tag('staging', f'ST file {iteration}: {actual_name}')

            # Step 3: On success, remove from inprocess
            if inprocess_path and os.path.exists(inprocess_path):
                os.remove(inprocess_path)

            self.run.log_step(f'st_{iteration}', 'COMPLETED', detail=actual_name)
            return True

        except Exception as e:
            # On error: move from inprocess to ST_Error
            error_dir = self.config.get('st_error_file', '')
            if error_dir and inprocess_path and os.path.exists(inprocess_path):
                os.makedirs(error_dir, exist_ok=True)
                error_path = os.path.join(error_dir, actual_name)
                shutil.move(inprocess_path, error_path)
                logger.warning(f"GRP{self.grp_number} ST {iteration}: {actual_name} → ST_Error/")
            self.run.log_step(f'st_{iteration}', 'ERROR', error=str(e))
            raise

    def _process_ac_file(self, iteration: int) -> bool:
        """Process one AC file: dbt run → on error move to error dir."""
        proc_dir = self.config.get('processing_ac_grp', '') + self.grp_number + os.sep
        filepath = self._find_iteration_file(proc_dir, iteration)
        if not filepath:
            return False

        actual_name = os.path.basename(filepath).split('.', 1)[1]
        self.run.log_step(f'ac_{iteration}', 'RUNNING', detail=actual_name)

        try:
            self._run_dbt_tag('ac_loader', f'AC file {iteration}: {actual_name}')
            self.run.log_step(f'ac_{iteration}', 'COMPLETED', detail=actual_name)
            return True
        except Exception as e:
            error_dir = self.config.get('st_error_file', '')
            if error_dir and os.path.exists(filepath):
                os.makedirs(error_dir, exist_ok=True)
                shutil.move(filepath, os.path.join(error_dir, actual_name))
                logger.warning(f"GRP{self.grp_number} AC {iteration}: {actual_name} → Error/")
            self.run.log_step(f'ac_{iteration}', 'ERROR', error=str(e))
            raise

    def _find_iteration_file(self, directory: str, iteration: int) -> str:
        if not os.path.isdir(directory):
            return ''
        for f in sorted(os.listdir(directory)):
            if f.startswith(f"{iteration}."):
                return os.path.join(directory, f)
        return ''

    # =========================================================================
    # POST-SYNC CHAIN: sequential after all files processed
    # =========================================================================
    def _post_sync_chain(self):
        chain = [
            ('ac_dedup', 'post_sync', 'tDBRow_1: AC dedup INSERT → entry_dup_grp'),
            ('adj_sts_update', 'post_sync', 'ADJ_STS_UPDATE: STATUS W→S correction'),
            ('upsert_2cases', 'post_sync', 'UPSERT_2_CASES: 10 parallel tDBRows'),
            ('txn_template_link', 'post_sync', 'tDBRow_4: txnTemplate ENTRY_ID linkage'),
            ('upsert_3rd_case', 'post_sync', 'UPSERT_3RD_CASE: unmatched → acstmerge'),
            ('final_load', 'final_load', 'FINAL_LOAD: MERGE + net-zero + INSERT + sign-flip + TRUNCATE'),
        ]
        for step_id, dbt_tag, description in chain:
            self._run_dbt_tag(dbt_tag, description, step_id=step_id)

    # =========================================================================
    # POSTJOB: file archival (4 parallel branches)
    # =========================================================================
    def _postjob_file_movement(self):
        """UN_PROCESSED_FILES_MOVEMENT: 4 parallel archive branches + cleanup.
        Flow: processing_grp → Archive (zip, strip prefix) → delete source.
        Also: cleanup ST_Inprocess, ST_Parse_grp leftover files."""
        if self._postjob_done:
            logger.info(f"GRP{self.grp_number} Postjob already executed, skipping")
            return
        self._postjob_done = True
        self.run.log_step('postjob', 'RUNNING', detail='File archival + cleanup')

        archive_st = self.config.get('archive_path_st', '')
        archive_ac = self.config.get('archive_path_ac', '')
        proc_st = self.config.get('processing_st_grp', '') + self.grp_number + os.sep
        proc_ac = self.config.get('processing_ac_grp', '') + self.grp_number + os.sep
        inprocess = self.config.get('st_inprocess', '')
        parse_dir = self.config.get('st_parse_path_grp', '') + self.grp_number + os.sep

        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = []
            # Branch 1+2: Archive ST files
            if os.path.isdir(proc_st) and archive_st:
                futs.append(ex.submit(self._archive_files, proc_st, archive_st, '*.ST'))
                futs.append(ex.submit(self._archive_files, proc_st, archive_st, '*.txt'))
            # Branch 3: Archive ALL AC files
            if os.path.isdir(proc_ac) and archive_ac:
                futs.append(ex.submit(self._archive_files, proc_ac, archive_ac, '*'))
            # Branch 4: Cleanup inprocess + parse dirs
            futs.append(ex.submit(self._cleanup_dir, inprocess))
            futs.append(ex.submit(self._cleanup_dir, parse_dir))

            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    logger.warning(f"Postjob error: {e}")

        self.run.log_step('postjob', 'COMPLETED',
            detail=f'Archived ST:{proc_st} AC:{proc_ac}, cleaned inprocess+parse')

    def _cleanup_dir(self, directory: str):
        """Remove all files in a directory (not the directory itself)."""
        if not directory or not os.path.isdir(directory):
            return
        for f in os.listdir(directory):
            fp = os.path.join(directory, f)
            if os.path.isfile(fp):
                try:
                    os.remove(fp)
                except Exception as e:
                    logger.warning(f"Cleanup failed: {fp}: {e}")

    def _archive_files(self, src_dir: str, archive_dir: str, pattern: str):
        """Zip files matching pattern, move to archive, strip sequence prefix, delete source."""
        os.makedirs(archive_dir, exist_ok=True)
        import re as _re
        import fnmatch
        for f in os.listdir(src_dir):
            filepath = os.path.join(src_dir, f)
            if not os.path.isfile(filepath):
                continue
            # Strip sequence prefix before pattern matching
            actual_name = _re.sub(r'^\d+\.', '', f)
            # Only archive files matching the glob pattern
            if not fnmatch.fnmatch(actual_name, pattern):
                continue
            # Zip
            zip_path = os.path.join(archive_dir, actual_name + '.zip')
            try:
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                    zf.write(filepath, actual_name)
                os.remove(filepath)
            except Exception as e:
                logger.warning(f"Archive failed for {f}: {e}")

    # =========================================================================
    # DBT INTEGRATION: real subprocess calls
    # =========================================================================
    def _run_dbt_tag(self, tag: str, description: str, step_id: str = None):
        """Run dbt models matching a tag. This is the REAL execution."""
        sid = step_id or tag
        self.run.log_step(sid, 'RUNNING', detail=description)

        dbt_vars = get_dbt_vars(self.config)
        vars_json = json.dumps(dbt_vars)

        cmd = [
            sys.executable, '-m', 'dbt', 'run',
            '--select', f'tag:{tag}',
            '--vars', vars_json,
            '--profiles-dir', self.dbt_project_dir,
            '--project-dir', self.dbt_project_dir,
        ]

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600,
                cwd=self.dbt_project_dir,
                env={**os.environ, 'DUCKDB_PATH': self.duckdb_path or ''},
            )

            if result.returncode != 0:
                error_msg = result.stderr[-500:] if result.stderr else result.stdout[-500:]
                self.run.log_step(sid, 'ERROR', error=error_msg, detail=description)
                raise RuntimeError(f"dbt run failed for tag:{tag}\n{error_msg}")

            # Parse row counts from dbt output
            rows = 0
            for line in result.stdout.split('\n'):
                if 'OK' in line and 'row' in line.lower():
                    import re as _re
                    m = _re.search(r'(\d+)\s+row', line)
                    if m:
                        rows += int(m.group(1))

            self.run.log_step(sid, 'COMPLETED', rows_out=rows, detail=description)

        except subprocess.TimeoutExpired:
            self.run.log_step(sid, 'ERROR', error='Timeout (600s)', detail=description)
            raise
        except FileNotFoundError:
            # dbt not installed — this is a HARD FAILURE, not a skip
            error_msg = 'dbt is not installed or not on PATH. Install with: pip install dbt-core dbt-duckdb'
            self.run.log_step(sid, 'ERROR', error=error_msg, detail=description)
            raise RuntimeError(error_msg)

    # =========================================================================
    # PARQUET EXPORT: DuckDB → Parquet files
    # =========================================================================
    def export_parquet(self, output_dir: str = None) -> list:
        """Export all staging/production tables from DuckDB as Parquet files."""
        if not self.duckdb_path or not os.path.exists(self.duckdb_path):
            logger.warning("No DuckDB file to export")
            return []

        import duckdb
        output_dir = output_dir or os.path.join(self.dbt_project_dir, 'parquet_export')
        os.makedirs(output_dir, exist_ok=True)

        conn = duckdb.connect(self.duckdb_path, read_only=True)
        tables = conn.execute("SELECT table_name FROM information_schema.tables "
                              "WHERE table_schema = 'main'").fetchall()

        exported = []
        for (table_name,) in tables:
            parquet_path = os.path.join(output_dir, f'{table_name}.parquet')
            try:
                conn.execute(f"COPY {table_name} TO '{parquet_path}' (FORMAT PARQUET)")
                size = os.path.getsize(parquet_path)
                exported.append({'table': table_name, 'path': parquet_path, 'size': size})
                logger.info(f"Exported: {table_name} → {parquet_path} ({size:,} bytes)")
            except Exception as e:
                logger.error(f"Export failed for {table_name}: {e}")

        conn.close()
        return exported

    # =========================================================================
    # SNOWFLAKE PUSH: Parquet → PUT → COPY INTO
    # =========================================================================
    def push_to_snowflake(self, parquet_files: list = None) -> dict:
        """Push Parquet files to Snowflake via PUT + COPY INTO."""
        if not self.snowflake_conn:
            sf_cfg = self.config.get('snowflake', {})
            if not sf_cfg.get('password'):
                logger.warning("No Snowflake credentials, skipping push")
                return {'status': 'skipped', 'reason': 'no credentials'}

            import snowflake.connector
            self.snowflake_conn = snowflake.connector.connect(
                account=sf_cfg['account'], host=sf_cfg['host'],
                user=sf_cfg['user'], password=sf_cfg['password'],
                database=sf_cfg['database'], schema=sf_cfg['schema'],
                warehouse=sf_cfg['warehouse'], role=sf_cfg.get('role', ''),
            )

        if not parquet_files:
            parquet_files = self.export_parquet()

        results = {}
        cur = self.snowflake_conn.cursor()

        for pf in parquet_files:
            table = pf['table']
            path = pf['path']
            try:
                # PUT file to internal stage
                put_sql = f"PUT 'file://{path}' @%{table} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                cur.execute(put_sql)

                # COPY INTO target table
                copy_sql = (f"COPY INTO {table} FROM @%{table} "
                           f"FILE_FORMAT=(TYPE=PARQUET) "
                           f"MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE "
                           f"PURGE=TRUE")
                cur.execute(copy_sql)
                rows = cur.fetchone()
                row_count = rows[0] if rows else 0

                results[table] = {'status': 'ok', 'rows': row_count}
                logger.info(f"Pushed: {table} → Snowflake ({row_count} rows)")

            except Exception as e:
                results[table] = {'status': 'error', 'error': str(e)}
                logger.error(f"Push failed for {table}: {e}")

        cur.close()
        return results

    # =========================================================================
    # ERROR EMAIL: tLogCatcher → tJavaRow_5 → tSendMail_1
    # =========================================================================
    def _send_error_email(self, error_msg: str):
        try:
            import smtplib
            from email.mime.text import MIMEText

            host = self.config.get('smtp_host', '')
            port = self.config.get('smtp_port', 25)
            to = self.config.get('email_to_talend_dev', '')
            frm = self.config.get('email_from', '')
            env = self.config.get('environment', '')

            if not all([host, to, frm]):
                return

            subject = f"{env} :- UNP MAIN JOB TLOOP SIDE GRP{self.grp_number}"
            body = (f"{self.config.get('jobName', 'AC_ST_ALL_LOAD_MAIN_GRP')} "
                    f"job got failed please look into it in TMC of Production Environment.-\n\n"
                    f"Job failed due to this error(message) {error_msg}\n"
                    f"Failure Component details: {self.run.current_step}")

            msg = MIMEText(body)
            msg['Subject'] = subject
            msg['From'] = frm
            msg['To'] = to

            with smtplib.SMTP(host, int(port)) as server:
                server.send_message(msg)
        except Exception as e:
            logger.warning(f"Email send failed: {e}")

    def _resolve_context_string(self, value: str) -> str:
        """Resolve context.X references in a string using current config values."""
        if not value or 'context.' not in str(value):
            return str(value) if value else ''
        result = str(value)
        for key, val in self.config.items():
            if isinstance(val, str):
                result = result.replace(f'context.{key}', val)
        return result

    def _cleanup(self):
        if self.snowflake_conn:
            try:
                self.snowflake_conn.close()
            except:
                pass


    # =========================================================================
    # EXECUTION PLAN RUNNER — consumes generated execution_plan.json
    # =========================================================================
    def execute_from_plan(self, plan_path: str):
        """Execute pipeline from a generated execution plan JSON."""
        with open(plan_path, 'r') as f:
            plan = json.load(f)

        self.run.log_step('plan_start', 'RUNNING', detail=f'Executing plan: {plan_path}')

        for step in plan:
            comp_id = step.get('comp_id', '')
            conditions = step.get('conditions', [])

            # Check RUN_IF conditions
            if conditions:
                skip = False
                for cond in conditions:
                    if not self.translator_eval_condition(cond):
                        skip = True
                        break
                if skip:
                    self.run.log_step(comp_id, 'SKIPPED', detail=f'Condition not met: {conditions}')
                    continue

            try:
                self._dispatch_action(step, plan)
            except Exception as e:
                self.run.log_step(comp_id, 'ERROR', error=str(e))
                if step.get('die_on_error', True):
                    raise

        self.run.log_step('plan_complete', 'COMPLETED')

    def _execute_plan_step(self, step: dict, full_plan: list):
        """Execute a single plan step (used by LOOP and PARALLELIZE).
        Delegates to _dispatch_action so ALL action types work in nested contexts."""
        self._dispatch_action(step, full_plan)

    def _dispatch_action(self, step: dict, plan: list):
        """Central action dispatcher — handles every action type.
        Used by both execute_from_plan() loop and nested LOOP/PARALLELIZE targets."""
        action = step.get('action', '')
        comp_id = step.get('comp_id', '')

        if action == 'DBT_MODEL':
            model = step.get('model_name', comp_id)
            self._run_dbt_tag(model, f'Model: {model}', step_id=comp_id)

        elif action == 'LOOP':
            loop_from = int(step.get('loop_from', 1))
            loop_to = int(step.get('loop_to', 1))
            loop_step_val = int(step.get('loop_step', 1))
            targets = step.get('targets', [])

            # Check if a FILE_LIST feeds this loop (Talend ITERATE pattern)
            # The file list is stored in config by a preceding FILE_LIST step
            file_list = None
            for key, val in self.config.items():
                if key.startswith('_filelist_') and isinstance(val, list) and val:
                    file_list = val
                    break

            if file_list:
                # File-driven loop: iterate over enumerated files
                actual_count = min(loop_to, len(file_list))
                for i in range(actual_count):
                    filepath = file_list[i]
                    # Update per-iteration file context (Talend globalMap equivalent)
                    self.config['CURRENT_FILEPATH'] = filepath
                    self.config['CURRENT_FILENAME'] = os.path.basename(filepath)
                    self.config['CURRENT_FILEDIRECTORY'] = os.path.dirname(filepath)
                    self.config['CURRENT_ITERATION'] = str(i + 1)
                    self.run.log_step(comp_id, 'RUNNING',
                        detail=f'File iteration {i+1}/{actual_count}: {os.path.basename(filepath)}')

                    for tgt in targets:
                        tgt_step = next((s for s in plan if s['comp_id'] == tgt), None)
                        if tgt_step:
                            self._execute_plan_step(tgt_step, plan)
            else:
                # Numeric loop: standard range iteration
                for i in range(loop_from, loop_to + 1, loop_step_val):
                    self.config['CURRENT_ITERATION'] = str(i)
                    self.run.log_step(comp_id, 'RUNNING', detail=f'Iteration {i}/{loop_to}')

                    for tgt in targets:
                        tgt_step = next((s for s in plan if s['comp_id'] == tgt), None)
                        if tgt_step:
                            self._execute_plan_step(tgt_step, plan)

        elif action == 'PARALLELIZE':
            targets = step.get('targets', [])
            with ThreadPoolExecutor(max_workers=len(targets) or 1) as ex:
                futs = []
                for tgt in targets:
                    tgt_step = next((s for s in plan if s['comp_id'] == tgt), None)
                    if tgt_step:
                        futs.append(ex.submit(self._execute_plan_step, tgt_step, plan))
                for fut in as_completed(futs):
                    fut.result()

        elif action == 'FILE_LIST':
            directory = step.get('directory', '')
            filemask = step.get('filemask', '*')
            resolved_dir = self._resolve_context_string(directory)
            import fnmatch as _fnm
            found_files = []
            if os.path.isdir(resolved_dir):
                order = step.get('order_by', '')
                raw = os.listdir(resolved_dir)
                if order and 'date' in order.lower():
                    raw.sort(key=lambda f: os.path.getmtime(os.path.join(resolved_dir, f)))
                else:
                    raw.sort()
                for f in raw:
                    fp = os.path.join(resolved_dir, f)
                    if os.path.isfile(fp) and _fnm.fnmatch(f, filemask):
                        found_files.append(fp)
            self.config[f'_filelist_{comp_id}'] = found_files
            self.config['CURRENT_FILEPATH'] = found_files[0] if found_files else ''
            self.config['CURRENT_FILENAME'] = os.path.basename(found_files[0]) if found_files else ''
            self.config['CURRENT_FILEDIRECTORY'] = resolved_dir
            self.run.log_step(comp_id, 'COMPLETED',
                detail=f'FileList: {resolved_dir}/{filemask} -> {len(found_files)} files')

        elif action == 'FILE_COPY':
            src = self._resolve_context_string(step.get('source', ''))
            dst = self._resolve_context_string(step.get('destination', ''))
            # Support CURRENT_FILEPATH as source
            if not src or not os.path.exists(src):
                src = self.config.get('CURRENT_FILEPATH', src)
            self.run.log_step(comp_id, 'RUNNING', detail=f'Copy: {src} -> {dst}')
            if os.path.exists(src) and dst:
                os.makedirs(os.path.dirname(dst) if os.path.dirname(dst) else '.', exist_ok=True)
                shutil.copy2(src, dst)
                if step.get('remove_source', 'false').lower() == 'true':
                    os.remove(src)
            self.run.log_step(comp_id, 'COMPLETED')

        elif action == 'FILE_DELETE':
            path = self._resolve_context_string(step.get('path', ''))
            if not path:
                path = self.config.get('CURRENT_FILEPATH', '')
            if path and os.path.exists(path):
                os.remove(path)
            self.run.log_step(comp_id, 'COMPLETED', detail=f'Deleted: {path}')

        elif action == 'FILE_ARCHIVE':
            src = self._resolve_context_string(step.get('source', ''))
            tgt = self._resolve_context_string(step.get('target', ''))
            encrypt = step.get('encrypt', 'false').lower() == 'true'
            if os.path.exists(src) and tgt:
                os.makedirs(os.path.dirname(tgt) if os.path.dirname(tgt) else '.', exist_ok=True)
                if encrypt and step.get('aes_strength'):
                    try:
                        import pyzipper
                        pwd = step.get('password', '').encode()
                        with pyzipper.AESZipFile(tgt, 'w',
                                compression=pyzipper.ZIP_DEFLATED,
                                encryption=pyzipper.WZ_AES) as zf:
                            zf.setpassword(pwd)
                            zf.write(src, os.path.basename(src))
                    except ImportError:
                        with zipfile.ZipFile(tgt, 'w', zipfile.ZIP_DEFLATED) as zf:
                            zf.write(src, os.path.basename(src))
                        logger.warning(f'pyzipper not installed, AES skipped for {src}')
                else:
                    with zipfile.ZipFile(tgt, 'w', zipfile.ZIP_DEFLATED) as zf:
                        zf.write(src, os.path.basename(src))
            self.run.log_step(comp_id, 'COMPLETED', detail=f'Archived: {src} -> {tgt}')

        elif action == 'RUN_CHILD_JOB':
            child = step.get('child_process', '')
            overrides = step.get('context_overrides', {})
            die = step.get('die_on_error', True)
            self.run.log_step(comp_id, 'RUNNING', detail=f'Child job: {child}')

            saved = {}
            if overrides:
                for k, v in overrides.items():
                    saved[k] = self.config.get(k)
                    self.config[k] = self._resolve_context_string(v)
                logger.info(f"Applied {len(overrides)} overrides: {list(overrides.keys())}")

            child_safe = child.lower().replace("/","_").replace("\\","_")
            child_plan = os.path.join(self.dbt_project_dir, 'execution_plans',
                                      f'{child_safe}_plan.json')
            try:
                if os.path.exists(child_plan):
                    self.execute_from_plan(child_plan)
                else:
                    self.run.log_step(comp_id, 'COMPLETED',
                        detail=f'Child plan not found: {child_plan}')
            except Exception as child_err:
                if die:
                    raise
                else:
                    self.run.log_step(comp_id, 'ERROR',
                        error=str(child_err), detail='die_on_error=False, continuing')
            finally:
                if saved:
                    for k, orig in saved.items():
                        if orig is not None:
                            self.config[k] = orig
                        elif k in self.config:
                            del self.config[k]

        elif action == 'SEND_MAIL':
            try:
                import smtplib
                from email.mime.text import MIMEText
                from email.mime.multipart import MIMEMultipart

                host = self._resolve_context_string(step.get('smtp_host', '') or self.config.get('smtp_host', ''))
                port_raw = self._resolve_context_string(step.get('smtp_port', '') or str(self.config.get('smtp_port', 25)))
                port = int(port_raw) if str(port_raw).isdigit() else 25
                to_addr = self._resolve_context_string(step.get('to', '') or self.config.get('email_to', ''))
                from_addr = self._resolve_context_string(step.get('from', '') or self.config.get('email_from', ''))
                cc_addr = self._resolve_context_string(step.get('cc', '') or '')
                subject = self._resolve_context_string(step.get('subject', ''))
                body = self._resolve_context_string(step.get('message', ''))
                need_auth = step.get('need_auth', 'false')
                importance = step.get('importance', '')

                if self.config.get('_log_catcher_active') and self.run.errors:
                    last_err = self.run.errors[-1] if self.run.errors else ''
                    body = body.replace('((String)globalMap.get("tLogCatcher_1_EXCEPTION_STACKTRACE"))', str(last_err))
                    body = body.replace('context.errorMessage', self.config.get('errorMessage', str(last_err)))
                    if not body.strip():
                        body = f"Job failed: {last_err}\nStep: {self.run.current_step}"

                if host and to_addr and from_addr:
                    msg = MIMEMultipart()
                    msg['Subject'] = subject
                    msg['From'] = from_addr
                    msg['To'] = to_addr
                    if cc_addr:
                        msg['Cc'] = cc_addr
                    if importance:
                        msg['X-Priority'] = '1' if importance.lower() == 'high' else '3'
                    msg.attach(MIMEText(body, 'plain'))
                    recipients = [to_addr] + ([cc_addr] if cc_addr else [])
                    with smtplib.SMTP(host, port) as srv:
                        if str(need_auth).lower() == 'true':
                            u = self._resolve_context_string(self.config.get('smtp_user', ''))
                            p = self._resolve_context_string(self.config.get('smtp_password', ''))
                            if u and p:
                                srv.login(u, p)
                        srv.sendmail(from_addr, recipients, msg.as_string())
                    self.run.log_step(comp_id, 'COMPLETED',
                        detail=f'Email sent to {to_addr}' + (f' cc:{cc_addr}' if cc_addr else ''))
                else:
                    self.run.log_step(comp_id, 'COMPLETED',
                        detail=f'SMTP incomplete: host={bool(host)} to={bool(to_addr)} from={bool(from_addr)}')
            except Exception as mail_err:
                logger.warning(f"SendMail failed: {mail_err}")
                self.run.log_step(comp_id, 'COMPLETED', detail=f'Email failed (non-fatal): {mail_err}')

        elif action == 'LOG_CATCHER':
            self.config['_log_catcher_active'] = True
            self.config['_log_catcher_comp'] = comp_id
            catch_types = []
            if step.get('catch_java'): catch_types.append('Java')
            if step.get('catch_tdie'): catch_types.append('tDie')
            if step.get('catch_twarn'): catch_types.append('tWarn')
            self.run.log_step(comp_id, 'COMPLETED',
                detail=f'LogCatcher active: catches {", ".join(catch_types) or "all"}')

        elif action == 'CHRONOMETER':
            since_comp = step.get('since', '')
            elapsed = 0
            for s in self.run.steps:
                if s['step'] == since_comp and s.get('timestamp'):
                    from datetime import datetime as _dt
                    start = _dt.fromisoformat(s['timestamp'])
                    elapsed = (_dt.now() - start).total_seconds()
                    break
            self.config['_elapsed_time'] = str(elapsed)
            self.config['elapsedTime'] = str(int(elapsed * 1000))
            self.run.log_step(comp_id, 'COMPLETED', detail=f'Timer: {elapsed:.2f}s since {since_comp}')

        elif action == 'PREJOB':
            self._prejob()
            self.run.log_step(comp_id, 'COMPLETED', detail='Prejob executed')

        elif action == 'POSTJOB':
            self._postjob_file_movement()
            self.run.log_step(comp_id, 'COMPLETED', detail='Postjob executed')

        elif action == 'DB_CONNECTION':
            conn_name = comp_id
            if not self.snowflake_conn:
                try:
                    sf_cfg = self.config.get('snowflake', {})
                    conn_host = self._resolve_context_string(step.get('host', '')) or sf_cfg.get('host', '')
                    conn_db = self._resolve_context_string(step.get('database', '')) or sf_cfg.get('database', '')
                    if sf_cfg.get('password'):
                        import snowflake.connector
                        self.snowflake_conn = snowflake.connector.connect(
                            account=sf_cfg.get('account', ''), host=conn_host,
                            user=sf_cfg.get('user', ''), password=sf_cfg['password'],
                            database=conn_db, schema=sf_cfg.get('schema', 'PUBLIC'),
                            warehouse=sf_cfg.get('warehouse', ''), role=sf_cfg.get('role', ''),
                            login_timeout=sf_cfg.get('loginTimeout', 15),
                        )
                        self.config[f'_connection_{conn_name}'] = 'active'
                        self.run.log_step(comp_id, 'COMPLETED',
                            detail=f'Connected: {conn_name} -> {conn_host}/{conn_db}')
                    else:
                        self.config[f'_connection_{conn_name}'] = 'dev_mode'
                        self.run.log_step(comp_id, 'COMPLETED',
                            detail=f'No credentials for {conn_name}, dev mode')
                except Exception as conn_err:
                    self.config[f'_connection_{conn_name}'] = f'failed:{conn_err}'
                    self.run.log_step(comp_id, 'ERROR', error=str(conn_err),
                        detail=f'Connection {conn_name} failed - fatal in Talend')
                    raise
            else:
                self.config[f'_connection_{conn_name}'] = 'reused'
                self.run.log_step(comp_id, 'COMPLETED', detail=f'{conn_name} reusing existing connection')

        else:
            self.run.log_step(comp_id, 'COMPLETED', detail=f'Unknown action: {action} (passthrough)')

    def translator_eval_condition(self, condition: str) -> bool:
        """Evaluate a RUN_IF condition. Safe pattern matching, no eval().
        Handles: !=null, ==null, !=value, ==value, >, <, &&, ||, .equals()"""
        if not condition:
            return True

        import re as _re
        cond = condition.strip()

        # AND: all parts true
        if '&&' in cond:
            return all(self.translator_eval_condition(p.strip()) for p in cond.split('&&'))

        # OR: any part true
        if '||' in cond:
            return any(self.translator_eval_condition(p.strip()) for p in cond.split('||'))

        def _resolve(expr):
            expr = expr.strip().strip('"').strip("'")
            m = _re.match(r'context\.(\w+)', expr)
            if m:
                return self.config.get(m.group(1))
            if expr in ('null', 'None'):
                return None
            if expr.lstrip('-').isdigit():
                return int(expr)
            try:
                return float(expr)
            except ValueError:
                return expr

        # != null
        m = _re.match(r'(.+?)\s*!=\s*null$', cond)
        if m:
            return _resolve(m.group(1)) is not None

        # == null
        m = _re.match(r'(.+?)\s*==\s*null$', cond)
        if m:
            return _resolve(m.group(1)) is None

        # != value (not null)
        m = _re.match(r'(.+?)\s*!=\s*(.+)', cond)
        if m and 'null' not in m.group(2).strip():
            l, r = _resolve(m.group(1)), _resolve(m.group(2))
            return str(l) != str(r) if l is not None else True

        # == value (not null)
        m = _re.match(r'(.+?)\s*==\s*(.+)', cond)
        if m and 'null' not in m.group(2).strip():
            l, r = _resolve(m.group(1)), _resolve(m.group(2))
            return str(l) == str(r) if l is not None else False

        # > value
        m = _re.match(r'(.+?)\s*>\s*(.+)', cond)
        if m:
            try:
                return float(str(_resolve(m.group(1)) or 0)) > float(str(_resolve(m.group(2)) or 0))
            except (ValueError, TypeError):
                return True

        # < value
        m = _re.match(r'(.+?)\s*<\s*(.+)', cond)
        if m:
            try:
                return float(str(_resolve(m.group(1)) or 0)) < float(str(_resolve(m.group(2)) or 0))
            except (ValueError, TypeError):
                return True

        # .equals("value")
        m = _re.match(r'(.+?)\.equals\(["\'](.*?)["\']\)', cond)
        if m:
            return str(_resolve(m.group(1))) == m.group(2)

        # Unrecognized — warn and default to execute
        logger.warning(f"GRP{self.grp_number} Unrecognized RUN_IF: {condition}, defaulting TRUE")
        return True

# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def run_group(grp_number: str, dbt_project_dir: str, config_path: str = None,
              push_snowflake: bool = False, dev_mode: bool = True) -> PipelineRun:
    """Run full pipeline for one group. Set dev_mode=False for production.
    Tries execution plan first (generic), falls back to hardcoded flow."""
    orch = MasterOrchestrator(grp_number, dbt_project_dir, config_path, dev_mode=dev_mode)

    # Try plan-driven execution first (Step 1 + Step 2 plans)
    step1_plan = os.path.join(dbt_project_dir, 'execution_plans',
                               'ac_st_all_load_main_grp_ag25v2_plan.json')
    step2_plan = os.path.join(dbt_project_dir, 'execution_plans',
                               'ac_st_post_main_grp_load_ag25v3_plan.json')

    if os.path.exists(step1_plan):
        logger.info(f"GRP{grp_number} Using execution plan: {step1_plan}")
        orch.run = PipelineRun(str(grp_number), orch.config)
        orch.run.start_time = datetime.now()
        orch.run.status = 'RUNNING'
        try:
            orch._prejob()
            orch.execute_from_plan(step1_plan)
            if os.path.exists(step2_plan):
                orch.execute_from_plan(step2_plan)
            orch.run.status = 'COMPLETED'
        except Exception as e:
            orch.run.status = 'FAILED'
            orch.run.log_step('plan_fatal', 'ERROR', error=str(e))
            orch._send_error_email(str(e))
        finally:
            try:
                orch._postjob_file_movement()
            except Exception:
                pass
            orch.run.end_time = datetime.now()
            orch._cleanup()
        run = orch.run
    else:
        # Fallback: hardcoded flow (for when generator hasn't been run yet)
        logger.info(f"GRP{grp_number} No execution plan found, using hardcoded flow")
        run = orch.execute_full_plan()

    # Parquet export + Snowflake push
    if run.status == 'COMPLETED' and push_snowflake:
        parquet = orch.export_parquet()
        if parquet:
            orch.push_to_snowflake(parquet)

    return run


def run_all_groups(dbt_project_dir: str, config_path: str = None,
                   max_parallel: int = 4, push_snowflake: bool = False,
                   dev_mode: bool = True):
    """Run all 20 groups. Set dev_mode=False for production."""
    results = {}
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(run_group, str(grp), dbt_project_dir, config_path, push_snowflake, dev_mode): grp
            for grp in range(1, 21)
        }
        for future in as_completed(futures):
            grp = futures[future]
            try:
                results[grp] = future.result()
            except Exception as e:
                logger.error(f"GRP{grp} failed: {e}")
    return results


# =============================================================================
# SCHEDULER: 30-minute interval (replaces TMC Daily trigger)
# =============================================================================

def start_scheduler(dbt_project_dir: str, config_path: str = None,
                    interval_minutes: int = 30, max_parallel: int = 4,
                    dev_mode: bool = False):
    """Start the 30-min scheduler. Defaults to dev_mode=False (production)."""
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        logger.error("apscheduler not installed. Run: pip install apscheduler")
        return

    scheduler = BlockingScheduler()

    def scheduled_run():
        logger.info(f"=== Scheduled run triggered at {datetime.now().isoformat()} ===")
        run_all_groups(dbt_project_dir, config_path, max_parallel, dev_mode=dev_mode)

    scheduler.add_job(scheduled_run, 'interval', minutes=interval_minutes,
                      next_run_time=datetime.now())

    logger.info(f"Scheduler started: every {interval_minutes} min, {max_parallel} parallel groups")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
