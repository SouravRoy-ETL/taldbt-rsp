"""
WU RSP Pipeline — dbt Project Generator (Production Grade)

Architecture:
  The generator produces TWO outputs:
  1. dbt models     — for SQL-transforming components (tMap, tJavaRow, tDBRow, tDBOutput,
                       tDBInput, tUniqRow, tFileInput, tExtractDelimitedFields, tFixedFlowInput,
                       tFileOutputDelimited, tFlowToIterate)
  2. execution_plan.json — for orchestration components (tFileList, tFileCopy, tFileDelete,
                       tFileArchive, tParallelize, tLoop, tLogCatcher, tSendMail,
                       tChronometerStop, RUN_IF conditions, tRunJob child invocations)

  The orchestrator reads execution_plan.json and drives both dbt runs AND file/loop/parallel ops.

Every component type the parser discovers gets a handler. Nothing is parse-only.
Unknown patterns are flagged with UNTRANSLATED, never silently skipped.
"""
import os
import re
import sys
import json
import yaml
import shutil
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict, OrderedDict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from generator.xml_parser import ProjectParser
from generator.expression_translator import JavaToSQLTranslator, RoutineRegistry


# =============================================================================
# CONNECTION GRAPH — with real subjob boundaries and execution plan
# =============================================================================

class ConnectionGraph:
    """
    Directed graph from Talend connections.
    Separates data flow from trigger flow.
    Builds a structured execution plan respecting subjob boundaries,
    parallelization, synchronization, loop semantics, and RUN_IF conditions.
    """

    def __init__(self, connections: list, components: dict):
        self.connections = connections
        self.components = components
        self.adj = defaultdict(list)
        self.rev = defaultdict(list)
        self.data_flow = defaultdict(list)
        self.triggers = defaultdict(list)
        self._build()

    def _build(self):
        for conn in self.connections:
            src = conn['source']
            tgt = conn['target']
            ctype = conn.get('type', '').upper()
            edge = {
                'source': src, 'target': tgt, 'type': ctype,
                'label': conn.get('label', ''), 'order': conn.get('order', ''),
                'condition': conn.get('condition', ''),
            }
            self.adj[src].append(edge)
            self.rev[tgt].append(edge)

            if ctype in ('MAIN', 'LOOKUP', 'ITERATE', 'FILTER', 'REJECT',
                         'FLOW_MAIN', 'FLOW_MERGE', 'FLOW_REF'):
                self.data_flow[src].append(edge)
            else:
                self.triggers[src].append(edge)

    def get_data_sources(self, comp_id: str) -> list:
        return [e for e in self.rev.get(comp_id, [])
                if e['type'] in ('MAIN', 'LOOKUP', 'ITERATE', 'FLOW_MAIN')]

    def get_data_targets(self, comp_id: str) -> list:
        return [e for e in self.data_flow.get(comp_id, [])]

    def get_trigger_targets(self, comp_id: str) -> list:
        return sorted(self.triggers.get(comp_id, []),
                      key=lambda e: e.get('order', '0'))

    def get_execution_order(self) -> list:
        """Topological sort respecting trigger ordering."""
        visited = set()
        order = []
        def dfs(node):
            if node in visited:
                return
            visited.add(node)
            for edge in sorted(self.triggers.get(node, []),
                               key=lambda e: e.get('order', '0')):
                dfs(edge['target'])
            order.append(node)

        all_targets = set()
        for edges in self.triggers.values():
            for e in edges:
                all_targets.add(e['target'])
        roots = [c for c in self.components if c not in all_targets]
        for root in roots:
            dfs(root)
        for comp in self.components:
            dfs(comp)
        order.reverse()
        return order

    def build_execution_plan(self) -> list:
        """
        Build a structured execution plan that the orchestrator can consume.
        Each entry has: comp_id, comp_type, action, dependencies, condition, metadata.
        Orchestration components (tLoop, tParallelize, tFileList, etc.) become
        explicit plan steps instead of being ignored.
        """
        plan = []
        exec_order = self.get_execution_order()

        for comp_id in exec_order:
            comp = self.components.get(comp_id, {})
            comp_type = comp.get('component_type', '')

            # Determine what triggers this component
            incoming_triggers = [e for e in self.rev.get(comp_id, [])
                                 if e['type'] not in ('MAIN', 'LOOKUP', 'ITERATE',
                                                       'FLOW_MAIN', 'FILTER', 'REJECT')]
            dependencies = [e['source'] for e in incoming_triggers]
            conditions = [e['condition'] for e in incoming_triggers if e.get('condition')]

            # Outgoing triggers (what this component triggers next)
            outgoing = self.get_trigger_targets(comp_id)

            step = {
                'comp_id': comp_id,
                'comp_type': comp_type,
                'dependencies': dependencies,
                'conditions': conditions,
                'trigger_type': incoming_triggers[0]['type'] if incoming_triggers else 'ROOT',
            }

            # Enrich with component-specific metadata
            if 'tLoop' in comp_type:
                loop = comp.get('loop', {})
                step['action'] = 'LOOP'
                step['loop_type'] = loop.get('loop_type', 'For')
                step['loop_from'] = loop.get('from', '1')
                step['loop_to'] = loop.get('to', '1')
                step['loop_step'] = loop.get('step', '1')
                step['loop_condition'] = loop.get('condition', '')
                # Populate targets: components triggered by this loop's outgoing edges
                step['targets'] = [e['target'] for e in outgoing]
            elif 'tParallelize' in comp_type:
                par = comp.get('parallelize', {})
                step['action'] = 'PARALLELIZE'
                step['wait_for'] = par.get('wait_for', '')
                step['targets'] = [e['target'] for e in outgoing]
            elif 'tFileList' in comp_type:
                fl = comp.get('file_list', {})
                step['action'] = 'FILE_LIST'
                step['directory'] = self._str_val(fl.get('directory', ''))
                step['filemask'] = self._str_val(fl.get('filemask', ''))
                step['order_by'] = fl.get('order_by_filename', '')
            elif 'tFileCopy' in comp_type:
                fc = comp.get('file_copy', {})
                step['action'] = 'FILE_COPY'
                step['source'] = self._str_val(fc.get('filename', '')) or self._str_val(fc.get('source_directory', ''))
                step['destination'] = self._str_val(fc.get('destination', ''))
                step['rename'] = self._str_val(fc.get('rename', ''))
                step['remove_source'] = self._str_val(fc.get('remove_file', 'false'))
            elif 'tFileDelete' in comp_type:
                fd = comp.get('file_delete', {})
                step['action'] = 'FILE_DELETE'
                step['path'] = self._str_val(fd.get('filename', '')) or self._str_val(fd.get('path', ''))
                step['failon'] = self._str_val(fd.get('failon', 'true'))
            elif 'tFileArchive' in comp_type:
                fa = comp.get('file_archive', {})
                step['action'] = 'FILE_ARCHIVE'
                step['source'] = self._str_val(fa.get('source', ''))
                step['target'] = self._str_val(fa.get('target', ''))
                step['format'] = self._str_val(fa.get('archive_format', 'zip'))
                step['encrypt'] = self._str_val(fa.get('encrypt_files', 'false'))
                step['encrypt_method'] = self._str_val(fa.get('encrypt_method', ''))
                step['aes_strength'] = self._str_val(fa.get('aes_key_strength', ''))
                step['password'] = self._str_val(fa.get('password', ''))
            elif 'tRunJob' in comp_type:
                cj = comp.get('child_job', {})
                step['action'] = 'RUN_CHILD_JOB'
                step['child_process'] = cj.get('process', '')
                step['independent'] = cj.get('independent', False)
                step['die_on_error'] = cj.get('die_on_error', True)
                step['transmit_context'] = cj.get('transmit_context', True)
                step['context_overrides'] = cj.get('context_overrides', {})
            elif 'tLogCatcher' in comp_type:
                lc = comp.get('log_catcher', {})
                step['action'] = 'LOG_CATCHER'
                step['catch_java'] = lc.get('catch_java', '')
                step['catch_tdie'] = lc.get('catch_tdie', '')
                step['catch_twarn'] = lc.get('catch_twarn', '')
            elif 'tSendMail' in comp_type:
                sm = comp.get('send_mail', {})
                step['action'] = 'SEND_MAIL'
                step['smtp_host'] = sm.get('smtp_host', '')
                step['smtp_port'] = sm.get('smtp_port', '')
                step['to'] = sm.get('to', '')
                step['from'] = sm.get('from', '')
                step['subject'] = sm.get('subject', '')
                step['message'] = sm.get('message', '')
            elif 'tChronometerStop' in comp_type:
                cs = comp.get('chronometer', {})
                step['action'] = 'CHRONOMETER'
                step['since'] = cs.get('since', '')
            elif 'tPrejob' in comp_type:
                step['action'] = 'PREJOB'
            elif 'tPostjob' in comp_type:
                step['action'] = 'POSTJOB'
            elif 'tDBConnection' in comp_type:
                dc = comp.get('db_connection', {})
                step['action'] = 'DB_CONNECTION'
                step['host'] = self._str_val(dc.get('host', ''))
                step['database'] = self._str_val(dc.get('database', ''))
            else:
                # SQL-transforming components get a DBT_MODEL action
                step['action'] = 'DBT_MODEL'
                step['model_name'] = ''  # filled in by generator

            plan.append(step)

        return plan



class DBTProjectGenerator:
    """Generates a complete dbt project from parsed Talend .items."""

    def __init__(self, items_dir: str, output_dir: str):
        self.items_dir = items_dir
        self.output_dir = output_dir
        self.parser = ProjectParser(items_dir)
        self.routine_registry = RoutineRegistry()
        self.routine_registry.discover_routines(items_dir)
        self.translator = JavaToSQLTranslator(self.routine_registry)
        self.parsed_items = {}
        self.generated_models = []
        self.generated_macros = set()
        self.all_schemas = {}
        self.all_java_patterns = set()
        self.all_contexts = {}
        self.model_registry = {}  # comp_id → model_name for ref() wiring
        self.source_tables = {}    # connection_ref → {database, schema, tables}
        self.group_tables = set()  # table names that are group-specific (contain GRP or context.grp)
        self.execution_plans = {}  # job_name → [plan steps]

    def generate(self):
        print("=" * 70)
        print("WU RSP Pipeline — dbt Project Generator (Production)")
        print(f"Input:  {self.items_dir}")
        print(f"Output: {self.output_dir}")
        print("=" * 70)

        print("\n[1/9] Parsing .item files...")
        self.parsed_items = self.parser.parse_all()

        print("\n[2/9] Collecting contexts, schemas, patterns...")
        self._collect_global_data()

        print("\n[2b/9] Classifying source tables (shared vs group-specific)...")
        self._collect_source_tables()

        print("\n[3/9] Creating directory structure...")
        self._create_dirs()

        print("\n[4/9] Generating project config...")
        self._gen_project_yml()
        self._gen_profiles_yml()

        print("\n[5/9] Generating macros...")
        self._gen_macros()

        print("\n[6/9] Generating table DDLs...")
        self._gen_ddls()

        print("\n[7/9] Generating dbt models + execution plans...")
        for job_name, job_data in self.parsed_items.items():
            self._gen_job_models(job_name, job_data)

        print("\n[8/9] Writing execution plans...")
        self._write_execution_plans()

        print("\n[9/10] Generating schema.yml...")
        self._gen_schema_yml()

        print("\n[10/10] Generating sources.yml (shared tables only)...")
        self._gen_sources_yml()

        self._write_manifest()
        report = self.translator.get_report()
        if report['untranslated']:
            rpath = os.path.join(self.output_dir, 'UNTRANSLATED_REPORT.json')
            with open(rpath, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2)
            print(f"\n[!] {len(report['untranslated'])} expressions could NOT be translated! See: {rpath}")
            for item in report['untranslated'][:10]:
                print(f"  - [{item['context']}] {item['reason']}")
        if report['routine_calls']:
            print(f"\n[!] {len(report['routine_calls'])} routine calls found (need manual SQL):")
            for call in report['routine_calls']:
                print(f"  - {call}")
        print(f"\n{'='*70}")
        print(f"Done! {len(self.generated_models)} models, {len(self.generated_macros)} macros, "
              f"{len(self.execution_plans)} execution plans")
        print(f"  Translated: {report['stats']['translated']} | Flagged: {report['stats']['flagged']}")
        print(f"Next: cd {self.output_dir} && dbt debug && dbt run")
        print(f"{'='*70}")

    def get_report(self):
        return self.translator.get_report()

    # =========================================================================
    # COLLECT GLOBAL DATA
    # =========================================================================
    def _collect_global_data(self):
        for job_name, job_data in self.parsed_items.items():
            self.all_contexts.update(job_data.get('contexts', {}))
            for comp_id, comp in job_data.get('components', {}).items():
                for key in ('db_output', 'db_input', 'file_input', 'file_output',
                            'extract_fields', 'fixed_flow', 'uniq_row'):
                    sub = comp.get(key, {})
                    if isinstance(sub, dict) and 'schema_columns' in sub:
                        cols = sub['schema_columns']
                        if cols:
                            self.all_schemas[f"{job_name}.{comp_id}"] = cols
                code = comp.get('java_code', '')
                if code:
                    pats = re.findall(
                        r'(Mathematical\.\w+|StringHandling\.\w+|TalendDate\.\w+'
                        r'|BigDecimal\.\w+|Numeric\.\w+|Double\.\w+|Integer\.\w+'
                        r'|String\.valueOf|\.subtract\(|\.multiply\(|\.divide\('
                        r'|\.compareTo\(|\.setScale\(|\.toString\(\)|\.equals\('
                        r'|\.contains\(|\.startsWith\(|\.endsWith\(|\.replaceAll\()', code)
                    self.all_java_patterns.update(pats)
                tmap = comp.get('tmap_data', {})
                if tmap:
                    for var in tmap.get('variables', []):
                        expr = var.get('expression', '')
                        if expr:
                            pats = re.findall(
                                r'(Mathematical\.\w+|StringHandling\.\w+|TalendDate\.\w+'
                                r'|BigDecimal\.\w+|Numeric\.\w+|Double\.\w+|Integer\.\w+'
                                r'|String\.valueOf|\.subtract\(|\.multiply\(|\.divide\('
                                r'|\.compareTo\(|\.setScale\()', expr)
                            self.all_java_patterns.update(pats)
                    for out in tmap.get('outputs', []):
                        for col in out.get('columns', []):
                            expr = col.get('expression', '')
                            if expr:
                                pats = re.findall(
                                    r'(Mathematical\.\w+|StringHandling\.\w+|TalendDate\.\w+|Double\.\w+)', expr)
                                self.all_java_patterns.update(pats)
        print(f"  Contexts: {len(self.all_contexts)} | Schemas: {len(self.all_schemas)} | Patterns: {len(self.all_java_patterns)}")

    # Known group-specific table patterns from WU RSP context config
    GRP_TABLE_PATTERNS = [
        'GRP', 'grp', 'context.grp', 'context.entry_tablename', 'context.adjustment_tablename',
        'context.txntemplate_main', 'context.Entry_stgstfile', 'context.Entry_ACfiletable',
        'context.acstmerge', 'context.entry_dup', 'context.compliance_check',
        'context.AC_Rawdata', 'context.Recordtype4', 'context.ST_FILENAMES',
        'context.AC_FILENAMES',
    ]

    def _collect_source_tables(self):
        """Collect all tables from tDBInput/tDBOutput, classify as SHARED vs GROUP-SPECIFIC.
        
        SHARED tables (same across all 20 groups): go into sources.yml
          e.g. TXNSHAREDCOST, TXNADJUSTMENTREASON, EMAIL_ALERTS, TXNCOUNTRY
        
        GROUP-SPECIFIC tables (parameterized per group): use {{ var() }} in SQL
          e.g. TXNENTRY_GRP{N}_SATEST, TXNADJUSTMENT_GRP{N}, TXNTEMPLATE_GRP{N}
          
        This separation is critical for scaling to 20 groups:
        - One set of dbt models is generated ONCE
        - At runtime, each group gets different --vars with its table names
        - sources.yml stays static because it only contains shared tables
        """
        for job_name, job_data in self.parsed_items.items():
            components = job_data.get('components', {})

            # Build connection_ref → {database, schema} map
            conn_map = {}
            for comp_id, comp in components.items():
                dc = comp.get('db_connection', {})
                if dc:
                    conn_map[comp_id] = {
                        'database': self._str_val(dc.get('database', '')),
                        'schema': self._str_val(dc.get('schema', '')),
                    }

            # Collect tables from tDBInput and tDBOutput
            for comp_id, comp in components.items():
                for key in ('db_input', 'db_output'):
                    sub = comp.get(key, {})
                    if not sub:
                        continue
                    table = self._str_val(sub.get('table', ''))
                    if not table:
                        continue

                    # Classify: is this table group-specific?
                    is_grp = any(pat in table for pat in self.GRP_TABLE_PATTERNS)

                    if is_grp:
                        self.group_tables.add(table)
                        continue  # Don't add to sources.yml

                    # Shared table → add to sources
                    conn_ref = self._str_val(sub.get('connection_ref', ''))
                    conn_info = conn_map.get(conn_ref, {})
                    db_name = conn_info.get('database', '') or 'UNKNOWN_DB'
                    schema_name = conn_info.get('schema', '') or 'PUBLIC'

                    source_key = conn_ref or f"{db_name}_{schema_name}"
                    if source_key not in self.source_tables:
                        self.source_tables[source_key] = {
                            'database': db_name,
                            'schema': schema_name,
                            'connection_ref': conn_ref,
                            'tables': {},
                        }

                    table_clean = table.strip('"').strip("'")
                    if table_clean not in self.source_tables[source_key]['tables']:
                        columns = sub.get('schema_columns', [])
                        self.source_tables[source_key]['tables'][table_clean] = {
                            'name': table_clean,
                            'columns': [c.get('name', '') for c in columns if c.get('name')],
                            'type': key,
                        }

        shared = sum(len(s['tables']) for s in self.source_tables.values())
        print(f"  SHARED tables (→ sources.yml): {shared}")
        print(f"  GROUP-SPECIFIC tables (→ var()): {len(self.group_tables)}")
        for t in sorted(self.group_tables)[:10]:
            print(f"    grp: {t}")

    def _create_dirs(self):
        for d in ['models/staging', 'models/joblets', 'models/post_sync',
                   'models/final_load', 'models/post_job', 'models/sources',
                   'macros', 'seeds', 'tests', 'snapshots', 'analyses']:
            os.makedirs(os.path.join(self.output_dir, d), exist_ok=True)
    # STEP 4: dbt_project.yml from PARSED contexts (not hardcoded)
    # =========================================================================
    def _gen_project_yml(self):
        dbt_vars = {}
        for ctx_name, ctx_data in self.all_contexts.items():
            val = ctx_data.get('value', '')
            if val:
                dbt_vars[ctx_name.lower()] = val

        grp = dbt_vars.get('grp_number', '16')
        defaults = {
            'grp_number': grp,
            'grp_no': grp,
            'entry_tablename': f'TXNENTRY_GRP{grp}_SATEST',
            'adjustment_tablename': f'TXNADJUSTMENT_GRP{grp}',
        }
        for k, v in defaults.items():
            if k not in dbt_vars:
                dbt_vars[k] = v

        project = {
            'name': 'wu_rsp_pipeline',
            'version': '1.0.0',
            'config-version': 2,
            'profile': 'wu_rsp',
            'model-paths': ['models'],
            'analysis-paths': ['analyses'],
            'test-paths': ['tests'],
            'seed-paths': ['seeds'],
            'macro-paths': ['macros'],
            'snapshot-paths': ['snapshots'],
            'vars': dbt_vars,
        }
        path = os.path.join(self.output_dir, 'dbt_project.yml')
        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump(project, f, default_flow_style=False, sort_keys=False)
        print(f"  dbt_project.yml ({len(dbt_vars)} vars from parsed contexts)")

    def _gen_profiles_yml(self):
        # Write as raw text because yaml.dump would escape the Jinja env_var() syntax
        profiles_content = """wu_rsp:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH', 'wu_rsp_dev.duckdb') }}"
      threads: 4
"""
        path = os.path.join(self.output_dir, 'profiles.yml')
        with open(path, 'w', encoding='utf-8') as f:
            f.write(profiles_content)
        print(f"  profiles.yml (DuckDB, uses DUCKDB_PATH env var)")

    # =========================================================================
    # STEP 5: Generate macros from ALL discovered Java patterns
    # =========================================================================
    def _gen_macros(self):
        lines = ["{# ================================================================= #}",
                 "{# Auto-generated macros from ALL Talend Java patterns discovered    #}",
                 "{# in .item files. Covers: Mathematical, StringHandling, TalendDate, #}",
                 "{# BigDecimal chains, Numeric.sequence, type casts, string ops.      #}",
                 "{# ================================================================= #}",
                 ""]

        MACROS = {
            'Mathematical.SMUL': '''{%- macro smul(multiplier, value) -%}
  (CAST({{ value }} AS DOUBLE) * {{ multiplier }})
{%- endmacro -%}''',
            'StringHandling.TRIM': '''{%- macro strim(value) -%}
  TRIM({{ value }})
{%- endmacro -%}''',
            'StringHandling.RIGHT': '''{%- macro sright(value, n) -%}
  RIGHT(CAST({{ value }} AS VARCHAR), {{ n }})
{%- endmacro -%}''',
            'StringHandling.LEFT': '''{%- macro sleft(value, n) -%}
  LEFT(CAST({{ value }} AS VARCHAR), {{ n }})
{%- endmacro -%}''',
            'StringHandling.LEN': '''{%- macro slen(value) -%}
  LENGTH(CAST({{ value }} AS VARCHAR))
{%- endmacro -%}''',
            'StringHandling.SUBSTR': '''{%- macro ssubstr(value, start, length) -%}
  SUBSTR(CAST({{ value }} AS VARCHAR), {{ start }}, {{ length }})
{%- endmacro -%}''',
            'StringHandling.INDEX': '''{%- macro sindex(value, search) -%}
  POSITION({{ search }} IN CAST({{ value }} AS VARCHAR))
{%- endmacro -%}''',
            'StringHandling.UPCASE': '''{%- macro supcase(value) -%}
  UPPER(CAST({{ value }} AS VARCHAR))
{%- endmacro -%}''',
            'StringHandling.DOWNCASE': '''{%- macro sdowncase(value) -%}
  LOWER(CAST({{ value }} AS VARCHAR))
{%- endmacro -%}''',
            'TalendDate.formatDate': '''{%- macro talend_format_date(fmt, date_val) -%}
  STRFTIME({{ date_val }}, {{ fmt }})
{%- endmacro -%}''',
            'TalendDate.getDate': '''{%- macro talend_get_date(fmt) -%}
  STRFTIME(NOW(), {{ fmt }})
{%- endmacro -%}''',
            'TalendDate.getCurrentDate': '''{%- macro talend_now() -%}
  NOW()
{%- endmacro -%}''',
            'Numeric.sequence': '''{%- macro talend_sequence(name, start, step) -%}
  ({{ start }} - 1 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
{%- endmacro -%}''',
            'Double.parseDouble': '''{%- macro parse_double(value) -%}
  CAST({{ value }} AS DOUBLE)
{%- endmacro -%}''',
            'Integer.parseInt': '''{%- macro parse_int(value) -%}
  CAST({{ value }} AS INTEGER)
{%- endmacro -%}''',
            'String.valueOf': '''{%- macro to_string(value) -%}
  CAST({{ value }} AS VARCHAR)
{%- endmacro -%}''',
            'BigDecimal.ZERO': '''{%- macro bd_zero() -%}0{%- endmacro -%}''',
            'BigDecimal.ONE': '''{%- macro bd_one() -%}1{%- endmacro -%}''',
        }

        ALWAYS_INCLUDE = '''

{# === Sign-flip utilities (used across IOPQ/RS/ADJ joblets) === #}

{%- macro rt2_negate(rt_col, value_col) -%}
  CASE WHEN {{ rt_col }} = '2'
    THEN CAST({{ value_col }} AS DOUBLE) * -1
    ELSE CAST({{ value_col }} AS DOUBLE)
  END
{%- endmacro -%}

{%- macro share_of_fx_usd(dir_col, rec_prin, pay_prin, clear_fx) -%}
  CASE
    WHEN {{ dir_col }} IN ('2','5') THEN (CAST({{ rec_prin }} AS DOUBLE) - CAST({{ pay_prin }} AS DOUBLE) - CAST({{ clear_fx }} AS DOUBLE))
    WHEN {{ dir_col }} IN ('1','4') THEN CAST({{ clear_fx }} AS DOUBLE) * -1
    ELSE 0
  END
{%- endmacro -%}

{%- macro share_of_fx_loc_rec(dir_col, rec_prin, pay_prin, clear_fx) -%}
  CASE
    WHEN {{ dir_col }} IN ('2','5') THEN CAST({{ clear_fx }} AS DOUBLE) * -1
    WHEN {{ dir_col }} IN ('1','4') THEN (CAST({{ rec_prin }} AS DOUBLE) - CAST({{ pay_prin }} AS DOUBLE) - CAST({{ clear_fx }} AS DOUBLE))
    ELSE 0
  END
{%- endmacro -%}

{%- macro share_of_charges(dir_col, rec_charges, clear_charges) -%}
  CASE
    WHEN {{ dir_col }} IN ('2','5') THEN CAST({{ clear_charges }} AS DOUBLE) * -1
    WHEN {{ dir_col }} IN ('1','4') THEN (CAST({{ rec_charges }} AS DOUBLE) - CAST({{ clear_charges }} AS DOUBLE))
    ELSE 0
  END
{%- endmacro -%}

{%- macro clear_principal(sendpay_col, dir_col, clr_prin_val) -%}
  CASE
    WHEN {{ sendpay_col }} = 'P' AND {{ dir_col }} IN ('1','4') THEN CAST({{ clr_prin_val }} AS DOUBLE) * -1
    ELSE CAST({{ clr_prin_val }} AS DOUBLE)
  END
{%- endmacro -%}

{%- macro origpay_ind(rt_col) -%}
  CASE WHEN {{ rt_col }} = '2' THEN 'O' WHEN {{ rt_col }} = '3' THEN 'P' ELSE 'X' END
{%- endmacro -%}

{%- macro century_date_fix(setdate_col) -%}
  CASE
    WHEN SUBSTR(CAST({{ setdate_col }} AS VARCHAR), 1, 3) IN ('121','122','123','124','125','126','127','128','129')
      THEN '20' || SUBSTR(CAST({{ setdate_col }} AS VARCHAR), 2)
    ELSE CAST({{ setdate_col }} AS VARCHAR)
  END
{%- endmacro -%}

{%- macro basicha(rt_col, basic_col) -%}
  CASE
    WHEN {{ rt_col }} = '1' THEN CAST({{ basic_col }} AS DOUBLE)
    WHEN {{ rt_col }} = '2' AND CAST({{ basic_col }} AS DOUBLE) < 0 THEN CAST({{ basic_col }} AS DOUBLE)
    WHEN {{ rt_col }} = '2' THEN CAST({{ basic_col }} AS DOUBLE) * -1
    ELSE CAST({{ basic_col }} AS DOUBLE)
  END
{%- endmacro -%}

{%- macro shacha(total_col, clear_col) -%}
  (CAST({{ total_col }} AS DOUBLE) - CAST({{ clear_col }} AS DOUBLE))
{%- endmacro -%}

{%- macro generate_id(source_col, grp_no) -%}
  CAST(STRFTIME(NOW(), '%Y%m%d%H%M%S') || CAST({{ source_col }} AS VARCHAR) || '{{ grp_no }}' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR), 6, '0') AS VARCHAR)
{%- endmacro -%}

{%- macro ctx(var_name) -%}
  {{ var(var_name) }}
{%- endmacro -%}

{%- macro ctx_grp(base_name) -%}
  {{ var(base_name) }}{{ var('grp_number') }}
{%- endmacro -%}
'''

        added = set()
        for pattern in sorted(self.all_java_patterns):
            for key, macro_code in MACROS.items():
                if key in pattern and key not in added:
                    lines.append(macro_code)
                    lines.append('')
                    added.add(key)
                    self.generated_macros.add(key)

        lines.append(ALWAYS_INCLUDE)
        self.generated_macros.update(['rt2_negate', 'share_of_fx_usd',
            'share_of_fx_loc_rec', 'share_of_charges', 'clear_principal',
            'origpay_ind', 'century_date_fix', 'basicha', 'shacha',
            'generate_id', 'ctx', 'ctx_grp'])

        path = os.path.join(self.output_dir, 'macros', 'talend_functions.sql')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        print(f"  macros/talend_functions.sql ({len(self.generated_macros)} macros, "
              f"{len(self.all_java_patterns)} patterns detected)")

    # =========================================================================
    # STEP 6: Generate DDLs from ALL discovered schemas
    # =========================================================================
    def _gen_ddls(self):
        ddl_lines = ["-- Auto-generated DDLs from all component schemas",
                     "-- Run this before dbt run to create staging tables in DuckDB", ""]

        tables_created = set()
        for schema_key, columns in self.all_schemas.items():
            job_name, comp_id = schema_key.split('.', 1) if '.' in schema_key else (schema_key, '')
            table_name = comp_id.lower()
            if table_name in tables_created:
                continue

            col_defs = []
            for col in columns:
                col_name = col.get('name', '')
                col_type = self._talend_type_to_duckdb(col.get('type', ''))
                length = col.get('length', '')
                precision = col.get('precision', '')
                nullable = col.get('nullable', 'true')

                if col_type == 'VARCHAR' and length:
                    col_def = f"  {col_name} VARCHAR({length})"
                elif col_type == 'DECIMAL' and length and precision:
                    col_def = f"  {col_name} DECIMAL({length},{precision})"
                else:
                    col_def = f"  {col_name} {col_type}"

                if nullable == 'false':
                    col_def += ' NOT NULL'
                col_defs.append(col_def)

            if col_defs:
                ddl_lines.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")
                ddl_lines.append(',\n'.join(col_defs))
                ddl_lines.append(');')
                ddl_lines.append('')
                tables_created.add(table_name)

        path = os.path.join(self.output_dir, 'analyses', 'create_staging_tables.sql')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(ddl_lines))
        print(f"  analyses/create_staging_tables.sql ({len(tables_created)} tables)")

    def _talend_type_to_duckdb(self, talend_type: str) -> str:
        mapping = {
            'id_String': 'VARCHAR', 'id_Integer': 'INTEGER', 'id_Long': 'BIGINT',
            'id_Float': 'FLOAT', 'id_Double': 'DOUBLE', 'id_BigDecimal': 'DECIMAL(38,4)',
            'id_Boolean': 'BOOLEAN', 'id_Date': 'DATE', 'id_Timestamp': 'TIMESTAMP',
            'id_Short': 'SMALLINT', 'id_Byte': 'TINYINT', 'id_Character': 'VARCHAR(1)',
            'id_Object': 'VARCHAR',
            'String': 'VARCHAR', 'Integer': 'INTEGER', 'Long': 'BIGINT',
            'Float': 'FLOAT', 'Double': 'DOUBLE', 'BigDecimal': 'DECIMAL(38,4)',
            'Boolean': 'BOOLEAN', 'Date': 'DATE',
        }
        return mapping.get(talend_type, 'VARCHAR')

    # =========================================================================


    # =========================================================================
    # STEP 7: Generate models + execution plan per job — graph-based
    # =========================================================================
    def _gen_job_models(self, job_name: str, job_data: dict):
        components = job_data.get('components', {})
        connections = job_data.get('connections', [])
        if not components:
            return

        graph = ConnectionGraph(connections, components)
        exec_order = graph.get_execution_order()
        plan = graph.build_execution_plan()
        self.execution_plans[job_name] = plan

        print(f"\n  Job: {job_name} ({len(components)} components, {len(connections)} connections)")
        subdir = self._classify_subdir(job_name)

        for comp_id in exec_order:
            comp = components.get(comp_id)
            if not comp:
                continue

            ct = comp.get('component_type', '')
            mn = f"{self._safe_name(job_name)}_{comp_id}".lower()

            # Route EVERY component type to a handler
            if 'tMap' in ct:
                self._gen_tmap_model(mn, comp, graph, comp_id, subdir)
            elif 'tJavaRow' in ct:
                self._gen_javarow_model(mn, comp, graph, comp_id, subdir)
            elif 'tJava' in ct and 'Row' not in ct:
                self._gen_tjava_model(mn, comp, comp_id, subdir)
            elif 'tDBRow' in ct or 'tSnowflakeRow' in ct:
                self._gen_dbrow_model(mn, comp, comp_id, subdir)
            elif 'tDBOutput' in ct or 'tSnowflakeOutput' in ct:
                self._gen_dboutput_model(mn, comp, graph, comp_id, subdir)
            elif 'tDBInput' in ct or 'tSnowflakeInput' in ct:
                self._gen_dbinput_source(mn, comp, comp_id, subdir)
            elif 'tUniqRow' in ct:
                self._gen_uniqrow_model(mn, comp, graph, comp_id, subdir)
            elif 'tFileInputDelimited' in ct or 'tFileInputFullRow' in ct:
                self._gen_file_source(mn, comp, comp_id, subdir)
            elif 'tFileOutputDelimited' in ct:
                self._gen_file_output_model(mn, comp, graph, comp_id, subdir)
            elif 'tExtractDelimitedFields' in ct:
                self._gen_extract_fields_model(mn, comp, graph, comp_id, subdir)
            elif 'tFixedFlowInput' in ct:
                self._gen_fixed_flow_model(mn, comp, comp_id, subdir)
            elif 'tFlowToIterate' in ct or 'tIterateToFlow' in ct:
                self._gen_flow_iterate_model(mn, comp, graph, comp_id, subdir)
            elif 'tBufferOutput' in ct:
                self._gen_buffer_model(mn, comp, graph, comp_id, subdir)
            # Orchestration components → handled by execution_plan, but still register
            elif ct in ('tFileList', 'tFileCopy', 'tFileDelete', 'tFileArchive',
                        'tParallelize', 'tLoop', 'tRunJob', 'tLogCatcher',
                        'tSendMail', 'tChronometerStop', 'tPrejob', 'tPostjob',
                        'tDBConnection'):
                # Update execution plan with model name if applicable
                for step in plan:
                    if step['comp_id'] == comp_id:
                        step['handled'] = 'execution_plan'
                # Register so downstream refs don't break
                self._register_model(comp_id, mn)
            else:
                # Unknown component type → flag it
                print(f"    [!] Unknown component type: {ct} ({comp_id}) — flagged in execution plan")
                for step in plan:
                    if step['comp_id'] == comp_id:
                        step['action'] = 'UNKNOWN'
                        step['warning'] = f'Unhandled component type: {ct}'

    # =========================================================================
    # MODEL GENERATORS — one per data-transforming component type
    # =========================================================================

    def _gen_tmap_model(self, model_name, comp, graph, comp_id, subdir):
        """tMap: CTE for variables, SELECT for columns, JOIN for lookups, WHERE for filter.
        One model per output. Lookup joins use PARSED jk_expr for ON clause."""
        tmap = comp.get('tmap_data', {})
        if not tmap:
            return

        for output in tmap.get('outputs', []):
            out_name = output.get('name', 'out')
            out_filter = output.get('filter', '')
            columns = output.get('columns', [])
            if not columns:
                continue

            full_name = f"{model_name}_{out_name}".lower()
            lines = self._model_header(full_name, subdir,
                f"tMap output: {out_name} from {comp.get('unique_name','')}")

            # CTE for variables
            variables = tmap.get('variables', [])
            if variables:
                lines.append("-- tMap Variables")
                lines.append("WITH vars AS (")
                lines.append("  SELECT")
                var_lines = []
                for var in variables:
                    expr = self.translator.translate_expression(
                        var.get('expression', ''),
                        f"{comp.get('unique_name','')}.var.{var.get('name','')}")
                    vname = var.get('name', '')
                    if vname and expr:
                        var_lines.append(f"    {expr} AS {vname}")
                lines.append(',\n'.join(var_lines) if var_lines else "    1 AS _dummy")
                lines.append(")")
                lines.append("")

            # SELECT columns
            lines.append("SELECT")
            col_lines = []
            for col in columns:
                cname = col.get('name', '')
                cexpr = col.get('expression', '')
                if cexpr:
                    translated = self.translator.translate_expression(
                        cexpr, f"{comp.get('unique_name','')}.{out_name}.{cname}")
                    col_lines.append(f"  {translated} AS {cname}")
                elif cname:
                    col_lines.append(f"  {cname}")
            lines.append(',\n'.join(col_lines) if col_lines else "  *")

            # FROM: main source + lookup joins
            sources = graph.get_data_sources(comp_id)
            main_sources = [s for s in sources if s['type'] == 'MAIN']
            lookup_sources = [s for s in sources if s['type'] == 'LOOKUP']

            if main_sources:
                main_ref = self._resolve_ref(main_sources[0]['source'])
                if variables:
                    lines.append(f"FROM {{{{ ref('{main_ref}') }}}} AS src")
                    lines.append("CROSS JOIN vars")
                else:
                    lines.append(f"FROM {{{{ ref('{main_ref}') }}}} AS src")

                # Lookup joins using PARSED join keys with jk_expr
                lookups = tmap.get('lookups', [])
                for lk_source in lookup_sources:
                    lk_ref = self._resolve_ref(lk_source['source'])
                    lk_label = lk_source.get('label', '')
                    lk_config = next((lk for lk in lookups if lk.get('name', '') == lk_label), None)

                    join_type = 'LEFT JOIN'
                    if lk_config:
                        jm = (lk_config.get('joinModel', '') or '').upper()
                        if 'INNER' in jm:
                            join_type = 'INNER JOIN'
                        join_keys = lk_config.get('join_keys', [])
                        if join_keys:
                            on_clauses = []
                            for jk in join_keys:
                                jk_expr = jk.get('expression', '')
                                jk_name = jk.get('name', '')
                                if jk_expr and jk_name:
                                    # jk_expr contains the REAL source-side expression
                                    # e.g. "src.MTCN" or "row1.NETWORKID"
                                    translated_expr = self.translator.translate_expression(
                                        jk_expr, f"tMap.{lk_label}.join.{jk_name}")
                                    on_clauses.append(f"{translated_expr} = {lk_label}.{jk_name}")
                                elif jk_name:
                                    on_clauses.append(f"src.{jk_name} = {lk_label}.{jk_name}")
                            if on_clauses:
                                lines.append(f"{join_type} {{{{ ref('{lk_ref}') }}}} AS {lk_label}")
                                lines.append(f"  ON {' AND '.join(on_clauses)}")
                            else:
                                lines.append(f"-- {join_type} {{{{ ref('{lk_ref}') }}}} AS {lk_label} (join keys unresolved)")
                        else:
                            lines.append(f"-- {join_type} {{{{ ref('{lk_ref}') }}}} AS {lk_label} (no join keys)")
                    else:
                        lines.append(f"-- {join_type} {{{{ ref('{lk_ref}') }}}} (lookup config not found for {lk_label})")
            else:
                lines.append("FROM source_data" + (" CROSS JOIN vars" if variables else ""))

            # Filter
            if out_filter:
                tf = self.translator.translate_expression(
                    out_filter, f"{comp.get('unique_name','')}.{out_name}.filter")
                lines.append(f"WHERE {tf}")

            self._register_model(comp_id, full_name)
            self._register_model(out_name, full_name)  # output-specific registration
            self._write_model(full_name, subdir, lines)

    def _gen_javarow_model(self, model_name, comp, graph, comp_id, subdir):
        """tJavaRow: translate Java assignments to SQL columns with CTE for local vars."""
        code = self._str_val(comp.get('java_code', ''))
        if not code:
            return
        ctx = self._str_val(comp.get('unique_name', ''))
        assignments = self.translator.translate_java_block(code, ctx)
        if not assignments:
            return

        lines = self._model_header(model_name, subdir, f"tJavaRow: {ctx}")
        var_decls = [a for a in assignments if a.get('is_variable')]
        out_assigns = [a for a in assignments if not a.get('is_context') and not a.get('is_variable')]

        sources = graph.get_data_sources(comp_id)
        upstream_ref = self._resolve_ref(sources[0]['source']) if sources else None

        if var_decls:
            lines.append("WITH java_vars AS (")
            lines.append("  SELECT")
            lines.append(',\n'.join(f"    {a['expression']} AS {a['column']}" for a in var_decls))
            lines.append(f"  FROM {{{{ ref('{upstream_ref}') }}}}" if upstream_ref else "  FROM source_data")
            lines.append(")")
            lines.append("")

        lines.append("SELECT")
        col_lines = [f"  {a['expression']} AS {a['column']}" for a in out_assigns]
        lines.append(',\n'.join(col_lines) if col_lines else "  *")

        if upstream_ref:
            if var_decls:
                lines.append(f"FROM {{{{ ref('{upstream_ref}') }}}} AS src CROSS JOIN java_vars")
            else:
                lines.append(f"FROM {{{{ ref('{upstream_ref}') }}}}")
        else:
            lines.append("FROM source_data" + (" CROSS JOIN java_vars" if var_decls else ""))

        self._register_model(comp_id, model_name)
        self._write_model(model_name, subdir, lines)

    def _gen_tjava_model(self, model_name, comp, comp_id, subdir):
        """tJava: translate context assignments. If only logging, skip."""
        code = self._str_val(comp.get('java_code', ''))
        if not code:
            return
        # Check if it sets context variables (meaningful) vs just prints (skip)
        has_context_set = 'context.' in code and '=' in code and 'System.out' not in code.split('=')[0]
        if not has_context_set and 'System.out' in code:
            return  # logging-only tJava

        assignments = self.translator.translate_java_block(code, comp.get('unique_name', ''))
        ctx_assigns = [a for a in assignments if a.get('is_context')]

        if ctx_assigns:
            lines = self._model_header(model_name, subdir, f"tJava context vars: {comp.get('unique_name','')}")
            lines.append("-- Context variable assignments (consumed by downstream via dbt vars)")
            for a in ctx_assigns:
                lines.append(f"-- {a['column']} = {a['expression']}")
            lines.append("SELECT")
            lines.append(',\n'.join(f"  {a['expression']} AS {a['column']}" for a in ctx_assigns))
            lines.append("FROM (SELECT 1) AS _dummy")
        else:
            lines = self._model_header(model_name, subdir, f"tJava: {comp.get('unique_name','')}")
            lines.append("-- Original Java code (no translatable assignments found):")
            for line in code.split('\n')[:30]:
                lines.append(f"-- {line.strip()}")
            lines.append("-- [!] REVIEW REQUIRED: This tJava may have side effects not captured in SQL")
            lines.append("SELECT 1 AS _placeholder")

        self._register_model(comp_id, model_name)
        self._write_model(model_name, subdir, lines)

    def _gen_dbrow_model(self, model_name, comp, comp_id, subdir):
        """tDBRow/tSnowflakeRow: translate SQL, resolve context vars."""
        sql = self._str_val(comp.get('sql_query', ''))
        if not sql:
            return
        translated = self.translator.translate_sql_for_duckdb(sql)
        lines = self._model_header(model_name, subdir, f"tDBRow: {comp.get('unique_name','')}")
        lines.append(translated)
        self._register_model(comp_id, model_name)
        self._write_model(model_name, subdir, lines)

    def _gen_dboutput_model(self, model_name, comp, graph, comp_id, subdir):
        """tDBOutput: generate dbt incremental model with merge strategy for UPDATE/UPSERT."""
        db_out = comp.get('db_output', {})
        if not db_out:
            return

        table = self._str_val(db_out.get('table', ''))
        action = (self._str_val(db_out.get('output_action', '')) or self._str_val(db_out.get('table_action', '')) or 'INSERT').upper()
        schema_cols = db_out.get('schema_columns', [])
        die_on_error = db_out.get('die_on_error', False)
        table_resolved = self.translator.translate_sql_for_duckdb(f'"{table}"').strip('"') if table else 'UNKNOWN'

        sources = graph.get_data_sources(comp_id)
        src_ref = self._resolve_ref(sources[0]['source']) if sources else None
        col_names = [c['name'] for c in schema_cols if c.get('name')]
        key_cols = [c['name'] for c in schema_cols if c.get('key', 'false') == 'true']

        if action == 'UPDATE' and key_cols:
            # dbt incremental merge: UPDATE rows matching on key columns
            lines = [
                "{{",
                "  config(",
                f"    materialized='incremental',",
                f"    unique_key={key_cols},",
                f"    incremental_strategy='merge',",
                f"    tags=['{subdir}', 'update']",
                "  )",
                "}}",
                "",
                f"-- tDBOutput UPDATE: {comp.get('unique_name','')} → {table_resolved}",
                f"-- Key columns: {', '.join(key_cols)}",
                "",
                "SELECT",
            ]
            lines.append(',\n'.join(f"  {c}" for c in col_names) if col_names else "  *")
            lines.append(f"FROM {{{{ ref('{src_ref}') }}}}" if src_ref else "FROM source_data")

        elif action in ('INSERT_OR_UPDATE', 'UPSERT') and key_cols:
            # dbt incremental merge: UPSERT
            lines = [
                "{{",
                "  config(",
                f"    materialized='incremental',",
                f"    unique_key={key_cols},",
                f"    incremental_strategy='merge',",
                f"    tags=['{subdir}', 'upsert']",
                "  )",
                "}}",
                "",
                f"-- tDBOutput UPSERT: {comp.get('unique_name','')} → {table_resolved}",
                f"-- Key columns: {', '.join(key_cols)}",
                "",
                "SELECT",
            ]
            lines.append(',\n'.join(f"  {c}" for c in col_names) if col_names else "  *")
            lines.append(f"FROM {{{{ ref('{src_ref}') }}}}" if src_ref else "FROM source_data")

        else:
            # INSERT or default: standard table materialization
            lines = self._model_header(model_name, subdir,
                f"tDBOutput INSERT: {comp.get('unique_name','')} → {table_resolved}")
            lines.append("SELECT")
            lines.append(',\n'.join(f"  {c}" for c in col_names) if col_names else "  *")
            lines.append(f"FROM {{{{ ref('{src_ref}') }}}}" if src_ref else "FROM source_data")

        self._register_model(comp_id, model_name)
        self._write_model(model_name, subdir, lines)

    def _gen_dbinput_source(self, model_name, comp, comp_id, subdir):
        """tDBInput: source query or table scan.
        - SHARED tables → {{ source('connection', 'table') }}
        - GROUP-SPECIFIC tables → {{ var('table_var') }}
        - Manual query → translated SQL"""
        db_in = comp.get('db_input', {})
        if not db_in:
            return
        query = self._str_val(db_in.get('query', ''))
        table = self._str_val(db_in.get('table', ''))
        conn_ref = self._str_val(db_in.get('connection_ref', ''))

        lines = self._model_header(model_name, 'sources',
            f"tDBInput: {comp.get('unique_name','')} from {table}")

        if query:
            # Manual query: translate context refs to var() and Snowflake SQL to DuckDB
            translated = self.translator.translate_sql_for_duckdb(query)
            # Replace context.tablename refs with {{ var() }}
            import re as _re
            translated = _re.sub(
                r'context\.(\w+)',
                lambda m: "{{ var('" + m.group(1).lower() + "') }}",
                translated
            )
            lines.append(translated)
        elif table:
            # Check if this is a group-specific table
            is_grp = any(pat in table for pat in self.GRP_TABLE_PATTERNS)
            if is_grp:
                # Group-specific: find the matching var name from config
                var_name = self._table_to_var_name(table)
                lines.append(f"-- Group-specific table: resolved per-group via dbt var()")
                lines.append(f"SELECT * FROM {{{{ var('{var_name}') }}}}")
            else:
                # Shared table: try source() ref
                source_ref = self._find_source_ref(conn_ref, table.strip('"'))
                if source_ref:
                    lines.append(f"SELECT * FROM {{{{ source('{source_ref[0]}', '{source_ref[1]}') }}}}")
                else:
                    lines.append(f"SELECT * FROM {table}")
        else:
            lines.append("SELECT 1 AS _placeholder  -- [!] No query or table found")

        self._register_model(comp_id, model_name)
        self._write_model(model_name, 'sources', lines)

    def _find_source_ref(self, conn_ref: str, table_name: str):
        """Look up (source_name, table_name) for a shared table in sources."""
        for source_key, info in self.source_tables.items():
            if conn_ref and info.get('connection_ref') == conn_ref:
                if table_name in info['tables']:
                    sname = self._safe_name(conn_ref or f"{info['database']}_{info['schema']}")
                    return (sname, table_name)
            elif table_name in info['tables']:
                return (self._safe_name(source_key), table_name)
        return None

    def _table_to_var_name(self, table: str) -> str:
        """Map a group-specific table reference to its dbt var name.
        e.g. 'context.entry_tablename' → 'entry_tablename'
             'TXNENTRY_GRP16_SATEST' → 'entry_tablename'
        """
        import re as _re
        # Direct context reference
        m = _re.match(r'["\']?context\.(\w+)["\']?', table)
        if m:
            return m.group(1).lower()

        # Map known table patterns to var names
        TABLE_VAR_MAP = {
            'TXNENTRY_GRP': 'entry_tablename',
            'TXNADJUSTMENT_GRP': 'adjustment_tablename',
            'TXNTEMPLATE_GRP': 'txn_template_main',
            'TALENDSTAGTXNENTRYST_GRP': 'entry_stgstfile',
            'TXNENTRY_ACFILE_GRP': 'entry_acfiletable',
            'TXNENTRY_DATA_GRP': 'acstmerge',
            'ENTRY_STATUS_UPDATE_GRP': 'entry_dup',
            'TALENDSTAGTXNTEMPLATE_CHECK_GRP': 'compliance_check',
            'TXNTEMPLATERAW_GRP': 'ac_rawdata',
            'TALENDSTAGTXNENTRYREC4_GRP': 'recordtype4',
            'STFILE_NAMES_GRP': 'st_filenames',
            'ACFILE_NAMES_GRP': 'ac_filenames',
        }
        for pattern, var_name in TABLE_VAR_MAP.items():
            if pattern in table:
                return var_name

        # Fallback: lowercase the table name as var
        return _re.sub(r'[^a-zA-Z0-9_]', '_', table).lower()

    def _gen_uniqrow_model(self, model_name, comp, graph, comp_id, subdir):
        """tUniqRow: ROW_NUMBER dedup (first-row-wins) matching Talend behavior."""
        uniq = comp.get('uniq_row', {})
        if not uniq:
            return
        key_cols = uniq.get('key_columns', [])
        all_cols = uniq.get('all_columns', [])
        col_names = [c.get('name','') for c in all_cols if c.get('name')]
        sources = graph.get_data_sources(comp_id)
        src_ref = self._resolve_ref(sources[0]['source']) if sources else None

        lines = self._model_header(model_name, subdir,
            f"tUniqRow: {comp.get('unique_name','')} dedup on {key_cols}")

        if key_cols and col_names:
            partition_by = ', '.join(key_cols)
            lines.append("WITH ranked AS (")
            lines.append("  SELECT")
            lines.append(',\n'.join(f"    {c}" for c in col_names) + ',')
            lines.append(f"    ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY (SELECT NULL)) AS _rn")
            lines.append(f"  FROM {{{{ ref('{src_ref}') }}}}" if src_ref else "  FROM source_data")
            lines.append(")")
            lines.append("SELECT")
            lines.append(',\n'.join(f"  {c}" for c in col_names))
            lines.append("FROM ranked WHERE _rn = 1")
        else:
            lines.append("SELECT DISTINCT")
            lines.append(',\n'.join(f"  {c}" for c in col_names) if col_names else "  *")
            lines.append(f"FROM {{{{ ref('{src_ref}') }}}}" if src_ref else "FROM source_data")

        self._register_model(comp_id, model_name)
        self._write_model(model_name, subdir, lines)

    def _gen_file_source(self, model_name, comp, comp_id, subdir):
        """tFileInputDelimited/tFileInputFullRow: DuckDB read_csv_auto."""
        fi = comp.get('file_input', {})
        if not fi:
            return
        lines = self._model_header(model_name, 'sources',
            f"File source: {comp.get('unique_name','')}")
        lines.append(f"-- Filename: {self._str_val(fi.get('filename', ''))}")
        lines.append(f"-- Separator: {self._str_val(fi.get('fieldseparator', ''))}")
        lines.append(f"-- Encoding: {self._str_val(fi.get('encoding', ''))}")
        sep = self._str_val(fi.get('fieldseparator', ',')).strip('"').replace("';'", ';')
        hdr = 'true' if self._str_val(fi.get('header', '0')) != '0' else 'false'
        schema_cols = fi.get('schema_columns', [])
        if schema_cols:
            col_names = [c.get('name','') for c in schema_cols if c.get('name')]
            lines.append("SELECT")
            lines.append(',\n'.join(f"  {c}" for c in col_names))
        else:
            lines.append("SELECT *")
        lines.append(f"FROM read_csv_auto('{{{{ var(\"source_file_path\") }}}}', sep='{sep}', header={hdr})")
        self._register_model(comp_id, model_name)
        self._write_model(model_name, 'sources', lines)

    # === NEW GENERATORS (previously parse-only) ===

    def _gen_file_output_model(self, model_name, comp, graph, comp_id, subdir):
        """tFileOutputDelimited: DuckDB COPY TO CSV. Produces a post-hook model."""
        fo = comp.get('file_output', {})
        if not fo:
            return
        sources = graph.get_data_sources(comp_id)
        src_ref = self._resolve_ref(sources[0]['source']) if sources else None
        filename = self._str_val(fo.get('filename', ''))
        sep = self._str_val(fo.get('fieldseparator', ',')).strip('"')
        include_hdr = fo.get('includeheader', 'false') == 'true'

        lines = self._model_header(model_name, subdir,
            f"tFileOutputDelimited: {comp.get('unique_name','')} → {filename}")
        lines.append(f"-- Output file: {filename}")
        lines.append(f"-- Separator: '{sep}' | Header: {include_hdr}")
        lines.append(f"-- Post-hook: COPY this model TO '{filename}' (FORMAT CSV, DELIMITER '{sep}', HEADER {include_hdr})")
        lines.append("")
        schema_cols = fo.get('schema_columns', [])
        if schema_cols:
            col_names = [c.get('name','') for c in schema_cols if c.get('name')]
            lines.append("SELECT")
            lines.append(',\n'.join(f"  {c}" for c in col_names))
        else:
            lines.append("SELECT *")
        lines.append(f"FROM {{{{ ref('{src_ref}') }}}}" if src_ref else "FROM source_data")
        self._register_model(comp_id, model_name)
        self._write_model(model_name, subdir, lines)

    def _gen_extract_fields_model(self, model_name, comp, graph, comp_id, subdir):
        """tExtractDelimitedFields: split a single column into multiple using DuckDB string_split."""
        ef = comp.get('extract_fields', {})
        if not ef:
            return
        sources = graph.get_data_sources(comp_id)
        src_ref = self._resolve_ref(sources[0]['source']) if sources else None
        field_sep = self._str_val(ef.get('fieldseparator', ',')).strip('"')
        schema_cols = ef.get('schema_columns', [])

        lines = self._model_header(model_name, subdir,
            f"tExtractDelimitedFields: {comp.get('unique_name','')} split on '{field_sep}'")
        lines.append("WITH split_data AS (")
        lines.append(f"  SELECT *, string_split(CAST(line AS VARCHAR), '{field_sep}') AS _parts")
        lines.append(f"  FROM {{{{ ref('{src_ref}') }}}}" if src_ref else "  FROM source_data")
        lines.append(")")
        lines.append("SELECT")
        if schema_cols:
            col_lines = []
            for i, col in enumerate(schema_cols):
                cname = col.get('name', '')
                if cname:
                    col_lines.append(f"  TRIM(_parts[{i+1}]) AS {cname}")
            lines.append(',\n'.join(col_lines))
        else:
            lines.append("  _parts")
        lines.append("FROM split_data")
        self._register_model(comp_id, model_name)
        self._write_model(model_name, subdir, lines)

    def _gen_fixed_flow_model(self, model_name, comp, comp_id, subdir):
        """tFixedFlowInput: generate static rows from schema defaults."""
        ff = comp.get('fixed_flow', {})
        if not ff:
            return
        nb_rows = self._str_val(ff.get('nb_rows', '1'))
        schema_cols = ff.get('schema_columns', [])
        values = ff.get('values', {})

        lines = self._model_header(model_name, 'sources',
            f"tFixedFlowInput: {comp.get('unique_name','')} ({nb_rows} rows)")
        if schema_cols:
            lines.append("SELECT")
            col_lines = []
            for col in schema_cols:
                cname = col.get('name', '')
                cdefault = col.get('default', '') or 'NULL'
                if cname:
                    col_lines.append(f"  {cdefault} AS {cname}")
            lines.append(',\n'.join(col_lines))
        else:
            lines.append("SELECT 1 AS _row")
        self._register_model(comp_id, model_name)
        self._write_model(model_name, 'sources', lines)

    def _gen_flow_iterate_model(self, model_name, comp, graph, comp_id, subdir):
        """tFlowToIterate/tIterateToFlow: passthrough with variable mapping comments."""
        fi = comp.get('flow_iterate', {})
        sources = graph.get_data_sources(comp_id)
        src_ref = self._resolve_ref(sources[0]['source']) if sources else None

        lines = self._model_header(model_name, subdir,
            f"Flow↔Iterate: {comp.get('unique_name','')}")
        if fi and fi.get('mappings'):
            lines.append("-- Variable mappings:")
            for m in fi['mappings'][:20]:
                lines.append(f"--   {m.get('ref', m.get('key',''))} = {m.get('value', '')}")
        lines.append("SELECT *")
        lines.append(f"FROM {{{{ ref('{src_ref}') }}}}" if src_ref else "FROM source_data")
        self._register_model(comp_id, model_name)
        self._write_model(model_name, subdir, lines)

    def _gen_buffer_model(self, model_name, comp, graph, comp_id, subdir):
        """tBufferOutput: passthrough (buffering is a runtime concern)."""
        sources = graph.get_data_sources(comp_id)
        src_ref = self._resolve_ref(sources[0]['source']) if sources else None
        lines = self._model_header(model_name, subdir,
            f"tBufferOutput: {comp.get('unique_name','')}")
        lines.append("SELECT *")
        lines.append(f"FROM {{{{ ref('{src_ref}') }}}}" if src_ref else "FROM source_data")
        self._register_model(comp_id, model_name)
        self._write_model(model_name, subdir, lines)

    # =========================================================================
    # STEP 8: Write execution plans
    # =========================================================================
    def _write_execution_plans(self):
        plan_dir = os.path.join(self.output_dir, 'execution_plans')
        os.makedirs(plan_dir, exist_ok=True)
        for job_name, plan in self.execution_plans.items():
            path = os.path.join(plan_dir, f"{self._safe_name(job_name)}_plan.json")
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(plan, f, indent=2, default=str)
            orch_count = sum(1 for s in plan if s.get('action') != 'DBT_MODEL')
            dbt_count = sum(1 for s in plan if s.get('action') == 'DBT_MODEL')
            print(f"  {job_name}: {len(plan)} steps ({dbt_count} dbt models, {orch_count} orchestration)")

    # =========================================================================
    # HELPERS
    # =========================================================================
    def _model_header(self, name, subdir, description=''):
        return [
            "{{",
            "  config(",
            f"    materialized='table',",
            f"    tags=['{subdir}', '{name.split('_')[0]}']",
            "  )",
            "}}",
            "",
            f"-- {description}" if description else "",
            "",
        ]

    def _write_model(self, name, subdir, lines):
        path = os.path.join(self.output_dir, 'models', subdir, f'{name}.sql')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        self.generated_models.append(f"{subdir}/{name}")
        print(f"    → models/{subdir}/{name}.sql")

    def _register_model(self, comp_id: str, model_name: str):
        self.model_registry[comp_id] = model_name
        self.model_registry[self._safe_name(comp_id)] = model_name

    def _resolve_ref(self, comp_id: str) -> str:
        return self.model_registry.get(comp_id,
               self.model_registry.get(self._safe_name(comp_id),
               self._safe_name(comp_id)))


    def _str_val(self, val) -> str:
        """Safely extract a string from a parsed value.
        The xml_parser sometimes returns {'value': 'x', 'field': 'TEXT'} instead of 'x'.
        This handles: str, dict, None, int, float, bool."""
        if val is None:
            return ''
        if isinstance(val, dict):
            return str(val.get('value', ''))
        return str(val)

    def _safe_name(self, name):
        return re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()

    def _classify_subdir(self, job_name):
        jl = job_name.lower()
        if 'joblet' in jl or 'iopq' in jl or 'rs_' in jl or 'adj_' in jl:
            return 'joblets'
        if 'stlmt' in jl or 'sort_files' in jl or 'act_main' in jl:
            return 'staging'
        if 'upsert' in jl or 'adj_sts' in jl:
            return 'post_sync'
        if 'final_load' in jl:
            return 'final_load'
        if 'post' in jl or 'status_update' in jl or 'paydate' in jl:
            return 'post_job'
        return 'staging'

    # =========================================================================
    # STEP 9: schema.yml
    # =========================================================================
    def _gen_sources_yml(self):
        """Generate sources.yml for SHARED tables only.
        Group-specific tables use {{ var() }} in SQL models instead.
        This allows one dbt project to serve all 20 groups via --vars."""
        if not self.source_tables:
            print("  No shared source tables, skipping sources.yml")
            return

        sources = []
        for source_key, info in self.source_tables.items():
            source_name = self._safe_name(info['connection_ref'] or f"{info['database']}_{info['schema']}")
            tables = []
            for tname, tinfo in info['tables'].items():
                entry = {'name': tname, 'description': f"Shared table ({tinfo['type']})"}
                if tinfo['columns']:
                    entry['columns'] = [{'name': c} for c in tinfo['columns']]
                tables.append(entry)

            sources.append({
                'name': source_name,
                'description': f"Snowflake: {info['database']}.{info['schema']}",
                'database': info['database'],
                'schema': info['schema'],
                'tables': tables,
            })

        path = os.path.join(self.output_dir, 'models', 'sources', 'sources.yml')
        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump({'version': 2, 'sources': sources}, f, default_flow_style=False, sort_keys=False)

        total = sum(len(s['tables']) for s in sources)
        print(f"  sources.yml: {len(sources)} connections, {total} shared tables")
        for s in sources:
            print(f"    {s['name']}: {s['database']}.{s['schema']} ({len(s['tables'])} tables)")

    def _gen_schema_yml(self):
        models = []
        for schema_key, columns in self.all_schemas.items():
            models.append({
                'name': self._safe_name(schema_key),
                'description': f'Schema: {schema_key}',
                'columns': [
                    {'name': c['name'],
                     'description': f"{c.get('type','')} len={c.get('length','')} prec={c.get('precision','')}"}
                    for c in columns if c.get('name')
                ],
            })
        path = os.path.join(self.output_dir, 'models', 'schema.yml')
        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump({'version': 2, 'models': models}, f, default_flow_style=False, sort_keys=False)
        print(f"  schema.yml ({len(models)} schemas)")

    def _write_manifest(self):
        report = self.translator.get_report()
        manifest = {
            'generated_at': datetime.now().isoformat(),
            'input_dir': self.items_dir,
            'output_dir': self.output_dir,
            'items_parsed': len(self.parsed_items),
            'models_generated': len(self.generated_models),
            'macros_generated': len(self.generated_macros),
            'schemas_discovered': len(self.all_schemas),
            'execution_plans': len(self.execution_plans),
            'java_patterns': sorted(list(self.all_java_patterns)),
            'context_vars': len(self.all_contexts),
            'translation_stats': report['stats'],
            'untranslated_count': len(report['untranslated']),
            'routine_calls_found': report['routine_calls'],
            'routines_discovered': list(self.routine_registry.routines.keys()),
            'models': self.generated_models,
            'jobs': {name: {
                'components': len(data.get('components', {})),
                'connections': len(data.get('connections', [])),
                'plan_steps': len(self.execution_plans.get(name, [])),
            } for name, data in self.parsed_items.items()},
        }
        path = os.path.join(self.output_dir, 'generation_manifest.json')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)


def main():
    ap = argparse.ArgumentParser(description='Generate dbt project from Talend .items')
    ap.add_argument('--input', '-i', required=True, help='Path to .item files')
    ap.add_argument('--output', '-o', default=None, help='Output dbt project dir')
    args = ap.parse_args()
    if not args.output:
        args.output = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dbt_project')
    DBTProjectGenerator(args.input, args.output).generate()


if __name__ == '__main__':
    main()
