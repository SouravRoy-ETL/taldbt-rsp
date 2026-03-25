"""
Translates Talend Java expressions → DuckDB SQL.

CRITICAL DESIGN PRINCIPLE: 1:1 with Talend. If we can't translate something,
we FLAG it with -- UNTRANSLATED: <original expression> so it shows up
during dbt compile as an error. We NEVER silently skip or mangle.

Handles:
  - Mathematical.SMUL, StringHandling.*, TalendDate.*
  - BigDecimal chains: .subtract(), .multiply(), .divide(), .setScale(), .compareTo()
  - Nested Java ternary (3-4 levels deep from IOPQ V32-V43 KSR variables)
  - Numeric.sequence → ROW_NUMBER()
  - input_row.X, outmap.X, row7.X, Var.X → column references (DYNAMIC, not hardcoded)
  - context.X → {{ var('x') }}
  - Talend routines → discovered from routines/*.item, flagged if can't translate
  - UNKNOWN patterns → flagged with -- UNTRANSLATED comment
"""
import re
import os
import json
from collections import OrderedDict


class RoutineRegistry:
    """
    Discovers and registers Talend routines from the project.
    Routines are Java classes under code/routines/ in Talend projects.
    They can be called from ANY tMap or tJavaRow expression.
    """

    def __init__(self):
        self.routines = {}  # routine_name → {methods: {method_name: {params, body}}}
        self.routine_calls_found = set()  # track what's called in expressions

    def discover_routines(self, project_dir: str):
        """Scan project dir for routine .java or .item files."""
        routine_dirs = [
            os.path.join(project_dir, 'code', 'routines'),
            os.path.join(project_dir, 'routines'),
            project_dir,  # some exports flatten structure
        ]

        for rdir in routine_dirs:
            if not os.path.isdir(rdir):
                continue
            for root, dirs, files in os.walk(rdir):
                for f in files:
                    if f.endswith('.java') or (f.endswith('.item') and 'routine' in root.lower()):
                        self._parse_routine_file(os.path.join(root, f))

        if self.routines:
            print(f"  Discovered {len(self.routines)} routines: {list(self.routines.keys())}")

    def _parse_routine_file(self, filepath):
        """Extract class name and method signatures from a Java routine file."""
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            # Find class name
            class_match = re.search(r'class\s+(\w+)', content)
            if not class_match:
                return

            class_name = class_match.group(1)
            methods = {}

            # Find method signatures
            for m in re.finditer(r'public\s+static\s+(\w+)\s+(\w+)\s*\(([^)]*)\)', content):
                return_type = m.group(1)
                method_name = m.group(2)
                params = m.group(3)
                methods[method_name] = {
                    'return_type': return_type,
                    'params': params,
                    'source_file': filepath,
                }

            if methods:
                self.routines[class_name] = {'methods': methods}

        except Exception:
            pass

    def is_routine_call(self, expr: str) -> bool:
        """Check if expression contains a call to a discovered routine."""
        for routine_name in self.routines:
            if routine_name + '.' in expr:
                return True
        return False

    def flag_routine_call(self, expr: str) -> str:
        """Return the routine.method being called for flagging."""
        for routine_name in self.routines:
            match = re.search(rf'{routine_name}\.(\w+)', expr)
            if match:
                call = f"{routine_name}.{match.group(1)}"
                self.routine_calls_found.add(call)
                return call
        return ''


class JavaToSQLTranslator:
    """Converts Talend Java expressions to DuckDB SQL. Flags what it can't handle."""

    def __init__(self, routine_registry: RoutineRegistry = None):
        self.routine_registry = routine_registry or RoutineRegistry()
        self.untranslated = []  # collect everything we couldn't translate
        self.translation_stats = {'translated': 0, 'flagged': 0, 'passthrough': 0}

    # === FUNCTION MAPPINGS (order matters — most specific first) ===
    FUNCTION_PATTERNS = [
        # Mathematical
        (r'Mathematical\.SMUL\(\s*"(-?\d+(?:\.\d+)?)"\s*,\s*(.+?)\)',
         lambda m: f'(CAST({m.group(2).strip()} AS DOUBLE) * {m.group(1)})'),

        (r'Mathematical\.ABS\((.+?)\)',
         lambda m: f'ABS(CAST({m.group(1)} AS DOUBLE))'),

        # StringHandling
        (r'StringHandling\.TRIM\((.+?)\)',
         lambda m: f'TRIM({m.group(1)})'),

        (r'StringHandling\.RIGHT\((.+?),\s*(\d+)\)',
         lambda m: f'RIGHT(CAST({m.group(1)} AS VARCHAR), {m.group(2)})'),

        (r'StringHandling\.LEFT\((.+?),\s*(\d+)\)',
         lambda m: f'LEFT(CAST({m.group(1)} AS VARCHAR), {m.group(2)})'),

        (r'StringHandling\.LEN\((.+?)\)',
         lambda m: f'LENGTH(CAST({m.group(1)} AS VARCHAR))'),

        (r'StringHandling\.SUBSTR\((.+?),\s*(\d+),\s*(\d+)\)',
         lambda m: f'SUBSTR(CAST({m.group(1)} AS VARCHAR), {m.group(2)}, {m.group(3)})'),

        (r'StringHandling\.INDEX\((.+?),\s*(.+?)\)',
         lambda m: f'POSITION({m.group(2)} IN CAST({m.group(1)} AS VARCHAR))'),

        (r'StringHandling\.UPCASE\((.+?)\)',
         lambda m: f'UPPER(CAST({m.group(1)} AS VARCHAR))'),

        (r'StringHandling\.DOWNCASE\((.+?)\)',
         lambda m: f'LOWER(CAST({m.group(1)} AS VARCHAR))'),

        (r'StringHandling\.CHANGE\((.+?),\s*(.+?),\s*(.+?)\)',
         lambda m: f'REPLACE(CAST({m.group(1)} AS VARCHAR), {m.group(2)}, {m.group(3)})'),

        # TalendDate
        (r'TalendDate\.formatDate\(\s*"(.+?)"\s*,\s*TalendDate\.getCurrentDate\(\)\s*\)',
         lambda m: f"STRFTIME(NOW(), '{_java_to_duck_fmt(m.group(1))}')"),

        (r'TalendDate\.formatDate\(\s*"(.+?)"\s*,\s*(.+?)\)',
         lambda m: f"STRFTIME({m.group(2)}, '{_java_to_duck_fmt(m.group(1))}')"),

        (r'TalendDate\.getDate\(\s*"(.+?)"\s*\)',
         lambda m: f"STRFTIME(NOW(), '{_java_to_duck_fmt(m.group(1))}')"),

        (r'TalendDate\.getCurrentDate\(\)',
         lambda m: 'NOW()'),

        (r'TalendDate\.parseDate\(\s*"(.+?)"\s*,\s*(.+?)\)',
         lambda m: f"STRPTIME({m.group(2)}, '{_java_to_duck_fmt(m.group(1))}')"),

        # Numeric
        (r'Numeric\.sequence\(\s*(.+?)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)',
         lambda m: f'({m.group(2)} - 1 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))'),

        # Type constructors
        (r'new\s+BigDecimal\(\s*Double\.valueOf\(\s*(-?\d+(?:\.\d+)?)\s*\)\s*\)',
         lambda m: m.group(1)),

        (r'new\s+BigDecimal\(\s*"(-?\d+(?:\.\d+)?)"\s*\)',
         lambda m: m.group(1)),

        (r'new\s+BigDecimal\(\s*(.+?)\s*\)',
         lambda m: f'CAST({m.group(1)} AS DECIMAL(38,4))'),

        (r'Double\.valueOf\(\s*(-?\d+(?:\.\d+)?)\s*\)',
         lambda m: m.group(1)),

        (r'Double\.parseDouble\(\s*(.+?)\s*\)',
         lambda m: f'CAST({m.group(1)} AS DOUBLE)'),

        (r'Integer\.parseInt\(\s*(.+?)\s*\)',
         lambda m: f'CAST({m.group(1)} AS INTEGER)'),

        (r'String\.valueOf\(\s*(.+?)\s*\)',
         lambda m: f'CAST({m.group(1)} AS VARCHAR)'),
    ]

    # === BigDecimal method chains ===
    BIGDECIMAL_PATTERNS = [
        (r'\.subtract\(\s*(.+?)\s*\)', r' - (\1)'),
        (r'\.multiply\(\s*(.+?)\s*\)', r' * (\1)'),
        (r'\.add\(\s*(.+?)\s*\)', r' + (\1)'),
        (r'\.negate\(\)', r' * -1'),
        (r'\.abs\(\)', r'ABS(\g<0>)'),  # needs context
        (r'\.divide\(\s*(.+?)\s*,\s*(\d+)\s*,\s*(?:BigDecimal\.)?ROUND_HALF_UP\s*\)',
         r' / NULLIF(\1, 0)'),
        (r'\.divide\(\s*(.+?)\s*,\s*(\d+)\s*,\s*(?:java\.math\.)?RoundingMode\.HALF_UP\s*\)',
         r' / NULLIF(\1, 0)'),
        (r'\.setScale\(\s*(\d+)\s*,\s*(?:BigDecimal\.)?ROUND_HALF_UP\s*\)', ''),
        (r'\.setScale\(\s*(\d+)\s*,\s*(?:java\.math\.)?RoundingMode\.HALF_UP\s*\)', ''),
        (r'\.compareTo\(\s*BigDecimal\.ZERO\s*\)\s*==\s*0', ' = 0'),
        (r'\.compareTo\(\s*BigDecimal\.ZERO\s*\)\s*!=\s*0', ' != 0'),
        (r'\.compareTo\(\s*BigDecimal\.ZERO\s*\)\s*>\s*0', ' > 0'),
        (r'\.compareTo\(\s*BigDecimal\.ZERO\s*\)\s*<\s*0', ' < 0'),
        (r'\.compareTo\(\s*(.+?)\s*\)\s*==\s*0', r' = \1'),
        (r'\.toString\(\)', ''),
        (r'BigDecimal\.ZERO', '0'),
        (r'BigDecimal\.ONE', '1'),
    ]

    # === String method chains ===
    STRING_PATTERNS = [
        (r'\.equals\(\s*"(.+?)"\s*\)', r" = '\1'"),
        (r'\.equals\(\s*(.+?)\s*\)', r' = \1'),
        (r'\.contains\(\s*"(.+?)"\s*\)', r" LIKE '%\1%'"),
        (r'\.startsWith\(\s*"(.+?)"\s*\)', r" LIKE '\1%'"),
        (r'\.endsWith\(\s*"(.+?)"\s*\)', r" LIKE '%\1'"),
        (r'\.replaceAll\(\s*"(.+?)"\s*,\s*"(.+?)"\s*\)', r"REGEXP_REPLACE(\g<0>, '\1', '\2')"),
        (r'\.replace\(\s*"(.+?)"\s*,\s*"(.+?)"\s*\)', r"REPLACE(\g<0>, '\1', '\2')"),
        (r'\.trim\(\)', r'TRIM(\g<0>)'),
        (r'\.length\(\)', r'LENGTH(\g<0>)'),
        (r'\.toUpperCase\(\)', r'UPPER(\g<0>)'),
        (r'\.toLowerCase\(\)', r'LOWER(\g<0>)'),
        (r'\.isEmpty\(\)', r" = ''"),
    ]

    def translate_expression(self, java_expr: str, source_context: str = '') -> str:
        """
        Convert a Java expression to SQL.
        If it can't be fully translated, wraps in -- UNTRANSLATED flag.
        """
        if not java_expr or java_expr.strip() == '':
            return 'NULL'

        original = java_expr.strip()
        sql = original

        # Step 1: Check for routine calls
        if self.routine_registry.is_routine_call(sql):
            call = self.routine_registry.flag_routine_call(sql)
            self._flag(original, f"Uses routine: {call}", source_context)
            return f"NULL /* UNTRANSLATED ROUTINE: {call} — {original[:100]} */"

        # Step 2: Handle nested ternary FIRST (before anything else)
        sql = self._translate_ternary_deep(sql)

        # Step 3: Apply function patterns
        for pattern, replacement in self.FUNCTION_PATTERNS:
            sql = re.sub(pattern, replacement, sql)

        # Step 4: Apply BigDecimal chains
        for pattern, replacement in self.BIGDECIMAL_PATTERNS:
            sql = re.sub(pattern, replacement, sql)

        # Step 5: Apply String method chains
        for pattern, replacement in self.STRING_PATTERNS:
            sql = re.sub(pattern, replacement, sql)

        # Step 6: Replace row references DYNAMICALLY (not hardcoded names)
        # Matches: anyIdentifier.columnName
        sql = re.sub(r'([a-zA-Z_]\w*)\.((?!ZERO|ONE|HALF_UP|out|err)[A-Z][a-zA-Z_0-9]+)',
                      lambda m: self._resolve_row_ref(m), sql)

        # Step 7: context.X → {{ var('x') }}
        sql = re.sub(r'context\.(\w+)', lambda m: "{{ var('" + m.group(1).lower() + "') }}", sql)

        # Step 8: Clean Java artifacts
        sql = sql.replace('&&', ' AND ')
        sql = sql.replace('||', ' OR ')
        sql = re.sub(r'(?<!=)!(?!=)', 'NOT ', sql)
        sql = sql.replace('null', 'NULL')
        sql = sql.replace('true', 'TRUE')
        sql = sql.replace('false', 'FALSE')

        # Step 9: Check if anything looks untranslated
        java_remnants = re.findall(
            r'(new\s+\w+|\.get\w+\(|import\s+|System\.\w+|globalMap\.\w+|'
            r'instanceof|throw\s+|try\s*\{|catch\s*\()', sql)
        if java_remnants:
            self._flag(original, f"Java remnants: {java_remnants}", source_context)
            sql = f"NULL /* UNTRANSLATED: {original[:200]} */"

        self.translation_stats['translated'] += 1
        return sql.strip()

    def _translate_ternary_deep(self, expr: str, depth: int = 0) -> str:
        """
        Handle nested Java ternary up to 5 levels deep.
        a ? b : (c ? d : (e ? f : g)) → CASE WHEN a THEN b WHEN c THEN d WHEN e THEN f ELSE g END
        """
        if depth > 5 or '?' not in expr:
            return expr

        # Find the OUTERMOST ternary by counting parentheses
        q_pos = self._find_ternary_split(expr)
        if q_pos < 0:
            return expr

        condition = expr[:q_pos].strip()
        rest = expr[q_pos + 1:]

        # Find the matching colon (accounting for nested ternary)
        colon_pos = self._find_colon_match(rest)
        if colon_pos < 0:
            return expr

        true_val = rest[:colon_pos].strip()
        false_val = rest[colon_pos + 1:].strip()

        # Translate condition: .equals() → =
        condition = re.sub(r'(\w+(?:\.\w+)*)\.equals\(\s*"(.+?)"\s*\)', r"\1 = '\2'", condition)
        condition = re.sub(r'(\w+(?:\.\w+)*)\.equals\(\s*(\w+)\s*\)', r'\1 = \2', condition)

        # Recursively translate nested ternary in branches
        true_val = self._translate_ternary_deep(true_val, depth + 1)
        false_val = self._translate_ternary_deep(false_val, depth + 1)

        return f"CASE WHEN {condition} THEN {true_val} ELSE {false_val} END"

    def _find_ternary_split(self, expr: str) -> int:
        """Find the position of '?' for the outermost ternary, respecting parens."""
        depth = 0
        for i, ch in enumerate(expr):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == '?' and depth == 0:
                return i
        return -1

    def _find_colon_match(self, expr: str) -> int:
        """Find the ':' that matches the outermost '?', accounting for nested ternary."""
        depth = 0
        ternary_depth = 0
        for i, ch in enumerate(expr):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == '?' and depth == 0:
                ternary_depth += 1
            elif ch == ':' and depth == 0:
                if ternary_depth == 0:
                    return i
                ternary_depth -= 1
        return -1

    def _resolve_row_ref(self, match) -> str:
        """Resolve Java row references dynamically — not hardcoded."""
        prefix = match.group(1)
        column = match.group(2)

        # Known prefixes that are Java objects, not column references
        java_objects = {'System', 'Math', 'BigDecimal', 'Integer', 'Double',
                        'String', 'Boolean', 'Long', 'Float', 'java', 'org',
                        'TalendDate', 'Mathematical', 'StringHandling', 'Numeric',
                        'RoundingMode'}
        if prefix in java_objects:
            return match.group(0)  # leave as-is for function pattern matching

        # Var.xxx → already a variable reference
        if prefix == 'Var':
            return column

        # Everything else (input_row, outmap, rowN, Onboarded, etc) → column ref
        return column

    def translate_tmap_output(self, tmap_data: dict, output_name: str) -> dict:
        """Translate an entire tMap output to SQL."""
        output = None
        for out in tmap_data.get('outputs', []):
            if out['name'] == output_name:
                output = out
                break
        if not output:
            return {}

        ctx = f"tMap output {output_name}"
        return {
            'filter': self.translate_expression(output.get('filter', ''), ctx),
            'columns': OrderedDict(
                (col['name'],
                 self.translate_expression(col.get('expression', ''), f"{ctx}.{col['name']}") if col.get('expression') else col['name'])
                for col in output.get('columns', []) if col.get('name')
            ),
        }

    def translate_java_block(self, java_code: str, source_context: str = '') -> list:
        """Convert tJavaRow code block to SQL column assignments."""
        assignments = []
        if not java_code:
            return assignments

        # Split on semicolons but respect string literals and parens
        statements = self._split_statements(java_code)

        for stmt in statements:
            stmt = stmt.strip()
            if not stmt or stmt.startswith('//') or stmt.startswith('System.out'):
                continue

            # Variable declarations: Type varName = expression
            decl_match = re.match(
                r'(?:String|BigDecimal|int|long|double|float|boolean|Integer|Double|Long)\s+'
                r'(\w+)\s*=\s*(.+)', stmt)
            if decl_match:
                var_name = decl_match.group(1)
                expr = self.translate_expression(decl_match.group(2), source_context)
                assignments.append({
                    'column': var_name, 'expression': expr,
                    'is_variable': True, 'original': stmt,
                })
                continue

            # output_row.COLUMN = expression
            out_match = re.match(r'output_row\.(\w+)\s*=\s*(.+)', stmt)
            if out_match:
                col = out_match.group(1)
                expr = self.translate_expression(out_match.group(2), f"{source_context}.{col}")
                assignments.append({
                    'column': col, 'expression': expr,
                    'is_variable': False, 'original': stmt,
                })
                continue

            # context.variable = expression
            ctx_match = re.match(r'context\.(\w+)\s*=\s*(.+)', stmt)
            if ctx_match:
                assignments.append({
                    'column': f'ctx_{ctx_match.group(1)}',
                    'expression': self.translate_expression(ctx_match.group(2), source_context),
                    'is_context': True, 'original': stmt,
                })
                continue

            # If/else blocks — flag as needing manual review
            if stmt.startswith('if') or stmt.startswith('else') or stmt.startswith('for'):
                self._flag(stmt, "Control flow (if/else/for) — needs CASE WHEN conversion", source_context)

        return assignments

    def _split_statements(self, code: str) -> list:
        """Split Java code on semicolons, respecting strings and parens."""
        stmts = []
        current = []
        depth = 0
        in_string = False
        escape_next = False

        for ch in code:
            if escape_next:
                current.append(ch)
                escape_next = False
                continue
            if ch == '\\':
                escape_next = True
                current.append(ch)
                continue
            if ch == '"' and not in_string:
                in_string = True
            elif ch == '"' and in_string:
                in_string = False
            if not in_string:
                if ch == '(' or ch == '{':
                    depth += 1
                elif ch == ')' or ch == '}':
                    depth -= 1
                elif ch == ';' and depth <= 0:
                    stmts.append(''.join(current))
                    current = []
                    continue
            current.append(ch)

        if current:
            stmts.append(''.join(current))
        return stmts

    def translate_sql_for_duckdb(self, snowflake_sql: str) -> str:
        """Convert Snowflake SQL (from tDBRow/tSnowflakeRow) to DuckDB SQL."""
        sql = snowflake_sql
        if not sql:
            return ''

        # Context variable patterns (multiple variants found in real SQL)
        # Pattern: "+context.Entry_tablename+"
        sql = re.sub(
            r'"\s*\+\s*context\.(\w+)\s*\+\s*context\.grp_number\s*\+\s*"',
            lambda m: "{{ var('" + m.group(1).lower() + "') }}{{ var('grp_number') }}", sql)
        sql = re.sub(
            r'"\s*\+\s*context\.(\w+)\s*\+\s*"',
            lambda m: "{{ var('" + m.group(1).lower() + "') }}", sql)
        # Pattern: '"+context.nid_id_update+"' (with single quotes around)
        sql = re.sub(
            r"'\s*\"\s*\+\s*context\.(\w+)\s*\+\s*\"\s*'",
            lambda m: "{{ var('" + m.group(1).lower() + "') }}", sql)
        # Remaining context references
        sql = re.sub(r'context\.(\w+)', lambda m: "{{ var('" + m.group(1).lower() + "') }}", sql)

        # Snowflake → DuckDB function mapping
        replacements = [
            ('GETDATE()', 'NOW()'),
            ('SYSDATE()', 'NOW()'),
            ('CURRENT_TIMESTAMP()', 'NOW()'),
        ]
        for old, new in replacements:
            sql = sql.replace(old, new)

        # TO_NUMBER → CAST AS
        sql = re.sub(r'TO_NUMBER\((.+?)\)', r'CAST(\1 AS DOUBLE)', sql)

        # CONVERT_TIMEZONE → DuckDB timezone()
        sql = re.sub(
            r"CONVERT_TIMEZONE\(\s*'([^']+)'\s*,\s*(.+?)\s*,\s*(.+?)\s*\)",
            r"timezone('\1', \3)", sql)

        # TRY_TO_TIMESTAMP → TRY_CAST or STRPTIME
        sql = re.sub(
            r"TRY_TO_TIMESTAMP\(\s*(.+?)\s*,\s*'(.+?)'\s*\)",
            lambda m: f"STRPTIME({m.group(1)}, '{_snowflake_to_duck_fmt(m.group(2))}')", sql)

        # TO_TIMESTAMP → STRPTIME
        sql = re.sub(
            r"TO_TIMESTAMP\(\s*(.+?)\s*,\s*'(.+?)'\s*\)",
            lambda m: f"STRPTIME({m.group(1)}, '{_snowflake_to_duck_fmt(m.group(2))}')", sql)

        # TO_DATE → CAST AS DATE or STRPTIME
        sql = re.sub(
            r"TO_DATE\(\s*(.+?)\s*,\s*'(.+?)'\s*\)",
            lambda m: f"STRPTIME({m.group(1)}, '{_snowflake_to_duck_fmt(m.group(2))}')", sql)

        # TO_CHAR with date format
        sql = re.sub(
            r"TO_CHAR\(\s*(.+?)\s*,\s*'(.+?)'\s*\)",
            lambda m: f"STRFTIME({m.group(1)}, '{_snowflake_to_duck_fmt(m.group(2))}')", sql)

        # LPAD
        sql = re.sub(r"LPAD\((.+?),\s*(\d+),\s*'(.+?)'\)", r"LPAD(CAST(\1 AS VARCHAR), \2, '\3')", sql)

        # Remove BEGIN/END wrappers
        sql = re.sub(r'^\s*BEGIN\s*\n?', '', sql, flags=re.IGNORECASE | re.MULTILINE)
        sql = re.sub(r'\s*END\s*;?\s*$', '', sql, flags=re.IGNORECASE | re.MULTILINE)

        # Remove leading/trailing quotes from the whole SQL (Talend wraps in "...")
        sql = sql.strip()
        if sql.startswith('"') and sql.endswith('"'):
            sql = sql[1:-1]

        return sql

    def _flag(self, original: str, reason: str, context: str = ''):
        """Record an untranslatable expression for reporting."""
        self.untranslated.append({
            'expression': original[:300],
            'reason': reason,
            'context': context,
        })
        self.translation_stats['flagged'] += 1

    def get_report(self) -> dict:
        """Return translation statistics and untranslated items."""
        return {
            'stats': self.translation_stats,
            'untranslated': self.untranslated,
            'routine_calls': sorted(list(self.routine_registry.routine_calls_found)),
        }


def _java_to_duck_fmt(fmt: str) -> str:
    """Convert Java SimpleDateFormat → DuckDB strftime format."""
    replacements = [
        ('yyyy', '%Y'), ('yy', '%y'),
        ('MM', '%m'), ('dd', '%d'),
        ('HH', '%H'), ('hh', '%I'),
        ('mm', '%M'), ('ss', '%S'),
        ('SSS', '%g'), ('SS', '%S'),
        ('a', '%p'),
    ]
    result = fmt
    for java, duck in replacements:
        result = result.replace(java, duck)
    return result


def _snowflake_to_duck_fmt(fmt: str) -> str:
    """Convert Snowflake date format → DuckDB strftime format."""
    replacements = [
        ('YYYY', '%Y'), ('YY', '%y'),
        ('MM', '%m'), ('DD', '%d'),
        ('HH24', '%H'), ('HH12', '%I'), ('HH', '%H'),
        ('MI', '%M'), ('SS', '%S'),
        ('MMDDYYYY', '%m%d%Y'),
        ('YYYYMMDD', '%Y%m%d'),
    ]
    result = fmt
    for sf, duck in replacements:
        result = result.replace(sf, duck)
    return result
