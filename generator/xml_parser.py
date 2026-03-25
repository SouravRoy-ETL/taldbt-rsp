"""
WU RSP Pipeline - Talend .item XML Parser (Complete)
Extracts ALL component details from ALL component types.
Every Q1-Q12 detail is captured: tMap expressions, Java code, SQL queries,
file paths, conditions, loop configs, parallel settings, dedup keys,
archive settings, schema columns, connection wiring — everything.
"""
import os
import re
import json
from pathlib import Path
from xml.etree import ElementTree as ET


class TalendItemParser:
    """Parses a single Talend .item XML file into fully structured data."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self.job_name = self.filename.replace('.item', '')
        self.tree = None
        self.root = None

    def parse(self) -> dict:
        self.tree = ET.parse(self.filepath)
        self.root = self.tree.getroot()
        return {
            'job_name': self.job_name,
            'filepath': self.filepath,
            'components': self._extract_components(),
            'connections': self._extract_connections(),
            'contexts': self._extract_contexts(),
            'metadata': self._extract_metadata(),
        }

    # =========================================================================
    # COMPONENTS — extract every node with type-specific handlers
    # =========================================================================
    def _extract_components(self) -> dict:
        components = {}
        for node in self.root.iter():
            if not (node.tag.endswith('node') or node.tag == 'node'):
                continue

            comp_type = node.get('componentName', '')
            comp_id = self._get_unique_name(node)
            if not comp_id:
                continue

            props = self._extract_all_properties(node)
            comp = {
                'component_type': comp_type,
                'unique_name': comp_id,
                'properties': props,
            }

            # === Type-specific extraction ===
            handler = self._get_handler(comp_type)
            if handler:
                comp.update(handler(node, props))

            components[comp_id] = comp
        return components

    def _get_unique_name(self, node) -> str:
        uid = node.get('componentId', '') or node.get('uniqueName', '')
        if not uid:
            for p in node.iter():
                if (p.tag.endswith('elementParameter') or p.tag == 'elementParameter'):
                    if p.get('name') == 'UNIQUE_NAME':
                        return p.get('value', '')
        return uid

    def _extract_all_properties(self, node) -> dict:
        props = {}
        for p in node.iter():
            if p.tag.endswith('elementParameter') or p.tag == 'elementParameter':
                name = p.get('name', '')
                if name:
                    props[name] = {
                        'value': p.get('value', ''),
                        'field': p.get('field', ''),
                    }
                    # Also capture child elementValues (for lists like context overrides)
                    children = []
                    for child in p:
                        if child.tag.endswith('elementValue') or child.tag == 'elementValue':
                            children.append({
                                'elementRef': child.get('elementRef', ''),
                                'value': child.get('value', ''),
                            })
                    if children:
                        props[name]['children'] = children
        return props

    def _get_handler(self, comp_type: str):
        """Route to the correct extraction handler by component type."""
        handlers = {
            'tMap': self._extract_tmap,
            'tJava': self._extract_java,
            'tJavaRow': self._extract_java,
            'tDBRow': self._extract_db_query,
            'tSnowflakeRow': self._extract_db_query,
            'tDBOutput': self._extract_db_output,
            'tSnowflakeOutput': self._extract_db_output,
            'tDBInput': self._extract_db_input,
            'tSnowflakeInput': self._extract_db_input,
            'tDBConnection': self._extract_db_connection,
            'tRunJob': self._extract_runjob,
            'tFileList': self._extract_filelist,
            'tFileCopy': self._extract_filecopy,
            'tFileDelete': self._extract_filedelete,
            'tFileArchive': self._extract_filearchive,
            'tFileInputDelimited': self._extract_file_input,
            'tFileInputFullRow': self._extract_file_input,
            'tFileOutputDelimited': self._extract_file_output,
            'tExtractDelimitedFields': self._extract_delimited_fields,
            'tFixedFlowInput': self._extract_fixed_flow,
            'tParallelize': self._extract_parallelize,
            'tLoop': self._extract_loop,
            'tFlowToIterate': self._extract_flow_iterate,
            'tIterateToFlow': self._extract_flow_iterate,
            'tUniqRow': self._extract_uniqrow,
            'tLogCatcher': self._extract_logcatcher,
            'tSendMail': self._extract_sendmail,
            'tChronometerStop': self._extract_chronometer,
            'tBufferOutput': self._extract_generic,
            'tPrejob': self._extract_generic,
            'tPostjob': self._extract_generic,
        }
        # Match by prefix (handles tSnowflakeRow, tDBRow_2 etc)
        for prefix, handler in handlers.items():
            if comp_type.startswith(prefix):
                return handler
        return self._extract_generic

    # =========================================================================
    # tMap — variables, inputs, outputs, filters, expressions
    # =========================================================================
    def _extract_tmap(self, node, props) -> dict:
        tmap = {'tmap_data': {'variables': [], 'inputs': [], 'outputs': [], 'lookups': []}}
        for child in node.iter():
            tag = str(child.tag)

            # Variables table
            if 'varTable' in tag:
                for entry in child:
                    if 'mapperTableEntry' in str(entry.tag):
                        tmap['tmap_data']['variables'].append({
                            'name': entry.get('name', ''),
                            'expression': entry.get('expression', ''),
                            'type': entry.get('type', ''),
                            'nullable': entry.get('nullable', ''),
                        })

            # Output tables
            if 'outputTable' in tag:
                out = {
                    'name': child.get('name', ''),
                    'filter': child.get('expressionFilter', ''),
                    'reject': child.get('reject', 'false'),
                    'rejectInnerJoin': child.get('rejectInnerJoin', 'false'),
                    'columns': [],
                }
                for entry in child:
                    if 'mapperTableEntry' in str(entry.tag):
                        out['columns'].append({
                            'name': entry.get('name', ''),
                            'expression': entry.get('expression', ''),
                            'type': entry.get('type', ''),
                            'nullable': entry.get('nullable', ''),
                            'operator': entry.get('operator', ''),
                        })
                if out['name']:
                    tmap['tmap_data']['outputs'].append(out)

            # Input/lookup tables
            if 'inputTable' in tag:
                inp = {
                    'name': child.get('name', ''),
                    'lookupMode': child.get('lookupMode', ''),
                    'matchingMode': child.get('matchingMode', ''),
                    'joinModel': child.get('joinModel', ''),
                    'persistent': child.get('persistent', ''),
                    'sizeState': child.get('sizeState', ''),
                    'columns': [],
                    'join_keys': [],
                }
                for entry in child:
                    if 'mapperTableEntry' in str(entry.tag):
                        col = {
                            'name': entry.get('name', ''),
                            'expression': entry.get('expression', ''),
                            'type': entry.get('type', ''),
                            'operator': entry.get('operator', ''),
                        }
                        inp['columns'].append(col)
                        # If expression references another table, it's a join key
                        if entry.get('expression', '') and '.' in entry.get('expression', ''):
                            inp['join_keys'].append(col)

                if inp['name']:
                    if inp['lookupMode'] or inp['joinModel']:
                        tmap['tmap_data']['lookups'].append(inp)
                    tmap['tmap_data']['inputs'].append(inp)

        return tmap

    # =========================================================================
    # tJava / tJavaRow — extract code
    # =========================================================================
    def _extract_java(self, node, props) -> dict:
        code = self._prop_val(props, 'CODE')
        if not code:
            code = self._prop_from_json(props, 'CODE')
        imports = self._prop_val(props, 'IMPORT')
        return {'java_code': code, 'java_imports': imports}

    # =========================================================================
    # tDBRow / tSnowflakeRow — extract SQL query
    # =========================================================================
    def _extract_db_query(self, node, props) -> dict:
        sql = self._prop_val(props, 'QUERY')
        if not sql:
            sql = self._prop_from_json(props, 'QUERY')
        table = self._prop_val(props, 'TABLE')
        connection = self._prop_val(props, 'CONNECTION')
        propagate = self._prop_val(props, 'PROPAGATE_QUERY_RESULT_SET')
        return {
            'sql_query': sql,
            'table': table,
            'connection_ref': connection,
            'propagate_resultset': propagate == 'true',
        }

    # =========================================================================
    # tDBOutput / tSnowflakeOutput — target table, action, schema
    # =========================================================================
    def _extract_db_output(self, node, props) -> dict:
        table = self._prop_val(props, 'TABLE')
        action = self._prop_val(props, 'DATA_ACTION') or self._prop_val(props, 'TABLE_ACTION')
        output_action = self._prop_val(props, 'OUTPUT_ACTION') or action
        table_action = self._prop_val(props, 'TABLE_ACTION')
        die_on_error = self._prop_val(props, 'DIE_ON_ERROR')
        connection = self._prop_val(props, 'CONNECTION')
        commit_every = self._prop_val(props, 'COMMIT_EVERY')
        use_batch = self._prop_val(props, 'USE_BATCH_SIZE')

        # If table contains context reference, preserve it
        if not table:
            table = self._prop_from_json(props, 'TABLE')

        # Extract schema columns from metadata
        schema_cols = self._extract_component_schema(node)

        return {
            'db_output': {
                'table': table,
                'table_action': table_action,
                'output_action': output_action,
                'die_on_error': die_on_error == 'true' if die_on_error else False,
                'connection_ref': connection,
                'commit_every': commit_every,
                'use_batch': use_batch,
                'schema_columns': schema_cols,
            }
        }

    # =========================================================================
    # tDBInput / tSnowflakeInput — source query, table
    # =========================================================================
    def _extract_db_input(self, node, props) -> dict:
        query = self._prop_val(props, 'QUERY')
        if not query:
            query = self._prop_from_json(props, 'QUERY')
        table = self._prop_val(props, 'TABLE')
        if not table:
            table = self._prop_from_json(props, 'TABLE')
        manual_query = self._prop_val(props, 'MANUAL_QUERY')
        connection = self._prop_val(props, 'CONNECTION')
        schema_cols = self._extract_component_schema(node)

        return {
            'db_input': {
                'query': query,
                'table': table,
                'manual_query': manual_query == 'true' if manual_query else False,
                'connection_ref': connection,
                'schema_columns': schema_cols,
            }
        }

    # =========================================================================
    # tDBConnection — connection details
    # =========================================================================
    def _extract_db_connection(self, node, props) -> dict:
        return {
            'db_connection': {
                'host': self._prop_val(props, 'HOST'),
                'port': self._prop_val(props, 'PORT'),
                'database': self._prop_val(props, 'DBNAME'),
                'schema': self._prop_val(props, 'SCHEMA_DB'),
                'user': self._prop_val(props, 'USER'),
                'password': self._prop_val(props, 'PASS'),
                'warehouse': self._prop_val(props, 'WAREHOUSE'),
                'db_type': self._prop_val(props, 'DB_TYPE'),
                'auth_type': self._prop_val(props, 'AUTHENTICATION_TYPE'),
            }
        }

    # =========================================================================
    # tRunJob — child job, context overrides
    # =========================================================================
    def _extract_runjob(self, node, props) -> dict:
        process = self._prop_val(props, 'PROCESS_TYPE_PROCESS') or self._prop_val(props, 'PROCESS')
        independent = self._prop_val(props, 'USE_INDEPENDENT_PROCESS')
        die_on_error = self._prop_val(props, 'DIE_ON_CHILD_ERROR')
        transmit = self._prop_val(props, 'TRANSMIT_WHOLE_CONTEXT')

        # Extract context overrides from PARAM_NAME/VALUE pairs
        overrides = {}
        for key, val in props.items():
            if key.startswith('PARAM_NAME_COLUMN'):
                idx = key.replace('PARAM_NAME_COLUMN', '')
                val_key = f'PARAM_VALUE_COLUMN{idx}'
                if val_key in props:
                    overrides[val['value']] = props[val_key]['value']

        return {
            'child_job': {
                'process': process,
                'independent': independent == 'true' if independent else False,
                'die_on_error': die_on_error != 'false' if die_on_error else True,
                'transmit_context': transmit != 'false' if transmit else True,
                'context_overrides': overrides,
            }
        }

    # =========================================================================
    # tFileList — directory scanning
    # =========================================================================
    def _extract_filelist(self, node, props) -> dict:
        return {
            'file_list': {
                'directory': self._prop_val(props, 'DIRECTORY'),
                'filemask': self._prop_val(props, 'FILES'),
                'case_sensitive': self._prop_val(props, 'CASE_SENSITIVE'),
                'include_subdir': self._prop_val(props, 'INCLUDSUBDIR'),
                'order_by_filename': self._prop_val(props, 'ORDER_BY_FILENAME'),
                'order_by_filesize': self._prop_val(props, 'ORDER_BY_FILESIZE'),
                'order_by_modifieddate': self._prop_val(props, 'ORDER_BY_MODIFIEDDATE'),
                'order_action_asc': self._prop_val(props, 'ORDER_ACTION_ASC'),
                'order_action_desc': self._prop_val(props, 'ORDER_ACTION_DESC'),
                'error': self._prop_val(props, 'ERROR'),
                'glob_expressions': self._prop_val(props, 'GLOBEXPRESSIONS'),
                'if_exclude': self._prop_val(props, 'IFEXCLUDE'),
                'exclude_filemask': self._prop_val(props, 'EXCLUDEFILEMASK'),
                'format_filepath_to_slash': self._prop_val(props, 'FORMAT_FILEPATH_TO_SLASH'),
                'list_mode': self._prop_val(props, 'LIST_MODE'),
            }
        }

    # =========================================================================
    # tFileCopy — file copy with rename
    # =========================================================================
    def _extract_filecopy(self, node, props) -> dict:
        return {
            'file_copy': {
                'filename': self._prop_val(props, 'FILENAME'),
                'source_directory': self._prop_val(props, 'SOURCE_DIRECTORY'),
                'destination': self._prop_val(props, 'DESTINATION'),
                'destination_rename': self._prop_val(props, 'DESTINATION_RENAME'),
                'rename': self._prop_val(props, 'RENAME'),
                'enable_copy_directory': self._prop_val(props, 'ENABLE_COPY_DIRECTORY'),
                'remove_file': self._prop_val(props, 'REMOVE_FILE'),
                'replace_file': self._prop_val(props, 'REPLACE_FILE'),
                'create_directory': self._prop_val(props, 'CREATE_DIRECTORY'),
                'failon': self._prop_val(props, 'FAILON'),
                'force_copy_delete': self._prop_val(props, 'FORCE_COPY_DELETE'),
                'preserve_last_modified': self._prop_val(props, 'PRESERVE_LAST_MODIFIED_TIME'),
            }
        }

    # =========================================================================
    # tFileDelete — file deletion
    # =========================================================================
    def _extract_filedelete(self, node, props) -> dict:
        return {
            'file_delete': {
                'filename': self._prop_val(props, 'FILENAME'),
                'directory': self._prop_val(props, 'DIRECTORY'),
                'path': self._prop_val(props, 'PATH'),
                'failon': self._prop_val(props, 'FAILON'),
                'folder': self._prop_val(props, 'FOLDER'),
                'folder_file': self._prop_val(props, 'FOLDER_FILE'),
                'note': self._prop_val(props, 'NOTE'),
            }
        }

    # =========================================================================
    # tFileArchive — zip with encryption
    # =========================================================================
    def _extract_filearchive(self, node, props) -> dict:
        return {
            'file_archive': {
                'source': self._prop_val(props, 'SOURCE'),
                'sub_directory': self._prop_val(props, 'SUB_DIRECTORY'),
                'target': self._prop_val(props, 'TARGET'),
                'source_file': self._prop_val(props, 'SOURCE_FILE'),
                'archive_format': self._prop_val(props, 'ARCHIVE_FORMAT'),
                'level': self._prop_val(props, 'LEVEL'),
                'all_files': self._prop_val(props, 'ALL_FILES'),
                'encoding': self._prop_val(props, 'ENCODING'),
                'encrypt_files': self._prop_val(props, 'ENCRYPT_FILES'),
                'encrypt_method': self._prop_val(props, 'ENCRYPT_METHOD'),
                'aes_key_strength': self._prop_val(props, 'AES_KEY_STRENGTH'),
                'password': self._prop_val(props, 'PASSWORD'),
                'overwrite': self._prop_val(props, 'OVERWRITE'),
                'mkdir': self._prop_val(props, 'MKDIR'),
            }
        }

    # =========================================================================
    # tFileInputDelimited / tFileInputFullRow — file reading config
    # =========================================================================
    def _extract_file_input(self, node, props) -> dict:
        return {
            'file_input': {
                'filename': self._prop_val(props, 'FILENAME'),
                'rowseparator': self._prop_val(props, 'ROWSEPARATOR'),
                'fieldseparator': self._prop_val(props, 'FIELDSEPARATOR'),
                'csvrowseparator': self._prop_val(props, 'CSVROWSEPARATOR'),
                'csv_option': self._prop_val(props, 'CSV_OPTION'),
                'escape_char': self._prop_val(props, 'ESCAPE_CHAR'),
                'text_enclosure': self._prop_val(props, 'TEXT_ENCLOSURE'),
                'header': self._prop_val(props, 'HEADER'),
                'footer': self._prop_val(props, 'FOOTER'),
                'limit': self._prop_val(props, 'LIMIT'),
                'nb_random': self._prop_val(props, 'NB_RANDOM'),
                'nb_rows': self._prop_val(props, 'NB_ROWS'),
                'encoding': self._prop_val(props, 'ENCODING'),
                'encoding_type': self._prop_val(props, 'ENCODING:ENCODING_TYPE'),
                'remove_empty_row': self._prop_val(props, 'REMOVE_EMPTY_ROW'),
                'uncompress': self._prop_val(props, 'UNCOMPRESS'),
                'die_on_error': self._prop_val(props, 'DIE_ON_ERROR'),
                'schema_opt_num': self._prop_val(props, 'SCHEMA_OPT_NUM'),
                'use_singlemode': self._prop_val(props, 'USE_SINGLEMODE'),
                'use_intable': self._prop_val(props, 'USE_INTABLE'),
                'use_inlinecontent': self._prop_val(props, 'USE_INLINECONTENT'),
                'advanced_separator': self._prop_val(props, 'ADVANCED_SEPARATOR'),
                'thousands_separator': self._prop_val(props, 'THOUSANDS_SEPARATOR'),
                'decimal_separator': self._prop_val(props, 'DECIMAL_SEPARATOR'),
                'splitrecord': self._prop_val(props, 'SPLITRECORD'),
                'random': self._prop_val(props, 'RANDOM'),
                'trimall': self._prop_val(props, 'TRIMALL'),
                'check_fields_num': self._prop_val(props, 'CHECK_FIELDS_NUM'),
                'check_date': self._prop_val(props, 'CHECK_DATE'),
                'use_existing_dynamic': self._prop_val(props, 'USE_EXISTING_DYNAMIC'),
                'temp_dir': self._prop_val(props, 'TEMP_DIR'),
                'schema_columns': self._extract_component_schema(node),
            }
        }

    # =========================================================================
    # tFileOutputDelimited — file writing config
    # =========================================================================
    def _extract_file_output(self, node, props) -> dict:
        return {
            'file_output': {
                'filename': self._prop_val(props, 'FILENAME'),
                'rowseparator': self._prop_val(props, 'ROWSEPARATOR'),
                'fieldseparator': self._prop_val(props, 'FIELDSEPARATOR'),
                'csvrowseparator': self._prop_val(props, 'CSVROWSEPARATOR'),
                'append': self._prop_val(props, 'APPEND'),
                'includeheader': self._prop_val(props, 'INCLUDEHEADER'),
                'compress': self._prop_val(props, 'COMPRESS'),
                'encoding': self._prop_val(props, 'ENCODING'),
                'encoding_type': self._prop_val(props, 'ENCODING:ENCODING_TYPE'),
                'create': self._prop_val(props, 'CREATE'),
                'split': self._prop_val(props, 'SPLIT'),
                'split_every': self._prop_val(props, 'SPLIT_EVERY'),
                'flushonrow': self._prop_val(props, 'FLUSHONROW'),
                'flushonrow_num': self._prop_val(props, 'FLUSHONROW_NUM'),
                'row_mode': self._prop_val(props, 'ROW_MODE'),
                'delete_emptyfile': self._prop_val(props, 'DELETE_EMPTYFILE'),
                'file_exist_exception': self._prop_val(props, 'FILE_EXIST_EXCEPTION'),
                'os_line_separator': self._prop_val(props, 'OS_LINE_SEPARATOR_AS_ROW_SEPARATOR'),
                'usestream': self._prop_val(props, 'USESTREAM'),
                'streamname': self._prop_val(props, 'STREAMNAME'),
                'schema_columns': self._extract_component_schema(node),
            }
        }

    # =========================================================================
    # tExtractDelimitedFields — field splitting
    # =========================================================================
    def _extract_delimited_fields(self, node, props) -> dict:
        return {
            'extract_fields': {
                'field': self._prop_val(props, 'FIELD'),
                'fieldseparator': self._prop_val(props, 'FIELDSEPARATOR'),
                'ignore_source_null': self._prop_val(props, 'IGNORE_SOURCE_NULL'),
                'die_on_error': self._prop_val(props, 'DIE_ON_ERROR'),
                'advanced_separator': self._prop_val(props, 'ADVANCED_SEPARATOR'),
                'thousands_separator': self._prop_val(props, 'THOUSANDS_SEPARATOR'),
                'decimal_separator': self._prop_val(props, 'DECIMAL_SEPARATOR'),
                'trim': self._prop_val(props, 'TRIM'),
                'check_fields_num': self._prop_val(props, 'CHECK_FIELDS_NUM'),
                'check_date': self._prop_val(props, 'CHECK_DATE'),
                'schema_opt_num': self._prop_val(props, 'SCHEMA_OPT_NUM'),
                'schema_columns': self._extract_component_schema(node),
            }
        }

    # =========================================================================
    # tFixedFlowInput — static row generation
    # =========================================================================
    def _extract_fixed_flow(self, node, props) -> dict:
        nb_rows = self._prop_val(props, 'NB_ROWS')
        # Extract fixed values from schema/properties
        values = {}
        for key, val in props.items():
            if key.startswith('VALUES') or key.startswith('VALUE'):
                values[key] = val['value']
        return {
            'fixed_flow': {
                'nb_rows': nb_rows,
                'values': values,
                'schema_columns': self._extract_component_schema(node),
            }
        }

    # =========================================================================
    # tParallelize — parallel execution config
    # =========================================================================
    def _extract_parallelize(self, node, props) -> dict:
        return {
            'parallelize': {
                'wait_for': self._prop_val(props, 'WAIT_FOR'),
                'sleep_duration': self._prop_val(props, 'SLEEP_DURATION'),
                'die_on_parallel_failure': self._prop_val(props, 'DIE_ON_PARALLEL_FAILURE'),
            }
        }

    # =========================================================================
    # tLoop — loop configuration
    # =========================================================================
    def _extract_loop(self, node, props) -> dict:
        return {
            'loop': {
                'loop_type': self._prop_val(props, 'LOOP_TYPE'),
                'from': self._prop_val(props, 'FROM'),
                'to': self._prop_val(props, 'TO'),
                'step': self._prop_val(props, 'STEP'),
                'values_increasing': self._prop_val(props, 'VALUES_INCREASING'),
                'condition': self._prop_val(props, 'CONDITION'),
            }
        }

    # =========================================================================
    # tFlowToIterate / tIterateToFlow — flow mapping
    # =========================================================================
    def _extract_flow_iterate(self, node, props) -> dict:
        # Extract variable mappings from DEFAULT_MAP or MAP table
        mappings = []
        for key, val in props.items():
            if 'MAP' in key.upper() or 'DEFAULT' in key.upper():
                mappings.append({'key': key, 'value': val['value']})
                if 'children' in val:
                    for child in val['children']:
                        mappings.append({
                            'ref': child.get('elementRef', ''),
                            'value': child.get('value', ''),
                        })
        return {
            'flow_iterate': {
                'default_map': self._prop_val(props, 'DEFAULT_MAP'),
                'mappings': mappings,
            }
        }

    # =========================================================================
    # tUniqRow — deduplication keys
    # =========================================================================
    def _extract_uniqrow(self, node, props) -> dict:
        key_columns = []
        # Keys are stored in KEY_ATTRIBUTE column schema
        schema_cols = self._extract_component_schema(node)
        for col in schema_cols:
            if col.get('key', 'false') == 'true':
                key_columns.append(col['name'])

        # Also check UNIQUE_KEY property
        for key, val in props.items():
            if 'UNIQUE' in key.upper() or 'KEY' in key.upper():
                if val.get('children'):
                    for child in val['children']:
                        if child.get('elementRef') == 'KEY_ATTRIBUTE' and child.get('value') == 'true':
                            pass  # already handled via schema

        return {
            'uniq_row': {
                'key_columns': key_columns,
                'all_columns': schema_cols,
                'only_once': self._prop_val(props, 'ONLY_ONCE'),
            }
        }

    # =========================================================================
    # tLogCatcher — error catching config
    # =========================================================================
    def _extract_logcatcher(self, node, props) -> dict:
        return {
            'log_catcher': {
                'catch_java': self._prop_val(props, 'CATCH_JAVA_EXCEPTION'),
                'catch_tdie': self._prop_val(props, 'CATCH_TDIE'),
                'catch_twarn': self._prop_val(props, 'CATCH_TWARN'),
                'catch_runtime': self._prop_val(props, 'CATCH_RUNTIME_EXCEPTION'),
                'catch_job_failure': self._prop_val(props, 'CATCH_TJOBFAILURE'),
            }
        }

    # =========================================================================
    # tSendMail — email notification
    # =========================================================================
    def _extract_sendmail(self, node, props) -> dict:
        return {
            'send_mail': {
                'smtp_host': self._prop_val(props, 'SMTP_HOST'),
                'smtp_port': self._prop_val(props, 'SMTP_PORT'),
                'to': self._prop_val(props, 'TO'),
                'from': self._prop_val(props, 'FROM'),
                'cc': self._prop_val(props, 'CC'),
                'subject': self._prop_val(props, 'SUBJECT'),
                'message': self._prop_val(props, 'MESSAGE'),
                'sender_name': self._prop_val(props, 'SENDERNAME'),
                'need_auth': self._prop_val(props, 'NEED_AUTH'),
                'importance': self._prop_val(props, 'IMPORTANCE'),
            }
        }

    # =========================================================================
    # tChronometerStop — execution timing
    # =========================================================================
    def _extract_chronometer(self, node, props) -> dict:
        return {
            'chronometer': {
                'since': self._prop_val(props, 'SINCE'),
                'display_duration': self._prop_val(props, 'DISPLAY_DURATION'),
                'display_component': self._prop_val(props, 'DISPLAY_COMPONENT_NAME'),
            }
        }

    # =========================================================================
    # Generic fallback
    # =========================================================================
    def _extract_generic(self, node, props) -> dict:
        return {}

    # =========================================================================
    # Schema extraction from metadata child elements
    # =========================================================================
    def _extract_component_schema(self, node) -> list:
        columns = []
        for meta in node.iter():
            if 'metadata' in str(meta.tag).lower() or 'schema' in str(meta.tag).lower():
                for col in meta:
                    if 'column' in str(col.tag).lower():
                        columns.append({
                            'name': col.get('name', ''),
                            'type': col.get('type', ''),
                            'length': col.get('length', ''),
                            'precision': col.get('precision', ''),
                            'key': col.get('key', 'false'),
                            'nullable': col.get('nullable', 'true'),
                            'default': col.get('default', ''),
                            'pattern': col.get('pattern', ''),
                        })
        return columns

    # =========================================================================
    # CONNECTIONS — every link between components
    # =========================================================================
    def _extract_connections(self) -> list:
        connections = []
        for conn in self.root.iter():
            if not (conn.tag.endswith('connection') or conn.tag == 'connection'):
                continue

            conn_type = conn.get('connectorName', '') or conn.get('lineStyle', '')
            source = conn.get('source', '')
            target = conn.get('target', '')
            label = conn.get('metaname', '') or conn.get('label', '')
            order = conn.get('outputId', '') or conn.get('order', '')
            offset_label = conn.get('offsetLabelX', '')

            # Extract condition for IF/RUN_IF connections
            condition = ''
            for p in conn.iter():
                if p.tag.endswith('elementParameter') or p.tag == 'elementParameter':
                    if p.get('name') in ('CONDITION', 'CONDITION_EXPRESSION', 'IF_CONDITION'):
                        condition = p.get('value', '')
                    elif p.get('name') == 'UNIQUE_NAME':
                        pass  # skip

            if source and target:
                connections.append({
                    'type': conn_type,
                    'source': source,
                    'target': target,
                    'label': label,
                    'order': order,
                    'condition': condition,
                })
        return connections

    # =========================================================================
    # CONTEXTS — all variables
    # =========================================================================
    def _extract_contexts(self) -> dict:
        contexts = {}
        for ctx_group in self.root.iter():
            if 'context' in str(ctx_group.tag).lower():
                group_name = ctx_group.get('name', 'default')
                for param in ctx_group:
                    if 'contextParameter' in str(param.tag):
                        name = param.get('name', '')
                        if name:
                            contexts[name] = {
                                'value': param.get('value', ''),
                                'type': param.get('type', ''),
                                'prompt': param.get('prompt', ''),
                                'comment': param.get('comment', ''),
                                'group': group_name,
                            }
        return contexts

    # =========================================================================
    # METADATA — all schemas
    # =========================================================================
    def _extract_metadata(self) -> dict:
        schemas = {}
        for meta in self.root.iter():
            if 'metadata' in str(meta.tag).lower() and meta.get('connector', ''):
                connector = meta.get('connector', '')
                name = meta.get('name', '') or meta.get('label', '')
                key = f"{name}_{connector}" if name else connector
                columns = []
                for col in meta:
                    if 'column' in str(col.tag).lower():
                        columns.append({
                            'name': col.get('name', ''),
                            'type': col.get('type', ''),
                            'length': col.get('length', ''),
                            'precision': col.get('precision', ''),
                            'key': col.get('key', 'false'),
                            'nullable': col.get('nullable', 'true'),
                            'default': col.get('default', ''),
                            'pattern': col.get('pattern', ''),
                        })
                if columns:
                    schemas[key] = columns
        return schemas

    # =========================================================================
    # Helper: safely extract a property value
    # =========================================================================
    def _prop_val(self, props: dict, key: str) -> str:
        if key in props:
            return props[key].get('value', '')
        return ''

    def _prop_from_json(self, props: dict, field_name: str) -> str:
        """Extract field from PROPERTIES JSON blob (Snowflake components)."""
        for key, val in props.items():
            if 'PROPERTIES' in key.upper():
                raw = val.get('value', '')
                if raw:
                    try:
                        data = json.loads(raw)
                        return data.get(field_name, data.get(field_name.lower(), ''))
                    except (json.JSONDecodeError, TypeError):
                        pass
        return ''


# =============================================================================
# Project-level parser
# =============================================================================
class ProjectParser:
    """Parses all .item files in a Talend project directory."""

    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        self.items = {}

    def parse_all(self) -> dict:
        item_files = []
        for root, dirs, files in os.walk(self.project_dir):
            for f in files:
                if f.endswith('.item'):
                    item_files.append(os.path.join(root, f))

        print(f"Found {len(item_files)} .item files")

        for fp in sorted(item_files):
            try:
                parser = TalendItemParser(fp)
                result = parser.parse()
                self.items[result['job_name']] = result
                comps = result['components']
                conns = result['connections']

                # Count by type
                type_counts = {}
                for c in comps.values():
                    t = c['component_type']
                    type_counts[t] = type_counts.get(t, 0) + 1
                type_summary = ', '.join(f"{t}:{n}" for t, n in sorted(type_counts.items()))

                print(f"  {result['job_name']}: {len(comps)} components, {len(conns)} connections")
                print(f"    Types: {type_summary}")
            except Exception as e:
                print(f"  ERROR parsing {fp}: {e}")

        return self.items

    def get_job(self, pattern: str) -> dict:
        for name, data in self.items.items():
            if pattern.lower() in name.lower():
                return data
        return None

    def get_execution_order(self, job_name: str) -> list:
        job = self.get_job(job_name)
        if not job:
            return []

        adj = {}
        for conn in job['connections']:
            if conn['type'] in ('SUBJOB_OK', 'ON_SUBJOB_OK', 'ON_COMPONENT_OK', 'COMPONENT_OK', 'RUN_IF'):
                src, tgt = conn['source'], conn['target']
                if src not in adj:
                    adj[src] = []
                adj[src].append({'target': tgt, 'type': conn['type'], 'condition': conn.get('condition', ''), 'order': conn.get('order', '')})

        visited = set()
        order = []

        def dfs(node):
            if node in visited:
                return
            visited.add(node)
            for edge in adj.get(node, []):
                dfs(edge['target'])
            order.append(node)

        for node in adj:
            dfs(node)

        order.reverse()
        return order

    def dump_json(self, output_path: str):
        """Dump all parsed data as JSON for inspection."""
        import json
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.items, f, indent=2, ensure_ascii=False, default=str)
        print(f"Dumped parsed data to {output_path}")
