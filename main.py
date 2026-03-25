"""
WU RSP Pipeline — CLI Entry Point

Commands:
  generate    Parse Talend .items → produce dbt project
  run         Run one group (Step 1 + Step 2)
  run-all     Run all 20 groups in parallel
  scheduler   Start 30-min scheduler (replaces TMC)
  export      Export DuckDB → Parquet
  push        Push Parquet → Snowflake
  ui          Launch Streamlit dashboard
"""
import argparse
import sys
import os
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def cmd_generate(args):
    from generator.generate import DBTProjectGenerator
    gen = DBTProjectGenerator(args.input, args.output)
    gen.generate()
    # Save translation report
    report = gen.translator.get_report()
    if report['untranslated']:
        report_path = os.path.join(args.output, 'UNTRANSLATED_REPORT.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n⚠ {len(report['untranslated'])} expressions could not be translated!")
        print(f"  See: {report_path}")
        for item in report['untranslated'][:10]:
            print(f"  - [{item['context']}] {item['reason']}: {item['expression'][:80]}")


def cmd_run(args):
    from engine.orchestrator import run_group
    dbt_dir = args.dbt_dir or os.path.join(os.path.dirname(__file__), 'dbt_project')
    dev = not getattr(args, 'prod', False)
    run = run_group(args.grp, dbt_dir, args.config, push_snowflake=args.push, dev_mode=dev)
    print(f"\nGRP{args.grp}: {run.status}")
    print(f"  ST: {run.st_files_processed} files | AC: {run.ac_files_processed} files")
    print(f"  Steps: {len(run.steps)} | Errors: {len(run.errors)}")
    if run.end_time and run.start_time:
        print(f"  Duration: {(run.end_time - run.start_time).total_seconds():.1f}s")
    for e in run.errors:
        print(f"  ✗ {e}")


def cmd_run_all(args):
    from engine.orchestrator import run_all_groups
    dbt_dir = args.dbt_dir or os.path.join(os.path.dirname(__file__), 'dbt_project')
    results = run_all_groups(dbt_dir, args.config, args.parallel, args.push, dev_mode=not getattr(args, 'prod', False))
    print(f"\nResults:")
    for grp in sorted(results.keys()):
        run = results[grp]
        icon = '✓' if run.status == 'COMPLETED' else '✗'
        print(f"  {icon} GRP{grp}: {run.status} (ST:{run.st_files_processed} AC:{run.ac_files_processed})")


def cmd_scheduler(args):
    from engine.orchestrator import start_scheduler
    dbt_dir = args.dbt_dir or os.path.join(os.path.dirname(__file__), 'dbt_project')
    start_scheduler(dbt_dir, args.config, args.interval, args.parallel, dev_mode=not getattr(args, 'prod', True))


def cmd_export(args):
    from engine.orchestrator import MasterOrchestrator
    dbt_dir = args.dbt_dir or os.path.join(os.path.dirname(__file__), 'dbt_project')
    orch = MasterOrchestrator(args.grp, dbt_dir, args.config)
    orch.duckdb_path = args.duckdb
    exported = orch.export_parquet(args.output)
    for e in exported:
        print(f"  {e['table']} → {e['path']} ({e['size']:,} bytes)")


def cmd_push(args):
    from engine.orchestrator import MasterOrchestrator
    dbt_dir = args.dbt_dir or os.path.join(os.path.dirname(__file__), 'dbt_project')
    orch = MasterOrchestrator(args.grp, dbt_dir, args.config)
    orch.duckdb_path = args.duckdb
    results = orch.push_to_snowflake()
    for table, result in results.items():
        status = '✓' if result['status'] == 'ok' else '✗'
        print(f"  {status} {table}: {result}")


def cmd_ui(args):
    import subprocess
    ui_path = os.path.join(os.path.dirname(__file__), 'ui', 'app.py')
    subprocess.run([sys.executable, '-m', 'streamlit', 'run', ui_path,
                    '--server.port', str(args.port)])


def main():
    p = argparse.ArgumentParser(description='WU RSP Pipeline')
    sp = p.add_subparsers(dest='command')

    # generate
    g = sp.add_parser('generate', help='Parse .items → dbt project')
    g.add_argument('--input', '-i', required=True)
    g.add_argument('--output', '-o', default='dbt_project')

    # run
    r = sp.add_parser('run', help='Run one group')
    r.add_argument('--grp', '-g', required=True)
    r.add_argument('--config', '-c', default=None)
    r.add_argument('--dbt-dir', '-d', default=None)
    r.add_argument('--push', action='store_true', help='Push to Snowflake after')
    r.add_argument('--prod', action='store_true', help='Production mode (enforce GRPS_TABLE)')

    # run-all
    a = sp.add_parser('run-all', help='Run all 20 groups')
    a.add_argument('--parallel', '-p', type=int, default=4)
    a.add_argument('--config', '-c', default=None)
    a.add_argument('--dbt-dir', '-d', default=None)
    a.add_argument('--push', action='store_true')
    a.add_argument('--prod', action='store_true', help='Production mode')

    # scheduler
    s = sp.add_parser('scheduler', help='Start 30-min scheduler')
    s.add_argument('--interval', type=int, default=30)
    s.add_argument('--parallel', '-p', type=int, default=4)
    s.add_argument('--config', '-c', default=None)
    s.add_argument('--dbt-dir', '-d', default=None)
    s.add_argument('--prod', action='store_true', help='Production mode (default for scheduler)')

    # export
    e = sp.add_parser('export', help='Export DuckDB → Parquet')
    e.add_argument('--grp', '-g', required=True)
    e.add_argument('--duckdb', required=True, help='Path to DuckDB file')
    e.add_argument('--output', '-o', default=None)
    e.add_argument('--config', '-c', default=None)
    e.add_argument('--dbt-dir', '-d', default=None)

    # push
    pu = sp.add_parser('push', help='Push Parquet → Snowflake')
    pu.add_argument('--grp', '-g', required=True)
    pu.add_argument('--duckdb', required=True)
    pu.add_argument('--config', '-c', default=None)
    pu.add_argument('--dbt-dir', '-d', default=None)

    # ui
    u = sp.add_parser('ui', help='Streamlit dashboard')
    u.add_argument('--port', type=int, default=8501)

    args = p.parse_args()
    cmds = {
        'generate': cmd_generate, 'run': cmd_run, 'run-all': cmd_run_all,
        'scheduler': cmd_scheduler, 'export': cmd_export, 'push': cmd_push,
        'ui': cmd_ui,
    }
    if args.command in cmds:
        cmds[args.command](args)
    else:
        p.print_help()


if __name__ == '__main__':
    main()
