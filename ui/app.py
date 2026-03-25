"""
RSP Pipeline Control Center - Streamlit TMC Replacement
Replaces Talend Management Console for the WU RSP pipeline.
"""
import streamlit as st
import os
import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

st.set_page_config(page_title="RSP Pipeline Control Center", page_icon="🏭", layout="wide")

# State management
if 'runs' not in st.session_state:
    st.session_state.runs = {}
if 'selected_grp' not in st.session_state:
    st.session_state.selected_grp = None


def main():
    st.title("RSP Pipeline Control Center")
    st.caption("UNIFIED_PORTAL — 20 Groups | Replaces Talend TMC")

    # Top action bar
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
        st.metric("Active Runs", sum(1 for r in st.session_state.runs.values() if r.get('status') == 'RUNNING'))
    with col2:
        if st.button("▶ Run All Groups", type="primary"):
            st.info("Starting all 20 groups...")
            # from engine.orchestrator import run_all_groups
            # results = run_all_groups(...)
    with col3:
        if st.button("🔄 Run Failed Only"):
            st.info("Rerunning failed groups...")
    with col4:
        if st.button("⏹ Stop All"):
            st.warning("Stopping all runs...")

    st.divider()

    # Group Status Grid
    st.subheader("Group Status")

    # Create 20-group status table
    group_data = []
    for grp in range(1, 21):
        run = st.session_state.runs.get(grp, {})
        group_data.append({
            'Group': f'GRP{grp}',
            'Status': run.get('status', 'IDLE'),
            'Current Step': run.get('current_step', '—'),
            'ST Files': run.get('st_files', 0),
            'AC Files': run.get('ac_files', 0),
            'Rows': run.get('total_rows', 0),
            'Last Run': run.get('last_run', '—'),
            'Duration': run.get('duration', '—'),
        })

    st.dataframe(
        group_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            'Status': st.column_config.TextColumn(width='small'),
            'Group': st.column_config.TextColumn(width='small'),
        }
    )

    # Group selector for drill-down
    st.divider()
    col_left, col_right = st.columns([1, 3])

    with col_left:
        st.subheader("Actions")
        selected = st.selectbox("Select Group", [f"GRP{i}" for i in range(1, 21)])
        grp_num = selected.replace("GRP", "")

        if st.button(f"▶ Run {selected}"):
            st.success(f"Triggering {selected}...")

        if st.button(f"📋 View Config {selected}"):
            st.session_state.selected_grp = grp_num

        st.divider()
        st.subheader("File Monitor")
        # Show files in source directories
        st_dir = st.text_input("ST_File dir", value="D:\\data\\Projects\\UNIFIEDPORTAL\\Source\\ST_File\\")
        ac_dir = st.text_input("AC_Target dir", value="D:\\data\\Projects\\UNIFIEDPORTAL\\Source\\AC_Target\\")

        if os.path.isdir(st_dir):
            st_files = [f for f in os.listdir(st_dir) if f.endswith('.txt') or f.endswith('.ST')]
            st.write(f"**ST files:** {len(st_files)}")
            for f in st_files[:5]:
                st.code(f, language=None)
        if os.path.isdir(ac_dir):
            ac_files = [f for f in os.listdir(ac_dir) if 'ACTIV' in f.upper()]
            st.write(f"**AC files:** {len(ac_files)}")
            for f in ac_files[:5]:
                st.code(f, language=None)

    with col_right:
        st.subheader(f"Run Detail — {selected}")

        run = st.session_state.runs.get(int(grp_num), {})
        steps = run.get('steps', [])

        if not steps:
            st.info(f"No run history for {selected}. Click 'Run' to start.")

            # Show the expected pipeline steps
            st.write("**Expected Pipeline Steps:**")
            expected = [
                ("1", "PREJOB", "Open Snowflake + load env/email config"),
                ("2", "SORT_FILES_COPY", "Scan ST_File/ + AC_Target/, route by NetworkID"),
                ("3", "MAIN_LOOP x15", "Process 1 ST + 1 AC file per iteration in parallel"),
                ("4", "AC_DEDUP_INSERT", "tDBRow_1: INSERT duplicates into entry_dup_grp"),
                ("5", "ADJ_STS_UPDATE", "STATUS W→S correction on staging"),
                ("6", "UPSERT_2_CASES", "10 parallel tDBRows: staging → production UPDATE"),
                ("7", "TXN_TEMPLATE_LINK", "tDBRow_4: ENTRY_ID linkage"),
                ("8", "UPSERT_3RD_CASE", "Unmatched records → acstmerge_grp"),
                ("9", "FINAL_LOAD", "REP MERGE → net-zero → INSERT → sign-flip → TRUNCATE"),
                ("10", "POSTJOB", "File archival + cleanup"),
                ("—", "STEP 2: POST JOB", "STS_UPDATE → PAYDATE_UPDATE → AC_STATUS_UPDATE"),
            ]
            for num, name, desc in expected:
                st.write(f"**{num}.** `{name}` — {desc}")
        else:
            for step in steps:
                status = step.get('status', '')
                icon = '✅' if status == 'COMPLETED' else '❌' if status == 'ERROR' else '🔄'
                with st.expander(f"{icon} {step['step']} — {status}", expanded=(status == 'ERROR')):
                    st.write(f"Rows in: {step.get('rows_in', 0)} | Rows out: {step.get('rows_out', 0)}")
                    st.write(f"Time: {step.get('timestamp', '')}")
                    if step.get('error'):
                        st.error(step['error'])
                    if status == 'ERROR':
                        if st.button(f"🔄 Rerun from {step['step']}", key=f"rerun_{step['step']}"):
                            st.info(f"Rerunning from {step['step']}...")

    # Generator section
    st.divider()
    st.subheader("🔧 dbt Project Generator")
    st.write("Generate the dbt project from Talend .item XML files.")

    gcol1, gcol2 = st.columns(2)
    with gcol1:
        items_path = st.text_input(
            "Path to .item files",
            value=r"D:\Users\C0007703\OneDrive - Western Union\Documents\rsp-dbt-migration-knwldgebase\UNIFIEDPORTAL"
        )
    with gcol2:
        output_path = st.text_input(
            "Output dbt project",
            value=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dbt_project')
        )

    if st.button("🚀 Generate dbt Project", type="primary"):
        with st.spinner("Parsing .item files and generating dbt models..."):
            try:
                from generator.generate import DBTProjectGenerator
                gen = DBTProjectGenerator(items_path, output_path)
                gen.generate()
                st.success(f"Generated {len(gen.generated_models)} dbt models!")

                # Show manifest
                manifest_path = os.path.join(output_path, 'generation_manifest.json')
                if os.path.exists(manifest_path):
                    with open(manifest_path) as f:
                        manifest = json.load(f)
                    st.json(manifest)
            except Exception as e:
                st.error(f"Generation failed: {e}")


if __name__ == '__main__':
    main()
