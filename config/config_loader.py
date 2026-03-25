"""
Config loader - resolves all 110 context variables for a given group number.
Replicates Talend's context override behavior from TMC.
"""
import yaml
import copy
import os
from datetime import datetime
from pathlib import Path

# Tables where grp_number is appended at runtime (Pattern A)
GRP_APPEND_TABLES = [
    "Entry_stgstfile_grp", "Entry_ACfiletable_grp", "acstmerge_grp",
    "entry_dup_grp", "compliance_check_grp", "compliance_id_sql_grp",
    "AC_Rawdata_grp", "AC_Raw_mainload_grp", "Recordtype4_grp",
    "ST_FILENAMES", "AC_FILENAMES"
]

# Paths where grp_number is appended
GRP_APPEND_PATHS = [
    "processing_st_grp", "processing_ac_grp", "st_parse_path_grp"
]

# Pattern B tables that need full replacement per group
GRP_REPLACE_TABLES = {
    "Entry_tablename": "TXNENTRY_GRP{N}",
    "Adjustment_tablename": "TXNADJUSTMENT_GRP{N}",
    "txnTemplate_main": "TXNTEMPLATE_GRP{N}",
}


def load_config(grp_number: str, config_path: str = None, dev_mode: bool = True) -> dict:
    """Load config for a specific group, resolving all table names and paths."""
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "context_template.yaml")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    cfg = copy.deepcopy(cfg)
    cfg["grp_no"] = str(grp_number)
    cfg["grp_number"] = str(grp_number)

    # Resolve Pattern A: append grp_number
    for key in GRP_APPEND_TABLES:
        if key in cfg:
            cfg[key + "_resolved"] = cfg[key] + str(grp_number)

    # Resolve Pattern B: replace {N}
    for key, pattern in GRP_REPLACE_TABLES.items():
        if dev_mode and key == "Entry_tablename":
            cfg[key] = f"TXNENTRY_GRP{grp_number}_SATEST"
        else:
            cfg[key] = pattern.replace("{N}", str(grp_number))

    # Resolve paths: append grp_number + separator
    for key in GRP_APPEND_PATHS:
        if key in cfg:
            cfg[key + "_resolved"] = cfg[key] + str(grp_number) + os.sep

    # Initialize runtime variables
    cfg["st_cldrn_code"] = None
    cfg["act_cldrn_code"] = None
    cfg["nid_id_update"] = ""
    cfg["batchID"] = ""
    cfg["fileIdentifier"] = ""
    cfg["NETWORKID"] = ""
    cfg["environment"] = ""
    cfg["email_from"] = ""
    cfg["email_to"] = ""
    cfg["email_to_talend_dev"] = ""

    return cfg


def resolve_table(cfg: dict, key: str) -> str:
    """Get the fully resolved table name for a config key."""
    resolved_key = key + "_resolved"
    if resolved_key in cfg:
        return cfg[resolved_key]
    return cfg.get(key, key)


def get_dbt_vars(cfg: dict) -> dict:
    """Convert config to dbt --vars dict for Jinja templating."""
    grp = cfg["grp_number"]
    return {
        "grp_number": grp,
        "grp_no": cfg["grp_no"],
        "entry_stgstfile": cfg.get("Entry_stgstfile_grp_resolved", f"TALENDSTAGTXNENTRYST_GRP{grp}"),
        "entry_acfiletable": cfg.get("Entry_ACfiletable_grp_resolved", f"TXNENTRY_ACFILE_GRP{grp}"),
        "acstmerge": cfg.get("acstmerge_grp_resolved", f"TXNENTRY_DATA_GRP{grp}"),
        "entry_dup": cfg.get("entry_dup_grp_resolved", f"ENTRY_STATUS_UPDATE_GRP{grp}"),
        "recordtype4": cfg.get("Recordtype4_grp_resolved", f"TALENDSTAGTXNENTRYREC4_GRP{grp}"),
        "st_filenames": cfg.get("ST_FILENAMES_resolved", f"STFILE_NAMES_GRP{grp}"),
        "ac_filenames": cfg.get("AC_FILENAMES_resolved", f"ACFILE_NAMES_GRP{grp}"),
        "entry_tablename": cfg["Entry_tablename"],
        "adjustment_tablename": cfg["Adjustment_tablename"],
        "txn_template_main": cfg["txnTemplate_main"],
        "sharedcost_tablename": cfg["Sharedcost_tablename"],
        "adjustment_reason": cfg["ADJUSTMENTREASON_tablename"],
        "adjustment_type": cfg["ADJUSTMENTTYPE_tablename"],
        "txn_country": cfg["txnCountry"],
        "table_noaccount": cfg["Table_noaccount_main"],
        "compliance_check": cfg.get("compliance_check_grp_resolved", f"TALENDSTAGTXNTEMPLATE_CHECK_GRP{grp}"),
        "ac_rawdata": cfg.get("AC_Rawdata_grp_resolved", f"TXNTEMPLATERAW_GRP{grp}"),
    }
