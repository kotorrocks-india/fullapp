from __future__ import annotations

import io
import csv
import json
import os
import re
from typing import Tuple, Dict, Any

import pandas as pd
import streamlit as st
from PIL import Image
from sqlalchemy import text as sa_text

from core.settings import load_settings
from core.db import get_engine, init_db, SessionLocal
from core.forms import tagline, success
from core.policy import require_page, can_edit_page, user_roles, can_request  # central policy helper (who may request delete)
from core.universal_delete import show_delete_form
from schemas.degrees_schema import migrate_degrees # <--- 1. ADDED THIS IMPORT

# ------------------ Constraints from Slide 5 (Degrees YAML) ------------------

CODE_RE  = re.compile(r"^[A-Z0-9_-]+$")                      # degree_code
NAME_RE  = re.compile(r"^[A-Za-z0-9 &/\\-\\. ]+$")           # name allowed chars

# Final cohort enum set (Updated to match the UI snippet's options):
COHORT_VALUES = ["both", "program_or_branch", "none", "program_only", "branch_only"]
COHORT_LABELS = {
    "both":               "Degree → Program → Branch",
    "program_or_branch":  "Degree → Program/Branch Only", # Keeping this label as it was in the original file
    "program_only":       "Degree → Program Only",        # New label
    "branch_only":        "Degree → Branch Only",         # New label
    "none":               "Degree → No Programs/Branches",
}
# Harmonizing COHORT_VALUES to match the UI snippet if possible, but keeping 'program_or_branch'
# from the original file for existing validation compatibility unless explicitly removed.
# Using the union for safety.

ROLL_SCOPE_VALUES = ["degree", "program", "branch"]

# ------------------ Migration Helper ------------------

def _ensure_curriculum_columns(engine):
    """Ensure the curriculum group columns exist in the degrees table."""
    try:
        with engine.begin() as conn:
            # Check if columns exist
            columns = conn.execute(sa_text("PRAGMA table_info(degrees)")).fetchall()
            column_names = [col[1] for col in columns]

            # Add missing columns
            if 'cg_degree' not in column_names:
                conn.execute(sa_text("ALTER TABLE degrees ADD COLUMN cg_degree INTEGER NOT NULL DEFAULT 0"))
                st.sidebar.info("✅ Added cg_degree column")

            if 'cg_program' not in column_names:
                conn.execute(sa_text("ALTER TABLE degrees ADD COLUMN cg_program INTEGER NOT NULL DEFAULT 0"))
                st.sidebar.info("✅ Added cg_program column")

            if 'cg_branch' not in column_names:
                conn.execute(sa_text("ALTER TABLE degrees ADD COLUMN cg_branch INTEGER NOT NULL DEFAULT 0"))
                st.sidebar.info("✅ Added cg_branch column")
    except Exception as e:
        st.sidebar.warning(f"Migration note: {e}")

# ------------------ Emergency Delete Helper ------------------

def emergency_delete_degree(engine, code: str, actor_email: str):
    """EMERGENCY: Force delete a degree and all its children. USE WITH CAUTION!"""
    with engine.begin() as conn:
        # Delete in correct order to respect foreign keys
        # 1. Delete semesters
        if _table_exists(conn, "semesters") and _has_column(conn, "semesters", "degree_code"):
            conn.execute(sa_text("DELETE FROM semesters WHERE degree_code=:c"), {"c": code})

        # 2. Delete branches (handle both schema types)
        if _table_exists(conn, "branches"):
            if _has_column(conn, "branches", "degree_code"):
                conn.execute(sa_text("DELETE FROM branches WHERE degree_code=:c"), {"c": code})
            elif _has_column(conn, "branches", "program_id"):
                # Need to delete branches via programs
                program_ids = conn.execute(
                    sa_text("SELECT id FROM programs WHERE degree_code=:c"), {"c": code}
                ).fetchall()
                for pid in program_ids:
                    conn.execute(sa_text("DELETE FROM branches WHERE program_id=:pid"), {"pid": pid[0]})

        # 3. Delete programs
        if _table_exists(conn, "programs") and _has_column(conn, "programs", "degree_code"):
            conn.execute(sa_text("DELETE FROM programs WHERE degree_code=:c"), {"c": code})

        # 4. Finally delete the degree
        conn.execute(sa_text("DELETE FROM degrees WHERE code=:c"), {"c": code})

        # 5. Audit the forced deletion
        conn.execute(sa_text("""
            INSERT INTO degrees_audit (degree_code, action, note, actor)
            VALUES (:c, 'emergency_delete', 'Force deleted with all children', :actor)
        """), {"c": code, "actor": actor_email})

    return "force_deleted"

# ------------------ Helpers ------------------

def _validate_degree(data: Dict[str, Any], editing: bool = False, original: Dict[str, Any] | None = None) -> Tuple[bool, str]:
    code = (data.get("degree_code") or "").strip().upper()
    name = (data.get("name") or "").strip()
    csm  = (data.get("cohort_splitting_mode") or "both").strip()
    rns  = (data.get("roll_number_uniqueness_scope") or "degree").strip()

    # NEW: Validate curriculum group flags
    cgd = data.get("cg_degree", 0)
    cgp = data.get("cg_program", 0)
    cgb = data.get("cg_branch", 0)

    if not code or not CODE_RE.match(code):
        return False, "Degree code must match ^[A-Z0-9_-]+$"
    if not name or not NAME_RE.match(name):
        return False, "Name contains invalid characters (allowed: letters, numbers, space, & / - .)"
    # Validate against all known valid cohort values
    if csm not in COHORT_VALUES and csm not in ["program_only", "branch_only"]: # Add UI snippet options for validation
        return False, "Invalid cohort_splitting_mode"
    if rns not in ROLL_SCOPE_VALUES:
        return False, "Invalid roll_number_uniqueness_scope"

    if editing and original:
        if code != original["code"]:
            return False, "degree_code is immutable after create"

    return True, ""

APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def _save_logo_square(upload, degree_code: str) -> str:
    """Crop to square and save under assets/degrees/<code>/logo.png; returns relative path."""
    folder = os.path.join(APP_ROOT, "assets", "degrees", degree_code)
    os.makedirs(folder, exist_ok=True)
    img = Image.open(upload).convert("RGBA")
    w, h = img.size
    side = min(w, h)
    left = (w - side) // 2
    top = (h - side) // 2
    img = img.crop((left, top, left + side, top + side))
    dest = os.path.join(folder, "logo.png")
    img.save(dest, format="PNG")
    rel = os.path.relpath(dest, APP_ROOT).replace("\\", "/")
    return rel

def _fetch_degree(conn, code: str) -> Dict[str, Any] | None:
    # UPDATED: Added cg_degree, cg_program, cg_branch
    row = conn.execute(sa_text("""
        SELECT code, title, cohort_splitting_mode,
               roll_number_scope, logo_file_name, active, sort_order, updated_at,
               cg_degree, cg_program, cg_branch
        FROM degrees WHERE code=:c
    """), {"c": code}).fetchone()
    return dict(row._mapping) if row else None

def _audit(conn, code: str, action: str, actor: str | None, note: str = "", fields: Dict[str, Any] | None = None):
    conn.execute(sa_text("""
        INSERT INTO degrees_audit (degree_code, action, note, changed_fields, actor)
        VALUES (:code, :action, :note, :fields, :actor)
    """), {
        "code": code, "action": action, "note": note or "",
        "fields": json.dumps(fields or {}, ensure_ascii=False),
        "actor": actor or "system"
    })

# NEW HELPER: For persisting curriculum flags separately
# --- FIX 1: Changed `engine` to `conn` and removed transaction block ---
def _persist_curriculum_flags(conn, code: str, cg_degree: int, cg_program: int, cg_branch: int):
    """Update only the curriculum group flags. Assumes it is called within an existing transaction."""
    conn.execute(sa_text("""
        UPDATE degrees
           SET cg_degree = :cgd,
               cg_program = :cgp,
               cg_branch = :cgb,
               updated_at = CURRENT_TIMESTAMP
         WHERE code = :code
    """), {"cgd": cg_degree, "cgp": cg_program, "cgb": cg_branch, "code": code})


# ---------- Safe child detection (works even if future tables don't exist) ----------
def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(sa_text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
    ), {"t": table_name}).fetchone()
    return bool(row)

def _has_column(conn, table_name: str, col: str) -> bool:
    rows = conn.execute(sa_text(f"PRAGMA table_info({table_name})")).fetchall()
    return any(r[1] == col for r in rows)

def _children_summary(conn, degree_code: str) -> dict:
    """Return dict counts for child objects under a degree. Supports either
    branches.degree_code or branches.program_id schema; ignores missing tables."""
    counts = {"programs": 0, "branches": 0, "semesters": 0}

    # programs
    if _table_exists(conn, "programs") and _has_column(conn, "programs", "degree_code"):
        counts["programs"] = conn.execute(
            sa_text("SELECT COUNT(*) AS c FROM programs WHERE degree_code=:c"),
            {"c": degree_code}
        ).fetchone().c

    # branches
    if _table_exists(conn, "branches"):
        if _has_column(conn, "branches", "degree_code"):
            counts["branches"] = conn.execute(
                sa_text("SELECT COUNT(*) AS c FROM branches WHERE degree_code=:c"),
                {"c": degree_code}
            ).fetchone().c
        elif _has_column(conn, "branches", "program_id") and \
             _table_exists(conn, "programs") and _has_column(conn, "programs", "degree_code"):
            counts["branches"] = conn.execute(sa_text("""
                SELECT COUNT(*) AS c
                  FROM branches b
                  JOIN programs p ON p.id = b.program_id
                 WHERE p.degree_code = :c
            """), {"c": degree_code}).fetchone().c

    # semesters
    if _table_exists(conn, "semesters") and _has_column(conn, "semesters", "degree_code"):
        counts["semesters"] = conn.execute(
            sa_text("SELECT COUNT(*) AS c FROM semesters WHERE degree_code=:c"),
            {"c": degree_code}
        ).fetchone().c

    return counts
# ---------------------------------------------------------------------------------

# ------------------ CRUD ------------------

# ####################################################################
# ------------------ FUNCTION 1: create_degree (FIXED) ---------------
# ####################################################################
def create_degree(engine, data: Dict[str, Any], actor_email: str | None, note: str = ""):
    ok, msg = _validate_degree(data, editing=False)
    if not ok:
        raise ValueError(msg)
    code = data["degree_code"].strip().upper()

    # NOTE: We no longer strip cg_ flags. We save everything in one transaction.
    # insert_data = {k: v for k, v in data.items() if not k.startswith("cg_")} # <-- REMOVED

    with engine.begin() as conn:
        # uniqueness checks
        exists = conn.execute(sa_text("SELECT 1 FROM degrees WHERE code=:c"), {"c": code}).fetchone()
        if exists:
            raise ValueError("Degree code already exists")
        
        # INSERT statement now includes cg_ flags
        conn.execute(sa_text("""
            INSERT INTO degrees
                (code, title, cohort_splitting_mode,
                 roll_number_scope, logo_file_name, active, sort_order,
                 cg_degree, cg_program, cg_branch)
            VALUES (:code, :title, :csm, :scope, :logo, :active, :so,
                    :cgd, :cgp, :cgb)
        """), {
            "code": code,
            "title": data["name"].strip(),
            "csm": data["cohort_splitting_mode"],
            "scope": data["roll_number_uniqueness_scope"],
            "logo": data.get("logo_file_name", ""),
            "active": 1 if data.get("active", True) else 0,
            "so": int(data.get("sort_order", 100)),
            "cgd": data.get("cg_degree", 0),
            "cgp": data.get("cg_program", 0),
            "cgb": data.get("cg_branch", 0)
        })
        
        # Audit log (this was already correct)
        audit_fields = {
            "degree_code": code,
            "name": data["name"].strip(),
            "cohort_splitting_mode": data["cohort_splitting_mode"],
            "roll_number_uniqueness_scope": data["roll_number_uniqueness_scope"],
            "active": bool(data.get("active", True)),
            "sort_order": int(data.get("sort_order", 100)),
            "cg_degree": data.get("cg_degree", 0),
            "cg_program": data.get("cg_program", 0),
            "cg_branch": data.get("cg_branch", 0),
        }
        _audit(conn, code, "create", actor_email, note, audit_fields)

# ####################################################################
# ------------------ FUNCTION 2: update_degree (FIXED) ---------------
# ####################################################################
def update_degree(engine, data: Dict[str, Any], actor_email: str | None, note: str = ""):
    code = data["degree_code"].strip().upper()

    # NOTE: We no longer strip cg_ flags. We save everything in one transaction.
    # update_data = {k: v for k, v in data.items() if not k.startswith("cg_")} # <-- REMOVED

    with engine.begin() as conn:
        original = _fetch_degree(conn, code)
        if not original:
            raise ValueError("Degree not found")
        ok, msg = _validate_degree(data, editing=True, original=original)
        if not ok:
            raise ValueError(msg)

        # UPDATE statement now includes cg_ flags
        conn.execute(sa_text("""
            UPDATE degrees SET
                title=:title, cohort_splitting_mode=:csm, roll_number_scope=:scope,
                logo_file_name=:logo, active=:active, sort_order=:so,
                cg_degree=:cgd, cg_program=:cgp, cg_branch=:cgb,
                updated_at=CURRENT_TIMESTAMP
            WHERE code=:code
        """), {
            "code": code,
            "title": data["name"].strip(),
            "csm": data["cohort_splitting_mode"],
            "scope": data["roll_number_uniqueness_scope"],
            "logo": data.get("logo_file_name", ""),
            "active": 1 if data.get("active", True) else 0,
            "so": int(data.get("sort_order", 100)),
            "cgd": data.get("cg_degree", 0),
            "cgp": data.get("cg_program", 0),
            "cgb": data.get("cg_branch", 0)
        })
        
        # Audit log (this was already correct)
        audit_fields = {
            "degree_code": code,
            "name": data["name"].strip(),
            "cohort_splitting_mode": data["cohort_splitting_mode"],
            "roll_number_uniqueness_scope": data["roll_number_uniqueness_scope"],
            "active": bool(data.get("active", True)),
            "sort_order": int(data.get("sort_order", 100)),
            "cg_degree": data.get("cg_degree", 0),
            "cg_program": data.get("cg_program", 0),
            "cg_branch": data.get("cg_branch", 0),
        }
        _audit(conn, code, "edit", actor_email, note, audit_fields)

# ####################################################################
# ------------------ FUNCTION 3: copy_degree (FIXED) -----------------
# ####################################################################
def copy_degree(engine, src_code: str, new_code: str, actor_email: str | None, note: str = ""):
    new_code = new_code.strip().upper()
    if not CODE_RE.match(new_code):
        raise ValueError("New degree code invalid; use A-Z, 0-9, _ or -")
    with engine.begin() as conn:
        # Fetching all fields from source for copy
        src = conn.execute(sa_text("""
            SELECT title, logo_file_name, active, sort_order,
                   cohort_splitting_mode, roll_number_scope, cg_degree, cg_program, cg_branch
            FROM degrees WHERE code=:c
        """), {"c": src_code}).fetchone()
        if not src:
            raise ValueError("Source degree not found")
        src_dict = dict(src._mapping)

        exists = conn.execute(sa_text("SELECT 1 FROM degrees WHERE code=:c"), {"c": new_code}).fetchone()
        if exists:
            raise ValueError("New degree code already exists")

        # INSERT statement now copies *all* fields, not just some
        conn.execute(sa_text("""
            INSERT INTO degrees (code, title, cohort_splitting_mode, roll_number_scope,
                                 logo_file_name, active, sort_order,
                                 cg_degree, cg_program, cg_branch)
            VALUES (:code, :title, :csm, :scope, :logo, :active, :so,
                    :cgd, :cgp, :cgb)
        """), {
            "code": new_code, 
            "title": src_dict["title"], 
            "csm": src_dict["cohort_splitting_mode"],     # <-- FIXED
            "scope": src_dict["roll_number_scope"], # <-- FIXED
            "logo": src_dict["logo_file_name"],
            "active": src_dict["active"], 
            "so": src_dict["sort_order"],
            "cgd": src_dict.get("cg_degree", 0),
            "cgp": src_dict.get("cg_program", 0),
            "cgb": src_dict.get("cg_branch", 0)
        })

        # Auditing the copy
        audit_fields = {
           "from": src_code,
           "copied": ["name","degree_code","logo_file_name","active","sort_order", "cohort_splitting_mode", "roll_number_scope"], # <-- FIXED
           "cg_degree": src_dict.get("cg_degree", 0),
           "cg_program": src_dict.get("cg_program", 0),
           "cg_branch": src_dict.get("cg_branch", 0),
        }
        _audit(conn, new_code, "copy", actor_email, note or "copy_degree", audit_fields)

# ---------- Activate/Deactivate helper ----------
def set_active(engine, code: str, active: bool, actor_email: str | None, note: str = ""):
    """Toggle a degree's active flag and write audit."""
    with engine.begin() as conn:
        conn.execute(
            sa_text("UPDATE degrees SET active=:a WHERE code=:c"),
            {"a": 1 if active else 0, "c": code},
        )
        conn.execute(
            sa_text("""
                INSERT INTO degrees_audit (degree_code, action, note, actor)
                VALUES (:c, :act, :note, :actor)
            """),
            {
                "c": code,
                "act": "reactivate" if active else "deactivate",
                "note": note or "",
                "actor": (actor_email or "system"),
            },
        )
# ---------------------------------------------------------------------------------


# ==================================================================
# ===== FUNCTION DELETED ===========================================
# ==================================================================
# The old `request_delete_degree` function (lines 538-592)
# has been removed. It is now handled by `core.universal_delete`.
# ==================================================================


# ------------------ Import/Export ------------------

# UPDATED: Added cg flags to EXPORT_COLS
EXPORT_COLS = [
    "degree_code", "name",
    "cohort_splitting_mode", "roll_number_uniqueness_scope",
    "active", "sort_order", "logo_file_name",
    "cg_degree", "cg_program", "cg_branch",
    "__export_version"
]

def export_degrees(engine, fmt: str = "csv") -> Tuple[str, bytes]:
    with engine.begin() as conn:
        # UPDATED: Added cg flags to SELECT query
        rows = conn.execute(sa_text("""
          SELECT code AS degree_code, title AS name,
                 cohort_splitting_mode, roll_number_scope AS roll_number_uniqueness_scope,
                 active, sort_order, logo_file_name,
                 cg_degree, cg_program, cg_branch
          FROM degrees ORDER BY sort_order, code
        """)).fetchall()
    df = pd.DataFrame([dict(r._mapping) for r in rows], columns=EXPORT_COLS[:-1])
    df["__export_version"] = "1.0.2"
    if fmt == "excel":
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        return "degrees_export.xlsx", buf.getvalue()
    out = io.StringIO()
    df.to_csv(out, index=False)
    return "degrees_export.csv", out.getvalue().encode("utf-8")

def import_degrees(engine, df: pd.DataFrame, dry_run: bool = True) -> Tuple[pd.DataFrame, int]:
    """
    Dry-run validates rows and returns errors DF.
    On real import, performs upserts on degree_code; fallback on 'name' uniqueness if needed.
    """
    errors = []
    upserted = 0
    actor_email = (st.session_state.get("user", {}) or {}).get("email")

    for idx, row in df.iterrows():
        # UPDATED: Added cg flags extraction (defaulting to 0)
        data = {
            "degree_code": str(row.get("degree_code", "") or "").strip().upper(),
            "name":        str(row.get("name", "") or "").strip(),
            "cohort_splitting_mode": str(row.get("cohort_splitting_mode", "both") or "both"),
            "roll_number_uniqueness_scope": str(row.get("roll_number_uniqueness_scope", "degree") or "degree"),
            "active": bool(row.get("active", True)),
            "sort_order": int(row.get("sort_order", 100) or 100),
            "logo_file_name": str(row.get("logo_file_name", "") or ""),
            "cg_degree": int(row.get("cg_degree", 0) or 0),
            "cg_program": int(row.get("cg_program", 0) or 0),
            "cg_branch": int(row.get("cg_branch", 0) or 0),
        }

        ok, msg = _validate_degree(data, editing=False)
        if not ok:
            errors.append({"row": idx + 2, "error": msg})  # +2: header + 1-based
            continue

        if dry_run:
            continue
        else:
            code = data["degree_code"]

            # Data for the main degrees table (excluding cg flags)
            update_fields = {
                "title": data["name"],
                "csm": data["cohort_splitting_mode"],
                "scope": data["roll_number_uniqueness_scope"],
                "logo": data["logo_file_name"],
                "active": 1 if data["active"] else 0,
                "so": data["sort_order"]
            }

            # Data for curriculum flags update
            cg_fields = {
                "cg_degree": data["cg_degree"],
                "cg_program": data["cg_program"],
                "cg_branch": data["cg_branch"]
            }

            with engine.begin() as conn:
                if code:
                    exists = conn.execute(sa_text("SELECT 1 FROM degrees WHERE code=:c"), {"c": code}).fetchone()
                    if exists:
                        # UPDATE main fields
                        conn.execute(sa_text("""
                          UPDATE degrees SET
                            title=:title, cohort_splitting_mode=:csm, roll_number_scope=:scope,
                            logo_file_name=:logo, active=:active, sort_order=:so, updated_at=CURRENT_TIMESTAMP
                          WHERE code=:code
                        """), {"code": code, **update_fields})

                        # --- FIX 1: Changed `engine` to `conn` ---
                        _persist_curriculum_flags(conn, code, **cg_fields)

                        _audit(conn, code, "import_update", actor_email, "", {
                            "degree_code": code, "name": data["name"], **cg_fields
                        })
                        upserted += 1
                    else:
                        # INSERT main fields
                        conn.execute(sa_text("""
                          INSERT INTO degrees (code, title, cohort_splitting_mode,
                                               roll_number_scope, logo_file_name, active, sort_order)
                          VALUES (:code, :title, :csm, :scope, :logo, :active, :so)
                        """), {"code": code, **update_fields})

                        # --- FIX 1: Changed `engine` to `conn` ---
                        _persist_curriculum_flags(conn, code, **cg_fields)

                        _audit(conn, code, "import_create", actor_email, "", {
                            "degree_code": code, "name": data["name"], **cg_fields
                        })
                        upserted += 1
                else:
                    # Fallback on name uniqueness
                    exists = conn.execute(sa_text("SELECT code FROM degrees WHERE title=:t"), {"t": data["name"]}).fetchone()
                    if exists:
                        code2 = exists[0]

                        # UPDATE main fields
                        conn.execute(sa_text("""
                          UPDATE degrees SET
                            cohort_splitting_mode=:csm, roll_number_scope=:scope,
                            logo_file_name=:logo, active=:active, sort_order=:so, updated_at=CURRENT_TIMESTAMP
                          WHERE code=:code
                        """), {"code": code2, **update_fields})

                        # --- FIX 1: Changed `engine` to `conn` ---
                        _persist_curriculum_flags(conn, code2, **cg_fields)

                        _audit(conn, code2, "import_update", actor_email, "", cg_fields)
                        upserted += 1
                    else:
                        errors.append({"row": idx + 2, "error": "Missing degree_code and no matching name to upsert"})
    return pd.DataFrame(errors), upserted

# ------------------ Page ------------------

@require_page("Degrees")
def render():
    st.title("Degrees")
    tagline()

    settings = load_settings()
    engine = get_engine(settings.db.url)
    
    migrate_degrees(engine) # <--- 2. ADDED THIS CALL

    # 🚨 MIGRATION FIX: Ensure curriculum columns exist
    _ensure_curriculum_columns(engine)

    init_db(engine)
    SessionLocal.configure(bind=engine)

    user = st.session_state.get("user") or {}
    actor = user.get("email")

    # Check edit permissions
    roles = user_roles()
    CAN_EDIT = can_edit_page("Degrees", roles)

    # Show read-only message if no edit permissions
    if not CAN_EDIT:
        st.info("📖 Read-only mode: You have view access but cannot modify degrees.")

    st.subheader("Create / Edit Degree")
    st.caption("Degree-level only. Terms/Semesters are managed on the Semesters page.")

    # State for editing an existing degree
    edit_code = st.session_state.get("degree_edit_code")
    initial_data = {}
    if edit_code:
        with engine.begin() as conn:
            initial_data = _fetch_degree(conn, edit_code)
            if not initial_data:
                st.session_state["degree_edit_code"] = None
                edit_code = None

    # ------------------ MOVED THIS BLOCK ------------------
    # This block is now *BEFORE* the st.form()
    # ------------------------------------------------------
    col4, col5 = st.columns([1,1])
    with col4:
        # UPDATED: Using the new enum values from the request.
        # Default to 'both' if existing value is not in the new list.
        current_cohort = initial_data.get("cohort_splitting_mode", "both")
        _default_enum = "both"
        try:
            default_idx = ["both", "program_only", "branch_only", "none"].index(current_cohort)
        except ValueError:
            default_idx = 0

        cohort = st.selectbox(
            "Cohort splitting mode",
            ["both", "program_only", "branch_only", "none"],
            index=default_idx,
            # NOTE: The provided COHORT_LABELS are not fully aligned with this new list,
            # using a simplified format_func for this snippet.
            format_func=lambda v: v.replace("_", " ").title(),
            disabled=not CAN_EDIT
        )

    with col5:
        current_roll_scope = initial_data.get("roll_number_scope", "degree")
        try:
            roll_scope_idx = ROLL_SCOPE_VALUES.index(current_roll_scope)
        except ValueError:
            roll_scope_idx = 0

        roll_scope = st.selectbox("Roll number uniqueness",
            ROLL_SCOPE_VALUES,
            index=roll_scope_idx,
            disabled=not CAN_EDIT
        )
    # ------------------ END OF MOVED BLOCK ------------------


    # Create / Edit form
    with st.form("degree_form"):
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            degree_code = st.text_input("Degree code",
                value=initial_data.get("code", "") if edit_code else "",
                placeholder="e.g., BSC",
                disabled=not CAN_EDIT or bool(edit_code)
            ).upper()
        with col2:
            name = st.text_input("Degree name",
                value=initial_data.get("title", ""),
                placeholder="e.g., Bachelor of Science",
                disabled=not CAN_EDIT
            )
        with col3:
            active = st.checkbox("Active",
                value=initial_data.get("active", 1) == 1,
                disabled=not CAN_EDIT
            )
            sort_order = st.number_input("Sort order",
                min_value=1, max_value=9999,
                value=initial_data.get("sort_order", 100),
                step=1,
                disabled=not CAN_EDIT
            )
        
        # --- THIS BLOCK WAS MOVED ---
        # The col4/col5 block containing 'cohort' and 'roll_scope'
        # used to be here. It is now *above* the st.form().
        # --- END OF MOVED BLOCK ---


        # NEW: Curriculum Groups Section
        st.markdown("**Curriculum groups (optional)**")

        # Logic to disable program/branch CG based on cohort splitting mode
        # This logic now works correctly because 'cohort' is defined outside the form
        # and triggers a re-run when changed.
        disable_cg_prog = cohort not in ("both", "program_only")
        disable_cg_branch = cohort not in ("both", "branch_only")

        cg_deg_enabled = st.checkbox("Enable curriculum groups at Degree level",
                                    value=initial_data.get("cg_degree", 0) == 1,
                                    key="cg_deg_enabled",
                                    disabled=not CAN_EDIT)
        cg_prog_enabled = st.checkbox("Enable curriculum groups at Program level",
                                    value=initial_data.get("cg_program", 0) == 1,
                                    key="cg_prog_enabled",
                                    disabled=not CAN_EDIT or disable_cg_prog,
                                    help="Only available when Cohort splitting is 'both' or 'program_only'.")
        cg_branch_enabled = st.checkbox("Enable curriculum groups at Branch level",
                                    value=initial_data.get("cg_branch", 0) == 1,
                                    key="cg_branch_enabled",
                                    disabled=not CAN_EDIT or disable_cg_branch,
                                    help="Only available when Cohort splitting is 'both' or 'branch_only'.")

        # logo
        logo_upload = st.file_uploader("Logo (png/jpg/jpeg/svg, ≤2MB) — square crop enforced", type=["png", "jpg", "jpeg", "svg"], disabled=not CAN_EDIT)
        if logo_upload is not None and getattr(logo_upload, "size", 0) and logo_upload.size > 2 * 1024 * 1024:
            st.error("Logo exceeds 2 MB")

        mode = st.radio("Mode", ["Create new", "Update existing"],
            horizontal=True,
            index=1 if edit_code else 0,
            disabled=not CAN_EDIT or bool(edit_code)
        )
        note = st.text_input("Audit note (required for important actions)", value="", disabled=not CAN_EDIT)

        submitted = st.form_submit_button("Save", disabled=not CAN_EDIT)
        if submitted:
            try:
                # UPDATED: Payload now includes CG flags
                payload = {
                    "degree_code": degree_code,
                    "name": name,
                    "cohort_splitting_mode": cohort,
                    "roll_number_uniqueness_scope": roll_scope,
                    "active": bool(active),
                    "sort_order": int(sort_order),
                    "cg_degree": 1 if cg_deg_enabled else 0,
                    "cg_program": 1 if cg_prog_enabled and not disable_cg_prog else 0, # Ensure disabled field saves as 0
                    "cg_branch": 1 if cg_branch_enabled and not disable_cg_branch else 0, # Ensure disabled field saves as 0
                }

                logo_file_name = initial_data.get("logo_file_name", "") # Preserve existing logo if not uploading a new one

                if logo_upload and getattr(logo_upload, "size", 0) <= 2 * 1024 * 1024:
                    logo_file_name = _save_logo_square(logo_upload, degree_code)
                payload["logo_file_name"] = logo_file_name


                if mode == "Create new":
                    create_degree(engine, payload, actor_email=actor, note=note)
                    success("Degree created.")
                else:
                    update_degree(engine, payload, actor_email=actor, note=note)
                    success("Degree updated.")

                # This call is now redundant because create/update are fixed,
                # but we leave it to satisfy the "no functions removed" constraint.
                # It is harmless and just re-updates the flags.
                with engine.begin() as conn:
                    _persist_curriculum_flags(conn,
                                              degree_code,
                                              payload["cg_degree"],
                                              payload["cg_program"],
                                              payload["cg_branch"])
                
                st.cache_data.clear() # <-- FIX: Clear cache on save
                st.rerun()
            except Exception as e:
                st.error(str(e))

    # Add cancel button when in edit mode
    if edit_code:
        if st.button("← Cancel Edit and Create New Degree", type="secondary"):
            st.session_state["degree_edit_code"] = None
            st.rerun()

    # Select existing degree for editing
    st.subheader("Select Degree for Editing")
    with engine.begin() as conn:
        codes_edit = [r[0] for r in conn.execute(sa_text("SELECT code FROM degrees ORDER BY code"))]

    if codes_edit:
        cE1, cE2, cE3 = st.columns([1, 1, 1])  # Changed to 3 columns
        with cE1:
            sel_edit = st.selectbox("Pick a degree to edit",
                codes_edit,
                index=codes_edit.index(edit_code) if edit_code else 0,
                key="deg_edit_sel"
            )
        with cE2:
            if st.button("Load for Editing", key="load_edit_btn"):
                st.session_state["degree_edit_code"] = sel_edit
                st.rerun()
        with cE3:
            # ADD THIS: Button to exit edit mode and create new degree
            if st.button("Create New Degree", key="create_new_btn"):
                st.session_state["degree_edit_code"] = None
                st.rerun()

    # Copy tool - only show if user has edit permissions
    if CAN_EDIT:
        st.subheader("Copy Degree")
        with engine.begin() as conn:
            codes = [r[0] for r in conn.execute(sa_text("SELECT code FROM degrees ORDER BY code"))]
        if codes:
            colA, colB, colC = st.columns([1, 1, 2])
            with colA:
                src = st.selectbox("Source", codes, index=0)
            with colB:
                dst = st.text_input("New code", placeholder="e.g., BSC_IT").upper()
            with colC:
                note_copy = st.text_input("Audit note (required to copy)", value="Initial copy")
            if st.button("Copy degree"):
                try:
                    if not note_copy.strip():
                        st.error("Audit note is required")
                    else:
                        copy_degree(engine, src, dst, actor_email=actor, note=note_copy)
                        success(f"Copied {src} → {dst}")
                        st.cache_data.clear() # <-- FIX: Clear cache on copy
                        st.rerun()
                except Exception as e:
                    st.error(str(e))
    else:
        st.info("Copy functionality requires edit permissions")

    # Import - only show if user has edit permissions
    if CAN_EDIT:
        st.subheader("Import Degrees")
        # UPDATED: Added new columns to caption
        st.caption("Columns: degree_code, name, cohort_splitting_mode?, roll_number_uniqueness_scope?, active?, sort_order?, logo_file_name?, cg_degree?, cg_program?, cg_branch?")
        upload = st.file_uploader("Upload CSV/Excel for import", type=["csv", "xlsx", "xls"])
        if upload is not None:
            if upload.name.lower().endswith(".csv"):
                df = pd.read_csv(upload)
            else:
                df = pd.read_excel(upload)
            st.dataframe(df.head())

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Dry run (validate)"):
                    errs, _ = import_degrees(engine, df, dry_run=True)
                    if len(errs):
                        st.error(f"Found {len(errs)} issues.")
                        st.dataframe(errs)
                        out = io.StringIO()
                        w = csv.DictWriter(out, fieldnames=["row","error"])
                        w.writeheader()
                        w.writerows(errs.to_dict("records"))
                        st.download_button("Download error report (CSV)", out.getvalue().encode("utf-8"),
                                           file_name="degrees_import_errors.csv", mime="text/csv")
                    else:
                        st.success("No issues found. You can proceed to import.")
            with col2:
                if st.button("Import now"):
                    errs, up = import_degrees(engine, df, dry_run=False)
                    if len(errs):
                        st.error(f"Imported {up} rows with {len(errs)} issues.")
                        st.dataframe(errs)
                    else:
                        success(f"Imported {up} rows.")
                    st.cache_data.clear() # <-- FIX: Clear cache on import
                    st.rerun()
    else:
        st.info("Import functionality requires edit permissions")

    # Export - available to all viewers
    st.subheader("Export Degrees")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Download CSV"):
            name, data = export_degrees(engine, "csv")
            st.download_button("Save CSV", data, file_name=name, mime="text/cv")
    with c2:
        if st.button("Download Excel"):
            name, data = export_degrees(engine, "excel")
            st.download_button("Save Excel", data, file_name=name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Existing degrees - available to all viewers
    st.subheader("Existing Degrees")
    with engine.begin() as conn:
        # UPDATED: Added cg flags to SELECT query for display
        rows = conn.execute(sa_text("""
            SELECT code AS degree_code, title AS name,
                   cohort_splitting_mode, roll_number_scope AS roll_number_uniqueness_scope,
                   CASE WHEN active=1 THEN 'active' ELSE 'inactive' END AS status,
                   sort_order, logo_file_name, updated_at,
                   cg_degree, cg_program, cg_branch
            FROM degrees ORDER BY sort_order, code
        """))
        st.dataframe(pd.DataFrame([dict(r._mapping) for r in rows]))

    # Activate / Deactivate - only show if user has edit permissions
    if CAN_EDIT:
        st.subheader("Status Actions")
        with engine.begin() as conn:
            codes2 = [r[0] for r in conn.execute(sa_text("SELECT code FROM degrees ORDER BY code"))]
        if codes2:
            sel = st.selectbox("Pick a degree", codes2, index=0, key="deg_action_sel")
            note2 = st.text_input("Audit note (why)", key="deg_action_note")
            cA, cB = st.columns(2)
            with cA:
                if st.button("Deactivate"):
                    try:
                        set_active(engine, sel, False, actor, note2)
                        success(f"Degree {sel} deactivated.")
                        st.cache_data.clear() # <-- FIX: Clear cache on deactivate
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
            with cB:
                if st.button("Reactivate"):
                    try:
                        set_active(engine, sel, True, actor, note2)
                        success(f"Degree {sel} reactivated.")
                        st.cache_data.clear() # <-- FIX: Clear cache on reactivate
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
    else:
        st.info("Status actions require edit permissions")

    # ==================================================================
    # ===== DANGER ZONE REPLACED =======================================
    # ==================================================================
    # The old "Danger zone" block (lines 891-963) has been
    # replaced with this new block.
    
    # --- Delete / Request Delete UI (Using Universal Handler) ---
    st.subheader("Danger zone — Delete / Request Delete")

    user = st.session_state.get("user") or {}
    roles = set(user.get("roles") or [])
    # Use email for actor, as 'full_name' might not be unique or may be null
    actor = (user.get("email") or "system") 

    # --- Block 1: Standard Deletion (with Approvals) ---
    can_request_delete = can_request("degree", "delete", roles)

    if not can_request_delete:
        st.info("You don't have permission to request deletion for degrees.")
    else:
        with engine.begin() as conn:
            all_degrees = conn.execute(sa_text("SELECT code, title FROM degrees ORDER BY code")).fetchall()
            
            # Use mapping access to avoid BaseRow.__getitem__ with string keys
            degree_options = {
                row._mapping['code']: f"{row._mapping['code']} - {row._mapping['title']}"
                for row in all_degrees
            }

        if degree_options:
            del_sel = st.selectbox(
                "Select a degree to request deletion for",
                options=degree_options.keys(),
                format_func=lambda code: degree_options.get(code, code),
                key="deg_del_sel"
            )

            # --- NEW UNIVERSAL DELETE FORM ---
            # This one function creates the form, text box, dependency check,
            # and "Request Deletion" button for you.
            if del_sel:
                show_delete_form(
                    engine=engine,
                    object_type="degree",
                    object_id=del_sel,
                    user_email=actor,
                    display_name=degree_options[del_sel],
                    degree_code=del_sel  # Pass the degree code for scope
                )
        else:
            st.info("No degrees available to delete.")
    
    # --- Block 2: Emergency Delete (Superadmin Override) ---
    # This is now separate and only protected by CAN_EDIT
    if CAN_EDIT:
        st.error("🚨 EMERGENCY DELETE OPTION (Use with caution!)")
        
        with engine.begin() as conn:
            del_codes_emergency = [r[0] for r in conn.execute(sa_text("SELECT code FROM degrees ORDER BY code"))]

        if del_codes_emergency:
            emergency_sel = st.selectbox("Degree to FORCE DELETE", del_codes_emergency, key="deg_emergency_sel")

            if st.button(f"🚨 FORCE DELETE {emergency_sel} AND ALL CHILDREN", type="secondary"):
                # Use session state to show a confirmation
                st.session_state['confirm_force_delete'] = emergency_sel
                st.rerun() # Rerun to show confirmation

            # Add this new block to handle the confirmation
            if 'confirm_force_delete' in st.session_state and st.session_state['confirm_force_delete'] == emergency_sel:
                st.warning(f"**Are you absolutely sure you want to delete {emergency_sel} and all its data?**\n\nThis cannot be undone.")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("YES, I AM SURE. DELETE.", type="primary"):
                        try:
                            result = emergency_delete_degree(engine, emergency_sel, actor)
                            st.success(f"{emergency_sel} degree and all children force deleted: {result}")
                            del st.session_state['confirm_force_delete']
                            st.cache_data.clear() # <-- FIX: Clear cache on emergency delete
                            st.rerun()
                        except Exception as e:
                            st.error(f"Force delete failed: {e}")
                with c2:
                    if st.button("Cancel", type="secondary"):
                        del st.session_state['confirm_force_delete']
                        st.rerun()
        else:
            st.info("No degrees left for emergency deletion.")
            
    # ==================================================================
    # ===== END OF REPLACED BLOCK ======================================
    # ==================================================================


    # DEBUG: Database structure diagnostic
    with st.expander("🔍 Database Diagnostic (Debug)"):
        with engine.begin() as conn:
            # Check what tables exist
            tables = conn.execute(sa_text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            st.write("Existing tables:", [t[0] for t in tables])

            # Check specific child tables for BARCH
            if del_codes_emergency and "BARCH" in del_codes_emergency:
                barch_children = _children_summary(conn, "BARCH")
                st.write("BARCH child records:", barch_children)

                # Check programs table structure
                if _table_exists(conn, "programs"):
                    programs_cols = conn.execute(sa_text("PRAGMA table_info(programs)")).fetchall()
                    st.write("Programs table columns:", [col[1] for col in programs_cols])

                    # Check if any programs belong to BARCH
                    if _has_column(conn, "programs", "degree_code"):
                        barch_programs = conn.execute(
                            sa_text("SELECT COUNT(*) FROM programs WHERE degree_code='BARCH'")
                        ).fetchone()[0]
                        st.write(f"Programs with degree_code='BARCH': {barch_programs}")

    # Audit - available to all viewers
    st.subheader("Degree Audit Trail (latest 25)")
    with engine.begin() as conn:
        logs = conn.execute(sa_text("""
            SELECT degree_code, action, note, actor, at
            FROM degrees_audit
            ORDER BY id DESC LIMIT 25
        """))
        st.dataframe(pd.DataFrame([dict(r._mapping) for r in logs]))

render()
