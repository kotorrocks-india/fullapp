# screens/students/page.py
# -------------------------------------------------------------------
# MODIFIED VERSION
# - Added 'boolean' to the 'Data Type' dropdown.
# - Fixed "Delete Field" to use 'code' instead of 'id' for consistency.
# -------------------------------------------------------------------
from __future__ import annotations

import traceback
from typing import Optional, Any

import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine, Connection


# ────────────────────────────────────────────────────────────────────────────────
# Settings helpers
# ────────────────────────────────────────────────────────────────────────────────

def _get_setting(conn: Connection, key: str, default: Any = None) -> Any:
    """Gets a setting value from the database."""
    try:
        row = conn.execute(
            sa_text("SELECT value FROM app_settings WHERE key = :key"),
            {"key": key}
        ).fetchone()
        if row:
            return row[0]
    except Exception:
        # Table might not exist yet on first run, but schema installer will handle.
        pass
    return default

def _set_setting(conn: Connection, key: str, value: Any):
    """Saves a setting value to the database."""
    conn.execute(sa_text("""
        INSERT INTO app_settings (key, value)
        VALUES (:key, :value)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """), {"key": key, "value": str(value)})

def _init_settings_table(conn: Connection) -> None:
    try:
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """))
    except Exception:
        pass

# ────────────────────────────────────────────────────────────────────────────────
# Small helpers
# ────────────────────────────────────────────────────────────────────────────────

def _k(s: str) -> str:
    """Per-page key namespace to avoid collisions if rendered twice."""
    return f"students__{s}"

def _ensure_engine(engine: Optional[Engine]) -> Engine:
    if engine is not None:
        return engine
    # Lazy init via your core helpers
    from core.settings import load_settings
    from core.db import get_engine
    settings = load_settings()
    return get_engine(settings.db.url)

def _table_exists(conn, name: str) -> bool:
    try:
        row = conn.execute(
            sa_text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"),
            {"n": name},
        ).fetchone()
        return bool(row)
    except Exception:
        return False

def _students_tables_exist(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            return _table_exists(conn, "student_profiles")
    except Exception:
        return False

def _students_tables_snapshot(engine: Engine) -> None:
    with st.expander("Database snapshot (students tables)", expanded=False):
        try:
            with engine.connect() as conn:
                names = (
                    "student_profiles",
                    "student_enrollments",
                    "student_initial_credentials",
                    "student_custom_profile_fields",
                    "student_custom_profile_data",
                    # helpful context
                    "degrees",
                    "programs",
                    "branches",
                    # NEW
                    "degree_batches",
                    "app_settings"
                )
                info = {n: _table_exists(conn, n) for n in names}
                st.write(info)
                if info.get("student_profiles"):
                    total = conn.execute(
                        sa_text("SELECT COUNT(*) FROM student_profiles")
                    ).scalar() or 0
                    st.caption(f"student_profiles count: {total}")
        except Exception:
            st.warning("Could not probe students tables.")
            st.code(traceback.format_exc())


# ────────────────────────────────────────────────────────────────────────────────
# Optional: Bulk Operations import (defensive)
# ────────────────────────────────────────────────────────────────────────────────
_bulk_err = None
_render_bulk_ops = None
try:
    from screens.students.bulk_ops import render as _render_bulk_ops  # noqa: E402
except Exception as _e:
    _bulk_err = _e


# ────────────────────────────────────────────────────────────────────────────────
# Optional: Schema installer (fallback only; normal runs should auto-install)
# ────────────────────────────────────────────────────────────────────────────────
_schema_import_err = None
_install_student_schema = None
try:
    # Preferred: schemas (plural)
    from schemas.students_schema import install_schema as _install_student_schema  # noqa: E402
except Exception as _e1:
    # Legacy fallback if you keep a shim
    try:
        from schema.students_schema import install_schema as _install_student_schema  # noqa: E402
    except Exception:
        _schema_import_err = _e1


# ────────────────────────────────────────────────────────────────────────────────
# Settings Tab Helpers (MODIFIED)
# ────────────────────────────────────────────────────────────────────────────────

def _render_custom_fields_settings(engine: Engine):
    """Manage custom profile fields for students."""
    st.markdown("### 📝 Custom Profile Fields")
    st.caption("Define additional fields to capture student information beyond standard profile data.")
    
    try:
        with engine.connect() as conn:
            # Fetch existing custom fields
            fields = conn.execute(sa_text("""
                SELECT id, code, label, dtype, required, active, sort_order
                FROM student_custom_profile_fields
                ORDER BY sort_order, code
            """)).fetchall()
            
            if fields:
                st.markdown("#### Existing Custom Fields")
                for field in fields:
                    with st.expander(f"**{field[2]}** (`{field[1]}`) - {'Active' if field[5] else 'Inactive'}"):
                        col1, col2, col3 = st.columns([2, 1, 1])
                        col1.text_input("Label", value=field[2], key=f"field_label_{field[0]}", disabled=True)
                        col2.text_input("Type", value=field[3], key=f"field_type_{field[0]}", disabled=True)
                        col3.checkbox("Required", value=bool(field[4]), key=f"field_req_{field[0]}", disabled=True)
                        
                        # MODIFIED: Delete by 'code' (field[1]) for consistency
                        if st.button("🗑️ Delete Field", key=f"del_field_{field[0]}"):
                            with engine.begin() as conn_b:
                                # First delete data, then the field
                                conn_b.execute(sa_text(
                                    "DELETE FROM student_custom_profile_data WHERE field_code = :code"
                                ), {"code": field[1]})
                                conn_b.execute(sa_text(
                                    "DELETE FROM student_custom_profile_fields WHERE code = :code"
                                ), {"code": field[1]})
                            st.success(f"Deleted field: {field[2]}")
                            st.rerun()
            else:
                st.info("No custom fields defined yet.")
        
        # Add new field
        with st.expander("➕ Add New Custom Field", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                new_code = st.text_input("Field Code*", placeholder="e.g., blood_group, is_hostel_resident", key=_k("new_field_code"))
                new_label = st.text_input("Field Label*", placeholder="e.g., Blood Group, Hostel Resident?", key=_k("new_field_label"))
            with col2:
                # MODIFIED: Added 'boolean'
                new_dtype = st.selectbox("Data Type*", ["text", "number", "date", "choice", "boolean"], key=_k("new_field_dtype"))
                new_required = st.checkbox("Required Field", key=_k("new_field_required"))
                new_active = st.checkbox("Active", value=True, key=_k("new_field_active"))

            
            if st.button("Add Custom Field", type="primary", key=_k("add_field_btn")):
                if not new_code or not new_label:
                    st.error("Field code and label are required")
                else:
                    try:
                        with engine.begin() as conn_b:
                            conn_b.execute(sa_text("""
                                INSERT INTO student_custom_profile_fields (code, label, dtype, required, active, sort_order)
                                VALUES (:code, :label, :dtype, :req, :active, 100)
                            """), {
                                "code": new_code.strip().lower().replace(" ", "_"),
                                "label": new_label.strip(),
                                "dtype": new_dtype,
                                "req": 1 if new_required else 0,
                                "active": 1 if new_active else 0
                            })
                        st.success(f"✅ Added custom field: {new_label}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add field: {e} (Is the code unique?)")
    
    except Exception as e:
        st.error(f"Failed to load custom fields: {e}")


def _render_roll_number_policy(engine: Engine):
    """Configure roll number derivation and validation policies."""
    st.markdown("### 🔢 Roll Number Policy")
    st.caption("Define how roll numbers are generated, validated, and scoped.")
    
    with engine.connect() as conn:
        derivation_mode = st.radio(
            "Roll Number Generation",
            ["hybrid", "manual", "auto"],
            index=["hybrid", "manual", "auto"].index(_get_setting(conn, "roll_derivation_mode", "hybrid")),
            help="Hybrid: Auto-generate with manual override. Manual: Always enter manually. Auto: Fully automated.",
            key=_k("roll_derivation_mode")
        )
        
        year_from_first4 = st.checkbox(
            "Extract year from first 4 digits",
            value=_get_setting(conn, "roll_year_from_first4", "True") == "True",
            help="e.g., '2021' from roll number '20211234'",
            key=_k("year_from_first4")
        )
        
        per_degree_regex = st.checkbox(
            "Allow per-degree regex patterns",
            value=_get_setting(conn, "roll_per_degree_regex", "True") == "True",
            help="Enable custom validation patterns for each degree",
            key=_k("per_degree_regex")
        )

        st.divider()
    
        if st.button("💾 Save Roll Number Policy", type="primary", key=_k("save_roll_policy")):
            with engine.begin() as conn_b:
                _set_setting(conn_b, "roll_derivation_mode", derivation_mode)
                _set_setting(conn_b, "roll_year_from_first4", year_from_first4)
                _set_setting(conn_b, "roll_per_degree_regex", per_degree_regex)
            st.success("✅ Roll number policy saved")
            st.rerun()


def _render_email_lifecycle_policy(engine: Engine):
    """Configure email lifecycle requirements (.edu and personal email)."""
    st.markdown("### 📧 Email Lifecycle Policy")
    st.caption("Manage .edu email requirements and post-graduation personal email transitions.")
    
    with engine.connect() as conn:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### .edu Email Requirement")
            edu_email_enabled = st.checkbox(
                "Require .edu email",
                value=_get_setting(conn, "email_edu_enabled", "True") == "True",
                help="Students must provide an institutional email",
                key=_k("edu_email_enabled")
            )
            
            edu_enforcement_months = st.number_input(
                "Enforcement period (months)",
                min_value=1,
                max_value=24,
                value=int(_get_setting(conn, "email_edu_months", 6)),
                help="Grace period after joining to provide .edu email",
                key=_k("edu_enforcement_months")
            )
            
            edu_domain = st.text_input(
                "Allowed domain(s)",
                value=_get_setting(conn, "email_edu_domain", "college.edu"),
                placeholder="e.g., college.edu",
                help="Comma-separated list of allowed domains",
                key=_k("edu_domain")
            )
        
        with col2:
            st.markdown("#### Post-Graduation Personal Email")
            personal_email_enabled = st.checkbox(
                "Require personal email after graduation",
                value=_get_setting(conn, "email_personal_enabled", "True") == "True",
                help="Students must provide personal email before graduation",
                key=_k("personal_email_enabled")
            )
            
            personal_enforcement_months = st.number_input(
                "Enforcement period (months after graduation)",
                min_value=1,
                max_value=24,
                value=int(_get_setting(conn, "email_personal_months", 6)),
                help="Time to provide personal email after graduation",
                key=_k("personal_enforcement_months")
            )
    
    st.divider()
    
    if st.button("💾 Save Email Policy", type="primary", key=_k("save_email_policy")):
        with engine.begin() as conn:
            _set_setting(conn, "email_edu_enabled", edu_email_enabled)
            _set_setting(conn, "email_edu_months", edu_enforcement_months)
            _set_setting(conn, "email_edu_domain", edu_domain)
            _set_setting(conn, "email_personal_enabled", personal_email_enabled)
            _set_setting(conn, "email_personal_months", personal_enforcement_months)
        st.success("✅ Email lifecycle policy saved")
        st.rerun()


def _render_student_status_settings(engine: Engine):
    """Configure available student statuses and their effects."""
    st.markdown("### 🎓 Student Status Configuration")
    st.caption("Define available student statuses and their behavioral effects.")
    
    # This remains hardcoded as per the YAML/original file
    default_statuses = {
        "Good": {
            "effects": {"include_in_current_ay": True},
            "badge": None,
            "note": "Active student in good standing"
        },
        "Hold": {
            "effects": {"include_in_current_ay": False},
            "badge": None,
            "note": "Hidden from current AY calculations"
        },
        "Left": {
            "effects": {"include_in_current_ay": False, "future_allocations": False},
            "badge": "Left",
            "note": "Student has left the institution"
        },
        "Transferred": {
            "effects": {"include_in_current_ay": False, "future_allocations": False},
            "badge": "Transferred",
            "note": "Transferred to another institution"
        },
        "Graduated": {
            "effects": {"include_in_current_ay": False, "eligible_for_transcript": True},
            "badge": "Graduated",
            "note": "Completed the program"
        },
        "Deceased": {
            "effects": {"include_in_current_ay": False, "record_frozen": True, "restricted_access": True},
            "badge": "Deceased",
            "note": "Record is frozen and access is restricted"
        },
        "YearDrop": {
            "effects": {"include_in_current_ay": True},
            "badge": "Year Drop",
            "note": "Student has dropped a year but remains enrolled"
        }
    }
    
    for status_name, config in default_statuses.items():
        with st.expander(f"**{status_name}** {('🏷️ ' + config['badge']) if config['badge'] else ''}"):
            st.caption(config['note'])
            effects = config['effects']
            cols = st.columns(3)
            for i, (effect, value) in enumerate(effects.items()):
                with cols[i % 3]:
                    icon = "✅" if value else "❌"
                    st.markdown(f"{icon} `{effect}`")
    
    st.divider()
    st.info("💡 Status definitions are configured in the YAML policy. Editing UI coming soon.")


def _render_division_settings(engine: Engine):
    """Configure division/section management rules."""
    st.markdown("### 🏫 Division/Section Settings")
    st.caption("Configure how students are organized into divisions or sections.")
    
    with engine.connect() as conn:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Division Configuration")
            divisions_enabled = st.checkbox(
                "Enable divisions per term",
                value=_get_setting(conn, "div_enabled", "True") == "True",
                key=_k("divisions_enabled")
            )
            
            free_form_names = st.checkbox(
                "Allow free-form division names",
                value=_get_setting(conn, "div_free_form", "True") == "True",
                help="If unchecked, use predefined list",
                key=_k("free_form_names")
            )
            
            unique_scope = st.selectbox(
                "Uniqueness scope",
                ["degree_year_term", "degree_year", "degree", "global"],
                index=["degree_year_term", "degree_year", "degree", "global"].index(_get_setting(conn, "div_unique_scope", "degree_year_term")),
                help="Where division names must be unique",
                key=_k("unique_scope")
            )
        
        with col2:
            st.markdown("#### Import & Copy Settings")
            
            import_optional = st.checkbox(
                "Division column optional in imports",
                value=_get_setting(conn, "div_import_optional", "True") == "True",
                key=_k("import_optional")
            )
            
            copy_from_previous = st.checkbox(
                "Enable copy from previous term",
                value=_get_setting(conn, "div_copy_prev", "True") == "True",
                help="Allow copying division assignments from prior term",
                key=_k("copy_from_previous")
            )
            
            block_publish_unassigned = st.checkbox(
                "Block publish when students unassigned",
                value=_get_setting(conn, "div_block_publish", "True") == "True",
                help="Prevent publishing marks/attendance if students lack divisions",
                key=_k("block_publish")
            )
        
        # Capacity settings
        with st.expander("🔢 Division Capacity (Optional)"):
            capacity_mode = st.radio(
                "Capacity tracking",
                ["off", "soft_limit", "hard_limit"],
                index=["off", "soft_limit", "hard_limit"].index(_get_setting(conn, "div_capacity_mode", "off")),
                help="Soft: warn on breach. Hard: block on breach.",
                key=_k("capacity_mode")
            )
            
            if capacity_mode != "off":
                default_capacity = st.number_input(
                    "Default division capacity",
                    min_value=1,
                    value=int(_get_setting(conn, "div_default_capacity", 60)),
                    key=_k("default_capacity")
                )
    
    if st.button("💾 Save Division Settings", type="primary", key=_k("save_division_settings")):
        with engine.begin() as conn:
            _set_setting(conn, "div_enabled", divisions_enabled)
            _set_setting(conn, "div_free_form", free_form_names)
            _set_setting(conn, "div_unique_scope", unique_scope)
            _set_setting(conn, "div_import_optional", import_optional)
            _set_setting(conn, "div_copy_prev", copy_from_previous)
            _set_setting(conn, "div_block_publish", block_publish_unassigned)
            _set_setting(conn, "div_capacity_mode", capacity_mode)
            if capacity_mode != "off":
                _set_setting(conn, "div_default_capacity", default_capacity)
        st.success("✅ Division settings saved")
        st.rerun()


def _render_publish_guardrails(engine: Engine):
    """Configure publish guardrails and validation checks."""
    st.markdown("### 🛡️ Publish Guardrails")
    st.caption("Define checks that must pass before publishing marks or attendance.")
    
    with engine.connect() as conn:
        guard_unassigned = st.checkbox("Block publish if program/branch/division unassigned", value=_get_setting(conn, "guard_unassigned", "True") == "True", key=_k("guard_unassigned"))
        guard_duplicates = st.checkbox("Block publish if duplicates unresolved", value=_get_setting(conn, "guard_duplicates", "True") == "True", key=_k("guard_duplicates"))
        guard_invalid = st.checkbox("Block publish if invalid roll or email", value=_get_setting(conn, "guard_invalid", "True") == "True", key=_k("guard_invalid"))
        guard_batch_mismatch = st.checkbox("Block publish if batch mismatch detected", value=_get_setting(conn, "guard_batch_mismatch", "True") == "True", key=_k("guard_batch_mismatch"))
        guard_capacity = st.checkbox("Block publish on hard capacity breach", value=_get_setting(conn, "guard_capacity", "False") == "True", key=_k("guard_capacity"))
    
    st.divider()
    
    if st.button("💾 Save Guardrails", type="primary", key=_k("save_guardrails")):
        with engine.begin() as conn:
            _set_setting(conn, "guard_unassigned", guard_unassigned)
            _set_setting(conn, "guard_duplicates", guard_duplicates)
            _set_setting(conn, "guard_invalid", guard_invalid)
            _set_setting(conn, "guard_batch_mismatch", guard_batch_mismatch)
            _set_setting(conn, "guard_capacity", guard_capacity)
        st.success("✅ Publish guardrails saved")
        st.rerun()


def _render_mover_settings(engine: Engine):
    """Configure student mover policies."""
    st.markdown("### 🚚 Student Mover Settings")
    st.caption("Control how students can be moved between batches, degrees, and divisions.")
    
    with engine.connect() as conn:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Within-Term Division Moves")
            within_term_enabled = st.checkbox(
                "Enable within-term division moves",
                value=_get_setting(conn, "mover_within_term", "True") == "True",
                key=_k("mover_within_term")
            )
            
            require_reason_within = st.checkbox(
                "Require reason for move",
                value=_get_setting(conn, "mover_within_reason", "True") == "True",
                key=_k("mover_within_reason")
            )
        
        with col2:
            st.markdown("#### Cross-Batch Moves")
            cross_batch_enabled = st.checkbox(
                "Enable cross-batch moves",
                value=_get_setting(conn, "mover_cross_batch", "True") == "True",
                key=_k("mover_cross_batch")
            )
            
            # THIS IS THE KEY SETTING YOU REQUESTED
            next_batch_only = st.checkbox(
                "Restrict to next batch only",
                value=_get_setting(conn, "mover_next_only", "True") == "True",
                help="Students can only move to the immediately following batch",
                key=_k("mover_next_only")
            )
            
            require_reason_cross = st.checkbox(
                "Require reason for move",
                value=_get_setting(conn, "mover_cross_reason", "True") == "True",
                key=_k("mover_cross_reason")
            )
    
    st.divider()
    
    if st.button("💾 Save Mover Settings", type="primary", key=_k("save_mover_settings")):
        with engine.begin() as conn:
            _set_setting(conn, "mover_within_term", within_term_enabled)
            _set_setting(conn, "mover_within_reason", require_reason_within)
            _set_setting(conn, "mover_cross_batch", cross_batch_enabled)
            _set_setting(conn, "mover_next_only", next_batch_only) # Saving your new rule
            _set_setting(conn, "mover_cross_reason", require_reason_cross)
        st.success("✅ Student mover settings saved")
        st.rerun()


def _render_access_permissions(engine: Engine):
    """Configure role-based access for student data."""
    st.markdown("### 🔐 Access Permissions")
    st.caption("Define which roles can view, edit, delete, or move student records.")
    
    # In a real app, these defaults would be loaded/saved
    permissions = {
        "View": ["superadmin", "tech_admin", "principal", "director", "office_admin"],
        "Edit": ["superadmin", "tech_admin", "office_admin"],
        "Delete": ["superadmin", "tech_admin"],
        "Move": ["superadmin", "tech_admin", "office_admin"]
    }
    
    for action, roles in permissions.items():
        with st.expander(f"**{action}** - {len(roles)} roles"):
            st.multiselect(
                f"Roles that can {action.lower()} students",
                ["superadmin", "tech_admin", "principal", "director", "office_admin", "faculty", "class_in_charge"],
                default=roles,
                key=_k(f"perm_{action.lower()}")
            )
    
    st.divider()
    
    # Special: Class-in-Charge permissions
    st.markdown("#### Class-in-Charge Scope")
    st.caption("Class teachers get limited access to their assigned students only.")
    
    col1, col2 = st.columns(2)
    col1.checkbox("View assigned students", value=True, disabled=True, key=_k("cic_view"))
    col2.checkbox("Edit assigned students", value=False, key=_k("cic_edit"))
    
    if st.button("💾 Save Access Permissions", type="primary", key=_k("save_permissions")):
        # This part is still a demo, but could be implemented like the others
        st.success("✅ Access permissions saved (demo - implement persistence)")


def _render_settings_tab(engine: Engine):
    """Main settings tab with all configuration sections."""
    st.subheader("⚙️ Student Settings")
    
    # Settings categories
    settings_sections = st.tabs([
        "📝 Custom Fields",
        "🔢 Roll Numbers",
        "📧 Email Policy",
        "🎓 Student Status",
        "🏫 Divisions",
        "🛡️ Guardrails",
        "🚚 Movers",
        "🔐 Access"
    ])
    
    with settings_sections[0]:
        _render_custom_fields_settings(engine)
    
    with settings_sections[1]:
        _render_roll_number_policy(engine)
    
    with settings_sections[2]:
        _render_email_lifecycle_policy(engine)
    
    with settings_sections[3]:
        _render_student_status_settings(engine)
    
    with settings_sections[4]:
        _render_division_settings(engine)
    
    with settings_sections[5]:
        _render_publish_guardrails(engine)
    
    with settings_sections[6]:
        _render_mover_settings(engine)
    
    with settings_sections[7]:
        _render_access_permissions(engine)


# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def render(engine: Optional[Engine] = None, **kwargs) -> None:
    engine = _ensure_engine(engine)

    st.title("👨‍🎓 Students")
    st.caption(f"Module file: `{__file__}`")

    # If tables are missing (e.g., app was started without auto_discover/run_all),
    # show a friendly fallback to install just the student schema.
    if not _students_tables_exist(engine):
        st.warning("⚠️ Student tables not found in database.")

        if _install_student_schema is None:
            st.error("❌ Schema installer could not be imported.")
            if _schema_import_err:
                st.code(
                    "Traceback:\n"
                    + "".join(
                        traceback.format_exception_only(
                            type(_schema_import_err), _schema_import_err
                        )
                    )
                )
            st.info("💡 Ensure `schemas/students_schema.py` exists and is importable.")
            return

        st.markdown(
            """
            ### 🔧 Database Setup Required

            The student management system requires a few tables to function.

            **This page includes a *safe fallback* button** to create those tables
            only if your app didn't already do it via `auto_discover("schemas")`
            and `run_all_installers(engine)` in `app.py`.

            **Tables to be created:**
            - `student_profiles` — Student personal info
            - `student_enrollments` — Enrollment records
            - `student_initial_credentials` — First-time credentials
            - `student_custom_profile_fields` — Custom field definitions
            - `student_custom_profile_data` — Custom field values
            - `degree_batches` — Formal batch hierarchy
            - `app_settings` — Application settings
            - `degree_year_scaffold` — Degree-year links
            - `batch_year_scaffold` — Batch-year links
            """
        )

        if st.button("🔧 Install Student Schema", type="primary", key=_k("install_btn")):
            try:
                with st.spinner("Creating database tables..."):
                    _install_student_schema(engine)
                st.success("✅ Student schema installed successfully!")
                st.cache_data.clear()
                st.balloons()
                st.rerun()
            except Exception as e:
                st.error(f"❌ Failed to install schema: {e}")
                st.code(traceback.format_exc())
        return

    # Show a quick DB snapshot (collapsed by default)
    _students_tables_snapshot(engine)

    # Page tabs
    tab_list, tab_bulk, tab_settings = st.tabs(
        ["Student List", "Bulk Operations", "Settings"]
    )

    # ── Student List tab
    with tab_list:
        try:
            st.subheader("All Students")
            with engine.connect() as conn:
                if not _table_exists(conn, "student_profiles"):
                    st.info("`student_profiles` not found. Use the schema installer.")
                else:
                    rows = conn.execute(
                        sa_text(
                            """
                            SELECT id,
                                   COALESCE(name, email, '') AS display_name,
                                   email,
                                   student_id,
                                   COALESCE(updated_at, '1970-01-01') AS uat
                            FROM student_profiles
                            ORDER BY uat DESC, id DESC
                            LIMIT 50
                            """
                        )
                    ).fetchall()

                    if not rows:
                        st.info("No student records yet. Use **Bulk Operations** to import.")
                    else:
                        data = [
                            {
                                "id": r[0],
                                "name": r[1],
                                "email": r[2],
                                "student_id": r[3],
                            }
                            for r in rows
                        ]
                        st.dataframe(data, use_container_width=True)
        except Exception:
            st.error("Student List failed.")
            st.code(traceback.format_exc())

    # ── Bulk Operations tab
    with tab_bulk:
        if _bulk_err:
            st.error("Bulk Operations import failed.")
            st.code(
                "Traceback (most recent call last):\n"
                + "".join(
                    traceback.format_exception_only(type(_bulk_err), _bulk_err)
                )
            )
        else:
            try:
                if _render_bulk_ops:
                    _render_bulk_ops(engine)
                else:
                    st.info("Bulk operations UI not available in this build.")
            except Exception:
                st.error("Bulk Operations failed.")
                st.code(traceback.format_exc())

    # ── Settings tab
    with tab_settings:
        try:
            _render_settings_tab(engine)
        except Exception:
            st.error("Settings tab failed.")
            st.code(traceback.format_exc())


# Always render on import so navigating away/back re-renders reliably.
render()
