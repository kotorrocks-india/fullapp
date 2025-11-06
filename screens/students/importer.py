# app/screens/students/importer.py
# -------------------------------------------------------------------
# MODIFIED VERSION
# - Template download is now DYNAMIC, includes active custom fields.
# - Import logic now DYNAMICALLY processes custom fields.
# - NEW: Added a full Student Data Exporter.
# -------------------------------------------------------------------

from __future__ import annotations

from typing import List, Tuple, Dict, Any, Optional, Set
from dataclasses import dataclass, field
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine, Connection
from collections import defaultdict
import logging
import traceback
from datetime import datetime, timedelta

# Import common helpers
from screens.faculty.utils import _safe_int_convert, _handle_error
from screens.faculty.db import _active_degrees

# Import from student db.py file
from screens.students.db import (
    _ensure_student_username_and_initial_creds,
    _get_student_credentials_to_export,
    _get_existing_enrollment_data,
    _db_get_batches_for_degree,
    _db_get_students_for_mover,
    _db_move_students
)

# NEW: Import settings helpers from page.py
# Try to reuse settings helpers from the students page; fall back to local copies
try:
    from screens.students.page import _get_setting, _init_settings_table  # type: ignore
except ImportError:
    # Local fallback implementations – keep importer working even if page helpers
    # are missing or import order is weird.
    def _get_setting(conn: Connection, key: str, default: Any = None) -> Any:
        """Gets a setting value from the app_settings table."""
        try:
            row = conn.execute(
                sa_text("SELECT value FROM app_settings WHERE key = :key"),
                {"key": key},
            ).fetchone()
            if row:
                return row[0]
        except Exception:
            # Table might not exist yet, or other transient error – just use default
            return default
        return default

    def _init_settings_table(conn: Connection) -> None:
        """Ensure the app_settings table exists."""
        try:
            conn.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS app_settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
            """))
        except Exception:
            # Don't crash the importer if something goes wrong here
            pass


log = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Degree Duration & Year Management (Unchanged)
# ------------------------------------------------------------------

def _get_degree_duration(conn: Connection, degree_code: str) -> Optional[int]:
    # ... (code preserved) ...
    try:
        # FIXED: Query degree_semester_struct (from semesters_schema.py)
        result = conn.execute(sa_text("""
            SELECT years FROM degree_semester_struct WHERE degree_code = :code
        """), {"code": degree_code}).fetchone()
        
        if result and result[0]:
            return int(result[0])
        
        return None
    except Exception as e:
        log.warning(f"Could not fetch degree duration: {e}")
        return None


def _ensure_degree_years_scaffold(conn: Connection, degree_code: str) -> bool:
    # ... (code preserved) ...
    duration = _get_degree_duration(conn, degree_code)
    if not duration or duration < 1:
        return False
    
    try:
        # Check if degree_year_scaffold table exists
        table_check = conn.execute(sa_text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='degree_year_scaffold'
        """)).fetchone()
        
        if not table_check:
            # Create table if it doesn't exist
            conn.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS degree_year_scaffold (
                    degree_code TEXT NOT NULL,
                    year_num INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (degree_code, year_num)
                )
            """))
            log.info("Created degree_year_scaffold table")
        
        for year_num in range(1, int(duration) + 1):
            # Check if this year scaffold already exists
            exists = conn.execute(sa_text("""
                SELECT 1 FROM degree_year_scaffold 
                WHERE degree_code = :code AND year_num = :year
            """), {"code": degree_code, "year": year_num}).fetchone()
            
            if not exists:
                conn.execute(sa_text("""
                    INSERT INTO degree_year_scaffold (degree_code, year_num, created_at)
                    VALUES (:code, :year, CURRENT_TIMESTAMP)
                """), {"code": degree_code, "year": year_num})
        
        log.info(f"✅ Created year scaffold for degree {degree_code} (years 1-{duration})")
        return True
    except Exception as e:
        log.error(f"Failed to create year scaffold: {e}")
        return False


def _get_valid_years_for_degree(conn: Connection, degree_code: str) -> List[int]:
    # ... (code preserved) ...
    valid_years = set()
    
    # 1. From degree duration
    duration = _get_degree_duration(conn, degree_code)
    if duration and duration > 0:
        valid_years.update(range(1, int(duration) + 1))
    
    # 2. From existing scaffolds (if table exists)
    try:
        scaffold_years = conn.execute(sa_text("""
            SELECT DISTINCT year_num FROM degree_year_scaffold 
            WHERE degree_code = :code ORDER BY year_num
        """), {"code": degree_code}).fetchall()
        valid_years.update([int(r[0]) for r in scaffold_years])
    except Exception:
        pass
    
    # 3. From existing enrollments
    try:
        enrollment_years = conn.execute(sa_text("""
            SELECT DISTINCT current_year FROM student_enrollments 
            WHERE degree_code = :code AND current_year IS NOT NULL
            ORDER BY current_year
        """), {"code": degree_code}).fetchall()
        valid_years.update([int(r[0]) for r in enrollment_years if r[0] is not None])
    except Exception:
        pass
    
    return sorted(list(valid_years))


def _create_batch_with_years(conn: Connection, degree_code: str, batch_code: str, batch_name: str, start_date: str) -> Tuple[bool, str]:
    # ... (code preserved) ...
    try:
        # Ensure degree year scaffold exists
        scaffold_ok = _ensure_degree_years_scaffold(conn, degree_code)
        
        # Check if batch already exists for this degree
        batch_exists = conn.execute(sa_text("""
            SELECT 1 FROM degree_batches 
            WHERE degree_code = :degree AND batch_code = :batch
        """), {"degree": degree_code, "batch": batch_code}).fetchone()
        
        if batch_exists:
            return False, f"❌ Batch '{batch_code}' already exists for degree {degree_code}"
        
        # Get valid years for this degree
        valid_years = _get_valid_years_for_degree(conn, degree_code)
        
        if not valid_years:
            return False, f"❌ Degree {degree_code} has no defined years. Set degree duration first."

        # Insert into the new batches table
        conn.execute(sa_text("""
            INSERT INTO degree_batches (degree_code, batch_code, batch_name, start_date)
            VALUES (:degree, :batch, :name, :start)
        """), {
            "degree": degree_code,
            "batch": batch_code,
            "name": batch_name or batch_code,
            "start": start_date
        })
        
        # Check if batch_year_scaffold table exists
        table_check = conn.execute(sa_text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='batch_year_scaffold'
        """)).fetchone()
        
        if not table_check:
            # Create table if it doesn't exist
            conn.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS batch_year_scaffold (
                    degree_code TEXT NOT NULL,
                    batch TEXT NOT NULL,
                    year_num INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (degree_code, batch, year_num)
                )
            """))
            log.info("Created batch_year_scaffold table")
        
        # Create scaffolding records for this batch
        for year_num in valid_years:
            conn.execute(sa_text("""
                INSERT INTO batch_year_scaffold (degree_code, batch, year_num, created_at)
                VALUES (:degree, :batch, :year, CURRENT_TIMESTAMP)
            """), {"degree": degree_code, "batch": batch_code, "year": year_num})
        
        message = f"✅ Created batch '{batch_code}' for {degree_code} with years {valid_years}"
        log.info(message)
        return True, message
        
    except Exception as e:
        error_msg = f"❌ Failed to create batch: {str(e)}"
        log.error(f"Batch creation failed: {traceback.format_exc()}")
        return False, error_msg


# ------------------------------------------------------------------
# Helpers for Stateful Import (Unchanged)
# ------------------------------------------------------------------

@dataclass
class EnrollmentCheckResult:
    # ... (code preserved) ...
    unmatched_batches: Set[str] = field(default_factory=set)
    existing_batches: List[str] = field(default_factory=list)
    unmatched_years: Set[str] = field(default_factory=set)
    existing_years: List[int] = field(default_factory=list)
    ignored_rows: int = 0
    invalid_years: Set[int] = field(default_factory=set)  # Years outside degree duration


def _pre_check_student_enrollments(df: pd.DataFrame, engine: Engine, degree_code: str) -> Tuple[EnrollmentCheckResult, pd.DataFrame]:
    # ... (code preserved) ...
    degree_code_clean = degree_code.strip()

    # 1. Clean DataFrame
    for col in ['degree_code', 'batch', 'current_year']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace('nan', '')

    df_filtered = df[df['degree_code'].str.lower() == degree_code_clean.lower()].copy()
    ignored_rows = len(df) - len(df_filtered)

    if df_filtered.empty:
        raise ValueError(f"No rows found in the CSV for the selected degree '{degree_code_clean}'.")

    # 2. Get degree duration & valid years
    with engine.connect() as conn:
        valid_years = _get_valid_years_for_degree(conn, degree_code_clean)
        degree_duration = _get_degree_duration(conn, degree_code_clean)
    
    if not valid_years and degree_duration:
        # Auto-scaffold if duration is set but no years exist
        with engine.begin() as conn:
            _ensure_degree_years_scaffold(conn, degree_code_clean)
            valid_years = _get_valid_years_for_degree(conn, degree_code_clean)

    # 3. Get CSV years and check validity
    csv_batches = set(df_filtered['batch'].dropna().unique()) - {''}
    csv_years_raw = set(df_filtered['current_year'].dropna().unique()) - {''}
    
    # Convert to int and check against valid years
    csv_years = set()
    invalid_years = set()
    
    for year_str in csv_years_raw:
        try:
            year_int = int(year_str)
            if valid_years and year_int not in valid_years:
                # Year is outside degree duration
                invalid_years.add(year_int)
            else:
                csv_years.add(year_int)
        except ValueError:
            invalid_years.add(year_str)

    # screens/students/importer.py (Corrected)

    # 4. Get existing batches from database
    with engine.connect() as conn:
        # Note: We ALREADY fetched valid_years correctly above.
        # We only need to fetch batches here.
        existing_data = _get_existing_enrollment_data(engine, degree_code_clean)
        db_batches = existing_data['batches']
    
    # We will use the comprehensive 'valid_years' list fetched earlier,
    # which respects degree duration, not just existing enrollments.
    db_years = sorted(list(valid_years)) # Use the correct list!

    # 5. Find mismatches
    result = EnrollmentCheckResult(
        unmatched_batches=csv_batches - set(db_batches),
        existing_batches=sorted(db_batches),
        unmatched_years=csv_years - set(db_years), # This will now be empty
        existing_years=db_years, # This will now be [1, 2, 3, 4, 5]
        ignored_rows=ignored_rows,
        invalid_years=invalid_years
    )

    return result, df_filtered

def _build_translation_map(
    mappings: Dict[str, Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    # ... (code preserved) ...
    translation_map = {}
    for aff_type, type_mappings in mappings.items():
        translation_map[aff_type] = {}
        for code, action in type_mappings.items():
            if action == "[USE_NEW]":
                # This should no longer be hit for batches, but keep for years
                translation_map[aff_type][code] = code
            else:
                translation_map[aff_type][code] = action

    return translation_map


# ------------------------------------------------------------------
# Main Import Logic (MODIFIED)
# ------------------------------------------------------------------

def _show_no_degrees_help(engine: Engine, context: str = "student operations"):
    # ... (code preserved) ...
    with engine.begin() as conn:
        degrees = _active_degrees(conn)
        if degrees:
            return True

    st.warning(f"⚠️ No degrees found. Set up degrees before {context}.")
    st.markdown("""
### 🚀 Getting Started

1. **Create Degrees** with defined duration (e.g., BTech = 5 years)
2. **Import Students** with batch and year values
3. **View and manage** students

""")

    with st.expander("➕ Quick Create Your First Degree", expanded=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            degree_code = st.text_input("Degree Code*", placeholder="e.g., BTech")
        with col2:
            degree_name = st.text_input("Degree Name", placeholder="e.g., Bachelor of Technology")
        with col3:
            duration = st.number_input("Years/Duration*", min_value=1, max_value=10, value=4)

        if st.button("✨ Create Degree", type="primary"):
            if not degree_code or not degree_code.strip():
                st.error("❌ Degree code is required")
            else:
                try:
                    with engine.begin() as conn:
                        existing = conn.execute(sa_text(
                            "SELECT 1 FROM degrees WHERE LOWER(code) = LOWER(:code)"
                        ), {"code": degree_code.strip()}).fetchone()

                        if existing:
                            st.error(f"❌ Degree '{degree_code}' already exists")
                        else:
                            # Insert into degrees table
                            conn.execute(sa_text("""
                                INSERT INTO degrees (code, title, active, sort_order, created_at, updated_at)
                                VALUES (:code, :name, 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            """), {
                                "code": degree_code.strip(),
                                "name": degree_name.strip() or degree_code.strip()
                            })
                            
                            # Insert into degree_semester_struct
                            conn.execute(sa_text("""
                                INSERT INTO degree_semester_struct (degree_code, years, terms_per_year, active, updated_at)
                                VALUES (:code, :years, 2, 1, CURRENT_TIMESTAMP)
                            """), {
                                "code": degree_code.strip(),
                                "years": int(duration)
                            })
                            
                            # Auto-scaffold years
                            _ensure_degree_years_scaffold(conn, degree_code.strip())
                            
                            st.success(f"✅ Created degree: **{degree_code}** ({duration} years)")
                            st.cache_data.clear()
                            st.balloons()
                            st.rerun()
                except Exception as e:
                    st.error(f"❌ Failed: {e}")
                    log.error(f"Degree creation failed: {e}")

    return False


def _import_students_with_validation(
    engine: Engine,
    df: pd.DataFrame,
    dry_run: bool,
    mappings: Optional[Dict[str, Dict[str, str]]] = None,
    conn_for_transaction: Optional[Connection] = None
) -> Tuple[List[Dict[str, Any]], int, List[Dict[str, Any]]]:
    """
    Import students with strict validation.
    MODIFIED: Now processes dynamic custom fields.
    """
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
    errors: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []
    success_count = 0
    translation_map = _build_translation_map(mappings) if mappings else {}

    # Metadata fetch
    with engine.begin() as meta_conn:
        all_degrees = _active_degrees(meta_conn)
        
        # MODIFIED: Get active custom field codes
        active_custom_fields_rows = meta_conn.execute(sa_text(
            "SELECT code FROM student_custom_profile_fields WHERE active = 1"
        )).fetchall()
        active_custom_fields = {row[0] for row in active_custom_fields_rows}

    # Transaction logic
    if conn_for_transaction:
        conn = conn_for_transaction
        trans = conn.begin_nested()
        should_close = False
    else:
        conn = engine.connect()
        trans = conn.begin()
        should_close = True

    try:
        for idx, row in df.iterrows():
            row_num = idx + 2
            try:
                # 1. Validate Profile Data
                email = str(row.get('email', '')).strip().lower()
                student_id = str(row.get('student_id', '')).strip() # This is the Roll Number
                name = str(row.get('name', '')).strip()

                if not name or not email or not student_id:
                    errors.append({'row': row_num, 'email': email, 'error': "Missing required fields: name, email, student_id (roll_no)"})
                    continue

                # 2. Upsert Profile
                profile_id = conn.execute(sa_text(
                    "SELECT id FROM student_profiles WHERE student_id = :sid"
                ), {"sid": student_id}).fetchone()

                if profile_id:
                    profile_id = profile_id[0]
                    conn.execute(sa_text("""
                        UPDATE student_profiles
                        SET name = :name, email = :email, phone = :phone, status = :status, updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                    """), {
                        "name": name, "email": email, "phone": row.get('phone'),
                        "status": row.get('status', 'active'), "id": profile_id
                    })
                else:
                    res = conn.execute(sa_text("""
                        INSERT INTO student_profiles (name, email, student_id, phone, status)
                        VALUES (:name, :email, :sid, :phone, :status)
                    """), {
                        "name": name, "email": email, "sid": student_id,
                        "phone": row.get('phone'), "status": row.get('status', 'active')
                    })
                    profile_id = res.lastrowid

                # 3. Ensure Credentials
                _ensure_student_username_and_initial_creds(conn, profile_id, name, student_id)

                # 4. Process Enrollment
                # ... (this part is preserved from before) ...
                degree_code = str(row.get('degree_code', '')).strip()
                if degree_code:
                    # ... (validation for degree, batch, year preserved) ...
                    if degree_code not in all_degrees:
                        errors.append({'row': row_num, 'email': email, 'error': f"Degree '{degree_code}' not found"})
                        continue
                    batch = str(row.get('batch', '')).strip()
                    current_year = str(row.get('current_year', '')).strip()
                    if not batch or not current_year:
                        errors.append({'row': row_num, 'email': email, 'error': "Missing batch or year"})
                        continue
                    try:
                        year_int = int(current_year)
                        valid_years = _get_valid_years_for_degree(conn, degree_code)
                        if valid_years and year_int not in valid_years:
                            errors.append({
                                'row': row_num, 
                                'email': email, 
                                'error': f"Year {year_int} is outside degree duration (valid: {valid_years})"
                            })
                            continue
                    except ValueError:
                        errors.append({'row': row_num, 'email': email, 'error': f"Invalid year value: {current_year}"})
                        continue
                    mapped_batch = translation_map.get('batch', {}).get(batch, batch)
                    mapped_year = translation_map.get('year', {}).get(current_year, current_year)
                    if mapped_batch == "[IGNORE]" or mapped_year == "[IGNORE]":
                        skipped_rows.append({"row": row_num, "email": email, "reason": "Ignored by mapping"})
                        continue
                    batch_exists = conn.execute(sa_text(
                        "SELECT 1 FROM degree_batches WHERE degree_code = :d AND batch_code = :b"
                    ), {"d": degree_code, "b": mapped_batch}).fetchone()
                    if not batch_exists:
                         legacy_batch = conn.execute(sa_text(
                            "SELECT 1 FROM student_enrollments WHERE degree_code = :d AND batch = :b LIMIT 1"
                        ), {"d": degree_code, "b": mapped_batch}).fetchone()
                         if not legacy_batch:
                            errors.append({'row': row_num, 'email': email, 'error': f"Batch '{mapped_batch}' does not exist. Create it first."})
                            continue
                    
                    program_code = str(row.get('program_code', '')).strip() or None
                    branch_code = str(row.get('branch_code', '')).strip() or None

                    # Upsert Enrollment
                    enrollment_id = conn.execute(sa_text("""
                        SELECT id FROM student_enrollments
                        WHERE student_profile_id = :pid AND degree_code = :degree
                    """), {
                        "pid": profile_id, "degree": degree_code
                    }).fetchone() # Assuming one primary enrollment per degree

                    if enrollment_id:
                        conn.execute(sa_text("""
                            UPDATE student_enrollments
                            SET batch = :batch, program_code = :prog, branch_code = :branch, current_year = :year,
                            enrollment_status = :status, updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id
                        """), {
                            "batch": mapped_batch, "prog": program_code, "branch": branch_code,
                            "year": mapped_year, "status": row.get('enrollment_status', 'active'),
                            "id": enrollment_id[0]
                        })
                    else:
                        conn.execute(sa_text("""
                            INSERT INTO student_enrollments (
                            student_profile_id, degree_code, program_code, branch_code,
                            batch, current_year, enrollment_status, is_primary
                            ) VALUES (:pid, :degree, :prog, :branch, :batch, :year, :status, 1)
                        """), {
                            "pid": profile_id, "degree": degree_code, "prog": program_code,
                            "branch": branch_code, "batch": mapped_batch, "year": mapped_year,
                            "status": row.get('enrollment_status', 'active')
                        })

                # 5. MODIFIED: Process Custom Fields
                for field_code in active_custom_fields:
                    if field_code in row:
                        value = row.get(field_code)
                        # Handle NaN (from empty pandas cells)
                        if pd.isna(value):
                            value = None
                        
                        # Upsert the custom data value
                        conn.execute(sa_text("""
                            INSERT INTO student_custom_profile_data (student_profile_id, field_code, value, updated_at)
                            VALUES (:pid, :code, :val, CURRENT_TIMESTAMP)
                            ON CONFLICT(student_profile_id, field_code) DO UPDATE SET
                                value = excluded.value,
                                updated_at = CURRENT_TIMESTAMP
                        """), {
                            "pid": profile_id,
                            "code": field_code,
                            "val": str(value) if value is not None else None
                        })

                success_count += 1

            except Exception as e:
                errors.append({'row': row_num, 'email': str(row.get('email', '')).strip().lower(), 'error': str(e)})

        if dry_run:
            trans.rollback()
        else:
            trans.commit()

    except Exception:
        if trans:
            trans.rollback()
        raise

    finally:
        if should_close:
            conn.close()

    return errors, success_count, skipped_rows


# ------------------------------------------------------------------
# STATEFUL IMPORT UI - (MODIFIED)
# ------------------------------------------------------------------

def _reset_student_import_state():
    # ... (code preserved) ...
    st.session_state.student_import_step = 'initial'
    st.session_state.student_import_mappings = {}
    st.session_state.student_import_validation_data = None
    st.session_state.student_import_df = None
    log.debug("Reset student import state")


def _add_student_import_export_section(engine: Engine):
    """
    ENHANCED UI with batch creation and strict year validation.
    MODIFIED: Dynamic template download.
    """
    st.divider()
    st.subheader("📥📤 Student Import/Export")

    if not _show_no_degrees_help(engine, "student import"):
        return

    # State Initialization
    if 'student_import_step' not in st.session_state:
        _reset_student_import_state()

    # Select Degree
    with engine.begin() as conn:
        degrees = _active_degrees(conn)
        if not degrees:
            st.error("❌ No degrees available")
            return

    if st.session_state.get('student_import_degree'):
        selected_degree = st.session_state.student_import_degree
        with engine.connect() as conn:
            degree_duration = _get_degree_duration(conn, selected_degree)
            valid_years = _get_valid_years_for_degree(conn, selected_degree)
        
        st.info(f"📋 **Degree:** `{selected_degree}` | **Duration:** {degree_duration} years | **Valid Years:** {valid_years}")
    else:
        selected_degree_raw = st.selectbox("Select Degree", options=degrees, key="degree_selector_student")

        if st.button("Confirm Degree Selection", type="primary"):
            st.session_state.student_import_degree = selected_degree_raw.strip()
            st.rerun()

        st.warning("⚠️ Confirm degree selection to proceed")
        return

    # Export Section (MODIFIED)
    with st.expander("📥 Download Template"):
        
        # --- NEW: Dynamic Template Header ---
        base_columns = "name,email,student_id,date_of_joining,phone,status,degree_code,program_code,branch_code,batch,current_year,enrollment_status"
        try:
            with engine.connect() as conn:
                active_fields = conn.execute(sa_text(
                    "SELECT code FROM student_custom_profile_fields WHERE active = 1 ORDER BY sort_order, code"
                )).fetchall()
                custom_columns = [f[0] for f in active_fields]
            
            all_columns_header = base_columns + "," + ",".join(custom_columns)
            st.info(f"Template will include {len(custom_columns)} active custom field(s): {', '.join(custom_columns)}")
        except Exception as e:
            all_columns_header = base_columns
            st.warning(f"Could not fetch custom fields for template: {e}")
        # --- END: Dynamic Template Header ---

        st.download_button(
            label="Download CSV Template",
            data=all_columns_header, # Use dynamic header
            file_name="student_template.csv",
            mime="text/csv"
        )

    # Batch Creation Section
    with st.expander("➕ Create New Batch"):
        # ... (code preserved) ...
        st.markdown(f"### Create Batch for {selected_degree}")
        
        with engine.connect() as conn:
            degree_duration = _get_degree_duration(conn, selected_degree)
            valid_years = _get_valid_years_for_degree(conn, selected_degree)
        
        if not degree_duration:
            st.error(f"❌ Degree '{selected_degree}' has no duration set. Update degree settings first.")
        elif not valid_years:
            st.error(f"❌ No valid years for degree. Please update degree configuration.")
        else:
            st.success(f"✅ Degree duration: **{degree_duration} years** | Valid years: **{valid_years}**")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                batch_code = st.text_input("Batch Code*", placeholder="e.g., 2021, 2022-A")
            with col2:
                batch_name = st.text_input("Batch Name", placeholder="e.g., 2021-2025 Batch")
            with col3:
                start_date = st.date_input("Start Date* (for sorting)", datetime.now())
            
            if st.button("Create Batch", type="primary", key="create_batch_btn"):
                if not batch_code or not batch_code.strip() or not start_date:
                    st.error("❌ Batch Code and Start Date are required")
                else:
                    with engine.begin() as conn:
                        success, message = _create_batch_with_years(
                            conn, 
                            selected_degree, 
                            batch_code.strip(),
                            batch_name.strip(),
                            str(start_date)
                        )
                    
                    if success:
                        st.success(message)
                        st.cache_data.clear() # Clear cache to refresh batch lists
                        st.rerun()
                    else:
                        st.error(message)

    # File Uploader
    st.markdown("### 📤 Import Student Data")
    up = st.file_uploader("Upload CSV", type="csv", key="student_uploader")

    if st.session_state.student_import_step != 'initial':
        if st.button("🔄 Cancel", key="cancel_import"):
            _reset_student_import_state()
            st.rerun()

    st.divider()

    # State: Initial (Validation)
    if st.session_state.student_import_step == 'initial':
        # ... (code preserved) ...
        st.markdown("#### Step 1: Validate File")

        if not up:
            st.warning("Upload CSV to begin")
            if 'student_import_df' in st.session_state:
                _reset_student_import_state()
            return

        if st.button("🔍 Validate File", type="primary"):
            try:
                up.seek(0)
                df = pd.read_csv(up)

                with st.spinner("Validating..."):
                    try:
                        validation_data, filtered_df = _pre_check_student_enrollments(df, engine, selected_degree)
                        
                        # Check for invalid years (STRICT VALIDATION)
                        if validation_data.invalid_years:
                            with engine.connect() as conn:
                                degree_duration = _get_degree_duration(conn, selected_degree)
                            
                            st.error(f"""
### ❌ Invalid Years Detected
Your CSV contains years outside the degree duration.
**Degree Duration:** {degree_duration} years (valid: 1-{degree_duration})
**Invalid Years in CSV:** {sorted(validation_data.invalid_years)}
**Fix:**
1. Update your CSV to use years 1-{degree_duration} only
2. Remove rows with invalid year values
3. Upload corrected CSV
                            """)
                            return

                        st.session_state.student_import_validation_data = validation_data
                        st.session_state.student_import_df = filtered_df
                        st.session_state.student_import_mappings = {"batch": {}, "year": {}}

                        if validation_data.ignored_rows > 0:
                            st.info(f"✅ {len(filtered_df)} rows for {selected_degree}, {validation_data.ignored_rows} ignored")

                        if validation_data.unmatched_batches:
                            st.session_state.student_import_step = 'map_batches'
                        elif validation_data.unmatched_years:
                            st.session_state.student_import_step = 'map_years'
                        else:
                            st.session_state.student_import_step = 'ready_to_import'

                        st.rerun()

                    except ValueError as ve:
                        st.error(f"""
### ❌ Validation Failed
{str(ve)}
**Fix:**
1. Ensure `degree_code` column has values matching: **{selected_degree}**
2. All rows must have this degree code
3. Upload corrected CSV
                        """)
                        return
                    except Exception as inner_e:
                        st.error(f"❌ Error: {str(inner_e)}")
                        log.error(f"Validation error: {traceback.format_exc()}")
                        return
            except pd.errors.ParserError:
                st.error("❌ CSV Format Error - Invalid CSV file")
                return
            except Exception as e:
                st.error(f"❌ Unexpected Error: {str(e)}")
                log.error(f"Unexpected error: {traceback.format_exc()}")
                return

    # State: Map Batches
    elif st.session_state.student_import_step == 'map_batches':
        # ... (code preserved) ...
        st.markdown("#### Step 2: Map Batches")
        st.warning("⚠️ New batches found. Map them to existing batches or ignore.")
        st.info("To add new batches, use the '➕ Create New Batch' expander above first, then re-validate your file.")

        data: EnrollmentCheckResult = st.session_state.student_import_validation_data
        
        # RULE 1: Remove "[-- Create New --]" option
        options = ["[Select]"] + data.existing_batches + ["[-- Ignore --]"]

        valid = True
        col1, col2 = st.columns([1, 2])
        col1.markdown("**CSV Batch**")
        col2.markdown("**Action**")

        for code in sorted(list(data.unmatched_batches)):
            col1.write(f"`{code}`")
            stored = st.session_state.student_import_mappings['batch'].get(code, "[Select]")
            if stored == "[IGNORE]":
                stored = "[-- Ignore --]"
            try:
                default_idx = options.index(stored)
            except:
                default_idx = 0
            choice = col2.selectbox(f"batch_{code}", options, key=f"map_batch_{code}", index=default_idx, label_visibility="collapsed")
            if choice == "[-- Ignore --]":
                st.session_state.student_import_mappings['batch'][code] = "[IGNORE]"
            else:
                st.session_state.student_import_mappings['batch'][code] = choice
            if choice == "[Select]":
                valid = False

        if st.button("➡️ Next", type="primary"):
            if not valid:
                st.error("❌ Select action for each batch")
            else:
                data = st.session_state.student_import_validation_data
                if data.unmatched_years:
                    st.session_state.student_import_step = 'map_years'
                else:
                    st.session_state.student_import_step = 'ready_to_import'
                st.rerun()

    # State: Map Years
    elif st.session_state.student_import_step == 'map_years':
        # ... (code preserved) ...
        st.markdown("#### Step 3: Map Years")
        st.warning("⚠️ New years found. Map them to existing years or ignore.")
        data: EnrollmentCheckResult = st.session_state.student_import_validation_data
        options = ["[Select]"] + [str(y) for y in data.existing_years] + ["[-- Use New --]", "[-- Ignore --]"]
        valid = True
        col1, col2 = st.columns([1, 2])
        col1.markdown("**CSV Year**")
        col2.markdown("**Action**")
        for code in sorted(list(data.unmatched_years)):
            col1.write(f"`{code}`")
            stored = st.session_state.student_import_mappings['year'].get(code, "[Select]")
            if stored == "[USE_NEW]":
                stored = "[-- Use New --]"
            elif stored == "[IGNORE]":
                stored = "[-- Ignore --]"
            try:
                default_idx = options.index(stored)
            except:
                default_idx = 0
            choice = col2.selectbox(f"year_{code}", options, key=f"map_year_{code}", index=default_idx, label_visibility="collapsed")
            if choice == "[-- Use New --]":
                st.session_state.student_import_mappings['year'][code] = "[USE_NEW]"
            elif choice == "[-- Ignore --]":
                st.session_state.student_import_mappings['year'][code] = "[IGNORE]"
            else:
                st.session_state.student_import_mappings['year'][code] = choice
            if choice == "[Select]":
                valid = False
        if st.button("➡️ Next", type="primary"):
            if not valid:
                st.error("❌ Select action for each year")
            else:
                st.session_state.student_import_step = 'ready_to_import'
                st.rerun()

    # State: Ready to Import
    elif st.session_state.student_import_step == 'ready_to_import':
        # ... (code preserved) ...
        st.markdown("#### Step 4: Review & Import")
        st.success("✅ Ready to import")
        if 'student_import_df' not in st.session_state or st.session_state.student_import_df is None:
            st.error("❌ Session data lost")
            return
        with st.expander("🔍 Mappings"):
            st.json(st.session_state.student_import_mappings)
        df_to_import = st.session_state.student_import_df
        mappings = st.session_state.student_import_mappings
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🧪 Dry Run", key="dry_run"):
                with engine.begin() as conn:
                    trans = conn.begin_nested()
                    try:
                        with st.spinner("Dry run..."):
                            errors, success, skipped = _import_students_with_validation(
                                engine, df_to_import, dry_run=True, mappings=mappings, conn_for_transaction=conn
                            )
                        if errors:
                            st.warning(f"⚠️ {success} OK, {len(errors)} errors")
                            st.dataframe(pd.DataFrame(errors)[['row', 'email', 'error']], use_container_width=True)
                        else:
                            st.success(f"✅ Dry run successful. All {success} records OK.")
                        if skipped:
                            st.info(f"ℹ️ {len(skipped)} skipped")
                    finally:
                        trans.rollback()
        with col2:
            if st.button("🚀 Import", key="execute", type="primary"):
                try:
                    with st.spinner("Importing..."):
                        errors, success, skipped = _import_students_with_validation(
                            engine, df_to_import, dry_run=False, mappings=mappings
                        )
                    if errors:
                        st.error(f"❌ {len(errors)} errors in {len(df_to_import)} rows")
                        st.dataframe(pd.DataFrame(errors)[['row', 'email', 'error']], use_container_width=True)
                        st.download_button(
                            "Download Errors",
                            pd.DataFrame(errors).to_csv(index=False),
                            "errors.csv",
                            "text/csv"
                        )
                    else:
                        st.success(f"✅ Imported {success} students")
                    if skipped:
                        st.info(f"ℹ️ {len(skipped)} skipped")
                    st.cache_data.clear()
                    _reset_student_import_state()
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Import failed: {str(e)}")
                    log.error(f"Import failed: {traceback.format_exc()}")


# ------------------------------------------------------------------
# UI SECTION 2: STUDENT MOVER (Unchanged)
# ------------------------------------------------------------------

def _add_student_mover_section(engine: Engine):
    # ... (code preserved) ...
    st.divider()
    st.subheader("🚚 Student Mover")

    if not _show_no_degrees_help(engine, "student moving"):
        return

    st.info("Move students between batches/degrees. A 30-day cooldown applies to all moves.")

    # RULE 2: Load the 'next batch only' setting
    with engine.begin() as conn:
        _init_settings_table(conn) # Ensure settings table exists
        all_degrees = _active_degrees(conn)
        next_batch_only = _get_setting(conn, "mover_next_only", "True") == "True"

        if not all_degrees:
            st.warning("No degrees found")
            return

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**1. Select Source**")
        from_degree = st.selectbox("From Degree", all_degrees, key="move_from_degree")

        with engine.begin() as conn:
            from_batches_data = _db_get_batches_for_degree(conn, from_degree)
            from_batches = [b['code'] for b in from_batches_data]

        if not from_batches:
            st.warning(f"No batches found for {from_degree}. Create batches in the Import tab.")
            return

        from_batch = st.selectbox("From Batch", from_batches, key="move_from_batch")

        if st.button("Get Students"):
            with engine.begin() as conn:
                df_students = _db_get_students_for_mover(conn, from_degree, from_batch)

            st.session_state.students_to_move_df = df_students

        if "students_to_move_df" not in st.session_state:
            st.write("Click 'Get Students'")
            return

        st.markdown("**2. Select Students**")
        df_students = st.session_state.students_to_move_df
        
        # Add human-readable cooldown column
        df_students['On Cooldown'] = False
        if 'Last Moved On' in df_students.columns:
            thirty_days_ago = datetime.now() - timedelta(days=30)
            last_moved_dt = pd.to_datetime(df_students['Last Moved On'], errors='coerce')
            df_students['On Cooldown'] = (last_moved_dt > thirty_days_ago)
        
        edited_df = st.data_editor(
            df_students, 
            key="mover_editor", 
            use_container_width=True,
            column_order=["Move", "Student ID", "Name", "On Cooldown", "Last Moved On"],
            column_config={
                "Profile ID": None, # Hide
                "Enrollment ID": None, # Hide
                "Last Moved On": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
                "On Cooldown": st.column_config.CheckboxColumn(disabled=True)
            },
            disabled=["Student ID", "Name", "Email", "Current Year", "Last Moved On", "On Cooldown"]
        )
        students_to_move_df = edited_df[edited_df["Move"] == True]

    with col2:
        st.markdown("**3. Select Destination**")
        to_degree = st.selectbox("To Degree", all_degrees, key="move_to_degree")

        with engine.begin() as conn:
            to_batches_data = _db_get_batches_for_degree(conn, to_degree)
            to_batches_all = [b['code'] for b in to_batches_data]

        # RULE 2: Filter 'to_batches' if policy is enabled
        to_batches_options = to_batches_all
        if next_batch_only and from_degree == to_degree and from_batch in to_batches_all:
            st.info("ℹ️ Policy active: Only the next sequential batch is shown.")
            try:
                current_index = to_batches_all.index(from_batch)
                if current_index + 1 < len(to_batches_all):
                    # Show only the next batch
                    to_batches_options = [to_batches_all[current_index + 1]]
                else:
                    # This is the last batch, no 'next'
                    to_batches_options = []
            except ValueError:
                pass # from_batch not in list, show all

        option = st.radio(
            "Batch", 
            ["Existing", "New"], 
            horizontal=True,
            # RULE 2: Disable "New" if policy is on
            disabled=next_batch_only
        )

        to_batch = None
        if option == "Existing":
            if not to_batches_options:
                st.error(f"No batches for {to_degree}" + (" (or no 'next' batch)" if next_batch_only else ""))
                return
            to_batch = st.selectbox("To Batch", to_batches_options, key="move_to_batch")
        else:
            to_batch = st.text_input("New Batch", key="move_new_batch")
            st.warning("Creating new batches this way is not recommended. Use the Import tab.")

        to_year = st.number_input("Year", min_value=1, max_value=10, value=1)

    st.divider()

    if students_to_move_df.empty:
        st.warning("Select students to move")
        return

    if not to_batch:
        st.warning("Select/enter destination batch")
        return

    st.warning(f"Move {len(students_to_move_df)} students to {to_degree} Batch {to_batch} Year {to_year}")

    if st.button("🚀 Execute", type="primary"):
        
        # RULE 3: Implement 30-day Cooldown Check
        on_cooldown_df = students_to_move_df[students_to_move_df["On Cooldown"] == True]
        valid_to_move_df = students_to_move_df[students_to_move_df["On Cooldown"] == False]
        
        enrollment_ids = valid_to_move_df["Enrollment ID"].tolist()
        
        moved = 0
        if not enrollment_ids:
            st.error("❌ No valid students to move (all selected are on cooldown).")
            return

        try:
            with engine.begin() as conn:
                moved = _db_move_students(conn, enrollment_ids, to_degree, to_batch, to_year)

            success_msg = f"✅ Moved {moved} students."
            warning_msg = ""
            
            if not on_cooldown_df.empty:
                warning_msg = f" ⚠️ {len(on_cooldown_df)} students were not moved as they are on a 30-day cooldown."
            
            st.success(success_msg + warning_msg)
            st.cache_data.clear()

            if "students_to_move_df" in st.session_state:
                del st.session_state.students_to_move_df

            st.rerun()

        except Exception as e:
            st.error(f"❌ Failed: {str(e)}")
            log.error(f"Move failed: {traceback.format_exc()}")


# ------------------------------------------------------------------
# UI SECTION 3: CREDENTIAL EXPORT (Unchanged)
# ------------------------------------------------------------------

def _add_student_credential_export_section(engine: Engine):
    # ... (code preserved) ...
    st.divider()
    st.subheader("🔑 Export Credentials")

    with engine.begin() as conn:
        degrees = _active_degrees(conn)

    st.info("Export usernames and initial passwords for students who have not logged in.")

    if st.button("Generate & Download", disabled=(not degrees)):
        try:
            with st.spinner("Generating..."):
                df_creds = _get_student_credentials_to_export(engine)

            if df_creds.empty:
                st.warning("No new credentials to export")
                return

            csv = df_creds.to_csv(index=False)

            st.download_button(
                "Download Credentials",
                data=csv,
                file_name="student_credentials.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"❌ Failed: {str(e)}")
            log.error(f"Export failed: {traceback.format_exc()}")


# ------------------------------------------------------------------
# NEW: UI SECTION 4: STUDENT DATA EXPORTER
# ------------------------------------------------------------------

@st.cache_data
def _get_student_data_to_export(_engine: Engine) -> pd.DataFrame:
    """
    Fetches all student profile, enrollment, and custom field data
    and merges it into a single wide DataFrame for export.
    """
    with _engine.connect() as conn:
        # 1. Fetch base profile and enrollment data
        base_sql = """
            SELECT
                p.id as student_profile_id,
                p.name,
                p.email,
                p.student_id,
                p.phone,
                p.status,
                e.degree_code,
                e.program_code,
                e.branch_code,
                e.batch,
                e.current_year,
                e.enrollment_status
            FROM student_profiles p
            LEFT JOIN student_enrollments e ON p.id = e.student_profile_id AND e.is_primary = 1
            ORDER BY p.student_id
        """
        base_df = pd.read_sql_query(base_sql, conn)
        
        if base_df.empty:
            return pd.DataFrame()

        # 2. Fetch all custom data in long format
        custom_sql = """
            SELECT
                student_profile_id,
                field_code,
                value
            FROM student_custom_profile_data
        """
        custom_df = pd.read_sql_query(custom_sql, conn)
        
        if custom_df.empty:
            # No custom data, just return base data
            return base_df

        # 3. Pivot custom data from long to wide
        try:
            pivoted_df = custom_df.pivot(
                index='student_profile_id',
                columns='field_code',
                values='value'
            ).reset_index()
        except Exception as e:
            log.error(f"Failed to pivot custom data: {e}")
            return base_df # Return base data on pivot failure

        # 4. Merge base data with pivoted custom data
        final_df = pd.merge(
            base_df,
            pivoted_df,
            on='student_profile_id',
            how='left'
        )
        
        # 5. Get all custom field codes to ensure columns exist even if no data
        all_custom_fields = conn.execute(sa_text(
            "SELECT code FROM student_custom_profile_fields ORDER BY code"
        )).fetchall()
        all_custom_codes = [f[0] for f in all_custom_fields]
        
        for code in all_custom_codes:
            if code not in final_df.columns:
                final_df[code] = None # Add empty column
                
        # Reorder columns: base, then custom
        final_columns = base_df.columns.tolist() + all_custom_codes
        # Remove duplicates
        final_columns_ordered = []
        for col in final_columns:
            if col not in final_columns_ordered:
                final_columns_ordered.append(col)

        return final_df[final_columns_ordered]


def _add_student_data_export_section(engine: Engine):
    """UI for the new full student data exporter."""
    st.divider()
    st.subheader("📊 Export Full Student Data")
    st.info("Download a single CSV file containing all student profile, enrollment, and custom field data.")

    if st.button("Generate & Download Student Data"):
        try:
            with st.spinner("Generating full student export..."):
                df_export = _get_student_data_to_export(engine)

            if df_export.empty:
                st.warning("No student data to export.")
                return

            csv = df_export.to_csv(index=False)

            st.download_button(
                "Download Data",
                data=csv,
                file_name="student_full_export.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"❌ Failed to export data: {str(e)}")
            log.error(f"Full export failed: {traceback.format_exc()}")
