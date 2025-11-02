# app/screens/students/importer.py
# -------------------------------------------------------------------
# FULLY ENHANCED IMPORTER WITH STRICT YEAR VALIDATION & BATCH CREATION
# -------------------------------------------------------------------
# Features:
# - Degree duration enforcement (e.g., BARCH = 5 years)
# - Automatic year scaffold creation (1-5 for 5-year degree)
# - Batch creation UI with validation
# - Strict validation: rejects years outside degree duration
# - Better error messages
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

log = logging.getLogger(__name__)


# ------------------------------------------------------------------
# NEW: Degree Duration & Year Management
# ------------------------------------------------------------------

def _get_degree_duration(conn: Connection, degree_code: str) -> Optional[int]:
    """
    Get the defined duration (years) for a degree from degree_struct table.
    """
    try:
        # Query degree_struct table (where years are stored)
        result = conn.execute(sa_text("""
            SELECT years FROM degree_struct WHERE degree_code = :code
        """), {"code": degree_code}).fetchone()
        
        if result and result[0]:
            return int(result[0])
        
        return None
    except Exception as e:
        log.warning(f"Could not fetch degree duration: {e}")
        return None


def _ensure_degree_years_scaffold(conn: Connection, degree_code: str) -> bool:
    """
    Creates dummy enrollment records for years 1 to degree_duration
    so the system knows these are valid years for the degree.
    
    Returns True if successful, False if degree has no duration set.
    """
    duration = _get_degree_duration(conn, degree_code)
    if not duration or duration < 1:
        return False
    
    try:
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
    """
    Get all valid years for a degree based on:
    1. Degree duration definition
    2. Existing year scaffolds
    3. Existing student enrollments
    
    Returns list of valid year numbers (1, 2, 3, 4, 5, etc.)
    """
    valid_years = set()
    
    # 1. From degree duration
    duration = _get_degree_duration(conn, degree_code)
    if duration and duration > 0:
        valid_years.update(range(1, int(duration) + 1))
    
    # 2. From existing scaffolds
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


def _create_batch_with_years(conn: Connection, degree_code: str, batch_code: str) -> Tuple[bool, str]:
    """
    Creates a batch and automatically generates enrollment year scaffolds.
    
    Returns: (success: bool, message: str)
    """
    try:
        # Ensure degree year scaffold exists
        scaffold_ok = _ensure_degree_years_scaffold(conn, degree_code)
        
        # Check if batch already exists for this degree
        batch_exists = conn.execute(sa_text("""
            SELECT 1 FROM student_enrollments 
            WHERE degree_code = :degree AND batch = :batch LIMIT 1
        """), {"degree": degree_code, "batch": batch_code}).fetchone()
        
        if batch_exists:
            return False, f"❌ Batch '{batch_code}' already exists for degree {degree_code}"
        
        # Get valid years for this degree
        valid_years = _get_valid_years_for_degree(conn, degree_code)
        
        if not valid_years:
            return False, f"❌ Degree {degree_code} has no defined years. Set degree duration first."
        
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
        log.error(error_msg)
        return False, error_msg


# ------------------------------------------------------------------
# Helpers for Stateful Import
# ------------------------------------------------------------------

@dataclass
class EnrollmentCheckResult:
    """Holds validation data for import."""
    unmatched_batches: Set[str] = field(default_factory=set)
    existing_batches: List[str] = field(default_factory=list)
    unmatched_years: Set[str] = field(default_factory=set)
    existing_years: List[int] = field(default_factory=list)
    ignored_rows: int = 0
    invalid_years: Set[int] = field(default_factory=set)  # Years outside degree duration


def _pre_check_student_enrollments(df: pd.DataFrame, engine: Engine, degree_code: str) -> Tuple[EnrollmentCheckResult, pd.DataFrame]:
    """
    Compares CSV data against database with STRICT year validation.
    """
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

    # 4. Get existing batches and years from database
    with engine.connect() as conn:
        batch_res = conn.execute(sa_text("""
            SELECT DISTINCT batch FROM student_enrollments 
            WHERE degree_code = :degree ORDER BY batch
        """), {"degree": degree_code_clean}).fetchall()
        
        year_res = conn.execute(sa_text("""
            SELECT DISTINCT current_year FROM student_enrollments 
            WHERE degree_code = :degree ORDER BY current_year
        """), {"degree": degree_code_clean}).fetchall()
        
        db_batches = [r[0] for r in batch_res]
        db_years = sorted([int(r[0]) for r in year_res if r[0] is not None])

    # 5. Find mismatches
    result = EnrollmentCheckResult(
        unmatched_batches=csv_batches - set(db_batches),
        existing_batches=sorted(db_batches),
        unmatched_years=csv_years - set(db_years),
        existing_years=db_years,
        ignored_rows=ignored_rows,
        invalid_years=invalid_years
    )

    return result, df_filtered


def _build_translation_map(
    mappings: Dict[str, Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    """Converts UI mappings into translation map."""
    translation_map = {}
    for aff_type, type_mappings in mappings.items():
        translation_map[aff_type] = {}
        for code, action in type_mappings.items():
            if action == "[USE_NEW]":
                translation_map[aff_type][code] = code
            else:
                translation_map[aff_type][code] = action

    return translation_map


# ------------------------------------------------------------------
# Main Import Logic
# ------------------------------------------------------------------

def _show_no_degrees_help(engine: Engine, context: str = "student operations"):
    """Shows setup guide when no degrees exist."""
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
                            conn.execute(sa_text("""
                                INSERT INTO degrees (code, name, active, sort_order, years_in_degree, created_at, updated_at)
                                VALUES (:code, :name, 1, 0, :years, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            """), {
                                "code": degree_code.strip(),
                                "name": degree_name.strip() or degree_code.strip(),
                                "years": int(duration)
                            })
                            
                            # Auto-scaffold years
                            with conn.begin_nested() as nested_conn:
                                _ensure_degree_years_scaffold(nested_conn, degree_code.strip())
                            
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
    """
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
    errors: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []
    success_count = 0
    translation_map = _build_translation_map(mappings) if mappings else {}

    # Metadata fetch
    with engine.begin() as meta_conn:
        all_degrees = _active_degrees(meta_conn)

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
                student_id = str(row.get('student_id', '')).strip()
                name = str(row.get('name', '')).strip()

                if not name or not email or not student_id:
                    errors.append({'row': row_num, 'email': email, 'error': "Missing required fields: name, email, student_id"})
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
                        VALUES (:name, :email, :sid, :phone, :status) RETURNING id
                    """), {
                        "name": name, "email": email, "sid": student_id,
                        "phone": row.get('phone'), "status": row.get('status', 'active')
                    })
                    profile_id = res.fetchone()[0]

                # 3. Ensure Credentials
                _ensure_student_username_and_initial_creds(conn, profile_id, email, name, student_id)

                # 4. Process Enrollment
                degree_code = str(row.get('degree_code', '')).strip()

                if degree_code:
                    if degree_code not in all_degrees:
                        errors.append({'row': row_num, 'email': email, 'error': f"Degree '{degree_code}' not found"})
                        continue

                    batch = str(row.get('batch', '')).strip()
                    current_year = str(row.get('current_year', '')).strip()

                    if not batch or not current_year:
                        errors.append({'row': row_num, 'email': email, 'error': "Missing batch or year"})
                        continue

                    # STRICT: Validate year is within degree duration
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

                    # Apply mappings
                    mapped_batch = translation_map.get('batch', {}).get(batch, batch)
                    mapped_year = translation_map.get('year', {}).get(current_year, current_year)

                    # Check for [IGNORE]
                    if mapped_batch == "[IGNORE]" or mapped_year == "[IGNORE]":
                        skipped_rows.append({"row": row_num, "email": email, "reason": "Ignored by mapping"})
                        continue

                    program_code = str(row.get('program_code', '')).strip() or None
                    branch_code = str(row.get('branch_code', '')).strip() or None

                    # Upsert Enrollment
                    enrollment_id = conn.execute(sa_text("""
                        SELECT id FROM student_enrollments
                        WHERE student_profile_id = :pid AND degree_code = :degree AND batch = :batch
                    """), {
                        "pid": profile_id, "degree": degree_code, "batch": mapped_batch
                    }).fetchone()

                    if enrollment_id:
                        conn.execute(sa_text("""
                            UPDATE student_enrollments
                            SET program_code = :prog, branch_code = :branch, current_year = :year,
                            enrollment_status = :status, updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id
                        """), {
                            "prog": program_code, "branch": branch_code,
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
# STATEFUL IMPORT UI - FULLY ENHANCED
# ------------------------------------------------------------------

def _reset_student_import_state():
    """Resets session state."""
    st.session_state.student_import_step = 'initial'
    st.session_state.student_import_mappings = {}
    st.session_state.student_import_validation_data = None
    st.session_state.student_import_df = None
    log.debug("Reset student import state")


def _add_student_import_export_section(engine: Engine):
    """
    ENHANCED UI with batch creation and strict year validation.
    """
    st.divider()
    st.subheader("🔥📤 Student Import/Export (Enhanced)")

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

    # Export Section
    with st.expander("📥 Export Template"):
        st.download_button(
            label="Download CSV Template",
            data="name,email,student_id,phone,status,degree_code,program_code,branch_code,batch,current_year,enrollment_status",
            file_name="student_template.csv",
            mime="text/csv"
        )

    # Batch Creation Section
    with st.expander("➕ Create New Batch"):
        st.markdown("### Create Batch for This Degree")
        
        with engine.connect() as conn:
            degree_duration = _get_degree_duration(conn, selected_degree)
            valid_years = _get_valid_years_for_degree(conn, selected_degree)
        
        if not degree_duration:
            st.error(f"❌ Degree '{selected_degree}' has no duration set. Update degree settings first.")
        elif not valid_years:
            st.error(f"❌ No valid years for degree. Please update degree configuration.")
        else:
            st.success(f"✅ Degree duration: **{degree_duration} years** | Valid years: **{valid_years}**")
            
            batch_name = st.text_input("New Batch Name", placeholder="e.g., 2021, 2022")
            
            if st.button("Create Batch", type="primary", key="create_batch_btn"):
                if not batch_name or not batch_name.strip():
                    st.error("❌ Batch name required")
                else:
                    with engine.begin() as conn:
                        success, message = _create_batch_with_years(conn, selected_degree, batch_name.strip())
                    
                    if success:
                        st.success(message)
                        st.cache_data.clear()
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
        st.markdown("#### Step 2: Map Batches")
        st.warning("⚠️ New batches found - map them")

        data: EnrollmentCheckResult = st.session_state.student_import_validation_data
        options = ["[Select]"] + data.existing_batches + ["[-- Create New --]", "[-- Ignore --]"]

        valid = True
        col1, col2 = st.columns([1, 2])
        col1.markdown("**CSV Batch**")
        col2.markdown("**Action**")

        for code in sorted(list(data.unmatched_batches)):
            col1.write(f"`{code}`")
            
            stored = st.session_state.student_import_mappings['batch'].get(code, "[Select]")
            if stored == "[USE_NEW]":
                stored = "[-- Create New --]"
            elif stored == "[IGNORE]":
                stored = "[-- Ignore --]"
            
            try:
                default_idx = options.index(stored)
            except:
                default_idx = 0
            
            choice = col2.selectbox(f"batch_{code}", options, key=f"map_batch_{code}", index=default_idx, label_visibility="collapsed")
            
            if choice == "[-- Create New --]":
                st.session_state.student_import_mappings['batch'][code] = "[USE_NEW]"
            elif choice == "[-- Ignore --]":
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
        st.markdown("#### Step 3: Map Years (STRICT)")
        st.warning("⚠️ New years found - but only 1-{degree_duration} are valid")

        data: EnrollmentCheckResult = st.session_state.student_import_validation_data
        with engine.connect() as conn:
            degree_duration = _get_degree_duration(conn, selected_degree)
        
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
                            st.success(f"✅ All {success} records OK")
                        
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
# UI SECTION 2: STUDENT MOVER (unchanged)
# ------------------------------------------------------------------

def _add_student_mover_section(engine: Engine):
    """Student mover UI."""
    st.divider()
    st.subheader("🚚 Student Mover")

    if not _show_no_degrees_help(engine, "student moving"):
        return

    st.info("Move students between batches/degrees")

    with engine.begin() as conn:
        all_degrees = _active_degrees(conn)

        if not all_degrees:
            st.warning("No degrees found")
            return

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**1. Select Students**")
        from_degree = st.selectbox("From Degree", all_degrees, key="move_from_degree")

        with engine.begin() as conn:
            from_batches = _db_get_batches_for_degree(conn, from_degree)

        if not from_batches:
            st.warning(f"No batches found for {from_degree}")
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
        edited_df = st.data_editor(df_students, key="mover_editor", use_container_width=True)
        students_to_move = edited_df[edited_df["Move"] == True]

    with col2:
        st.markdown("**3. Destination**")
        to_degree = st.selectbox("To Degree", all_degrees, key="move_to_degree")

        with engine.begin() as conn:
            to_batches = _db_get_batches_for_degree(conn, to_degree)

        option = st.radio("Batch", ["Existing", "New"], horizontal=True)

        if option == "Existing":
            if not to_batches:
                st.error(f"No batches for {to_degree}")
                return
            to_batch = st.selectbox("To Batch", to_batches, key="move_to_batch")
        else:
            to_batch = st.text_input("New Batch", key="move_new_batch")

        to_year = st.number_input("Year", min_value=1, max_value=10, value=1)

    st.divider()

    if students_to_move.empty:
        st.warning("Select students to move")
        return

    if not to_batch:
        st.warning("Select/enter batch")
        return

    st.warning(f"Move {len(students_to_move)} students to {to_degree} Batch {to_batch} Year {to_year}")

    if st.button("🚀 Execute", type="primary"):
        enrollment_ids = students_to_move["Enrollment ID"].tolist()

        try:
            with engine.begin() as conn:
                moved = _db_move_students(conn, enrollment_ids, to_degree, to_batch, to_year)

            st.success(f"✅ Moved {moved} students")
            st.cache_data.clear()

            if "students_to_move_df" in st.session_state:
                del st.session_state.students_to_move_df

            st.rerun()

        except Exception as e:
            st.error(f"❌ Failed: {str(e)}")
            log.error(f"Move failed: {traceback.format_exc()}")


# ------------------------------------------------------------------
# UI SECTION 3: CREDENTIAL EXPORT (unchanged)
# ------------------------------------------------------------------

def _add_student_credential_export_section(engine: Engine):
    """Export credentials."""
    st.divider()
    st.subheader("🔑 Export Credentials")

    with engine.begin() as conn:
        degrees = _active_degrees(conn)

    st.info("Export usernames and initial passwords")

    if st.button("Generate & Download", disabled=(not degrees)):
        try:
            with st.spinner("Generating..."):
                df_creds = _get_student_credentials_to_export(engine)

            if df_creds.empty:
                st.warning("No credentials to export")
                return

            csv = df_creds.to_csv(index=False)

            st.download_button(
                "Download",
                data=csv,
                file_name="credentials.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"❌ Failed: {str(e)}")
            log.error(f"Export failed: {traceback.format_exc()}")
