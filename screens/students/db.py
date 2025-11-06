# app/screens/students/db.py
# -------------------------------------------------------------------
# MODIFIED VERSION
# - Fetches batches from new 'degree_batches' table for proper sorting.
# - Fetches 'updated_at' for student mover cooldown.
# - Adds student_mover_audit logging for moves.
# - Adds publish guardrail checks & a small Streamlit renderer.
# -------------------------------------------------------------------

from __future__ import annotations

from typing import List, Dict, Any
import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text as sa_text
import logging

...
# --- Batch & Year Helpers -----------------------------------------------------


@st.cache_data
def _db_get_batches_for_degree(_conn: Connection, degree_code: str) -> List[Dict[str, Any]]:
    """
    Fetches batches for a given degree from the new degree_batches table,
    ordered by sort_order then batch_code.

    Also detects "legacy" batches that appear in student_enrollments but not
    yet defined in degree_batches, and appends them at the end.
    """
    # 1) Get batches from the formal degree_batches table
    rows = _conn.execute(
        sa_text("""
            SELECT batch_code, batch_name, start_date
            FROM degree_batches
            WHERE degree_code = :degree
            ORDER BY start_date, batch_code
        """),
        {"degree": degree_code}
    ).fetchall()

    # 2) Detect legacy batches (in enrollments but not degree_batches)
    legacy_rows = _conn.execute(
        sa_text("""
            SELECT DISTINCT e.batch
            FROM student_enrollments e
            LEFT JOIN degree_batches db
              ON e.degree_code = db.degree_code
             AND e.batch = db.batch_code
            WHERE e.degree_code = :degree
              AND db.id IS NULL
            ORDER BY e.batch
        """),
        {"degree": degree_code}
    ).fetchall()

    batch_list: List[Dict[str, Any]] = [
        {"code": r[0], "name": r[1], "start_date": r[2]} for r in rows
    ]

    for legacy in legacy_rows:
        code = legacy[0]
        if code is None:
            continue
        # Avoid duplicates if somehow overlapping
        if not any(b["code"] == code for b in batch_list):
            batch_list.append({"code": code, "name": code, "start_date": None})

    return batch_list


@st.cache_data
def _db_get_years_for_degree(_conn: Connection, degree_code: str) -> List[int]:
    # ...
    rows = _conn.execute(
        sa_text(
            "SELECT DISTINCT current_year FROM student_enrollments WHERE degree_code = :degree ORDER BY current_year"
        ),
        {"degree": degree_code},
    ).fetchall()
    return [r[0] for r in rows if r[0] is not None]


# --- Student Credentials Helpers ---------------------------------------------


def _generate_student_username(conn: Connection, full_name: str, student_id: str) -> str:
    """
    Generate a simple username based on student name and id.

    This is intentionally simple: first part of the name + last 4 digits of id.
    You can replace this later with the Slide 13 pattern.
    """
    if not full_name:
        base = "student"
    else:
        base = full_name.strip().split()[0].lower()

    suffix = ""
    if student_id and len(student_id) >= 4:
        suffix = student_id[-4:]
    elif student_id:
        suffix = student_id

    username = f"{base}{suffix}".lower()

    # Ensure uniqueness by appending a counter if needed
    counter = 0
    candidate = username
    while True:
        row = conn.execute(
            sa_text("SELECT 1 FROM student_profiles WHERE lower(username)=lower(:u)"),
            {"u": candidate},
        ).fetchone()
        if not row:
            break
        counter += 1
        candidate = f"{username}{counter}"

    return candidate


def _initial_student_password_from_name(full_name: str, student_id: str) -> str:
    """
    Generate an initial password from name & student_id.

    This roughly follows the {first5lower}@{4digits} idea, but can be
    replaced later with stricter Slide 13 password policies.
    """
    if not full_name:
        prefix = "stud"
    else:
        prefix = full_name.replace(" ", "")[:5].lower()

    digits = "0000"
    if student_id and len(student_id) >= 4:
        digits = student_id[-4:]
    elif student_id:
        digits = student_id.zfill(4)[-4:]

    return f"{prefix}@{digits}"


def _ensure_student_username_and_initial_creds(
    conn: Connection,
    student_profile_id: int,
    full_name: str,
    student_id: str,
) -> None:
    """
    Ensures a student profile has a username and a re-exportable initial password.
    """
    prof = conn.execute(
        sa_text("SELECT username FROM student_profiles WHERE id = :id"),
        {"id": student_profile_id},
    ).fetchone()

    if not prof:
        return

    username = prof[0]

    if not username:
        username = _generate_student_username(conn, full_name, student_id)
        conn.execute(
            sa_text(
                "UPDATE student_profiles SET username = :u, updated_at = CURRENT_TIMESTAMP WHERE id = :id"
            ),
            {"u": username, "id": student_profile_id},
        )

    # Ensure initial credentials row exists & is exportable
    cred = conn.execute(
        sa_text(
            """
            SELECT id, plaintext, consumed 
            FROM student_initial_credentials 
            WHERE student_profile_id = :sid
        """
        ),
        {"sid": student_profile_id},
    ).fetchone()

    if cred is None:
        password = _initial_student_password_from_name(full_name, student_id)
        conn.execute(
            sa_text(
                """
                INSERT INTO student_initial_credentials
                    (student_profile_id, username, plaintext, consumed)
                VALUES
                    (:sid, :username, :plaintext, 0)
            """
            ),
            {"sid": student_profile_id, "username": username, "plaintext": password},
        )
    else:
        # If credentials exist but were consumed, we do NOT automatically reset them here.
        # A separate "force reset" workflow can be wired in later.
        pass


@st.cache_data
def _get_student_credentials_to_export(_engine: Engine) -> pd.DataFrame:
    """
    Fetches all student credentials marked for export.
    
    NOTE: The _engine parameter uses leading underscore to prevent Streamlit
    from trying to hash it (SQLAlchemy Engine objects are not hashable).
    """
    with _engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT 
                c.id AS cred_id,
                p.student_id,
                p.name,
                p.email,
                c.username,
                c.plaintext
            FROM student_initial_credentials c
            JOIN student_profiles p ON p.id = c.student_profile_id
            WHERE c.consumed = 0
            ORDER BY p.student_id
        """)).fetchall()
    
    if not rows:
        return pd.DataFrame(columns=[
            "cred_id", "student_id", "name", "email", "username", "password"
        ])
    
    df = pd.DataFrame(rows, columns=[
        "cred_id", "student_id", "name", "email", "username", "password"
    ])
    return df


def mark_credentials_exported(engine: Engine, cred_ids: List[int]) -> None:
    """
    Mark initial credentials as exported (available to show only once).
    """
    if not cred_ids:
        return
    
    with engine.begin() as conn:
        params = {f"id_{i}": cid for i, cid in enumerate(cred_ids)}
        in_clause = ", ".join([f":{key}" for key in params.keys()])
        conn.execute(sa_text(f"""
            UPDATE student_initial_credentials
            SET consumed = 1
            WHERE id IN ({in_clause})
        """), params)


# --- Student Mover Helpers ----------------------------------------------------


@st.cache_data
def _db_get_students_for_mover(_conn: Connection, degree_code: str, batch: str) -> pd.DataFrame:
    """
    Get students for the mover tool.
    **MODIFIED**: Also fetches 'e.updated_at' for cooldown logic.
    
    NOTE: The _conn parameter uses leading underscore to prevent Streamlit
    from trying to hash it.
    """
    rows = _conn.execute(
        sa_text("""
            SELECT 
                p.id, 
                p.student_id, 
                p.name, 
                p.email,
                e.current_year,
                e.id AS enrollment_id,
                e.updated_at
            FROM student_enrollments e
            JOIN student_profiles p ON p.id = e.student_profile_id
            WHERE e.degree_code = :degree
              AND e.batch = :batch
              AND e.is_primary = 1
              AND (e.enrollment_status IS NULL OR e.enrollment_status='active')
            ORDER BY p.student_id
        """),
        {"degree": degree_code, "batch": batch}
    ).fetchall()
    
    df = pd.DataFrame(rows, columns=[
        "Profile ID", "Student ID", "Name", "Email", "Current Year", "Enrollment ID", "Last Moved On"
    ])
    df["Move"] = False
    return df


def _db_move_students(
    conn: Connection,
    enrollment_ids_to_move: List[int],
    to_degree: str,
    to_batch: str,
    to_year: int,
    reason: str | None = None,
) -> int:
    """Move students by updating their enrollment records.

    NOTE:
        - This function is NOT cached because it modifies data.
        - It also logs moves into the student_mover_audit table so that
          we have an audit trail of before/after state. The `reason`
          parameter is optional; existing callers do not need to pass it.
    """
    if not enrollment_ids_to_move:
        return 0

    # Create a parameter list for the IN clause
    params = {f"id_{i}": eid for i, eid in enumerate(enrollment_ids_to_move)}
    in_clause = ", ".join([f":{key}" for key in params.keys()])

    # Ensure audit table exists and capture BEFORE state for the selected enrollments
    _ensure_student_mover_audit_table(conn)
    before_rows = conn.execute(sa_text(f"""
        SELECT
            id,
            student_profile_id,
            degree_code,
            batch,
            current_year,
            program_code,
            branch_code,
            division_code
        FROM student_enrollments
        WHERE id IN ({in_clause})
    """), params).fetchall()

    # Perform the actual move/update
    res = conn.execute(sa_text(f"""
        UPDATE student_enrollments
        SET degree_code = :to_degree,
            batch = :to_batch,
            current_year = :to_year,
            program_code = NULL,
            branch_code = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id IN ({in_clause})
    """), {
        "to_degree": to_degree,
        "to_batch": to_batch,
        "to_year": to_year,
        **params,
    })

    # Insert audit rows based on BEFORE state and the new target
    for row in before_rows:
        (
            enrollment_id,
            student_profile_id,
            from_degree_code,
            from_batch,
            from_year,
            from_program_code,
            from_branch_code,
            from_division_code,
        ) = row

        conn.execute(sa_text("""
            INSERT INTO student_mover_audit (
                moved_by,
                student_profile_id,
                enrollment_id,
                from_degree_code,
                from_batch,
                from_year,
                from_program_code,
                from_branch_code,
                from_division_code,
                to_degree_code,
                to_batch,
                to_year,
                reason
            ) VALUES (
                :moved_by,
                :student_profile_id,
                :enrollment_id,
                :from_degree_code,
                :from_batch,
                :from_year,
                :from_program_code,
                :from_branch_code,
                :from_division_code,
                :to_degree_code,
                :to_batch,
                :to_year,
                :reason
            )
        """), {
            "moved_by": None,  # can be wired to the logged-in user later
            "student_profile_id": student_profile_id,
            "enrollment_id": enrollment_id,
            "from_degree_code": from_degree_code,
            "from_batch": from_batch,
            "from_year": from_year,
            "from_program_code": from_program_code,
            "from_branch_code": from_branch_code,
            "from_division_code": from_division_code,
            "to_degree_code": to_degree,
            "to_batch": to_batch,
            "to_year": to_year,
            "reason": reason,
        })

    return res.rowcount


# --- Student Importer Helpers ---


# --- Student Importer Helpers ---

@st.cache_data
def _get_existing_enrollment_data(_engine: Engine, degree_code: str) -> Dict[str, List[str]]:
    """
    Fetches existing Batches and Years for a specific degree.
    **MODIFIED**: Uses new batch logic.
    
    NOTE: The _engine parameter uses leading underscore to prevent Streamlit
    from trying to hash it (SQLAlchemy Engine objects are not hashable).
    """
    with _engine.connect() as conn:
        # Get batches from the formal degree_batches table
        batches = _db_get_batches_for_degree(conn, degree_code)
        
        # Get years directly from the student_enrollments table
        year_res = conn.execute(
            sa_text("SELECT DISTINCT current_year FROM student_enrollments WHERE degree_code = :degree ORDER BY current_year"),
            {"degree": degree_code}
        ).fetchall()

        return {
            "batches": [b['code'] for b in batches], # Use just the code for matching
            "years": [str(r[0]) for r in year_res],  # Convert years to string for comparison
        }
# --- Other DB Helpers ---


def get_all_degrees(conn):
    """Get all active degrees from database."""
    rows = conn.execute(sa_text("""
        SELECT code
        FROM degrees
        WHERE active=1
        ORDER BY sort_order, code
    """)).fetchall()
    return [r[0] for r in rows]


def get_programs_for_degree(conn: Connection, degree_code: str) -> List[Dict[str, Any]]:
    """
    Fetch programs for a specific degree. Uses the degree_programs table
    introduced by the new programs_branches_schema.
    """
    rows = conn.execute(sa_text("""
        SELECT id, program_code
        FROM degree_programs
        WHERE lower(degree_code)=lower(:d)
          AND active=1
        ORDER BY sort_order, program_code
    """), {"d": degree_code}).fetchall()
    return [dict(id=r[0], program_code=r[1]) for r in rows]


def get_branches_for_degree_program(
    conn: Connection,
    degree_code: str,
    program_code: str | None,
) -> List[Dict[str, Any]]:
    """
    Fetch branches for a specific (degree, program) combination.

    If program_code is None, this fetches branches directly under the degree
    (for degrees that don't use programs).
    """
    if program_code:
        rows = conn.execute(sa_text("""
            SELECT b.id, b.branch_code
            FROM branches b
            JOIN degree_programs p
              ON p.id = b.degree_program_id
            WHERE lower(p.degree_code)=lower(:d)
              AND lower(p.program_code)=lower(:p)
              AND b.active=1
            ORDER BY b.sort_order, b.branch_code
        """), {"d": degree_code, "p": program_code}).fetchall()
    else:
        # Branches directly under a degree (when no programs selected)
        rows = conn.execute(sa_text("""
            SELECT b.id, b.branch_code
            FROM branches b
            WHERE lower(b.degree_code)=lower(:d)
              AND b.active=1
            ORDER BY b.sort_order, b.branch_code
        """), {"d": degree_code}).fetchall()

    return [dict(id=r[0], branch_code=r[1]) for r in rows]


# --- Student Mover Audit & Publish Guardrails ---------------------------------


def _ensure_student_mover_audit_table(conn: Connection) -> None:
    """Create the student_mover_audit table if it does not exist.

    We keep this here (and not in the schema installer) so that older
    databases can start logging moves without needing a full migration run.
    """
    conn.execute(sa_text(
        """
        CREATE TABLE IF NOT EXISTS student_mover_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            moved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            moved_by TEXT,
            student_profile_id INTEGER NOT NULL,
            enrollment_id INTEGER NOT NULL,
            from_degree_code TEXT,
            from_batch TEXT,
            from_year INTEGER,
            from_program_code TEXT,
            from_branch_code TEXT,
            from_division_code TEXT,
            to_degree_code TEXT,
            to_batch TEXT,
            to_year INTEGER,
            reason TEXT
        )
        """
    ))


def check_student_publish_guardrails(
    conn: Connection,
    degree_code: str,
    batch_code: str | None = None,
) -> Dict[str, Any]:
    """Evaluate student-related guardrails before publishing marks/attendance.

    Uses app_settings flags set from the Students > Settings > Publish Guardrails
    section, and inspects student_profiles + student_enrollments.

    Returns a structure of the form:
        {
            "ok": bool,
            "failures": {
                "unassigned_program_branch_division": [...],
                "invalid_roll_or_email": [...],
                "batch_mismatch": [...],
                "duplicates": {
                    "student_ids": [...],
                    "emails": [...]
                }
            }
        }
    """

    def _get_setting(key: str, default: str = "False") -> str:
        row = conn.execute(
            sa_text("SELECT value FROM app_settings WHERE key=:k"),
            {"k": key},
        ).fetchone()
        if row and row[0] is not None:
            return str(row[0])
        return default

    # Guardrail toggles
    guard_unassigned = _get_setting("guard_unassigned", "True") == "True"
    guard_invalid = _get_setting("guard_invalid", "True") == "True"
    guard_batch_mismatch = _get_setting("guard_batch_mismatch", "True") == "True"
    guard_duplicates = _get_setting("guard_duplicates", "True") == "True"

    failures: Dict[str, Any] = {}

    # Shared WHERE parameters
    params: Dict[str, Any] = {"degree": degree_code}
    where_batch = ""
    if batch_code:
        where_batch = "AND e.batch = :batch"
        params["batch"] = batch_code

    # 1) Unassigned program / branch / division
    if guard_unassigned:
        rows = conn.execute(sa_text(f"""
            SELECT
                p.student_id,
                p.name,
                e.program_code,
                e.branch_code,
                e.division_code
            FROM student_enrollments e
            JOIN student_profiles p
                ON p.id = e.student_profile_id
            WHERE lower(e.degree_code) = lower(:degree)
              {where_batch}
              AND e.is_primary = 1
              AND (
                    e.program_code IS NULL OR trim(e.program_code) = ''
                 OR e.branch_code  IS NULL OR trim(e.branch_code)  = ''
                 OR e.division_code IS NULL OR trim(e.division_code) = ''
              )
        """), params).fetchall()

        if rows:
            failures["unassigned_program_branch_division"] = [
                {
                    "student_id": r[0],
                    "name": r[1],
                    "program_code": r[2],
                    "branch_code": r[3],
                    "division_code": r[4],
                }
                for r in rows
            ]

    # 2) Invalid roll or email
    if guard_invalid:
        rows = conn.execute(sa_text(f"""
            SELECT
                p.student_id,
                p.name,
                p.email
            FROM student_enrollments e
            JOIN student_profiles p
                ON p.id = e.student_profile_id
            WHERE lower(e.degree_code) = lower(:degree)
              {where_batch}
              AND e.is_primary = 1
              AND (
                    p.student_id IS NULL OR trim(p.student_id) = ''
                 OR p.email      IS NULL OR trim(p.email)      = ''
                 OR instr(p.email, '@') = 0
              )
        """), params).fetchall()

        if rows:
            failures["invalid_roll_or_email"] = [
                {"student_id": r[0], "name": r[1], "email": r[2]}
                for r in rows
            ]

    # 3) Batch mismatch: enrollment batch not in degree_batches for that degree
    if guard_batch_mismatch:
        rows = conn.execute(sa_text(f"""
            SELECT DISTINCT e.batch
            FROM student_enrollments e
            WHERE lower(e.degree_code) = lower(:degree)
              {where_batch}
              AND e.is_primary = 1
              AND e.batch IS NOT NULL
              AND trim(e.batch) <> ''
              AND NOT EXISTS (
                    SELECT 1
                    FROM degree_batches b
                    WHERE lower(b.degree_code) = lower(e.degree_code)
                      AND b.batch_code = e.batch
              )
        """), params).fetchall()

        if rows:
            failures["batch_mismatch"] = [r[0] for r in rows]

    # 4) Duplicates (roll and email) in the current degree/batch scope
    if guard_duplicates:
        dup_ids = conn.execute(sa_text(f"""
            SELECT p.student_id, COUNT(*) AS c
            FROM student_enrollments e
            JOIN student_profiles p
                ON p.id = e.student_profile_id
            WHERE lower(e.degree_code) = lower(:degree)
              {where_batch}
              AND e.is_primary = 1
            GROUP BY p.student_id
            HAVING p.student_id IS NOT NULL
               AND trim(p.student_id) <> ''
               AND c > 1
        """), params).fetchall()

        dup_emails = conn.execute(sa_text(f"""
            SELECT p.email, COUNT(*) AS c
            FROM student_enrollments e
            JOIN student_profiles p
                ON p.id = e.student_profile_id
            WHERE lower(e.degree_code) = lower(:degree)
              {where_batch}
              AND e.is_primary = 1
            GROUP BY p.email
            HAVING p.email IS NOT NULL
               AND trim(p.email) <> ''
               AND c > 1
        """), params).fetchall()

        if dup_ids or dup_emails:
            failures["duplicates"] = {
                "student_ids": [
                    {"student_id": r[0], "count": r[1]} for r in dup_ids
                ],
                "emails": [
                    {"email": r[0], "count": r[1]} for r in dup_emails
                ],
            }

    return {
        "ok": not failures,
        "failures": failures,
    }


def render_guardrail_failures_ui(result: Dict[str, Any]) -> None:
    """Render guardrail failures using Streamlit expanders.

    This is optional sugar for any publish screen that wants a quick
    way to show what blocked publishing.
    """
    failures = (result or {}).get("failures") or {}
    if not failures:
        st.success("✅ No guardrail issues detected.")
        return

    # Unassigned program/branch/division
    if "unassigned_program_branch_division" in failures:
        data = failures["unassigned_program_branch_division"]
        if data:
            with st.expander("🚧 Students with unassigned Program / Branch / Division", expanded=True):
                st.dataframe(pd.DataFrame(data))

    # Invalid roll/email
    if "invalid_roll_or_email" in failures:
        data = failures["invalid_roll_or_email"]
        if data:
            with st.expander("✉️ Students with invalid roll or email", expanded=True):
                st.dataframe(pd.DataFrame(data))

    # Batch mismatches
    if "batch_mismatch" in failures:
        data = failures["batch_mismatch"]
        if data:
            with st.expander("📅 Batch codes not defined for this degree", expanded=False):
                st.write("The following batches exist in student enrollments but not in degree_batches:")
                st.dataframe(pd.DataFrame({"batch": data}))

    # Duplicates
    if "duplicates" in failures:
        dup = failures["duplicates"] or {}
        dup_ids = dup.get("student_ids") or []
        dup_emails = dup.get("emails") or []

        if dup_ids or dup_emails:
            with st.expander("♻️ Duplicate roll numbers / emails", expanded=False):
                if dup_ids:
                    st.markdown("**Duplicate student IDs**")
                    st.dataframe(pd.DataFrame(dup_ids))
                if dup_emails:
                    st.markdown("**Duplicate emails**")
                    st.dataframe(pd.DataFrame(dup_emails))
