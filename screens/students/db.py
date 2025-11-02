# app/screens/students/db.py
# FIXED VERSION - All @st.cache_data functions properly handle unhashable parameters

from __future__ import annotations

from typing import List, Dict, Any
import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text as sa_text
import logging

# Import the faculty helpers that we can re-use
from screens.faculty.db import (
    _generate_faculty_username,
    _initial_faculty_password_from_name
)

log = logging.getLogger(__name__)

# --- Student Credential Helpers ---

def _generate_student_username(conn, full_name: str, student_id: str) -> str:
    """Generates a unique student username."""
    base_username = f"s{student_id.lower().strip()}"
    exists = conn.execute(sa_text(
        "SELECT 1 FROM student_profiles WHERE username = :u"
    ), {"u": base_username}).fetchone()

    if not exists:
        return base_username
    return _generate_faculty_username(conn, full_name, table='student_profiles')


def _initial_student_password_from_name(full_name: str, student_id: str) -> str:
    """Generates a deterministic initial password for a student."""
    try:
        name_part = "".join(filter(str.isalpha, full_name.lower().split()[0]))[:4]
        id_part = "".join(filter(str.isalnum, student_id))[-4:]

        if len(id_part) < 4:
            id_part = id_part.zfill(4)

        pw = f"{name_part}@{id_part}"

        if len(pw) < 8:
            pw = f"{pw}abcd"[:8]

        return pw
    except Exception:
        return f"student@{student_id[-4:]}"


def _ensure_student_username_and_initial_creds(
    conn: Connection,
    student_profile_id: int,
    email: str,
    full_name: str,
    student_id: str
) -> None:
    """
    Ensures a student profile has a username and a re-exportable initial password.
    """
    prof = conn.execute(sa_text(
        "SELECT username FROM student_profiles WHERE id = :id"
    ), {"id": student_profile_id}).fetchone()

    if not prof:
        return

    username = prof[0]

    if not username:
        username = _generate_student_username(conn, full_name, student_id)
        conn.execute(sa_text(
            "UPDATE student_profiles SET username = :u, updated_at = CURRENT_TIMESTAMP WHERE id = :id"
        ), {"u": username, "id": student_profile_id})

    initial_pw = _initial_student_password_from_name(full_name, student_id)

    cred = conn.execute(sa_text(
        "SELECT id, consumed FROM student_initial_credentials WHERE student_profile_id = :pid"
    ), {"pid": student_profile_id}).fetchone()

    if cred:
        if int(cred[1]) != 0:
            conn.execute(sa_text("""
                UPDATE student_initial_credentials
                SET username=:u, plaintext=:p, consumed=0, created_at=CURRENT_TIMESTAMP
                WHERE student_profile_id=:pid
            """), {"u": username, "p": initial_pw, "pid": student_profile_id})
    else:
        conn.execute(sa_text("""
            INSERT INTO student_initial_credentials(student_profile_id, username, plaintext, consumed)
            VALUES(:pid, :u, :p, 0)
        """), {"pid": student_profile_id, "u": username, "p": initial_pw})

    conn.execute(sa_text("""
        UPDATE student_profiles
        SET first_login_pending=1,
            password_export_available=1,
            updated_at=CURRENT_TIMESTAMP
        WHERE id=:pid
    """), {"pid": student_profile_id})


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
                p.name, p.email, p.student_id,
                e.degree_code, e.batch, e.current_year,
                sc.username, sc.plaintext
            FROM student_initial_credentials sc
            JOIN student_profiles p ON sc.student_profile_id = p.id
            LEFT JOIN student_enrollments e ON p.id = e.student_profile_id AND e.is_primary = 1
            WHERE p.password_export_available = 1 AND sc.consumed = 0
            ORDER BY e.degree_code, e.batch, p.name
        """)).fetchall()

        conn.execute(sa_text("""
            UPDATE student_profiles SET password_export_available = 0
            WHERE id IN (SELECT student_profile_id FROM student_initial_credentials WHERE consumed = 0)
        """))

        return pd.DataFrame(rows, columns=[
            "Name", "Email", "Student ID", "Degree", "Batch", "Year", "Username", "Initial Password"
        ])


# --- Student Mover Helpers ---

@st.cache_data
def _db_get_batches_for_degree(_conn, degree_code: str) -> list:
    """
    Get distinct batches for a degree.
    
    NOTE: The _conn parameter uses leading underscore to prevent Streamlit
    from trying to hash it.
    """
    rows = _conn.execute(
        sa_text("SELECT DISTINCT batch FROM student_enrollments WHERE degree_code = :degree ORDER BY batch"),
        {"degree": degree_code}
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data
def _db_get_students_for_mover(_conn, degree_code: str, batch: str) -> pd.DataFrame:
    """
    Get students for the mover tool.
    
    NOTE: The _conn parameter uses leading underscore to prevent Streamlit
    from trying to hash it.
    """
    rows = _conn.execute(
        sa_text("""
            SELECT p.id, p.student_id, p.name, p.email, e.current_year, e.id as enrollment_id
            FROM student_profiles p
            JOIN student_enrollments e ON p.id = e.student_profile_id
            WHERE e.degree_code = :degree AND e.batch = :batch
            ORDER BY p.student_id
        """),
        {"degree": degree_code, "batch": batch}
    ).fetchall()

    df = pd.DataFrame(rows, columns=["Profile ID", "Student ID", "Name", "Email", "Current Year", "Enrollment ID"])
    df["Move"] = False
    return df


def _db_move_students(
    conn: Connection,
    enrollment_ids_to_move: List[int],
    to_degree: str,
    to_batch: str,
    to_year: int
) -> int:
    """
    Moves students by updating their enrollment records.
    
    NOTE: This function is NOT cached because it modifies data.
    """
    if not enrollment_ids_to_move:
        return 0

    # Create a parameter list for the IN clause
    params = {f"id_{i}": eid for i, eid in enumerate(enrollment_ids_to_move)}
    in_clause = ", ".join([f":{key}" for key in params.keys()])

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
        **params
    })

    return res.rowcount


# --- Student Importer Helpers ---

@st.cache_data
def _get_existing_enrollment_data(_engine: Engine, degree_code: str) -> Dict[str, List[str]]:
    """
    Fetches existing Batches and Years for a specific degree.
    
    NOTE: The _engine parameter uses leading underscore to prevent Streamlit
    from trying to hash it (SQLAlchemy Engine objects are not hashable).
    """
    with _engine.connect() as conn:
        batch_res = conn.execute(
            sa_text("SELECT DISTINCT batch FROM student_enrollments WHERE degree_code = :degree ORDER BY batch"),
            {"degree": degree_code}
        ).fetchall()

        year_res = conn.execute(
            sa_text("SELECT DISTINCT current_year FROM student_enrollments WHERE degree_code = :degree ORDER BY current_year"),
            {"degree": degree_code}
        ).fetchall()

        return {
            "batches": [r[0] for r in batch_res],
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
    return [dict(code=r[0]) for r in rows]


def get_programs_for_degree(conn, degree_code: str):
    """Get all programs for a specific degree."""
    rows = conn.execute(sa_text("""
        SELECT id, program_code
        FROM programs
        WHERE lower(degree_code)=lower(:d) AND active=1
        ORDER BY sort_order, program_code
    """), {"d": degree_code}).fetchall()
    
    return [dict(id=r[0], program_code=r[1]) for r in rows]


def get_branches_for_degree_program(conn, degree_code: str, program_code: str | None):
    """Get branches for a degree/program combination."""
    if program_code:
        rows = conn.execute(sa_text("""
            SELECT b.id, b.branch_code
            FROM branches b
            JOIN programs p ON p.id = b.program_id
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
