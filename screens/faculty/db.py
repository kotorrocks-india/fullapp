# -------------------------------------------------------------------
# app/screens/faculty/db.py
# All database helper functions (SELECTs, etc.)
# -------------------------------------------------------------------
from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional
from collections import defaultdict
import random, string

import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Connection

# -------------------- degree / designation helpers --------------------
def _active_degrees(conn: Connection) -> List[str]:
    try:
        rows = conn.execute(sa_text(
            "SELECT code FROM degrees WHERE active=1 ORDER BY sort_order, code"
        )).fetchall()
        return [r[0] for r in rows] if rows else []
    except Exception as e:
        print(f"DEBUG: _active_degrees: {e}")
        return []

def _people_for_degree_including_positions(conn: Connection, degree_code: str) -> List[Dict[str, Any]]:
    """
    Return distinct active people tied to a degree EITHER via:
      - active faculty_affiliations in that degree, OR
      - active position_assignments that apply to the degree (incl. institution scope).
    Excludes tech admins. Includes designation from affiliation if present.
    """
    try:
        rows = conn.execute(sa_text("""
            WITH fac AS (
                SELECT DISTINCT lower(fp.email) AS email, fp.name,
                       COALESCE(fa.designation, '') AS designation
                FROM faculty_profiles fp
                JOIN faculty_affiliations fa ON lower(fp.email)=lower(fa.email)
                WHERE lower(fa.degree_code)=lower(:d)
                  AND fa.active=1
                  AND fp.status='active'
            ),
            pos AS (
                SELECT DISTINCT lower(pa.assignee_email) AS email
                FROM position_assignments pa
                JOIN administrative_positions ap ON ap.position_code=pa.position_code
                WHERE pa.is_active=1
                  AND (
                        ap.scope='institution'
                     OR lower(pa.degree_code)=lower(:d)
                  )
                  AND (pa.start_date IS NULL OR DATE(pa.start_date) <= DATE('now'))
                  AND (pa.end_date   IS NULL OR DATE(pa.end_date)   >= DATE('now'))
            ),
            unioned AS (
                SELECT email FROM fac
                UNION
                SELECT email FROM pos
            )
            SELECT u.email,
                   COALESCE(fp.name, u.email) AS name,
                   COALESCE(fa.designation, '') AS designation
            FROM unioned u
            LEFT JOIN users uu ON lower(uu.email)=u.email AND uu.active=1
            LEFT JOIN tech_admins ta ON ta.user_id = uu.id
            LEFT JOIN faculty_profiles fp ON lower(fp.email)=u.email
            LEFT JOIN faculty_affiliations fa
                   ON lower(fa.email)=u.email
                  AND lower(fa.degree_code)=lower(:d)
                  AND fa.active=1
            WHERE ta.user_id IS NULL
            ORDER BY name
        """), {"d": degree_code}).fetchall()

        return [{"email": r[0], "name": r[1], "designation": r[2]} for r in rows]
    except Exception as e:
        print(f"DEBUG: _people_for_degree_including_positions: {e}")
        return []


def _add_fixed_role_admins_to_degree(conn: Connection, degree: str):
    """Ensure P/D are present as core in faculty (NOT MR; never tech admins)."""
    try:
        with conn.begin_nested():
            fixed_roles = conn.execute(sa_text("""
                SELECT u.email, u.full_name, aa.designation, u.employee_id
                FROM academic_admins aa
                JOIN users u ON aa.user_id=u.id
                WHERE aa.fixed_role IN ('director','principal') AND u.active=1
            """)).fetchall()
            for email, name, designation, employee_id in fixed_roles:
                if not designation:  # skip if no designation on admin row
                    continue
                exists = conn.execute(sa_text(
                    "SELECT 1 FROM faculty_profiles WHERE lower(email)=lower(:e)"
                ), {"e": email}).fetchone()
                if not exists:
                    conn.execute(sa_text("""
                        INSERT INTO faculty_profiles(name,email,employee_id,status)
                        VALUES (:n,:e,:emp,'active')
                    """), {"n": name, "e": email, "emp": employee_id})

                aff = conn.execute(sa_text("""
                    SELECT 1 FROM faculty_affiliations
                    WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d) AND active=1
                """), {"e": email, "d": degree}).fetchone()
                if not aff:
                    conn.execute(sa_text("""
                        INSERT INTO faculty_affiliations(email,degree_code,branch_code,designation,type,active)
                        VALUES(:e,:d,'',:g,'core',1)
                    """), {"e": email, "d": degree, "g": designation})
                    # keep designation enabled
                    conn.execute(sa_text("""
                        INSERT INTO designations(designation,is_active) VALUES(:g,1)
                        ON CONFLICT(designation) DO UPDATE SET is_active=1
                    """), {"g": designation})
                    conn.execute(sa_text("""
                        INSERT INTO designation_degree_enables(designation,degree_code,enabled)
                        VALUES(:g,:d,1)
                        ON CONFLICT(designation,degree_code) DO UPDATE SET enabled=1
                    """), {"g": designation, "d": degree})
    except Exception as e:
        st.warning(f"Note: Could not auto-assign fixed role admins: {e}")

def _designation_catalog(conn: Connection) -> List[str]:
    """Exclude admin roles and affiliation-type keywords."""
    try:
        rows = conn.execute(sa_text("""
            SELECT designation FROM designations WHERE is_active=1
            AND designation NOT IN (SELECT DISTINCT designation FROM academic_admins WHERE designation IS NOT NULL)
            AND lower(designation) NOT LIKE '%visit%' AND lower(designation) NOT LIKE '%core%' AND lower(designation) NOT LIKE '%custom%'
            ORDER BY designation
        """)).fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        print(f"DEBUG: _designation_catalog: {e}")
        return []

def _all_designations(conn: Connection) -> List[str]:
    return _designation_catalog(conn)

def _degree_enabled_map(conn: Connection, degree_code: str) -> dict:
    try:
        rows = conn.execute(sa_text("""
            SELECT designation, enabled FROM designation_degree_enables
            WHERE lower(degree_code)=lower(:d) ORDER BY designation
        """), {"d": degree_code}).fetchall()
        return {r[0].lower(): bool(r[1]) for r in rows}
    except Exception as e:
        print(f"DEBUG: _degree_enabled_map: {e}")
        return {}

def _designation_enabled(conn: Connection, degree_code: str, designation: str) -> bool:
    try:
        row = conn.execute(sa_text("""
            SELECT enabled FROM designation_degree_enables
            WHERE lower(degree_code)=lower(:d) AND lower(designation)=lower(:g)
        """), {"d": degree_code, "g": designation}).fetchone()
        return bool(row and row[0])
    except Exception as e:
        print(f"DEBUG: _designation_enabled: {e}")
        return False

def _branches_for_degree(conn: Connection, degree_code: str) -> List[str]:
    try:
        rows = conn.execute(sa_text("""
            SELECT DISTINCT branch_code FROM branches
            WHERE lower(degree_code)=lower(:d) AND active=1 ORDER BY branch_code
        """), {"d": degree_code}).fetchall()
        return [r[0] for r in rows if r[0]]
    except Exception as e:
        print(f"DEBUG: _branches_for_degree: {e}")
        return []

def _affiliation_types(conn: Connection) -> List[Tuple[str,str]]:
    try:
        rows = conn.execute(sa_text("""
            SELECT type_code, description FROM affiliation_types
            WHERE is_active=1 ORDER BY is_system DESC, type_code
        """)).fetchall()
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        print(f"DEBUG: _affiliation_types: {e}")
        return []

# -------------------- profile / custom fields helpers --------------------
def _duplicate_candidates(conn: Connection, name: str, email: str) -> List[Tuple[str,str]]:
    base = (name or "").lower().replace("dr.", "").replace("dr ","").replace("prof.","").replace("prof ","")
    q = f"%{base.strip()}%" if base.strip() else None
    try:
        return conn.execute(sa_text("""
            SELECT name, email FROM faculty_profiles
            WHERE (:q IS NOT NULL AND lower(name) LIKE :q) OR (lower(email)=lower(:e))
            LIMIT 10
        """), {"q": q, "e": (email or "").lower()}).fetchall()
    except Exception as e:
        print(f"DEBUG: _duplicate_candidates: {e}")
        return []

def _get_custom_profile_fields(conn: Connection) -> List[Dict[str,Any]]:
    try:
        rows = conn.execute(sa_text("""
            SELECT field_name, display_name, field_type, field_options, is_active
            FROM faculty_profile_custom_fields ORDER BY display_name
        """)).fetchall()
        return [{"field_name": r[0], "display_name": r[1], "field_type": r[2],
                 "field_options": r[3], "is_active": bool(r[4])} for r in rows]
    except Exception as e:
        print(f"DEBUG: _get_custom_profile_fields: {e}")
        return []

def _get_all_custom_field_data(conn: Connection) -> Dict[str, Dict[str, Any]]:
    try:
        rows = conn.execute(sa_text("SELECT email, field_name, value FROM faculty_profile_custom_data")).fetchall()
        out = defaultdict(dict)
        for email, fname, val in rows:
            out[email.lower()][fname] = val
        return out
    except Exception as e:
        print(f"DEBUG: _get_all_custom_field_data: {e}")
        return defaultdict(dict)

def _save_custom_field_value(conn: Connection, email: str, field_name: str, field_value: str):
    conn.execute(sa_text("""
        INSERT INTO faculty_profile_custom_data(email, field_name, value)
        VALUES (:e,:f,:v)
        ON CONFLICT(email, field_name) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP
    """), {"e": email.lower(), "f": field_name, "v": field_value})

def _get_custom_field_mapping(conn: Connection) -> Dict[str, str]:
    """
    Returns a mapping: lowercase(display_name) -> field_name for all **active** custom profile fields.
    Safe to call from importer / bulk ops.
    """
    try:
        fields = _get_custom_profile_fields(conn)
        return {
            (f.get("display_name") or "").strip().lower(): f.get("field_name")
            for f in fields if f.get("is_active")
        }
    except Exception as e:
        print(f"DEBUG: _get_custom_field_mapping: {e}")
        return {}

# -------------------- creds / usernames --------------------
def _faculty_username_from_name(full_name: str) -> tuple[str,str,str]:
    tokens = [t for t in (full_name or "").strip().split() if t]
    given, surname = (tokens[0] if tokens else ""), (tokens[-1] if len(tokens)>1 else "")
    base5 = (given[:5] or (surname[:5] if surname else "xxxxx")).ljust(5,"x")
    last_initial = (surname[:1] or "x")
    digits = "".join(random.choices(string.digits, k=4))
    return base5, last_initial, digits

def _generate_faculty_username(conn: Connection, full_name: str, retries: int = 6) -> str:
    base5, last_initial, digits = _faculty_username_from_name(full_name)
    candidate = "failed"
    for _ in range(retries):
        candidate = f"{base5}{last_initial}{digits}".lower()
        exists = conn.execute(sa_text("SELECT 1 FROM faculty_profiles WHERE username=:u"), {"u": candidate}).fetchone()
        if not exists:
            return candidate
        digits = "".join(random.choices(string.digits, k=4))
    fallback = f"{random.choice(string.ascii_lowercase)}{''.join(random.choices(string.digits, k=3))}"
    return f"{base5}{last_initial}{digits}{fallback}".lower()

def _initial_faculty_password_from_name(full_name: str, digits: str) -> str:
    base5, last_initial, _ = _faculty_username_from_name(full_name)
    return f"{base5.lower()}{(last_initial or 'x').lower()}@{digits}"

def _list_faculty_profiles_with_creds(conn) -> list[dict]:
    """Exclude MR and all tech admins from the faculty list."""
    try:
        cols = {c[1] for c in conn.execute(sa_text("PRAGMA table_info(faculty_profiles)")).fetchall()}
        has_username   = 'username' in cols
        has_firstlogin = 'first_login_pending' in cols
        has_export     = 'password_export_available' in cols

        select_cols = ["fp.id","fp.email","fp.name","fp.phone","fp.employee_id","fp.status"]
        select_cols.append("COALESCE(fp.username,'') AS username" if has_username else "'' AS username")
        select_cols.append("COALESCE(fp.first_login_pending,1) AS first_login_pending" if has_firstlogin else "1 AS first_login_pending")
        select_cols.append("COALESCE(fp.password_export_available,0) AS password_export_available" if has_export else "0 AS password_export_available")

        cred_tbl = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='faculty_initial_credentials'"
        )).fetchone()
        if cred_tbl:
            select_cols.append("ic.plaintext AS initial_password")
            join_sql = "LEFT JOIN faculty_initial_credentials ic ON ic.faculty_profile_id=fp.id AND ic.consumed=0"
        else:
            select_cols.append("NULL AS initial_password")
            join_sql = ""

        where_sql = """
         WHERE NOT EXISTS (
           SELECT 1 FROM academic_admins aa JOIN users u ON aa.user_id=u.id
           WHERE lower(u.email)=lower(fp.email) AND aa.fixed_role='management_representative' AND u.active=1
         )
         AND NOT EXISTS (
           SELECT 1 FROM tech_admins ta JOIN users tu ON ta.user_id=tu.id
           WHERE lower(tu.email)=lower(fp.email) AND tu.active=1
         )
        """
        q = f"SELECT {', '.join(select_cols)} FROM faculty_profiles fp {join_sql} {where_sql} ORDER BY fp.name"
        rows = conn.execute(sa_text(q)).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as e:
        print(f"DEBUG: _list_faculty_profiles_with_creds: {e}")
        return []

# -------------------- admin/tech flags --------------------
def _sync_academic_admins_to_faculty(conn: Connection) -> None:
    try:
        rows = conn.execute(sa_text("""
            SELECT u.email, u.full_name, u.employee_id
            FROM academic_admins aa
            JOIN users u ON aa.user_id=u.id
            LEFT JOIN tech_admins ta ON ta.user_id = u.id
            WHERE aa.fixed_role IN ('principal','director','management_representative')
              AND u.active=1
              AND ta.user_id IS NULL
        """)).fetchall()
        for email, name, emp in rows:
            exists = conn.execute(sa_text(
                "SELECT 1 FROM faculty_profiles WHERE lower(email)=lower(:e)"
            ), {"e": email}).fetchone()
            if not exists:
                conn.execute(sa_text("""
                    INSERT INTO faculty_profiles(name,email,employee_id,status)
                    VALUES(:n,:e,:emp,'active')
                """), {"n": name, "e": email, "emp": emp})
    except Exception as e:
        print(f"DEBUG: _sync_academic_admins_to_faculty: {e}")

def _is_academic_admin(conn: Connection, email: str) -> tuple[bool, str | None, str | None]:
    """Return (is_admin, designation, fixed_role). Tech admins yield False."""
    try:
        tech = conn.execute(sa_text("""
            SELECT 1 FROM tech_admins ta JOIN users u ON ta.user_id=u.id
            WHERE lower(u.email)=lower(:e) AND u.active=1
        """), {"e": email}).fetchone()
        if tech: return (False, None, None)
        row = conn.execute(sa_text("""
            SELECT aa.designation, aa.fixed_role
            FROM academic_admins aa JOIN users u ON aa.user_id=u.id
            WHERE lower(u.email)=lower(:e) AND u.active=1
        """), {"e": email}).fetchone()
        return (True, row[0], row[1]) if row else (False, None, None)
    except Exception as e:
        print(f"DEBUG: _is_academic_admin: {e}")
        return (False, None, None)

# -------------------- curriculum structure helpers --------------------
def _get_curriculum_groups_for_degree(conn: Connection, degree_code: str) -> List[Dict[str, Any]]:
    groups: list[dict] = []
    try:
        table_exists = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='curriculum_groups'"
        )).fetchone()
        if table_exists:
            rows = conn.execute(sa_text("""
                SELECT group_code, group_name, description, active
                FROM curriculum_groups
                WHERE lower(degree_code)=lower(:d) AND active=1
                ORDER BY sort_order, group_name
            """), {"d": degree_code}).fetchall()
            groups = [{"group_code": r[0], "group_name": r[1],
                       "description": r[2] or "", "active": bool(r[3])} for r in rows]
    except Exception as e:
        print(f"DEBUG: _get_curriculum_groups_for_degree: {e}")
    return groups

def _get_programs_for_degree(conn: Connection, degree_code: str) -> List[Dict[str, Any]]:
    try:
        rows = conn.execute(sa_text("""
            SELECT branch_code, branch_name, active
            FROM branches WHERE lower(degree_code)=lower(:d) AND active=1
            ORDER BY sort_order, branch_name
        """), {"d": degree_code}).fetchall()
        return [{"group_code": r[0], "group_name": r[1], "description": "", "active": bool(r[2])} for r in rows]
    except Exception as e:
        print(f"DEBUG: _get_programs_for_degree: {e}")
        return []

def _get_active_faculty_for_degree(conn: Connection, degree_code: str) -> List[Dict[str, Any]]:
    try:
        rows = conn.execute(sa_text("""
            SELECT DISTINCT fp.email, fp.name, fp.employee_id, fa.designation, fa.type
            FROM faculty_profiles fp
            JOIN faculty_affiliations fa ON lower(fp.email)=lower(fa.email)
            WHERE lower(fa.degree_code)=lower(:d) AND fa.active=1 AND fp.status='active'
            ORDER BY fp.name
        """), {"d": degree_code}).fetchall()
        return [{"email": r[0], "name": r[1], "employee_id": r[2], "designation": r[3], "type": r[4]} for r in rows]
    except Exception as e:
        print(f"DEBUG: _get_active_faculty_for_degree: {e}")
        return []

def _get_degree_info(conn: Connection, degree_code: str) -> Dict[str, Any] | None:
    try:
        row = conn.execute(sa_text(
            "SELECT code, name, active, sort_order FROM degrees WHERE lower(code)=lower(:d)"
        ), {"d": degree_code}).fetchone()
        return {"code": row[0], "name": row[1], "active": bool(row[2]), "sort_order": row[3]} if row else None
    except Exception as e:
        print(f"DEBUG: _get_degree_info: {e}")
        return None

def _validate_affiliation_data(conn: Connection, email: str, degree_code: str, designation: str) -> tuple[bool, str]:
    try:
        profile = conn.execute(sa_text("SELECT 1 FROM faculty_profiles WHERE lower(email)=lower(:e)"), {"e": email}).fetchone()
        degree_ok = conn.execute(sa_text("SELECT 1 FROM degrees WHERE lower(code)=lower(:d) AND active=1"), {"d": degree_code}).fetchone()
        enabled = conn.execute(sa_text("""
            SELECT enabled FROM designation_degree_enables
            WHERE lower(designation)=lower(:g) AND lower(degree_code)=lower(:d)
        """), {"g": designation, "d": degree_code}).fetchone()
        if not profile:     return (False, f"Faculty profile not found for {email}")
        if not degree_ok:   return (False, f"Degree {degree_code} not found or inactive")
        if not enabled or not enabled[0]: return (False, f"Designation '{designation}' not enabled for {degree_code}")
        return (True, "")
    except Exception as e:
        print(f"DEBUG: _validate_affiliation_data: {e}")
        return (False, f"Validation error: {e}")

# -------------------- positions data --------------------
def _get_available_positions(conn: Connection) -> List[Dict[str, Any]]:
    try:
        rows = conn.execute(sa_text("""
            SELECT position_code, position_title, description, scope, is_active
            FROM administrative_positions WHERE is_active=1
            ORDER BY position_title
        """)).fetchall()
        return [{"position_code": r[0], "position_title": r[1], "description": r[2],
                 "scope": r[3], "is_active": bool(r[4])} for r in rows]
    except Exception as e:
        print(f"DEBUG: _get_available_positions: {e}")
        return []

def _get_all_positions(conn: Connection) -> List[Dict[str, Any]]:
    try:
        rows = conn.execute(sa_text("""
            SELECT position_code, position_title, description, scope, is_active
            FROM administrative_positions
            ORDER BY is_active DESC, position_title
        """)).fetchall()
        return [{"position_code": r[0], "position_title": r[1], "description": r[2],
                 "scope": r[3], "is_active": bool(r[4])} for r in rows]
    except Exception as e:
        print(f"DEBUG: _get_all_positions: {e}")
        return []

def _get_position_assignments_for_degree(conn: Connection, degree_code: str) -> List[Dict[str, Any]]:
    try:
        rows = conn.execute(sa_text("""
            SELECT pa.id, pa.position_code, ap.position_title, pa.assignee_email,
                   fp.name AS assignee_name, pa.assignee_type, pa.degree_code, pa.branch_code,
                   pa.group_code, pa.start_date, pa.end_date, pa.is_active
            FROM position_assignments pa
            JOIN administrative_positions ap ON pa.position_code = ap.position_code
            LEFT JOIN faculty_profiles fp ON lower(pa.assignee_email)=lower(fp.email)
            WHERE lower(pa.degree_code)=lower(:d) AND pa.is_active=1
            ORDER BY ap.position_title, fp.name
        """), {"d": degree_code}).fetchall()
        return [{
            "id": r[0], "position_code": r[1], "position_title": r[2], "assignee_email": r[3],
            "assignee_name": r[4], "assignee_type": r[5], "degree_code": r[6], "branch_code": r[7],
            "group_code": r[8], "start_date": r[9], "end_date": r[10], "is_active": bool(r[11])
        } for r in rows]
    except Exception as e:
        print(f"DEBUG: _get_position_assignments_for_degree: {e}")
        return []

def _get_all_position_assignments(conn: Connection) -> List[Dict[str, Any]]:
    try:
        rows = conn.execute(sa_text("""
            SELECT pa.id, pa.position_code, ap.position_title, pa.assignee_email,
                   fp.name AS assignee_name, pa.assignee_type, pa.degree_code, pa.branch_code,
                   pa.group_code, pa.start_date, pa.end_date, pa.is_active
            FROM position_assignments pa
            JOIN administrative_positions ap ON pa.position_code = ap.position_code
            LEFT JOIN faculty_profiles fp ON lower(pa.assignee_email)=lower(fp.email)
            WHERE pa.is_active=1
            ORDER BY ap.position_title, pa.degree_code, fp.name
        """)).fetchall()
        return [{
            "id": r[0], "position_code": r[1], "position_title": r[2], "assignee_email": r[3],
            "assignee_name": r[4], "assignee_type": r[5], "degree_code": r[6], "branch_code": r[7],
            "group_code": r[8], "start_date": r[9], "end_date": r[10], "is_active": bool(r[11])
        } for r in rows]
    except Exception as e:
        print(f"DEBUG: _get_all_position_assignments: {e}")
        return []

# -------------------- effective teaching load (relief) --------------------
def _calculate_effective_teaching_load(
    conn: Connection,
    email: str,
    degree_code: Optional[str] = None,
    base_credits: int = 0,
) -> Dict[str, Any]:
    """
    Scope-aware, date-aware admin-relief calculator.
    - only active assignments (is_active=1)
    - respects start/end dates
    - institution relief always applies; degree/program/branch/cg require degree match if degree_code given
    """
    relief = 0
    try:
        pa_cols = {c[1] for c in conn.execute(sa_text("PRAGMA table_info(position_assignments)")).fetchall()}
        if "credit_relief" not in pa_cols:
            return {"base_required": int(base_credits or 0), "admin_relief": 0, "effective_required": int(base_credits or 0)}

        ap_cols = {c[1] for c in conn.execute(sa_text("PRAGMA table_info(administrative_positions)")).fetchall()}
        has_scope = "scope" in ap_cols

        where_sql = [
            "lower(pa.assignee_email)=lower(:e)",
            "pa.is_active=1",
            "(pa.start_date IS NULL OR DATE(pa.start_date) <= DATE('now'))",
            "(pa.end_date   IS NULL OR DATE(pa.end_date)   >= DATE('now'))",
        ]
        params = {"e": email}

        scope_sql = []
        if has_scope:
            scope_sql.append("ap.scope='institution'")
            if degree_code:
                params["d"] = degree_code
                scope_sql += [
                    "(ap.scope='degree'           AND lower(pa.degree_code)=lower(:d))",
                    "(ap.scope='program'          AND lower(pa.degree_code)=lower(:d))",
                    "(ap.scope='branch'           AND lower(pa.degree_code)=lower(:d))",
                    "(ap.scope='curriculum_group' AND lower(pa.degree_code)=lower(:d))",
                ]
            else:
                scope_sql.append("(pa.degree_code IS NULL)")
        else:
            if degree_code:
                params["d"] = degree_code
                scope_sql += ["pa.degree_code IS NULL", "lower(pa.degree_code)=lower(:d)"]
            else:
                scope_sql.append("pa.degree_code IS NULL")

        q = f"""
            SELECT COALESCE(SUM(pa.credit_relief),0)
            FROM position_assignments pa
            {"JOIN administrative_positions ap ON ap.position_code=pa.position_code" if has_scope else ""}
            WHERE {' AND '.join(where_sql)}
              AND (
                {" OR ".join(scope_sql)}
              )
        """
        relief = int(conn.execute(sa_text(q), params).scalar() or 0)
    except Exception as e:
        print(f"DEBUG: _calculate_effective_teaching_load: {e}")
        relief = 0

    base = int(base_credits or 0)
    return {"base_required": base, "admin_relief": relief, "effective_required": max(0, base - relief)}

# -------------------- degree structure flags --------------------
def _degree_has_branches(conn: Connection, degree_code: str) -> bool:
    try:
        n = conn.execute(sa_text(
            "SELECT COUNT(*) FROM branches WHERE lower(degree_code)=lower(:d) AND active=1"
        ), {"d": degree_code}).scalar()
        return (n or 0) > 0
    except Exception as e:
        print(f"DEBUG: _degree_has_branches: {e}")
        return False

def _degree_has_curriculum_groups(conn: Connection, degree_code: str) -> bool:
    try:
        exists = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='curriculum_groups'"
        )).fetchone()
        if not exists:
            return False
        n = conn.execute(sa_text("""
            SELECT COUNT(*) FROM curriculum_groups
            WHERE lower(degree_code)=lower(:d) AND active=1
        """), {"d": degree_code}).scalar()
        return (n or 0) > 0
    except Exception as e:
        print(f"DEBUG: _degree_has_curriculum_groups: {e}")
        return False

# -------------------- NEW: union people for credits policy --------------------
def _people_for_degree_including_positions(conn: Connection, degree_code: str) -> List[Dict[str, Any]]:
    """
    Returns distinct active people associated with a degree EITHER via:
      - active faculty_affiliations in that degree, OR
      - active position_assignments that are institution-wide OR for this degree
    Excludes tech admins. Includes designation (from affiliation if present).
    """
    try:
        rows = conn.execute(sa_text("""
            WITH fac AS (
                SELECT DISTINCT lower(fp.email) AS email, fp.name,
                       COALESCE(fa.designation, '') AS designation
                FROM faculty_profiles fp
                JOIN faculty_affiliations fa ON lower(fp.email)=lower(fa.email)
                WHERE lower(fa.degree_code)=lower(:d) AND fa.active=1 AND fp.status='active'
            ),
            pos AS (
                SELECT DISTINCT lower(pa.assignee_email) AS email
                FROM position_assignments pa
                JOIN administrative_positions ap ON ap.position_code=pa.position_code
                WHERE pa.is_active=1
                  AND (ap.scope='institution' OR lower(pa.degree_code)=lower(:d))
            ),
            unioned AS (
                SELECT email FROM fac
                UNION
                SELECT email FROM pos
            )
            SELECT u.email,
                   COALESCE(fp.name, u.email) AS name,
                   COALESCE(fa.designation, '') AS designation
            FROM unioned u
            LEFT JOIN users uu ON lower(uu.email)=u.email AND uu.active=1
            LEFT JOIN tech_admins ta ON ta.user_id = uu.id
            LEFT JOIN faculty_profiles fp ON lower(fp.email)=u.email
            LEFT JOIN faculty_affiliations fa ON lower(fa.email)=u.email AND lower(fa.degree_code)=lower(:d) AND fa.active=1
            WHERE ta.user_id IS NULL
            ORDER BY name
        """), {"d": degree_code}).fetchall()
        return [{"email": r[0], "name": r[1], "designation": r[2]} for r in rows]
    except Exception as e:
        print(f"DEBUG: _people_for_degree_including_positions: {e}")
        return []
