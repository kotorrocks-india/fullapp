# screens/academic_years/db.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Connection
import json # REQUIRED: For parsing term_spec_json
try:
    # Attempt to import the utility function for term calculation
    from screens.academic_years.utils import compute_term_windows_for_ay
except ImportError:
    # Define a fallback if utils import fails (prevents hard crash but warns)
    def compute_term_windows_for_ay(profile, ay_code, shift_days=0):
        raise NotImplementedError("Term calculation utility not available.")


# -----------------------------
# Low-level execution helpers
# -----------------------------

def _exec(conn: Connection, sql: str, params: Optional[Dict[str, Any]] = None):
    return conn.execute(sa_text(sql), params or {})

def _table_exists(conn: Connection, table: str) -> bool:
    try:
        rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
        return len(rows) > 0
    except Exception:
        return False

def _col_exists(conn: Connection, table: str, col: str) -> bool:
    try:
        rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
        names = {r[1].lower() for r in rows}  # r[1] is column name
        return col.lower() in names
    except Exception:
        return False

# -----------------------------
# Academic Years (CRUD + utils)
# ... (No changes in this section) ...
# -----------------------------

def get_all_ays(conn: Connection,
                status_filter: Optional[List[str]] = None,
                search_query: Optional[str] = None) -> List[Dict[str, Any]]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "academic_years"):
        return []
    where = ["1=1"]
    params: Dict[str, Any] = {}
    if status_filter:
        allowed = [s for s in status_filter if s in ("planned", "open", "closed")]
        if allowed:
            where.append("status IN :st")
            params["st"] = tuple(allowed)
    if search_query:
        where.append("ay_code LIKE :q")
        params["q"] = f"%{search_query}%"
    rows = _exec(conn, f"""
        SELECT ay_code AS code, start_date, end_date, status, updated_at
        FROM academic_years
        WHERE {' AND '.join(where)}
        ORDER BY start_date DESC
    """, params).fetchall()
    return [dict(getattr(r, "_mapping", r)) for r in rows]

def get_ay_by_code(conn: Connection, code: str) -> Optional[Dict[str, Any]]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "academic_years"):
        return None
    row = _exec(conn, """
        SELECT ay_code AS code, start_date, end_date, status, updated_at
        FROM academic_years WHERE ay_code=:c
    """, {"c": code}).fetchone()
    return (dict(getattr(row, "_mapping", row)) if row else None)

def get_latest_ay_code(conn: Connection) -> Optional[str]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "academic_years"):
        return None
    row = _exec(conn, """
        SELECT ay_code FROM academic_years
        WHERE start_date IS NOT NULL
        ORDER BY start_date DESC LIMIT 1
    """).fetchone()
    return (row[0] if row else None)

def _log_ay_audit(conn: Connection, ay_code: str, action: str, actor: str, note: Optional[str] = None, changed_fields: Optional[str] = None):
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "academic_years_audit"):
        return
    _exec(conn, """
        INSERT INTO academic_years_audit(ay_code, action, note, changed_fields, actor)
        VALUES (:ayc, :act, :note, :fields, :actor)
    """, {"ayc": ay_code, "act": action, "note": note, "fields": changed_fields, "actor": actor})

def insert_ay(conn: Connection, ay_code: str, start_date, end_date,
              status: str = "planned", actor: str = "system") -> None:
    # ... (code omitted for brevity) ...
    _exec(conn, """
        INSERT INTO academic_years(ay_code, start_date, end_date, status)
        VALUES (:c, :s, :e, :st)
    """, {"c": ay_code, "s": start_date, "e": end_date, "st": status})
    _log_ay_audit(conn, ay_code, "create", actor, note=f"Created with dates {start_date} to {end_date}")

def update_ay_dates(conn: Connection, ay_code: str, start_date, end_date,
                    actor: str = "system") -> None:
    # ... (code omitted for brevity) ...
    _exec(conn, """
        UPDATE academic_years
           SET start_date=:s, end_date=:e, updated_at=CURRENT_TIMESTAMP
         WHERE ay_code=:c
    """, {"c": ay_code, "s": start_date, "e": end_date})
    _log_ay_audit(conn, ay_code, "edit", actor, changed_fields=f'{{"start_date": "{start_date}", "end_date": "{end_date}"}}')

def update_ay_status(conn: Connection, ay_code: str, new_status: str,
                     actor: str = "system", reason: Optional[str] = None) -> None:
    # ... (code omitted for brevity) ...
    _exec(conn, """
        UPDATE academic_years
           SET status=:st, updated_at=CURRENT_TIMESTAMP
         WHERE ay_code=:c
    """, {"c": ay_code, "st": new_status})
    _log_ay_audit(conn, ay_code, new_status, actor, note=f"Changed status to {new_status}", changed_fields=f'{{"status": "{new_status}"}}')

def delete_ay(conn: Connection, ay_code: str, actor: str = "system") -> None:
    # ... (code omitted for brevity) ...
    _log_ay_audit(conn, ay_code, "delete", actor, note="Record deleted")
    _exec(conn, "DELETE FROM academic_years WHERE ay_code=:c", {"c": ay_code})

def check_overlap(conn: Connection, start_date, end_date, exclude_code: Optional[str] = None) -> Optional[str]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "academic_years"):
        return None
    row = _exec(conn, """
        SELECT ay_code
          FROM academic_years
         WHERE (:exclude IS NULL OR ay_code <> :exclude)
           AND start_date IS NOT NULL
           AND end_date   IS NOT NULL
           AND start_date < end_date
           AND start_date <= :end
           AND end_date   >= :start
         ORDER BY start_date DESC
         LIMIT 1
    """, {"exclude": exclude_code, "start": start_date, "end": end_date}).fetchone()
    return (row[0] if row else None)

# -----------------------------
# Degrees / Programs / Branches
# -----------------------------

def get_all_degrees(conn: Connection) -> List[Dict[str, Any]]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "degrees"):
        return []
    rows = _exec(conn, """
        SELECT code
          FROM degrees
         WHERE active=1
         ORDER BY sort_order, code
    """).fetchall()
    return [dict(code=r[0]) for r in rows]

def get_degree_duration(conn: Connection, degree_code: str) -> int:
    # ... (code omitted for brevity) ...
    default_duration = 10 
    if not _table_exists(conn, "degree_semester_struct"):
        return default_duration
    if not _col_exists(conn, "degree_semester_struct", "years"):
        return default_duration 
    row = _exec(conn, """
        SELECT years 
        FROM degree_semester_struct 
        WHERE degree_code=:c AND active=1
    """, {"c": degree_code}).fetchone()
    if row and row[0] and row[0] > 0:
        return int(row[0])
    else:
        return default_duration 

# NEW: Get expected terms per year
def get_degree_terms_per_year(conn: Connection, degree_code: str) -> int:
    """
    Fetches the expected terms_per_year for a specific degree from the
    'degree_semester_struct' table (per semesters_schema.py).
    """
    default_terms = 0 # Return 0 if not found, to bypass validation
    if not _table_exists(conn, "degree_semester_struct"):
        return default_terms
    if not _col_exists(conn, "degree_semester_struct", "terms_per_year"):
        return default_terms 

    row = _exec(conn, """
        SELECT terms_per_year 
        FROM degree_semester_struct 
        WHERE degree_code=:c AND active=1
    """, {"c": degree_code}).fetchone()
    
    if row and row[0] and row[0] > 0:
        return int(row[0])
    else:
        return default_terms

def get_programs_for_degree(conn: Connection, degree_code: str) -> List[Dict[str, Any]]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "programs"):
        return []
    rows = _exec(conn, """
        SELECT program_code
          FROM programs
         WHERE lower(degree_code)=lower(:d) AND active=1
         ORDER BY sort_order, program_code
    """, {"d": degree_code}).fetchall()
    return [dict(program_code=r[0]) for r in rows]

def get_branches_for_degree_program(conn: Connection, degree_code: str, program_code: Optional[str]) -> List[Dict[str, Any]]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "branches"):
        return []
    if _col_exists(conn, "branches", "program_id") and _table_exists(conn, "programs"):
        if program_code:
            rows = _exec(conn, """
                SELECT b.branch_code
                  FROM branches b
                  JOIN programs p ON p.id=b.program_id
                 WHERE lower(p.degree_code)=lower(:d)
                   AND lower(p.program_code)=lower(:p)
                   AND b.active=1
                 ORDER BY b.sort_order, b.branch_code
            """, {"d": degree_code, "p": program_code}).fetchall()
        else:
            rows = _exec(conn, """
                SELECT b.branch_code
                  FROM branches b
                  JOIN programs p ON p.id=b.program_id
                 WHERE lower(p.degree_code)=lower(:d)
                   AND b.active=1
                 ORDER BY b.sort_order, b.branch_code
            """, {"d": degree_code}).fetchall()
        return [dict(branch_code=r[0]) for r in rows]
    if program_code and _col_exists(conn, "branches", "program_code"):
        rows = _exec(conn, """
            SELECT branch_code
              FROM branches
             WHERE lower(degree_code)=lower(:d)
               AND lower(program_code)=lower(:p)
               AND active=1
             ORDER BY sort_order, branch_code
        """, {"d": degree_code, "p": program_code}).fetchall()
    else:
        rows = _exec(conn, """
            SELECT branch_code
              FROM branches
             WHERE lower(degree_code)=lower(:d) AND active=1
             ORDER BY sort_order, branch_code
        """, {"d": degree_code}).fetchall()
    return [dict(branch_code=r[0]) for r in rows]

# -----------------------------
# Calendar Profiles
# -----------------------------

def get_assignable_calendar_profiles(conn: Connection) -> List[Dict[str, Any]]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "calendar_profiles"):
        return []
    rows = _exec(conn, """
        SELECT id, code, name, model, anchor_mmdd, term_spec_json, locked, is_system
        FROM calendar_profiles
        ORDER BY is_system DESC, name ASC
    """).fetchall()
    return [dict(getattr(r, "_mapping", r)) for r in rows]

def get_calendar_profile_by_id(conn: Connection, profile_id: int) -> Optional[Dict[str, Any]]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "calendar_profiles"):
        return None
    row = _exec(conn, """
        SELECT id, code, name, model, anchor_mmdd, term_spec_json, locked, is_system
        FROM calendar_profiles
        WHERE id=:id
    """, {"id": profile_id}).fetchone()
    return (dict(getattr(row, "_mapping", row)) if row else None)

# NEW: Get term count from profile
def get_profile_term_count(conn: Connection, profile_id: int) -> int:
    """Fetches a profile and counts the number of terms in its JSON spec."""
    profile = get_calendar_profile_by_id(conn, profile_id)
    if not profile:
        return 0
    try:
        terms = json.loads(profile.get("term_spec_json", "[]"))
        return len(terms)
    except Exception:
        return 0

def insert_calendar_profile(
    conn: Connection,
    code: str,
    name: str,
    model: str,
    anchor_mmdd: str,
    term_spec_json: str
) -> None:
    # ... (code omitted for brevity) ...
    _exec(conn, """
        INSERT INTO calendar_profiles (code, name, model, anchor_mmdd, term_spec_json)
        VALUES (:code, :name, :model, :anchor, :spec)
    """, {
        "code": code,
        "name": name,
        "model": model,
        "anchor": anchor_mmdd,
        "spec": term_spec_json
    })


def _get_default_calendar_code(conn: Connection) -> Optional[str]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "app_settings"):
        return None
    row = _exec(conn, "SELECT value FROM app_settings WHERE key='default_calendar_code'").fetchone()
    return row[0] if row else None

def _get_calendar_profile_by_code(conn: Connection, code: str) -> Optional[Dict[str, Any]]:
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "calendar_profiles"):
        return None
    row = _exec(conn, """
        SELECT code, term_spec_json, 0 AS shift_days /* Shift days is 0 for the base profile */
        FROM calendar_profiles
        WHERE code=:c
    """, {"c": code}).fetchone()
    return (dict(getattr(row, "_mapping", row)) if row else None)


# -----------------------------
# Calendar Assignment CRUD
# ... (No changes in this section) ...
# -----------------------------
def _log_calendar_assignment_audit(conn: Connection, target_key: str, action: str, actor: str, note: Optional[str] = None, changed_fields: Optional[str] = None):
    # ... (code omitted for brevity) ...
    if not _table_exists(conn, "calendar_assignments_audit"):
        return
    _exec(conn, """
        INSERT INTO calendar_assignments_audit(target_key, action, note, changed_fields, actor)
        VALUES (:key, :act, :note, :fields, :actor)
    """, {"key": target_key, "act": action, "note": note, "fields": changed_fields, "actor": actor})


def insert_calendar_assignment(
    conn: Connection,
    level: str,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
    effective_from_ay: str,
    progression_year: int,
    calendar_id: int,
    shift_days: int,
    actor: str
) -> None:
    # ... (code omitted for brevity) ...
    params = {
        "lvl": level,
        "d": degree_code,
        "p": program_code or '',
        "b": branch_code or '',
        "ay": effective_from_ay,
        "py": progression_year,
        "cid": calendar_id,
        "shift": shift_days,
    }
    _exec(conn, """
        INSERT OR REPLACE INTO calendar_assignments (
            level, degree_code, program_code, branch_code, 
            effective_from_ay, progression_year, calendar_id, shift_days, active
        ) VALUES (
            :lvl, :d, :p, :b, :ay, :py, :cid, :shift, 1
        )
    """, params)
    target_key = f"{level}:{degree_code}:{program_code or ''}:{branch_code or ''}@{effective_from_ay}@PY{progression_year}"
    note = f"Set to calendar_id={calendar_id}, shift={shift_days} days"
    _log_calendar_assignment_audit(conn, target_key, "create/update", actor, note)


# -----------------------------
# Term computation
# ... (No changes in this section) ...
# -----------------------------

def _resolve_calendar_profile(
    conn: Connection,
    ay_code: str,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
    progression_year: int
) -> Tuple[Optional[Dict[str, Any]], int, Optional[str]]:
    # ... (code omitted for brevity) ...
    keys_to_try = [
        {'level': 'branch', 'b': branch_code, 'p': program_code, 'py': progression_year},
        {'level': 'program', 'b': '', 'p': program_code, 'py': progression_year},
        {'level': 'degree', 'b': '', 'p': '', 'py': progression_year},
        {'level': 'branch', 'b': branch_code, 'p': program_code, 'py': 1},
        {'level': 'program', 'b': '', 'p': program_code, 'py': 1},
        {'level': 'degree', 'b': '', 'p': '', 'py': 1},
    ]
    sql_base = """
        SELECT p.code, p.term_spec_json, a.shift_days, a.level, a.effective_from_ay
        FROM calendar_assignments a
        JOIN calendar_profiles p ON p.id = a.calendar_id
        WHERE a.active = 1
          AND a.level = :level
          AND a.degree_code = :d
          AND a.program_code = :p
          AND a.branch_code = :b
          AND a.progression_year = :py
          AND a.effective_from_ay <= :ay
        ORDER BY a.effective_from_ay DESC
        LIMIT 1
    """
    params = {"d": degree_code, "ay": ay_code}
    for key in keys_to_try:
        if (key['level'] == 'branch' and not branch_code) or \
           (key['level'] == 'program' and not program_code):
            continue
        params.update({
            "level": key['level'],
            "p": key['p'] or '',
            "b": key['b'] or '',
            "py": key['py']
        })
        row = _exec(conn, sql_base, params).fetchone()
        if row:
            profile_dict = {"code": row[0], "term_spec_json": row[1]}
            shift_days = row[2]
            source_key = f"Level: {row[3].upper()}, Effective: {row[4]}, PY: {key['py']}"
            return profile_dict, shift_days, source_key
    default_code = _get_default_calendar_code(conn)
    if default_code:
        profile = _get_calendar_profile_by_code(conn, default_code)
        if profile:
            return profile, 0, f"System Default ({default_code})" 
    return None, 0, None


def compute_terms_with_validation(conn: Connection,
                                  ay_code: str,
                                  degree_code: str,
                                  program_code: Optional[str],
                                  branch_code: Optional[str],
                                  progression_year: int
                                  ) -> Tuple[List[Dict[str, Any]], List[str]]:
    # ... (code omitted for brevity) ...
    warnings: List[str] = []
    ay = get_ay_by_code(conn, ay_code)
    if not ay:
        return [], [f"AY '{ay_code}' not found."]
    profile, shift_days, source_key = _resolve_calendar_profile(
        conn, ay_code, degree_code, program_code, branch_code, progression_year
    )
    if not profile:
        return [], ["No specific or default calendar assignment found for this selection."]
    warnings.append(f"Calendar resolved via: {source_key}. Shift: {shift_days} days.")
    if 'compute_term_windows_for_ay' not in globals():
        return [], ["Term calculation utility is not imported or defined."]
    try:
        terms = compute_term_windows_for_ay(profile, ay_code, shift_days=shift_days)
        return terms, warnings
    except Exception as e:
        warnings.append(f"Error calculating terms using profile '{profile.get('code')}': {e}")
        return [], warnings
