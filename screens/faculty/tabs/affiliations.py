# screens/faculty/tabs/affiliations.py
from __future__ import annotations
from typing import Set, List, Optional, Tuple, Any, Dict

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from screens.faculty.utils import _handle_error
from screens.faculty.db import (
    _designation_catalog,
    _degree_enabled_map,
    # _branches_for_degree removed - implemented locally for schema-awareness
    _affiliation_types,
    _is_academic_admin,
)

# ==============================================================================
# Helpers (robust to Row/RowMapping/dict/positional rows)
# ==============================================================================

def _row_get(row: Any, key: str, pos_fallback: Optional[int] = None):
    if row is None:
        return None
    # SQLAlchemy RowMapping
    try:
        return row._mapping.get(key)  # type: ignore[attr-defined]
    except Exception:
        pass
    # attribute
    try:
        return getattr(row, key)
    except Exception:
        pass
    # dict-like
    try:
        return row.get(key)
    except Exception:
        pass
    # positional fallback
    if pos_fallback is not None:
        try:
            return row[pos_fallback]
        except Exception:
            return None
    return None


def _as_code_list(rows: List[Any], code_key: str) -> List[str]:
    out: List[str] = []
    for r in rows or []:
        val = _row_get(r, code_key, pos_fallback=0)
        if val not in (None, ""):
            out.append(str(val))
    return out


def _has_column(conn, table: str, column: str) -> bool:
    try:
        rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
        cols = {str(r[1]).lower() for r in rows}
        return column.lower() in cols
    except Exception:
        return False


def _table_exists(conn, table_name: str) -> bool:
    """Check if a table exists in the database."""
    try:
        row = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
        ), {"t": table_name}).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_cols(conn, table: str) -> set[str]:
    """Get all column names for a table."""
    try:
        return {c[1] for c in conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()}
    except Exception:
        return set()


def _ensure_affiliation_audit_table(conn) -> None:
    conn.execute(sa_text(
        """
        CREATE TABLE IF NOT EXISTS faculty_affiliation_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            affiliation_id INTEGER,
            email TEXT NOT NULL,
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            group_code TEXT,
            old_designation TEXT,
            new_designation TEXT,
            old_type TEXT,
            new_type TEXT,
            old_override INTEGER,
            new_override INTEGER,
            actor_email TEXT,
            reason TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    ))


def _ensure_affiliation_audit_primary_columns(conn) -> None:
    try:
        if not _has_column(conn, "faculty_affiliation_audit", "old_primary"):
            conn.execute(sa_text("ALTER TABLE faculty_affiliation_audit ADD COLUMN old_primary INTEGER"))
    except Exception:
        pass
    try:
        if not _has_column(conn, "faculty_affiliation_audit", "new_primary"):
            conn.execute(sa_text("ALTER TABLE faculty_affiliation_audit ADD COLUMN new_primary INTEGER"))
    except Exception:
        pass


def _insert_affiliation_audit(conn, *, affiliation_id, email, degree_code,
                              program_code, branch_code, group_code,
                              old_desg, new_desg, old_type, new_type,
                              old_ovr, new_ovr, actor_email, reason,
                              old_primary: Optional[int]=None, new_primary: Optional[int]=None):
    # Ensure table exists and primary columns present
    try:
        _ensure_affiliation_audit_table(conn)
        _ensure_affiliation_audit_primary_columns(conn)
    except Exception:
        pass

    has_primary_cols = (
        _has_column(conn, "faculty_affiliation_audit", "old_primary") and
        _has_column(conn, "faculty_affiliation_audit", "new_primary")
    )

    params = {
        "aff_id": affiliation_id,
        "em": email,
        "deg": degree_code,
        "prog": (program_code or None),
        "br": (branch_code or None),
        "grp": (group_code or None),
        "od": old_desg, "nd": new_desg,
        "ot": old_type, "nt": new_type,
        "oo": int(old_ovr) if old_ovr is not None else None,
        "no": int(new_ovr) if new_ovr is not None else None,
        "actor": actor_email, "reason": (reason or None),
        "op": old_primary if old_primary is not None else 0,
        "np": new_primary if new_primary is not None else 0,
    }

    if has_primary_cols:
        conn.execute(sa_text("""
            INSERT INTO faculty_affiliation_audit(
                affiliation_id, email, degree_code, program_code, branch_code, group_code,
                old_designation, new_designation, old_type, new_type,
                old_override, new_override, actor_email, reason,
                old_primary, new_primary
            ) VALUES (
                :aff_id, :em, :deg, :prog, :br, :grp,
                :od, :nd, :ot, :nt, :oo, :no, :actor, :reason, :op, :np
            )
        """), params)
    else:
        conn.execute(sa_text("""
            INSERT INTO faculty_affiliation_audit(
                affiliation_id, email, degree_code, program_code, branch_code, group_code,
                old_designation, new_designation, old_type, new_type,
                old_override, new_override, actor_email, reason
            ) VALUES (
                :aff_id, :em, :deg, :prog, :br, :grp,
                :od, :nd, :ot, :nt, :oo, :no, :actor, :reason
            )
        """), params)


def _ensure_unique_scope_index(conn) -> None:
    try:
        conn.execute(sa_text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_faculty_aff_scope
            ON faculty_affiliations(
              lower(email),
              degree_code,
              COALESCE(program_code,''),
              COALESCE(branch_code,''),
              COALESCE(group_code,'')
            )
            """
        ))
    except Exception:
        pass


def _find_existing_affiliation(conn, *, email: str, degree: str,
                               program: Optional[str], branch: Optional[str], group_: Optional[str]):
    # Exact match but case-insensitive on program/branch/group
    row = conn.execute(sa_text(
        """
        SELECT id, designation, type, allowed_credit_override
          FROM faculty_affiliations
         WHERE lower(email)=lower(:e)
           AND lower(degree_code)=lower(:d)
           AND COALESCE(lower(program_code),'') = lower(COALESCE(:p,''))
           AND COALESCE(lower(branch_code),'')  = lower(COALESCE(:b,''))
           AND COALESCE(lower(group_code),'')   = lower(COALESCE(:g,''))
         LIMIT 1
        """
    ), {
        "e": email, "d": degree,
        "p": (program or ""), "b": (branch or ""), "g": (group_ or "")
    }).fetchone()

    if row:
        return row

    # Fallback: if there is exactly ONE active row for this email+degree, treat it as the target
    rows = conn.execute(sa_text(
        """
        SELECT id, designation, type, allowed_credit_override
          FROM faculty_affiliations
         WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d) AND active=1
         ORDER BY updated_at DESC, id DESC
        """
    ), {"e": email, "d": degree}).fetchall()

    if len(rows) == 1:
        return rows[0]

    return None


def _run_dedupe(conn) -> int:
    conn.execute(sa_text(
        """
        CREATE TEMP TABLE tmp_keep AS
        SELECT a.rowid AS rid
          FROM faculty_affiliations a
          LEFT JOIN faculty_affiliations b
            ON lower(a.email)=lower(b.email)
           AND a.degree_code=b.degree_code
           AND COALESCE(a.program_code,'')=COALESCE(b.program_code,'')
           AND COALESCE(a.branch_code,'') =COALESCE(b.branch_code,'')
           AND COALESCE(a.group_code,'')  =COALESCE(b.group_code,'')
           AND (
                COALESCE(b.updated_at,'') > COALESCE(a.updated_at,'')
                OR (COALESCE(b.updated_at,'') = COALESCE(a.updated_at,'') AND b.rowid > a.rowid)
           )
        WHERE b.rowid IS NULL
        """
    ))
    before = conn.execute(sa_text("SELECT COUNT(1) FROM faculty_affiliations")).fetchone()[0]
    conn.execute(sa_text("DELETE FROM faculty_affiliations WHERE rowid NOT IN (SELECT rid FROM tmp_keep)"))
    after = conn.execute(sa_text("SELECT COUNT(1) FROM faculty_affiliations")).fetchone()[0]
    conn.execute(sa_text("DROP TABLE IF EXISTS tmp_keep"))
    return int(before - after)


# ---------------- Primary support ----------------

def _ensure_primary_schema(conn) -> None:
    try:
        if not _has_column(conn, "faculty_affiliations", "is_primary"):
            conn.execute(sa_text("ALTER TABLE faculty_affiliations ADD COLUMN is_primary INTEGER DEFAULT 0"))
    except Exception:
        pass
    try:
        conn.execute(sa_text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_aff_primary_per_degree
              ON faculty_affiliations(lower(email), lower(degree_code))
             WHERE is_primary = 1 AND active = 1
            """
        ))
    except Exception:
        pass


def _current_primary_id(conn, *, email: str, degree: str) -> Optional[int]:
    if not _has_column(conn, "faculty_affiliations", "is_primary"):
        return None
    row = conn.execute(sa_text(
        """
        SELECT id FROM faculty_affiliations
         WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d)
           AND is_primary=1 AND active=1
         LIMIT 1
        """
    ), {"e": email, "d": degree}).fetchone()
    return int(row[0]) if row else None


def set_primary(conn, *, email: str, degree: str, target_id: int,
                actor_email: Optional[str] = None, reason: Optional[str] = None):
    if not _has_column(conn, "faculty_affiliations", "is_primary"):
        return

    prev = conn.execute(sa_text(
        """
        SELECT id, program_code, branch_code, group_code, designation, type,
               COALESCE(allowed_credit_override,0)
          FROM faculty_affiliations
         WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d)
           AND is_primary=1 AND active=1
         LIMIT 1
        """
    ), {"e": email, "d": degree}).fetchone()

    if prev and int(prev[0]) == int(target_id):
        return  # already primary

    if prev:
        conn.execute(sa_text(
            "UPDATE faculty_affiliations SET is_primary=0, updated_at=CURRENT_TIMESTAMP WHERE id=:id"
        ), {"id": int(prev[0])})
        try:
            _insert_affiliation_audit(
                conn,
                affiliation_id=int(prev[0]), email=email, degree_code=degree,
                program_code=prev[1], branch_code=prev[2], group_code=prev[3],
                old_desg=prev[4], new_desg=prev[4],
                old_type=prev[5], new_type=prev[5],
                old_ovr=int(prev[6]), new_ovr=int(prev[6]),
                actor_email=actor_email, reason=reason or "Unset primary",
                old_primary=1, new_primary=0
            )
        except Exception:
            pass

    conn.execute(sa_text(
        "UPDATE faculty_affiliations SET is_primary=1, active=1, updated_at=CURRENT_TIMESTAMP WHERE id=:id"
    ), {"id": int(target_id)})

    try:
        tgt = conn.execute(sa_text(
            """
            SELECT program_code, branch_code, group_code, designation, type,
                   COALESCE(allowed_credit_override,0)
              FROM faculty_affiliations
             WHERE id=:id
            """
        ), {"id": int(target_id)}).fetchone()
        _insert_affiliation_audit(
            conn,
            affiliation_id=int(target_id), email=email, degree_code=degree,
            program_code=tgt[0], branch_code=tgt[1], group_code=tgt[2],
            old_desg=tgt[3], new_desg=tgt[3],
            old_type=tgt[4], new_type=tgt[4],
            old_ovr=int(tgt[5]), new_ovr=int(tgt[5]),
            actor_email=actor_email, reason=reason or "Set as primary",
            old_primary=0, new_primary=1
        )
    except Exception:
        pass


# ==============================================================================
# Schema-aware branch querying (handles multiple schema types)
# ==============================================================================

def _branches_for_degree(conn, degree_code: str) -> List[Any]:
    """
    Robust branch retrieval that supports multiple schema types:
    - Branches with both program_id AND degree_code
    - Branches with only program_id (linked via programs)
    - Branches with only degree_code (direct link)
    
    Returns list of rows with branch_code in position 1 or as 'branch_code' key.
    """
    if not _table_exists(conn, "branches"):
        return []
    
    bcols = _table_cols(conn, "branches")
    has_pid = "program_id" in bcols
    has_deg = "degree_code" in bcols
    params = {"deg": degree_code}

    # Case 1: Schema supports linking branches to BOTH programs and degrees
    if has_pid and has_deg:
        # Find ALL branches for the degree
        # This includes branches linked via a program OR linked directly to degree
        rows = conn.execute(sa_text("""
            SELECT b.id, b.branch_code, b.branch_name, 
                   COALESCE(p.program_code, '') as program_code,
                   COALESCE(p.degree_code, b.degree_code) as degree_code,
                   b.active, b.sort_order
              FROM branches b
              LEFT JOIN programs p ON p.id = b.program_id
             WHERE (p.degree_code = :deg OR b.degree_code = :deg)
             ORDER BY p.degree_code, p.program_code, b.sort_order, lower(b.branch_code)
        """), params).fetchall()

    # Case 2: Schema ONLY supports linking branches to programs
    elif has_pid:
        rows = conn.execute(sa_text("""
            SELECT b.id, b.branch_code, b.branch_name, 
                   p.program_code, p.degree_code,
                   b.active, b.sort_order
              FROM branches b
              LEFT JOIN programs p ON p.id = b.program_id
             WHERE p.degree_code = :deg
             ORDER BY p.degree_code, p.program_code, b.sort_order, lower(b.branch_code)
        """), params).fetchall()

    # Case 3: Schema ONLY supports linking branches to degrees (legacy)
    elif has_deg:
        rows = conn.execute(sa_text("""
            SELECT id, branch_code, branch_name, 
                   '' as program_code, degree_code,
                   active, sort_order
              FROM branches
             WHERE degree_code = :deg
             ORDER BY degree_code, sort_order, lower(branch_code)
        """), params).fetchall()
    
    else:
        # Schema doesn't support linking branches at all
        return []

    return list(rows)


# --- Scoped designation propagation ------------------------------------------
def _propagate_designation_scoped(
    conn,
    *,
    email: str,
    degree: str,
    new_desg: str,
    scope: str,              # "degree" | "program"
    program: str | None,     # required when scope="program"
    actor_email: str | None = None,
    reason: str | None = None,
):
    """
    Enforce designation updates with user-selected scope.
    scope="degree"  -> update ALL rows for (email, degree_code)
    scope="program" -> update ALL rows for (email, degree_code, program_code)
    Writes audit entries only for rows that actually changed.
    """
    where = ["lower(email)=lower(:e)", "lower(degree_code)=lower(:d)"]
    params = {"e": email, "d": degree}
    if scope == "program":
        where.append("COALESCE(lower(program_code),'') = lower(:p)")
        params["p"] = (program or "")

    rows = conn.execute(sa_text(f"""
        SELECT id,
               designation,
               COALESCE(program_code,''), COALESCE(branch_code,''), COALESCE(group_code,''),
               COALESCE(allowed_credit_override,0)
          FROM faculty_affiliations
         WHERE {' AND '.join(where)}
    """), params).fetchall()

    for r in rows:
        rid, old_desg = int(r[0]), (r[1] or "")
        if old_desg != new_desg:
            conn.execute(sa_text("""
                UPDATE faculty_affiliations
                   SET designation=:g, updated_at=CURRENT_TIMESTAMP
                 WHERE id=:id
            """), {"g": new_desg, "id": rid})
            try:
                _insert_affiliation_audit(
                    conn,
                    affiliation_id=rid, email=email, degree_code=degree,
                    program_code=r[2] or None, branch_code=r[3] or None, group_code=r[4] or None,
                    old_desg=old_desg, new_desg=new_desg,
                    old_type=None, new_type=None,
                    old_ovr=int(r[5]), new_ovr=int(r[5]),
                    actor_email=actor_email, reason=(reason or f"Designation propagation ({scope})"),
                )
            except Exception:
                pass


# ----------------------------- Main UI -----------------------------

def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    st.subheader("Affiliations")

    # DB guardrails
    try:
        with engine.begin() as guard:
            _ensure_unique_scope_index(guard)
            _ensure_affiliation_audit_table(guard)
            _ensure_primary_schema(guard)
            _ensure_affiliation_audit_primary_columns(guard)
    except Exception:
        pass

    adf = pd.DataFrame()
    editable_emails, desgs, type_codes = [], [], []
    programs: List[str] = []
    groups: List[str] = []
    branches: List[str] = []
    degree_uses_programs = False
    degree_uses_groups = False
    degree_uses_branches = False

    # -------- Data preload (modern schema) --------
    try:
        with engine.begin() as conn:
            # Programs
            if _table_exists(conn, "programs"):
                programs_data = conn.execute(sa_text(
                    """
                    SELECT DISTINCT program_code
                      FROM programs
                     WHERE lower(degree_code)=lower(:d)
                     ORDER BY program_code
                    """
                ), {"d": degree}).fetchall()
                programs = _as_code_list(programs_data, "program_code")

            # Curriculum Groups
            if _table_exists(conn, "curriculum_groups"):
                groups_data = conn.execute(sa_text(
                    """
                    SELECT DISTINCT group_code
                      FROM curriculum_groups
                     WHERE lower(degree_code)=lower(:d)
                     ORDER BY group_code
                    """
                ), {"d": degree}).fetchall()
                groups = _as_code_list(groups_data, "group_code")

            # Branches (via schema-aware helper that supports degree_code or program_id linkage)
            if _table_exists(conn, "branches"):
                b_rows = _branches_for_degree(conn, degree) or []
                seen = set()
                for r in b_rows:
                    bc = _row_get(r, "branch_code", pos_fallback=1)
                    if bc and str(bc) not in seen:
                        seen.add(str(bc))
                        branches.append(str(bc))

            degree_uses_programs = len(programs) > 0
            degree_uses_groups = len(groups) > 0
            degree_uses_branches = len(branches) > 0

            # include name and primary (if present)
            has_primary = _has_column(conn, "faculty_affiliations", "is_primary")
            select_cols = (
                "a.id, a.email, COALESCE(fp.name,''), a.degree_code,"
                " COALESCE(a.program_code,''), COALESCE(a.branch_code,''), COALESCE(a.group_code,''),"
                " a.designation, a.type, a.allowed_credit_override, a.active"
                + (", COALESCE(a.is_primary,0)" if has_primary else "")
            )
            afrows = conn.execute(
                sa_text(f"""
                    SELECT {select_cols}
                      FROM faculty_affiliations a
                      LEFT JOIN faculty_profiles fp ON lower(fp.email)=lower(a.email)
                     WHERE lower(a.degree_code)=lower(:d)
                     ORDER BY a.email,
                              COALESCE(a.program_code,''), COALESCE(a.branch_code,''), COALESCE(a.group_code,'')
                """),
                {"d": degree},
            ).fetchall()

            data = []
            for r in afrows:
                # 0 id, 1 email, 2 name, 3 degree, 4 prog, 5 branch, 6 group, 7 desg, 8 type, 9 ovr, 10 active, [11 is_primary?]
                is_admin, _, fixed_role = _is_academic_admin(conn, r[1])
                is_immutable = (is_admin and fixed_role in ("principal", "director"))
                email_display = f"🔒 {r[1]}" if is_immutable else r[1]
                is_primary_val = int(r[11]) if len(r) >= 12 else 0
                data.append(
                    {
                        "ID": r[0],
                        "Email": email_display,
                        "Name": r[2],
                        "Degree Code": r[3],
                        "Program Code": r[4],
                        "Branch Code": r[5],
                        "Group Code": r[6],
                        "Designation": r[7],
                        "Type": r[8],
                        "Override": r[9],
                        "Active": r[10],
                        "Primary": "⭐" if is_primary_val == 1 else "",
                        "_is_primary": is_primary_val,
                        "_is_immutable": is_immutable,
                        "_raw_email": r[1],
                    }
                )
            if data:
                adf = pd.DataFrame(data)

            editable_emails = [
                row[0]
                for row in conn.execute(
                    sa_text(
                        """SELECT fp.email
                             FROM faculty_profiles fp
                            WHERE NOT EXISTS (
                                   SELECT 1
                                     FROM academic_admins aa
                                     JOIN users u ON aa.user_id = u.id
                                    WHERE lower(u.email) = lower(fp.email)
                                      AND aa.fixed_role IN ('principal', 'director', 'management_representative')
                                      AND u.active = 1
                                  )
                            ORDER BY fp.name"""
                    )
                ).fetchall()
            ]

            catalog = _designation_catalog(conn)
            enabled_map = _degree_enabled_map(conn, degree)
            desgs = [d for d in catalog if enabled_map.get(d.lower(), False)]

            type_rows = _affiliation_types(conn)
            type_codes = [t[0] for t in type_rows if t[0] in ("core", "visiting")]

    except Exception as e:
        _handle_error(e, "Could not load affiliations.")
        st.exception(e)
        return

    # -------- Table view --------
    if adf.empty:
        st.info(f"No affiliations yet for '{degree}'. Use the form below to add.")
    else:
        # Sorting controls
        sort_by = st.selectbox(
            "Sort by", ["Name", "Email", "Designation"],
            index=0, key=f"{key_prefix}_af_sort_by"
        )
        sort_dir = st.radio("Order", ["Ascending", "Descending"], index=0,
                            horizontal=True, key=f"{key_prefix}_af_sort_dir")
        try:
            adf = adf.sort_values(
                by=sort_by if sort_by in adf.columns else "Email",
                ascending=(sort_dir == "Ascending")
            )
        except Exception:
            pass

        display_cols = []
        if "Primary" in adf.columns:
            display_cols.append("Primary")
        display_cols.extend(["Name", "Email", "Degree Code"])
        if 'Program Code' in adf.columns and adf['Program Code'].notna().any():
            display_cols.append("Program Code")
        if 'Branch Code' in adf.columns and adf['Branch Code'].notna().any():
            display_cols.append("Branch Code")
        if 'Group Code' in adf.columns and adf['Group Code'].notna().any():
            display_cols.append("Group Code")
        display_cols.extend(["Designation", "Type", "Override", "Active"])

        st.dataframe(adf[display_cols], use_container_width=True, hide_index=True)

        colx, coly = st.columns([1, 1])
        with colx:
            if st.button("🧹 Dedupe exact duplicates", use_container_width=True):
                try:
                    with engine.begin() as conn:
                        removed = _run_dedupe(conn)
                    st.success(f"Removed {removed} duplicate row(s).")
                    st.rerun()
                except Exception as e:
                    _handle_error(e, "Dedupe failed.")
        with coly:
            limit = st.number_input("Show last N audit entries", min_value=5, max_value=500, value=25, step=5)
            try:
                with engine.begin() as conn:
                    _ensure_affiliation_audit_table(conn)
                    has_primary_audit = _has_column(conn, "faculty_affiliation_audit", "old_primary")
                    select_cols = (
                        "changed_at, email, degree_code,"
                        " COALESCE(program_code,''), COALESCE(branch_code,''), COALESCE(group_code,''),"
                        " old_designation, new_designation, old_type, new_type,"
                        " COALESCE(old_override,''), COALESCE(new_override,''),"
                        " COALESCE(actor_email,''), COALESCE(reason,'')"
                        + (", COALESCE(old_primary,''), COALESCE(new_primary,'')" if has_primary_audit else "")
                    )
                    audit = conn.execute(sa_text(f"""
                        SELECT {select_cols}
                          FROM faculty_affiliation_audit
                         WHERE lower(degree_code)=lower(:d)
                         ORDER BY changed_at DESC, id DESC
                         LIMIT :lim
                    """), {"d": degree, "lim": int(limit)}).fetchall()
                if audit:
                    cols = ["When","Email","Degree","Program","Branch","Group",
                            "Old Desg","New Desg","Old Type","New Type",
                            "Old Ovr","New Ovr","Actor","Reason"]
                    if has_primary_audit:
                        cols += ["Old Primary","New Primary"]
                    adf_a = pd.DataFrame(audit, columns=cols)
                    st.dataframe(adf_a, use_container_width=True, hide_index=True)
                else:
                    st.caption("No audit entries yet.")
            except Exception as e:
                _handle_error(e, "Could not load audit trail.")

    if not can_edit:
        st.caption("View-only.")
        return

    st.divider()

    # -------- Editor --------
    st.markdown("### Edit / Create an affiliation")

    # email picker (exclude fixed roles: principal, director, management_representative)
    try:
        with engine.begin() as conn:
            emails = [r[0] for r in conn.execute(
                sa_text(
                    """SELECT fp.email
                         FROM faculty_profiles fp
                        WHERE NOT EXISTS (
                               SELECT 1
                                 FROM academic_admins aa
                                 JOIN users u ON aa.user_id = u.id
                                WHERE lower(u.email) = lower(fp.email)
                                  AND aa.fixed_role IN ('principal', 'director', 'management_representative')
                                  AND u.active = 1
                              )
                        ORDER BY fp.name, fp.email"""
                )
            ).fetchall()]
    except Exception:
        emails = editable_emails

    em = st.selectbox("Faculty (email)", options=[""] + emails, key=f"{key_prefix}_af_email")

    cols1 = st.columns(3)
    i = 0
    prog = ""
    if degree_uses_programs:
        with cols1[i]:
            prog = st.selectbox("Program (optional)", options=[""] + programs,
                                key=f"{key_prefix}_af_program",
                                help="Leave blank to assign at degree-only scope.")
        i += 1

    br = ""
    if degree_uses_branches:
        with cols1[i]:
            br = st.selectbox("Branch (optional)", options=[""] + branches,
                              key=f"{key_prefix}_af_branch",
                              help="Requires a Program to be selected.")
        i += 1

    grp = ""
    if degree_uses_groups:
        with cols1[i]:
            grp = st.selectbox("Curriculum Group (optional)", options=[""] + groups,
                               key=f"{key_prefix}_af_group",
                               help="Leave blank unless needed.")
        i += 1

    with cols1[i]:
        dg = st.selectbox("Designation", options=desgs, key=f"{key_prefix}_af_desg")

    cols2 = st.columns([2, 2])
    with cols2[0]:
        type_codes = type_codes or ["core", "visiting"]
        tp = st.selectbox("Type", options=type_codes, key=f"{key_prefix}_af_type")
    with cols2[1]:
        ovr = st.number_input("Allowed credit override", min_value=0, max_value=10000, value=0,
                              key=f"{key_prefix}_af_ovr")

    if br and not prog:
        st.warning("Selecting a Branch requires selecting a Program (same rule as the importer).")

    # Visible scope hint
    st.caption(f"Editing scope → Degree={degree} • Program={prog or '—'} • Branch={br or '—'} • CG={grp or '—'}")

    # Immutable check & find existing row (for diff banner)
    existing_affiliation_id: Optional[int] = None
    is_immutable_affiliation = False
    old_row = None

    try:
        with engine.begin() as conn:
            imm = conn.execute(
                sa_text(
                    "SELECT 1 FROM academic_admins aa JOIN users u ON aa.user_id = u.id "
                    "WHERE lower(u.email) = lower(:e) AND aa.fixed_role IN ('principal','director') AND u.active = 1"
                ),
                {"e": em},
            ).fetchone()
            is_immutable_affiliation = bool(imm)

            if not is_immutable_affiliation and em and degree:
                row = _find_existing_affiliation(conn,
                                                 email=em, degree=degree,
                                                 program=(prog or None), branch=(br or None), group_=(grp or None))
                if row:
                    existing_affiliation_id = int(row[0])
                    old_row = (row[1], row[2], row[3])  # (designation, type, override)
    except Exception:
        existing_affiliation_id = None
        old_row = None

    st.session_state.setdefault(f"{key_prefix}_af_src", {})
    if existing_affiliation_id and old_row is not None:
        st.session_state[f"{key_prefix}_af_src"] = {
            "id": existing_affiliation_id,
            "old": old_row,  # (designation, type, override)
            "email": (em or "").lower(),
            "degree": degree,
            "prog": (prog or ""),
            "br": (br or ""),
            "grp": (grp or "")
        }

    if is_immutable_affiliation:
        st.error("🔒 Cannot edit affiliations for Fixed Role Designations.")
        return

    # If we didn't find the current row (e.g., user changed scope), fall back to remembered source
    if (not existing_affiliation_id) or (old_row is None):
        src = st.session_state.get(f"{key_prefix}_af_src")
        if src and src.get("email") == (em or "").lower() and src.get("degree") == degree:
            existing_affiliation_id = src.get("id")
            old_row = src.get("old")

    # ---------------- Snapshot & Viable options (NOT repeating what they already have) ----------------
    if em:
        with engine.begin() as conn:
            all_rows = conn.execute(sa_text("""
                SELECT id,
                       COALESCE(program_code,''), COALESCE(branch_code,''), COALESCE(group_code,''),
                       designation, type, COALESCE(allowed_credit_override,0),
                       COALESCE(active,1), COALESCE(is_primary,0)
                  FROM faculty_affiliations
                 WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d)
                 ORDER BY is_primary DESC, updated_at DESC, id DESC
            """), {"e": em, "d": degree}).fetchall()

        # ---- Current snapshot
        st.markdown("#### 👤 Current snapshot")
        if all_rows:
            df_cur = pd.DataFrame(
                [
                    {
                        "Primary": "⭐" if int(r[8] or 0) == 1 else "",
                        "Program": r[1] or "",
                        "Branch": r[2] or "",
                        "CG": r[3] or "",
                        "Designation": r[4] or "",
                        "Type": r[5] or "",
                        "Override": int(r[6] or 0),
                        "Active": int(r[7] or 1),
                    }
                    for r in all_rows
                ]
            )
            st.dataframe(df_cur, use_container_width=True, hide_index=True)
        else:
            st.caption("No affiliations for this faculty in this degree yet.")

        # ---- Compute designation options (exclude any they already have)
        current_desigs = sorted({str(r[4]) for r in all_rows if r[4]})
        viable_desig_options = [d for d in desgs if d not in current_desigs]

        # ---- Build full scope universe (degree/program/branch/(cg)) and exclude current scopes
        base_combos = [("", "", "")]
        for P in programs:
            base_combos.append((P, "", ""))
        if branches:
            for P in programs or [""]:
                for B in branches:
                    if P:  # branch requires program
                        base_combos.append((P, B, ""))

        combos = []
        if groups:
            for (P, B, _) in base_combos:
                combos.append((P, B, ""))  # keep no-CG row
                for G in groups:
                    combos.append((P, B, G))
        else:
            combos = base_combos

        # Existing scope keys
        existing_scope_keys = {(str(r[1] or ""), str(r[2] or ""), str(r[3] or "")) for r in all_rows}
        viable_scope_combos = [c for c in combos if c not in existing_scope_keys]

        st.markdown("#### ✅ Viable options (not held)")

        cols_vo = st.columns([2, 3])
        with cols_vo[0]:
            if viable_desig_options:
                st.caption("Designation options not already held:")
                st.write(", ".join(viable_desig_options))
            else:
                st.caption("No new designation options (faculty already has all configured designations).")

        with cols_vo[1]:
            if viable_scope_combos:
                labels = []
                label_to_combo: Dict[str, Tuple[str,str,str]] = {}
                for (P,B,G) in viable_scope_combos:
                    label = ("Degree-only" if (P=="" and B=="" and G=="")
                             else f"Program={P or '—'}"
                                  + (f", Branch={B}" if B else "")
                                  + (f", CG={G}" if G else ""))
                    labels.append(label); label_to_combo[label] = (P,B,G)

                selected_labels = st.multiselect(
                    "Add these scope(s) for this faculty (uses the Designation/Type/Override chosen above):",
                    options=labels, default=[]
                )

                if st.button("Create selected scopes", key=f"{key_prefix}_af_viable_apply"):
                    if not selected_labels:
                        st.error("Select at least one scope to create.")
                    else:
                        try:
                            with engine.begin() as conn:
                                for lbl in selected_labels:
                                    (P,B,G) = label_to_combo[lbl]
                                    res = conn.execute(sa_text("""
                                        INSERT INTO faculty_affiliations
                                            (email, degree_code, program_code, branch_code, group_code,
                                             designation, type, allowed_credit_override, active)
                                        VALUES(:e, :d, :p, :b, :g, :des, :t, :o, 1)
                                        RETURNING id
                                    """), {
                                        "e": em.lower(), "d": degree,
                                        "p": (P or None), "b": (B or None), "g": (G or None),
                                        "des": dg, "t": tp, "o": int(ovr),
                                    }).fetchone()
                                    new_id = int(res[0]) if res else None
                                    _insert_affiliation_audit(
                                        conn,
                                        affiliation_id=new_id, email=em.lower(), degree_code=degree,
                                        program_code=P or None, branch_code=B or None, group_code=G or None,
                                        old_desg=None, new_desg=dg,
                                        old_type=None, new_type=tp,
                                        old_ovr=None, new_ovr=int(ovr),
                                        actor_email=st.session_state.get("current_user_email", None),
                                        reason="Viable scope add"
                                    )
                            st.success(f"Created {len(selected_labels)} affiliation(s).")
                            st.rerun()
                        except Exception as ex:
                            _handle_error(ex, "Failed to create selected scopes.")
            else:
                st.caption("No new scope options. This faculty already holds every available scope for this degree.")

    # ---------------- End of Snapshot & Viable ----------------

    # Change banner + scope toggle
    confirm_change = True
    change_reason = ""
    will_change = False
    if existing_affiliation_id and old_row is not None:
        old_desg, old_type, old_ovr = old_row[0], old_row[1], int(old_row[2] or 0)
        will_change = (old_desg != dg) or ((old_type or '').lower() != (tp or '').lower()) or (int(old_ovr) != int(ovr))
        if will_change:
            st.warning("You are about to change an existing affiliation.")
            diff_df = pd.DataFrame([
                {"Field": "Designation", "Current": old_desg, "New": dg},
                {"Field": "Type",        "Current": old_type, "New": tp},
                {"Field": "Override",    "Current": int(old_ovr), "New": int(ovr)},
            ])
            st.dataframe(diff_df, hide_index=True, use_container_width=True)
            # Scope toggle (only meaningful when programs exist and >1)
            apply_scope = "degree"
            if degree_uses_programs and len(programs) > 1:
                scope_choice = st.radio(
                    "Apply designation change to:",
                    options=["This program only", "All programs in this degree"],
                    index=1,
                    horizontal=True,
                    key=f"{key_prefix}_af_apply_scope"
                )
                apply_scope = "program" if scope_choice == "This program only" else "degree"
            else:
                apply_scope = "degree"

            # store chosen scope in session for Save button
            st.session_state[f"{key_prefix}_apply_scope"] = apply_scope

            confirm_change = st.checkbox(
                "I understand and want to apply this change.",
                value=False, key=f"{key_prefix}_af_confirm"
            )
            change_reason = st.text_input(
                "Optional: reason/note for audit trail",
                key=f"{key_prefix}_af_reason",
                placeholder="e.g., department restructure, data correction, etc."
            )
    else:
        # no existing row -> default propagation scope to degree (safe)
        st.session_state[f"{key_prefix}_apply_scope"] = "degree"

    # Primary toggle
    current_primary_id = None
    try:
        with engine.begin() as conn:
            if em:
                current_primary_id = _current_primary_id(conn, email=em, degree=degree)
    except Exception:
        current_primary_id = None
    default_primary_checked = bool(
        existing_affiliation_id and current_primary_id and int(existing_affiliation_id) == int(current_primary_id)
    )
    make_primary = st.checkbox(
        "Make this the primary for this degree", value=default_primary_checked,
        key=f"{key_prefix}_af_make_primary",
        help="Exactly one active primary per degree."
    )

    user_email_actor = st.session_state.get("current_user_email", None)

    # --- BULK MULTI-SCOPE ASSIGNER ---------------------------------------------
    st.markdown("#### ✅ Assign multiple scopes for this faculty")
    with st.expander("Select multiple Program / Branch / CG combinations"):
        # Bulk scope toggle
        bulk_apply_scope = "degree"
        if degree_uses_programs and len(programs) > 1:
            choice = st.radio(
                "When designation changes during bulk, update:",
                options=["This program only", "All programs in this degree"],
                index=1,
                horizontal=True,
                key=f"{key_prefix}_af_ms_apply_scope"
            )
            bulk_apply_scope = "program" if choice == "This program only" else "degree"

        bulk_reason = st.text_input(
            "Optional: reason/note for audit trail (applies to all bulk ops)",
            key=f"{key_prefix}_af_ms_reason",
            placeholder="e.g., load for new semester, faculty moved across branches"
        )
        include_cg = st.checkbox(
            "Include Curriculum Groups in the selection grid",
            value=(len(groups) > 0),
            key=f"{key_prefix}_af_ms_usecg"
        )

        # Build combos; Branch requires Program
        base_combos = [("", "", "")]
        for P in programs:
            base_combos.append((P, "", ""))
        if branches:
            for P in programs or [""]:
                for B in branches:
                    if P:  # branch requires program
                        base_combos.append((P, B, ""))

        combos = []
        if include_cg and groups:
            for (P, B, _) in base_combos:
                combos.append((P, B, ""))  # keep no-CG row
                for G in groups:
                    combos.append((P, B, G))
        else:
            combos = base_combos

        # Existing map for this faculty-degree
        existing_map: Dict[Tuple[str,str,str], Dict[str, Any]] = {}
        try:
            with engine.begin() as conn:
                has_primary_col = _has_column(conn, "faculty_affiliations", "is_primary")
                select_cols = (
                    "COALESCE(program_code,''), COALESCE(branch_code,''), COALESCE(group_code,''),"
                    " designation, type, COALESCE(allowed_credit_override,0), id, COALESCE(active,1)"
                    + (", COALESCE(is_primary,0)" if has_primary_col else ", 0")
                )
                rows = conn.execute(sa_text(f"""
                    SELECT {select_cols}
                      FROM faculty_affiliations
                     WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d)
                """), {"e": em, "d": degree}).fetchall()
            for r in rows:
                key = (str(r[0]), str(r[1]), str(r[2]))
                existing_map[key] = {
                    "desg": r[3], "type": r[4], "ovr": int(r[5]),
                    "id": int(r[6]), "active": int(r[7]), "primary": int(r[8]) if len(r) > 8 else 0
                }
        except Exception as ex:
            _handle_error(ex, "Failed to load existing affiliations for bulk editor.")

        # Checkbox grid
        selections: Dict[Tuple[str,str,str], bool] = {}
        for (P, B, G) in combos:
            label = ("Degree-only" if (P == "" and B == "" and G == "") else
                     "Program=" + (P or "—") + (f", Branch={B}" if B else "") + (f", CG={G}" if G else ""))
            key_ck = f"{key_prefix}_af_ms_{P or 'deg'}_{B or 'none'}_{G or 'none'}"
            default = (P, B, G) in existing_map and existing_map[(P, B, G)]["active"] == 1
            selections[(P, B, G)] = st.checkbox(label, value=default, key=key_ck)

        # Primary picker (optional)
        primary_options = ["Keep current"]
        option_map: Dict[str, Optional[Tuple[str,str,str]]] = {"Keep current": None}
        for (P, B, G) in combos:
            label = ("Degree-only" if (P == "" and B == "" and G == "") else
                     "Program=" + (P or "—") + (f", Branch={B}" if B else "") + (f", CG={G}" if G else ""))
            primary_options.append(label)
            option_map[label] = (P, B, G)
        primary_choice = st.selectbox("Primary for this degree", options=primary_options,
                                      key=f"{key_prefix}_af_ms_primary")

        st.caption("Checked = ensure an **active** affiliation exists at that scope with the Designation/Type/Override chosen above. Unchecked = **deactivate** if currently present.")

        if st.button("Apply multi-scope changes", key=f"{key_prefix}_af_ms_apply"):
            if not em or not dg or not tp:
                st.error("Faculty, Designation, and Type are required.")
                st.stop()
            if any((B and not P) for (P, B, G) in selections.keys()):
                st.error("Branch requires Program. Please fix your selections.")
                st.stop()

            to_create, to_update_or_reactivate, to_deactivate = [], [], []
            for (P, B, G), checked in selections.items():
                present = (P, B, G) in existing_map
                if checked:
                    if not present:
                        to_create.append((P, B, G))
                    else:
                        r = existing_map[(P, B, G)]
                        will_change_row = (
                            (r["desg"] != dg)
                            or ((r["type"] or "").lower() != (tp or "").lower())
                            or (int(r["ovr"]) != int(ovr))
                            or (r["active"] == 0)
                        )
                        if will_change_row:
                            to_update_or_reactivate.append((P, B, G, r["id"], r["desg"], r["type"], r["ovr"]))
                else:
                    if present and existing_map[(P, B, G)]["active"] == 1:
                        r = existing_map[(P, B, G)]
                        to_deactivate.append((P, B, G, r["id"], r["desg"], r["type"], r["ovr"]))

            summary = f"{len(to_create)} create(s), {len(to_update_or_reactivate)} update/reactivation(s), {len(to_deactivate)} deactivation(s)"
            confirm_bulk = st.checkbox(f"Confirm bulk apply: {summary}", key=f"{key_prefix}_af_ms_confirm")
            if not confirm_bulk:
                st.error("Please confirm the bulk changes to proceed.")
                st.stop()

            try:
                with engine.begin() as conn:
                    # Creates
                    for (P, B, G) in to_create:
                        res = conn.execute(sa_text("""
                            INSERT INTO faculty_affiliations
                                (email, degree_code, program_code, branch_code, group_code,
                                 designation, type, allowed_credit_override, active)
                            VALUES(:e, :d, :p, :b, :g, :des, :t, :o, 1)
                            RETURNING id
                        """), {
                            "e": em.lower(), "d": degree,
                            "p": (P or None), "b": (B or None), "g": (G or None),
                            "des": dg, "t": tp, "o": int(ovr),
                        }).fetchone()
                        new_id = int(res[0]) if res else None
                        _insert_affiliation_audit(
                            conn,
                            affiliation_id=new_id, email=em.lower(), degree_code=degree,
                            program_code=P or None, branch_code=B or None, group_code=G or None,
                            old_desg=None, new_desg=dg,
                            old_type=None, new_type=tp,
                            old_ovr=None, new_ovr=int(ovr),
                            actor_email=user_email_actor, reason=bulk_reason or "Bulk create"
                        )

                    # Updates / re-activations
                    for (P, B, G, row_id, od, ot, oo) in to_update_or_reactivate:
                        conn.execute(sa_text("""
                            UPDATE faculty_affiliations
                               SET designation=:g, type=:t, allowed_credit_override=:o,
                                   active=1, updated_at=CURRENT_TIMESTAMP
                             WHERE id=:id
                        """), {"g": dg, "t": tp, "o": int(ovr), "id": int(row_id)})
                        _insert_affiliation_audit(
                            conn,
                            affiliation_id=int(row_id), email=em.lower(), degree_code=degree,
                            program_code=P or None, branch_code=B or None, group_code=G or None,
                            old_desg=od, new_desg=dg,
                            old_type=ot, new_type=tp,
                            old_ovr=int(oo), new_ovr=int(ovr),
                            actor_email=user_email_actor, reason=bulk_reason or "Bulk update/reactivation"
                        )

                    # Deactivations
                    for (P, B, G, row_id, od, ot, oo) in to_deactivate:
                        conn.execute(sa_text("""
                            UPDATE faculty_affiliations
                               SET active=0, updated_at=CURRENT_TIMESTAMP
                             WHERE id=:id
                        """), {"id": int(row_id)})
                        _insert_affiliation_audit(
                            conn,
                            affiliation_id=int(row_id), email=em.lower(), degree=degree,
                            program_code=P or None, branch_code=B or None, group_code=G or None,
                            old_desg=od, new_desg=od,
                            old_type=ot, new_type=ot,
                            old_ovr=int(oo), new_ovr=int(oo),
                            actor_email=user_email_actor, reason=bulk_reason or "Deactivated (bulk)"
                        )

                    # Primary selection (optional)
                    chosen = ({"Keep current": None} | {(
                        "Degree-only" if (P=="" and B=="" and G=="") else
                        f"Program={P or '—'}" + (f", Branch={B}" if B else "") + (f", CG={G}" if G else "")
                    ): (P,B,G) for (P,B,G) in combos}).get(primary_choice)
                    if chosen is not None:
                        P, B, G = chosen
                        if not selections.get((P, B, G), False):
                            st.error("To set primary, the chosen scope must be checked/active.")
                            st.stop()
                        row_id = None
                        if (P, B, G) in existing_map:
                            row_id = existing_map[(P, B, G)]["id"]
                        if row_id is None:
                            r = conn.execute(sa_text("""
                                SELECT id FROM faculty_affiliations
                                 WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d)
                                   AND COALESCE(lower(program_code),'') = lower(COALESCE(:p,''))
                                   AND COALESCE(lower(branch_code),'')  = lower(COALESCE(:b,''))
                                   AND COALESCE(lower(group_code),'')   = lower(COALESCE(:g,''))
                                 LIMIT 1
                            """), {"e": em.lower(), "d": degree, "p": P or "", "b": B or "", "g": G or ""}).fetchone()
                            row_id = int(r[0]) if r else None
                        if row_id:
                            set_primary(conn, email=em.lower(), degree=degree,
                                        target_id=int(row_id), actor_email=user_email_actor,
                                        reason=bulk_reason or "Bulk set primary")

                    # --- Scoped propagation after bulk ---
                    _propagate_designation_scoped(
                        conn,
                        email=em.lower(),
                        degree=degree,
                        new_desg=dg,
                        scope=bulk_apply_scope,
                        program=(prog or None),
                        actor_email=user_email_actor,
                        reason=bulk_reason or "Bulk apply → align designation (scoped)"
                    )

                st.success(f"Bulk changes applied: {summary}")
                st.rerun()
            except Exception as ex:
                _handle_error(ex, "Failed to apply bulk changes.")
    # ---------------------------------------------------------------------------

    # -------- Save (single row) --------
    if st.button("Save affiliation", key=f"{key_prefix}_af_save"):
        if not em or not dg:
            st.error("Faculty and Designation are required.")
            return
        if not tp:
            st.error("Affiliation Type is required.")
            return
        if br and not prog:
            st.error("Branch requires Program. Please select a Program or clear the Branch.")
            return

        # read chosen scope (default degree)
        apply_scope = st.session_state.get(f"{key_prefix}_apply_scope", "degree")

        try:
            with engine.begin() as conn:
                target = _find_existing_affiliation(conn, email=em, degree=degree,
                                                    program=(prog or None), branch=(br or None), group_=(grp or None))

                # If we have a remembered source and target is different row -> merge/replace scope
                if existing_affiliation_id and target and int(target[0]) != int(existing_affiliation_id):
                    if is_immutable_affiliation:
                        st.error("Cannot move/replace an immutable affiliation.")
                        return

                    if old_row is not None:
                        old_desg, old_type, old_ovr = old_row[0], old_row[1], int(old_row[2] or 0)
                        will_change = (old_desg != dg) or ((old_type or '').lower() != (tp or '').lower()) or (int(old_ovr) != int(ovr))
                        if will_change and not confirm_change:
                            st.error("Please confirm the change to proceed.")
                            return

                        conn.execute(sa_text("""
                            UPDATE faculty_affiliations
                               SET designation=:g, type=:t, allowed_credit_override=:o,
                                   active=1, updated_at=CURRENT_TIMESTAMP
                             WHERE id=:id
                        """), {"g": dg, "t": tp, "o": int(ovr), "id": int(target[0])})

                        _insert_affiliation_audit(
                            conn,
                            affiliation_id=int(target[0]), email=em.lower(), degree_code=degree,
                            program_code=prog, branch_code=br, group_code=grp,
                            old_desg=target[1], new_desg=dg,
                            old_type=target[2], new_type=tp,
                            old_ovr=(target[3] or 0), new_ovr=int(ovr),
                            actor_email=user_email_actor, reason=change_reason
                        )

                        # Primary (if asked)
                        if make_primary:
                            try:
                                set_primary(conn, email=em.lower(), degree=degree, target_id=int(target[0]),
                                            actor_email=user_email_actor, reason=change_reason or "Set primary (merge)")
                            except Exception:
                                pass

                        # Propagate (scoped)
                        _propagate_designation_scoped(
                            conn,
                            email=em.lower(),
                            degree=degree,
                            new_desg=dg,
                            scope=apply_scope,
                            program=(prog or None),
                            actor_email=user_email_actor,
                            reason=change_reason or "Align designation (save:merge)"
                        )

                        conn.execute(sa_text("DELETE FROM faculty_affiliations WHERE id=:id"),
                                     {"id": int(existing_affiliation_id)})

                    st.success("Replaced existing affiliation at this scope.")
                    st.rerun()
                    return

                # In-place update
                if existing_affiliation_id:
                    if old_row is not None:
                        old_desg, old_type, old_ovr = old_row[0], old_row[1], int(old_row[2] or 0)
                        will_change = (old_desg != dg) or ((old_type or '').lower() != (tp or '').lower()) or (int(old_ovr) != int(ovr))
                        if will_change and not confirm_change:
                            st.error("Please confirm the change to proceed.")
                            return

                        conn.execute(sa_text(
                            """
                            UPDATE faculty_affiliations
                               SET designation=:g, type=:t, allowed_credit_override=:o,
                                   active=1, updated_at=CURRENT_TIMESTAMP
                             WHERE id=:id
                            """
                        ), {"g": dg, "t": tp, "o": int(ovr), "id": int(existing_affiliation_id)})

                        _insert_affiliation_audit(
                            conn,
                            affiliation_id=existing_affiliation_id, email=em.lower(), degree_code=degree,
                            program_code=prog, branch_code=br, group_code=grp,
                            old_desg=old_desg, new_desg=dg,
                            old_type=old_type, new_type=tp,
                            old_ovr=old_ovr, new_ovr=int(ovr),
                            actor_email=user_email_actor, reason=change_reason
                        )

                        # Primary (if asked)
                        if make_primary and existing_affiliation_id:
                            try:
                                set_primary(conn, email=em.lower(), degree=degree,
                                            target_id=int(existing_affiliation_id),
                                            actor_email=user_email_actor,
                                            reason=change_reason or "Set primary (update)")
                            except Exception:
                                pass

                        # Propagate (scoped)
                        _propagate_designation_scoped(
                            conn,
                            email=em.lower(),
                            degree=degree,
                            new_desg=dg,
                            scope=apply_scope,
                            program=(prog or None),
                            actor_email=user_email_actor,
                            reason=change_reason or "Align designation (save:update)"
                        )

                    st.success("Affiliation updated.")
                    st.rerun()
                    return

                # Creating: if exists, update it (no duplicates)
                if target:
                    _insert_affiliation_audit(
                        conn,
                        affiliation_id=int(target[0]), email=em.lower(), degree_code=degree,
                        program_code=prog, branch_code=br, group_code=grp,
                        old_desg=target[1], new_desg=dg,
                        old_type=target[2], new_type=tp,
                        old_ovr=(target[3] or 0), new_ovr=int(ovr),
                        actor_email=user_email_actor, reason="UI: updated existing instead of creating duplicate"
                    )
                    conn.execute(sa_text(
                        """
                        UPDATE faculty_affiliations
                           SET designation=:g, type=:t, allowed_credit_override=:o,
                               active=1, updated_at=CURRENT_TIMESTAMP
                         WHERE id=:id
                        """
                    ), {"g": dg, "t": tp, "o": int(ovr), "id": int(target[0])})

                    # Primary (if asked)
                    if make_primary and target and target[0]:
                        try:
                            set_primary(conn, email=em.lower(), degree=degree,
                                        target_id=int(target[0]), actor_email=user_email_actor,
                                        reason=change_reason or "Set primary (dupe path)")
                        except Exception:
                            pass

                    # Propagate (scoped)
                    _propagate_designation_scoped(
                        conn,
                        email=em.lower(),
                        degree=degree,
                        new_desg=dg,
                        scope=apply_scope,
                        program=(prog or None),
                        actor_email=user_email_actor,
                        reason=change_reason or "Align designation (save:dupe)"
                    )

                    st.success("Updated existing affiliation.")
                    st.rerun()
                    return

                # Create new
                res = conn.execute(
                    sa_text(
                        """
                        INSERT INTO faculty_affiliations
                            (email, degree_code, program_code, branch_code, group_code,
                             designation, type, allowed_credit_override, active)
                        VALUES(:e, :d, :p, :b, :g, :des, :t, :o, 1)
                        RETURNING id
                        """
                    ),
                    {
                        "e": em.lower(),
                        "d": degree,
                        "p": (prog or None),
                        "b": (br or None),
                        "g": (grp or None),
                        "des": dg,
                        "t": tp,
                        "o": int(ovr),
                    },
                ).fetchone()
                new_id = int(res[0]) if res else None

                _insert_affiliation_audit(
                    conn,
                    affiliation_id=new_id, email=em.lower(), degree_code=degree,
                    program_code=prog, branch_code=br, group_code=grp,
                    old_desg=None, new_desg=dg,
                    old_type=None, new_type=tp,
                    old_ovr=None, new_ovr=int(ovr),
                    actor_email=user_email_actor, reason=change_reason
                )

                # Primary (if asked)
                if make_primary and new_id:
                    try:
                        set_primary(conn, email=em.lower(), degree=degree,
                                    target_id=int(new_id), actor_email=user_email_actor,
                                    reason=change_reason or "Set primary (create)")
                    except Exception:
                        pass

                # Propagate (scoped)
                _propagate_designation_scoped(
                    conn,
                    email=em.lower(),
                    degree=degree,
                    new_desg=dg,
                    scope=apply_scope,
                    program=(prog or None),
                    actor_email=user_email_actor,
                    reason=change_reason or "Align designation (save:create)"
                )

                st.success("Affiliation saved.")
                st.rerun()
        except Exception as ex:
            if "UNIQUE constraint failed" in str(ex):
                _handle_error(ex, "This affiliation already exists; please edit it.")
            else:
                _handle_error(ex, "Failed to save affiliation.")
