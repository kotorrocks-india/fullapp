# cascade_handlers.py

from typing import Dict, Optional

from sqlalchemy import text as sa_text

from .schema_helpers import _table_exists, _cols, _count, _has_col


# ───────────────────────────────────────────────────────────────────────────────
# Program helpers
# ───────────────────────────────────────────────────────────────────────────────

def _program_children_counts(conn, program_code: str) -> dict:
    """Return counts of common children tied to a program."""
    out = {}

    # branches
    if _table_exists(conn, "branches"):
        c = _cols(conn, "branches")
        if "program_code" in c:
            out["branches"] = _count(
                conn,
                "SELECT COUNT(*) FROM branches WHERE LOWER(program_code)=LOWER(:pc)",
                {"pc": program_code},
            )
        elif "program_id" in c and _table_exists(conn, "programs"):
            pid_row = conn.execute(
                sa_text(
                    "SELECT id FROM programs WHERE LOWER(program_code)=LOWER(:pc)"
                ),
                {"pc": program_code},
            ).fetchone()
            pid = pid_row[0] if pid_row else None
            if pid is not None:
                out["branches"] = _count(
                    conn, "SELECT COUNT(*) FROM branches WHERE program_id=:pid", {"pid": pid}
                )

    # semesters (if modeled by program_code)
    if _table_exists(conn, "semesters") and "program_code" in _cols(conn, "semesters"):
        out["semesters"] = _count(
            conn,
            "SELECT COUNT(*) FROM semesters WHERE LOWER(program_code)=LOWER(:pc)",
            {"pc": program_code},
        )

    # curriculum_groups (optional future)
    if _table_exists(conn, "curriculum_groups") and "program_code" in _cols(conn, "curriculum_groups"):
        out["curriculum_groups"] = _count(
            conn,
            "SELECT COUNT(*) FROM curriculum_groups WHERE LOWER(program_code)=LOWER(:pc)",
            {"pc": program_code},
        )

    # subjects/offerings/enrollments (if present)
    for tbl, fld in [
        ("subjects", "program_code"),
        ("offerings", "program_code"),
        ("enrollments", "program_code"),
    ]:
        if _table_exists(conn, tbl) and (fld in _cols(conn, tbl)):
            out[tbl] = _count(
                conn,
                f"SELECT COUNT(*) FROM {tbl} WHERE LOWER({fld})=LOWER(:pc)",
                {"pc": program_code},
            )

    return out


def _program_delete_cascade(conn, program_code: str):
    """Hard-delete children then the program."""
    # Delete branches
    if _table_exists(conn, "branches"):
        c = _cols(conn, "branches")
        if "program_code" in c:
            conn.execute(
                sa_text("DELETE FROM branches WHERE LOWER(program_code)=LOWER(:pc)"),
                {"pc": program_code},
            )
        elif "program_id" in c and _table_exists(conn, "programs"):
            pid_row = conn.execute(
                sa_text("SELECT id FROM programs WHERE LOWER(program_code)=LOWER(:pc)"),
                {"pc": program_code},
            ).fetchone()
            if pid_row:
                conn.execute(sa_text("DELETE FROM branches WHERE program_id=:pid"), {"pid": pid_row[0]})

    # Delete semesters tied to program (if modeled)
    if _table_exists(conn, "semesters") and "program_code" in _cols(conn, "semesters"):
        conn.execute(
            sa_text("DELETE FROM semesters WHERE LOWER(program_code)=LOWER(:pc)"),
            {"pc": program_code},
        )

    # Delete curriculum groups (optional)
    if _table_exists(conn, "curriculum_groups") and "program_code" in _cols(conn, "curriculum_groups"):
        conn.execute(
            sa_text("DELETE FROM curriculum_groups WHERE LOWER(program_code)=LOWER(:pc)"),
            {"pc": program_code},
        )

    # Finally delete program
    conn.execute(
        sa_text("DELETE FROM programs WHERE LOWER(program_code)=LOWER(:pc)"),
        {"pc": program_code},
    )


# ───────────────────────────────────────────────────────────────────────────────
# Degree helpers
# ───────────────────────────────────────────────────────────────────────────────

def _degree_delete_cascade(conn, degree_code: str):
    """Hard-delete a degree and all its children."""
    # 1. Delete semesters
    if _table_exists(conn, "semesters") and _has_col(conn, "semesters", "degree_code"):
        conn.execute(sa_text("DELETE FROM semesters WHERE degree_code=:c"), {"c": degree_code})

    # 2. Delete branches (both schema types)
    if _table_exists(conn, "branches"):
        if _has_col(conn, "branches", "degree_code"):
            conn.execute(sa_text("DELETE FROM branches WHERE degree_code=:c"), {"c": degree_code})
        elif _has_col(conn, "branches", "program_id"):
            program_ids = conn.execute(
                sa_text("SELECT id FROM programs WHERE degree_code=:c"), {"c": degree_code}
            ).fetchall()
            for pid in program_ids:
                conn.execute(sa_text("DELETE FROM branches WHERE program_id=:pid"), {"pid": pid[0]})

    # 3. Delete programs
    if _table_exists(conn, "programs") and _has_col(conn, "programs", "degree_code"):
        conn.execute(sa_text("DELETE FROM programs WHERE degree_code=:c"), {"c": degree_code})

    # 4. Finally delete the degree
    conn.execute(sa_text("DELETE FROM degrees WHERE code=:c"), {"c": degree_code})


# ───────────────────────────────────────────────────────────────────────────────
# Faculty helpers (NEW)
# ───────────────────────────────────────────────────────────────────────────────

def _faculty_delete_cascade(conn, faculty_id: int) -> None:
    """
    Delete common dependent rows tied to a faculty record, then return.
    The caller (action handler) will delete the row from faculty_profiles.
    Uses dynamic checks so it works across slightly different schemas.
    """
    # 1) Custom field values
    if _table_exists(conn, "faculty_custom_field_values"):
        if _has_col(conn, "faculty_custom_field_values", "faculty_id"):
            conn.execute(
                sa_text("DELETE FROM faculty_custom_field_values WHERE faculty_id=:fid"),
                {"fid": faculty_id},
            )

    # 2) Affiliations
    if _table_exists(conn, "faculty_affiliations"):
        if _has_col(conn, "faculty_affiliations", "faculty_id"):
            conn.execute(
                sa_text("DELETE FROM faculty_affiliations WHERE faculty_id=:fid"),
                {"fid": faculty_id},
            )

    # 3) Roles / mappings
    if _table_exists(conn, "faculty_roles"):
        if _has_col(conn, "faculty_roles", "faculty_id"):
            conn.execute(
                sa_text("DELETE FROM faculty_roles WHERE faculty_id=:fid"),
                {"fid": faculty_id},
            )

    # 4) Initial credentials (if stored separately)
    if _table_exists(conn, "faculty_initial_credentials"):
        if _has_col(conn, "faculty_initial_credentials", "faculty_id"):
            conn.execute(
                sa_text("DELETE FROM faculty_initial_credentials WHERE faculty_id=:fid"),
                {"fid": faculty_id},
            )

    # 5) Any scheduled loads / teaching maps / other adjunct tables (best-effort)
    for tbl in [
        "faculty_teachings",
        "faculty_workloads",
        "faculty_documents",
        "faculty_tags_map",
    ]:
        if _table_exists(conn, tbl) and _has_col(conn, tbl, "faculty_id"):
            conn.execute(sa_text(f"DELETE FROM {tbl} WHERE faculty_id=:fid"), {"fid": faculty_id})


# ───────────────────────────────────────────────────────────────────────────────
# Semester rebuild helper (used by approvals on structure/binding change)
# ───────────────────────────────────────────────────────────────────────────────

def _rebuild_semesters_for_approval(conn, degree_code: str, binding_mode: str, label_mode: str) -> int:
    """
    Rebuild all semesters for a degree based on binding mode and label mode.
    Returns number of inserted semester rows.
    """
    # Clear existing semesters for the degree
    if _table_exists(conn, "semesters") and _has_col(conn, "semesters", "degree_code"):
        conn.execute(sa_text("DELETE FROM semesters WHERE degree_code=:dc"), {"dc": degree_code})
    else:
        return 0  # nothing to do if semesters table not present

    def label(y: int, t: int, n: int) -> str:
        if label_mode == "year_term":
            return f"Year {y} • Term {t}"
        return f"Semester {n}"

    inserted = 0

    if binding_mode == "degree":
        # Read structure from degree_semester_struct
        if not _table_exists(conn, "degree_semester_struct"):
            return 0
        row = conn.execute(
            sa_text("SELECT years, terms_per_year FROM degree_semester_struct WHERE degree_code=:dc"),
            {"dc": degree_code},
        ).fetchone()
        if not row:
            return 0

        years, tpy = int(row[0]), int(row[1])
        n = 0
        for y in range(1, years + 1):
            for t in range(1, tpy + 1):
                n += 1
                conn.execute(
                    sa_text(
                        """
                        INSERT INTO semesters(degree_code, year_index, term_index, semester_number, label, active)
                        VALUES(:dc, :y, :t, :n, :lbl, 1)
                        """
                    ),
                    {"dc": degree_code, "y": y, "t": t, "n": n, "lbl": label(y, t, n)},
                )
                inserted += 1
        return inserted

    elif binding_mode == "program":
        # Build for each program under this degree using program_semester_struct
        if not (_table_exists(conn, "programs") and _table_exists(conn, "program_semester_struct")):
            return 0

        # Find programs of the degree
        programs = conn.execute(
            sa_text("SELECT id, program_code FROM programs WHERE degree_code=:dc"), {"dc": degree_code}
        ).fetchall()

        for pid, pcode in programs:
            row = conn.execute(
                sa_text("SELECT years, terms_per_year FROM program_semester_struct WHERE program_id=:pid"),
                {"pid": pid},
            ).fetchone()
            if not row:
                continue
            years, tpy = int(row[0]), int(row[1])

            n = 0
            for y in range(1, years + 1):
                for t in range(1, tpy + 1):
                    n += 1
                    conn.execute(
                        sa_text(
                            """
                            INSERT INTO semesters(degree_code, program_code, year_index, term_index, semester_number, label, active)
                            VALUES(:dc, :pc, :y, :t, :n, :lbl, 1)
                            """
                        ),
                        {
                            "dc": degree_code,
                            "pc": pcode,
                            "y": y,
                            "t": t,
                            "n": n,
                            "lbl": label(y, t, n),
                        },
                    )
                    inserted += 1
        return inserted

    elif binding_mode == "branch":
        # Build for each branch under this degree using branch_semester_struct
        if not (_table_exists(conn, "branches") and _table_exists(conn, "branch_semester_struct")):
            return 0

        # Find branches (either branches.degree_code or via programs)
        if _has_col(conn, "branches", "degree_code"):
            branches = conn.execute(
                sa_text("SELECT id, branch_code FROM branches WHERE degree_code=:dc"), {"dc": degree_code}
            ).fetchall()
        else:
            branches = conn.execute(
                sa_text(
                    """
                    SELECT b.id, b.branch_code
                      FROM branches b
                      JOIN programs p ON b.program_id=p.id
                     WHERE p.degree_code=:dc
                    """
                ),
                {"dc": degree_code},
            ).fetchall()

        for bid, bcode in branches:
            row = conn.execute(
                sa_text("SELECT years, terms_per_year FROM branch_semester_struct WHERE branch_id=:bid"),
                {"bid": bid},
            ).fetchone()
            if not row:
                continue
            years, tpy = int(row[0]), int(row[1])

            n = 0
            for y in range(1, years + 1):
                for t in range(1, tpy + 1):
                    n += 1
                    conn.execute(
                        sa_text(
                            """
                            INSERT INTO semesters(degree_code, branch_code, year_index, term_index, semester_number, label, active)
                            VALUES(:dc, :bc, :y, :t, :n, :lbl, 1)
                            """
                        ),
                        {
                            "dc": degree_code,
                            "bc": bcode,
                            "y": y,
                            "t": t,
                            "n": n,
                            "lbl": label(y, t, n),
                        },
                    )
                    inserted += 1
        return inserted

    # Unknown binding mode — nothing done
    return 0
