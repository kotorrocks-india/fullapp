# schemas/administrative_positions_schema.py
from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from core.schema_registry import register

@register
def install_administrative_positions(engine: Engine) -> None:
    """
    Create tables for administrative positions (Dean, HOD, etc.)
    These are separate from faculty teaching designations.
    """
    with engine.begin() as conn:
        # Table for position types
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS administrative_positions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_code TEXT NOT NULL UNIQUE COLLATE NOCASE,
                position_title TEXT NOT NULL,
                description TEXT,
                scope TEXT NOT NULL DEFAULT 'degree', -- 'institution', 'degree', 'program', 'branch', 'curriculum_group'
                default_credit_relief INTEGER DEFAULT 0, -- Added default relief here
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME
            )
        """))

        # Table for position assignments
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS position_assignments(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_code TEXT NOT NULL,
                assignee_email TEXT NOT NULL,
                assignee_type TEXT NOT NULL, -- 'faculty' or 'immutable_role'
                degree_code TEXT,
                program_code TEXT,                -- <<< ADDED program_code column
                branch_code TEXT,
                group_code TEXT,                  -- <<< ADDED group_code column
                start_date DATE,
                end_date DATE,
                credit_relief INTEGER DEFAULT 0,  -- Correctly included
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME,
                FOREIGN KEY(position_code) REFERENCES administrative_positions(position_code) ON DELETE CASCADE
                -- Optional: Add foreign key to curriculum_groups if desired
                -- FOREIGN KEY(group_code, degree_code) REFERENCES curriculum_groups(group_code, degree_code) ON DELETE SET NULL
            )
        """))

        # Indexes
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_pos_assignments_email
            ON position_assignments(lower(assignee_email), is_active)
        """))

        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_pos_assignments_position
            ON position_assignments(position_code, is_active)
        """))

        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_pos_assignments_degree
            ON position_assignments(degree_code, is_active)
        """))

        # ADDED: Index for program_code
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_pos_assignments_program
            ON position_assignments(program_code, is_active)
        """))

        # ADDED: Index for group_code
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_pos_assignments_group
            ON position_assignments(group_code, is_active)
        """))

        # Insert some default position types
        # Note: Added default_credit_relief values (adjust as needed)
        default_positions = [
        # (code, title, desc, scope, relief)
            ('dean', 'Dean', 'Dean of a degree program', 'degree', 4),
            ('hod', 'Head of Department', 'Head of a branch/department', 'branch', 3),
            ('program_head', 'Program Head', 'Head of a specific program', 'program', 2),
            ('vice_principal', 'Vice Principal', 'Vice Principal of institution', 'institution', 5),
            ('cg_coord', 'CG Coordinator', 'Coordinator for a Curriculum Group', 'curriculum_group', 2) # Example CG scope
        ]

        for code, title, desc, scope, relief in default_positions:
            conn.execute(sa_text("""
                INSERT INTO administrative_positions(position_code, position_title, description, scope, default_credit_relief, is_active)
                VALUES(:code, :title, :desc, :scope, :relief, 1)
                ON CONFLICT(position_code) DO UPDATE SET
                    position_title = excluded.position_title,
                    description = excluded.description,
                    scope = excluded.scope,
                    default_credit_relief = excluded.default_credit_relief -- Update relief on conflict too
            """), {"code": code, "title": title, "desc": desc, "scope": scope, "relief": relief})
