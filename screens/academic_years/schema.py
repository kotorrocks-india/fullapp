# screens/academic_years/schema.py
from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
import json, datetime # Make sure json and datetime are imported

def _exec(conn, sql: str, params: dict | None = None):
    conn.execute(sa_text(sql), params or {})

def install_academic_years(engine: Engine):
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS academic_years(
          ay_code   TEXT PRIMARY KEY COLLATE NOCASE,
          start_date TEXT NOT NULL,
          end_date   TEXT NOT NULL,
          status     TEXT NOT NULL DEFAULT 'planned' CHECK(status IN ('planned','open','closed')),
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME
        );""")
        _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_ay_code ON academic_years(ay_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_ay_status ON academic_years(status)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_ay_start_date ON academic_years(start_date)")

def install_ay_audit(engine: Engine):
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS academic_years_audit(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ay_code TEXT NOT NULL,
          action  TEXT NOT NULL,
          note    TEXT,
          changed_fields TEXT,
          actor   TEXT,
          at DATETIME DEFAULT CURRENT_TIMESTAMP
        );""")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_ay_audit_code ON academic_years_audit(ay_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_ay_audit_at ON academic_years_audit(at)")

def install_app_settings(engine: Engine):
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS app_settings(
          key TEXT PRIMARY KEY,
          value TEXT
        );""")

def install_calendar_profiles(engine: Engine):
    """Profiles are immutable once used; edit via clone to new row."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS calendar_profiles(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT NOT NULL UNIQUE COLLATE NOCASE, -- e.g. jul_2term
          name TEXT NOT NULL,
          
          -- MODIFIED: Removed the CHECK constraint to allow any text
          model TEXT NOT NULL,  
          
          anchor_mmdd TEXT NOT NULL,  -- e.g. '07-01' or '01-01' (informational)
          term_spec_json TEXT NOT NULL, -- JSON array of {label?, start_mmdd, end_mmdd}
          locked INTEGER NOT NULL DEFAULT 0, -- becomes 1 if in use
          is_system INTEGER NOT NULL DEFAULT 0,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME
        );""")
        _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_calprof_code ON calendar_profiles(code)")

def install_calendar_assignments(engine: Engine):
    """
    Assignments apply at degree/program/branch with precedence Branch > Program > Degree.
    This table now includes 'progression_year' to allow different calendars for Year 1, 2, etc.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS calendar_assignments(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          level TEXT NOT NULL CHECK(level IN ('degree','program','branch')),
          degree_code  TEXT NOT NULL COLLATE NOCASE,
          program_code TEXT COLLATE NOCASE DEFAULT '',
          branch_code  TEXT COLLATE NOCASE DEFAULT '',
          effective_from_ay TEXT NOT NULL COLLATE NOCASE, -- 'YYYY-YY'
          progression_year INTEGER NOT NULL DEFAULT 1, 
          calendar_id INTEGER NOT NULL,
          shift_days INTEGER NOT NULL DEFAULT 0, -- −30..+30
          active INTEGER NOT NULL DEFAULT 1,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME,
          UNIQUE(level, degree_code, program_code, branch_code, effective_from_ay, progression_year)
        );""")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_calasg_level_keys ON calendar_assignments(level, degree_code, program_code, branch_code, progression_year)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_calasg_effay ON calendar_assignments(effective_from_ay)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_calasg_active ON calendar_assignments(active)")

def install_calendar_assignments_audit(engine: Engine):
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS calendar_assignments_audit(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          target_key TEXT NOT NULL, -- e.g. 'branch:DEG1:PROG1:BR01@2025-26@PY1'
          action TEXT NOT NULL,     -- create, update, deactivate
          actor  TEXT,
          note   TEXT,
          changed_fields TEXT,      -- JSON
          at DATETIME DEFAULT CURRENT_TIMESTAMP
        );""")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_calasg_audit_target ON calendar_assignments_audit(target_key)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_calasg_audit_at ON calendar_assignments_audit(at)")

def seed_default_calendar_profiles(engine: Engine):
    """Seed default and example calendar profiles."""
    with engine.begin() as conn:
        # 1. System Default
        _exec(conn, """
        INSERT OR IGNORE INTO calendar_profiles(code, name, model, anchor_mmdd, term_spec_json, is_system)
        VALUES ('jul_2term', 'System Default (July 2-Term)', '2-Term', '07-01', :spec, 1);
        """, {
            "spec": json.dumps([
                {"label": "Term 1", "start_mmdd": "07-01", "end_mmdd": "12-15"},
                {"label": "Term 2", "start_mmdd": "01-10", "end_mmdd": "06-15"},
            ])
        })
        
        # 2. Example: B.Arch Year 1 (October Start)
        _exec(conn, """
        INSERT OR IGNORE INTO calendar_profiles(code, name, model, anchor_mmdd, term_spec_json, is_system)
        VALUES ('oct_2term', 'B.Arch Year 1 (Oct Start)', '2-Term', '10-01', :spec, 0);
        """, {
            "spec": json.dumps([
                {"label": "Sem 1", "start_mmdd": "10-01", "end_mmdd": "02-15"},
                {"label": "Sem 2", "start_mmdd": "03-01", "end_mmdd": "06-20"},
            ])
        })

        # 3. Example: B.Arch Year 2 (June Start)
        _exec(conn, """
        INSERT OR IGNORE INTO calendar_profiles(code, name, model, anchor_mmdd, term_spec_json, is_system)
        VALUES ('jun_2term', 'B.Arch Year 2 (June Start)', '2-Term', '06-15', :spec, 0);
        """, {
            "spec": json.dumps([
                {"label": "Sem 3", "start_mmdd": "06-15", "end_mmdd": "11-10"},
                {"label": "Sem 4", "start_mmdd": "11-20", "end_mmdd": "04-15"},
            ])
        })

        # 4. Default calendar setting
        _exec(conn, """
        INSERT OR IGNORE INTO app_settings(key, value) VALUES ('default_calendar_code', 'jul_2term');
        """)

def install_all(engine: Engine):
    install_academic_years(engine)
    install_ay_audit(engine)
    install_app_settings(engine)
    install_calendar_profiles(engine)
    install_calendar_assignments(engine)
    install_calendar_assignments_audit(engine)
    seed_default_calendar_profiles(engine)
