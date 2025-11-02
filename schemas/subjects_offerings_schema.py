# app/schemas/subjects_offerings_schema.py
"""
Subjects catalog, offerings, and syllabus schema.
Supports degree/program/branch-level subject assignments.
Safe to run multiple times.
"""

from __future__ import annotations
from sqlalchemy import text as sa_text

def _exec(conn, sql: str):
    """Execute SQL idempotently."""
    conn.execute(sa_text(sql))

def _has_column(conn, table: str, col: str) -> bool:
    """Check if a column exists in a SQLite table."""
    rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any(r[1].lower() == col.lower() for r in rows)

def _table_exists(conn, table: str) -> bool:
    """Check if a table exists."""
    result = conn.execute(sa_text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
    ), {"t": table}).fetchone()
    return result is not None


def migrate_subjects_offerings(engine):
    """
    Ensures subjects catalog, offerings, and syllabus tables exist.
    Supports degree/program/branch-level subject assignments.
    """
    with engine.begin() as conn:
        
        # =================================================================
        # 1. SUBJECTS CATALOG TABLE
        # =================================================================
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS subjects_catalog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject_code TEXT NOT NULL,
                subject_name TEXT NOT NULL,
                subject_type TEXT NOT NULL DEFAULT 'Core',
                
                -- Scope: degree/program/branch
                degree_code TEXT NOT NULL,
                program_code TEXT,
                branch_code TEXT,
                
                -- Credits breakdown (L/T/P/S)
                credits_total REAL NOT NULL DEFAULT 0.0,
                L INTEGER NOT NULL DEFAULT 0,
                T INTEGER NOT NULL DEFAULT 0,
                P INTEGER NOT NULL DEFAULT 0,
                S INTEGER NOT NULL DEFAULT 0,
                
                -- Additional credits
                student_credits REAL,
                teaching_credits REAL,
                
                -- Assessment marks configuration
                internal_marks_max INTEGER DEFAULT 40,
                exam_marks_max INTEGER DEFAULT 60,
                jury_viva_marks_max INTEGER DEFAULT 0,
                
                -- Pass/fail rules
                min_internal_percent REAL DEFAULT 50.0,
                min_external_percent REAL DEFAULT 40.0,
                min_overall_percent REAL DEFAULT 40.0,
                
                -- Attainment configuration
                direct_source_mode TEXT DEFAULT 'overall',
                direct_internal_threshold_percent REAL DEFAULT 50.0,
                direct_external_threshold_percent REAL DEFAULT 40.0,
                direct_internal_weight_percent REAL DEFAULT 40.0,
                direct_external_weight_percent REAL DEFAULT 60.0,
                direct_target_students_percent REAL DEFAULT 50.0,
                
                indirect_target_students_percent REAL DEFAULT 50.0,
                indirect_min_response_rate_percent REAL DEFAULT 75.0,
                
                overall_direct_weight_percent REAL DEFAULT 80.0,
                overall_indirect_weight_percent REAL DEFAULT 20.0,
                
                -- Metadata
                description TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 100,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                
                -- Unique constraint: subject_code must be unique per scope
                UNIQUE(subject_code, degree_code, program_code, branch_code),
                
                FOREIGN KEY (degree_code) REFERENCES degrees(code)
            )
        """)
        
        # Indexes for subjects_catalog
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_degree ON subjects_catalog(degree_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_program ON subjects_catalog(program_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_branch ON subjects_catalog(branch_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_code ON subjects_catalog(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_active ON subjects_catalog(active)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_status ON subjects_catalog(status)")
        
        # =================================================================
        # 2. SUBJECT OFFERINGS TABLE (Per AY-term instances)
        # =================================================================
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS subject_offerings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Links to catalog
                subject_id INTEGER NOT NULL,
                subject_code TEXT NOT NULL,
                
                -- Scope identifiers
                degree_code TEXT NOT NULL,
                program_code TEXT,
                branch_code TEXT,
                
                -- Temporal scope
                ay_label TEXT NOT NULL,
                year INTEGER NOT NULL,
                term INTEGER NOT NULL,
                
                -- Offering-specific overrides (nullable means use catalog defaults)
                credits_total REAL,
                L INTEGER,
                T INTEGER,
                P INTEGER,
                S INTEGER,
                
                internal_marks_max INTEGER,
                exam_marks_max INTEGER,
                jury_viva_marks_max INTEGER,
                
                min_internal_percent REAL,
                min_external_percent REAL,
                min_overall_percent REAL,
                
                -- Attainment overrides
                direct_source_mode TEXT,
                direct_internal_threshold_percent REAL,
                direct_external_threshold_percent REAL,
                direct_internal_weight_percent REAL,
                direct_external_weight_percent REAL,
                direct_target_students_percent REAL,
                
                indirect_target_students_percent REAL,
                indirect_min_response_rate_percent REAL,
                
                overall_direct_weight_percent REAL,
                overall_indirect_weight_percent REAL,
                
                -- Status and metadata
                status TEXT NOT NULL DEFAULT 'draft',
                has_marks_entered INTEGER NOT NULL DEFAULT 0,
                frozen INTEGER NOT NULL DEFAULT 0,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                
                -- Unique per AY-term-scope
                UNIQUE(subject_code, degree_code, program_code, branch_code, ay_label, year, term),
                
                FOREIGN KEY (subject_id) REFERENCES subjects_catalog(id),
                FOREIGN KEY (degree_code) REFERENCES degrees(code)
            )
        """)
        
        # Indexes for subject_offerings
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_subject ON subject_offerings(subject_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_code ON subject_offerings(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_degree ON subject_offerings(degree_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_ay ON subject_offerings(ay_label)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_year_term ON subject_offerings(year, term)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_status ON subject_offerings(status)")
        
        # =================================================================
        # 3. SYLLABUS POINTS TABLE
        # =================================================================
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS syllabus_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Links to offering
                offering_id INTEGER NOT NULL,
                subject_code TEXT NOT NULL,
                
                -- Scope identifiers (denormalized for easy querying)
                degree_code TEXT NOT NULL,
                program_code TEXT,
                branch_code TEXT,
                ay_label TEXT NOT NULL,
                year INTEGER NOT NULL,
                term INTEGER NOT NULL,
                
                -- Syllabus point details
                sequence INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                tags TEXT,
                resources TEXT,
                hours_weight REAL,
                
                -- Metadata
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_updated_by TEXT,
                
                -- Unique per offering
                UNIQUE(offering_id, sequence),
                
                FOREIGN KEY (offering_id) REFERENCES subject_offerings(id) ON DELETE CASCADE
            )
        """)
        
        # Indexes for syllabus_points
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_syllabus_offering ON syllabus_points(offering_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_syllabus_subject ON syllabus_points(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_syllabus_sequence ON syllabus_points(offering_id, sequence)")
        
        # =================================================================
        # 4. AUDIT TABLES
        # =================================================================
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS subjects_catalog_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject_id INTEGER,
                subject_code TEXT NOT NULL,
                degree_code TEXT NOT NULL,
                program_code TEXT,
                branch_code TEXT,
                action TEXT NOT NULL,
                note TEXT,
                changed_fields TEXT,
                actor TEXT,
                at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_audit_code ON subjects_catalog_audit(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_audit_at ON subjects_catalog_audit(at)")
        
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS subject_offerings_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                offering_id INTEGER,
                subject_code TEXT NOT NULL,
                degree_code TEXT NOT NULL,
                program_code TEXT,
                branch_code TEXT,
                ay_label TEXT NOT NULL,
                year INTEGER NOT NULL,
                term INTEGER NOT NULL,
                action TEXT NOT NULL,
                note TEXT,
                changed_fields TEXT,
                actor TEXT,
                at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_audit_code ON subject_offerings_audit(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_audit_at ON subject_offerings_audit(at)")
        
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS syllabus_points_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                point_id INTEGER,
                offering_id INTEGER,
                subject_code TEXT NOT NULL,
                action TEXT NOT NULL,
                note TEXT,
                changed_fields TEXT,
                actor TEXT,
                at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_syllabus_audit_point ON syllabus_points_audit(point_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_syllabus_audit_at ON syllabus_points_audit(at)")
        
        # =================================================================
        # 5. CREDIT PROFILES TABLE (Optional - for future use)
        # =================================================================
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS credit_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_name TEXT NOT NULL,
                profile_version TEXT NOT NULL DEFAULT 'v1',
                degree_code TEXT NOT NULL,
                program_code TEXT,
                branch_code TEXT,
                
                -- Profile configuration (JSON or structured)
                config TEXT,
                
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(profile_name, profile_version, degree_code, program_code, branch_code),
                
                FOREIGN KEY (degree_code) REFERENCES degrees(code)
            )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_profiles_degree ON credit_profiles(degree_code)")
