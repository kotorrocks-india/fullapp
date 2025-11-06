# schemas/subjects_offerings_schema.py
"""
Complete subjects, offerings, and template-based syllabus schema.
Clean implementation for new database - no migration code needed.
"""

from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger(__name__)


def _exec(conn, sql: str, params: dict = None):
    """Execute SQL with parameters."""
    return conn.execute(sa_text(sql), params or {})


# ===========================================================================
# SUBJECTS CATALOG (Timeless master data - no AY)
# ===========================================================================

def install_subjects_catalog(engine: Engine):
    """Master catalog of all subjects."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subjects_catalog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Identification
            subject_code TEXT NOT NULL,
            subject_name TEXT NOT NULL,
            subject_type TEXT NOT NULL DEFAULT 'Core',
            
            -- Scope
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            curriculum_group_code TEXT,
            
            -- Credits (L-T-P-S)
            credits_total REAL NOT NULL DEFAULT 0,
            L INTEGER NOT NULL DEFAULT 0,
            T INTEGER NOT NULL DEFAULT 0,
            P INTEGER NOT NULL DEFAULT 0,
            S INTEGER NOT NULL DEFAULT 0,
            student_credits REAL,
            teaching_credits REAL,
            
            -- Flexible workload breakup (JSON: [{code,name,hours}, ...])
            workload_breakup_json TEXT,
            
            -- Assessment
            internal_marks_max INTEGER NOT NULL DEFAULT 40,
            exam_marks_max INTEGER NOT NULL DEFAULT 60,
            jury_viva_marks_max INTEGER NOT NULL DEFAULT 0,
            
            -- Pass criteria
            min_internal_percent REAL NOT NULL DEFAULT 50.0,
            min_external_percent REAL NOT NULL DEFAULT 40.0,
            min_overall_percent REAL NOT NULL DEFAULT 40.0,
            
            -- Attainment config
            direct_source_mode TEXT NOT NULL DEFAULT 'overall',
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
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            
            UNIQUE(subject_code, degree_code, program_code, branch_code, curriculum_group_code)
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_code ON subjects_catalog(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_degree ON subjects_catalog(degree_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_active ON subjects_catalog(active)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_subjects_cg ON subjects_catalog(curriculum_group_code)")
        
        # Audit
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subjects_catalog_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id INTEGER NOT NULL,
            subject_code TEXT NOT NULL,
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            curriculum_group_code TEXT,
            action TEXT NOT NULL,
            note TEXT,
            changed_fields TEXT,
            actor TEXT NOT NULL,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        logger.info("✓ Installed subjects_catalog")


# ===========================================================================
# SYLLABUS TEMPLATES (Reusable curriculum definitions)
# ===========================================================================

def install_syllabus_templates(engine: Engine):
    """Reusable syllabus templates shared across offerings."""
    with engine.begin() as conn:
        # Template metadata
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Identification
            code TEXT NOT NULL UNIQUE,
            subject_code TEXT NOT NULL,
            
            -- Versioning
            version TEXT NOT NULL,
            version_number INTEGER NOT NULL DEFAULT 1,
            is_current INTEGER NOT NULL DEFAULT 0,
            deprecated_from_ay TEXT,
            
            -- Scope (optional specialization)
            degree_code TEXT,
            program_code TEXT,
            branch_code TEXT,
            
            -- Metadata
            name TEXT NOT NULL,
            description TEXT,
            effective_from_ay TEXT,
            
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_templates_subject ON syllabus_templates(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_templates_degree ON syllabus_templates(degree_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_templates_current ON syllabus_templates(is_current)")
        
        # Template points (sections / units)
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_template_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            point_type TEXT NOT NULL,  -- e.g., 'unit', 'co', 'po', 'assessment'
            code TEXT,
            title TEXT,
            description TEXT,
            metadata_json TEXT,
            
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            
            FOREIGN KEY (template_id) REFERENCES syllabus_templates(id) ON DELETE CASCADE,
            UNIQUE(template_id, sequence)
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_points_template ON syllabus_template_points(template_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_points_type ON syllabus_template_points(point_type)")
        
        # Audit table
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_templates_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER,
            template_code TEXT,
            action TEXT NOT NULL,
            note TEXT,
            changed_fields TEXT,
            actor TEXT NOT NULL,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        logger.info("✓ Installed syllabus_templates & points")


# ===========================================================================
# SUBJECT OFFERINGS (AY-specific, linked to catalog + templates)
# ===========================================================================

def install_subject_offerings(engine: Engine):
    """Subject offerings per AY-term."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Link to catalog
            subject_code TEXT NOT NULL,
            
            -- Scope
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            curriculum_group_code TEXT,
            
            -- When (AY-specific)
            ay_label TEXT NOT NULL,
            year INTEGER NOT NULL,
            term INTEGER NOT NULL,
            
            -- Instructor
            instructor_email TEXT,
            
            -- Syllabus template link
            syllabus_template_id INTEGER,
            syllabus_customized INTEGER NOT NULL DEFAULT 0,
            
            -- Status
            status TEXT NOT NULL DEFAULT 'draft',
            active INTEGER NOT NULL DEFAULT 1,
            
            -- Audit
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            
            FOREIGN KEY (syllabus_template_id) REFERENCES syllabus_templates(id),
            UNIQUE(subject_code, degree_code, program_code, branch_code, curriculum_group_code, ay_label, year, term)
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_subject ON subject_offerings(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_ay ON subject_offerings(ay_label)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_template ON subject_offerings(syllabus_template_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_cg ON subject_offerings(curriculum_group_code)")
        
        # Audit
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            offering_id INTEGER NOT NULL,
            subject_code TEXT NOT NULL,
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            curriculum_group_code TEXT,
            ay_label TEXT NOT NULL,
            year INTEGER NOT NULL,
            term INTEGER NOT NULL,
            action TEXT NOT NULL,
            note TEXT,
            changed_fields TEXT,
            actor TEXT NOT NULL,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        logger.info("✓ Installed subject_offerings")


# ===========================================================================
# SYLLABUS OVERRIDES (Per-offering customizations)
# ===========================================================================

def install_syllabus_overrides(engine: Engine):
    """Overrides of template points for a specific offering."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_point_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            offering_id INTEGER NOT NULL,
            template_point_id INTEGER,
            sequence INTEGER NOT NULL,
            
            -- Override details
            override_type TEXT NOT NULL,  -- 'add', 'modify', 'remove'
            point_type TEXT NOT NULL,
            code TEXT,
            title TEXT,
            description TEXT,
            metadata_json TEXT,
            
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            
            FOREIGN KEY (offering_id) REFERENCES subject_offerings(id) ON DELETE CASCADE,
            FOREIGN KEY (template_point_id) REFERENCES syllabus_template_points(id) ON DELETE SET NULL,
            UNIQUE(offering_id, sequence)
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_overrides_offering ON syllabus_point_overrides(offering_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_overrides_type ON syllabus_point_overrides(override_type)")
        
        # Audit
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_overrides_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            override_id INTEGER NOT NULL,
            offering_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            action TEXT NOT NULL,
            note TEXT,
            changed_fields TEXT,
            actor TEXT NOT NULL,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        logger.info("✓ Installed syllabus_overrides")


# ===========================================================================
# MASTER INSTALL FUNCTION
# ===========================================================================

def install_subjects_offerings_schema(engine: Engine):
    """
    Install complete subjects & syllabus schema.
    Safe to run multiple times (idempotent).
    """
    logger.info("Installing subjects & offerings schema...")
    
    try:
        install_subjects_catalog(engine)
        install_syllabus_templates(engine)
        install_subject_offerings(engine)
        install_syllabus_overrides(engine)
        
        logger.info("✅ Subjects & offerings schema installed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Schema installation failed: {e}", exc_info=True)
        return False
