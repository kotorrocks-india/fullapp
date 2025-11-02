# schemas/syllabus_templates_schema.py
"""
Template-Based Syllabus Schema - Clean Implementation
No migration needed - designed for new databases.
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
# CORE TABLES
# ===========================================================================

def create_syllabus_templates_table(engine: Engine):
    """Create syllabus_templates table for reusable curriculum templates."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Template identification
            code TEXT NOT NULL UNIQUE,  -- e.g., 'MECH101_v1', 'CS101_2024'
            subject_code TEXT NOT NULL,
            
            -- Versioning
            version TEXT NOT NULL,  -- e.g., 'v1', 'v2', '2024', 'Spring2024'
            version_number INTEGER NOT NULL DEFAULT 1,
            
            -- Metadata
            name TEXT NOT NULL,  -- e.g., 'Thermodynamics Standard Syllabus v1'
            description TEXT,
            
            -- Lifecycle
            effective_from_ay TEXT,  -- When this version becomes active
            deprecated_from_ay TEXT,  -- When this version is superseded
            is_current INTEGER NOT NULL DEFAULT 1,  -- Only one current per subject
            
            -- Scope (optional - can apply to specific degree/program/branch)
            degree_code TEXT,
            program_code TEXT,
            branch_code TEXT,
            
            -- Audit
            created_by TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            updated_at DATETIME,
            
            -- Prevent duplicate versions per subject
            UNIQUE(subject_code, version)
        )
        """)
        
        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_syllabus_templates_subject ON syllabus_templates(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_syllabus_templates_current ON syllabus_templates(is_current)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_syllabus_templates_effective ON syllabus_templates(effective_from_ay)")
        
        logger.info("Created syllabus_templates table")

def create_syllabus_template_points_table(engine: Engine):
    """Create syllabus_template_points table for template content."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_template_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Links to template
            template_id INTEGER NOT NULL,
            
            -- Content
            sequence INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            tags TEXT,  -- Comma-separated or JSON
            resources TEXT,  -- URLs, book chapters, etc.
            hours_weight REAL,  -- Contact hours for this topic
            
            -- Learning outcomes (optional - JSON array)
            learning_outcomes TEXT,
            
            -- Assessment methods (optional - JSON array)
            assessment_methods TEXT,
            
            -- Audit
            created_by TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            updated_at DATETIME,
            
            -- Foreign key
            FOREIGN KEY (template_id) REFERENCES syllabus_templates(id) ON DELETE CASCADE,
            
            -- Ensure unique sequence per template
            UNIQUE(template_id, sequence)
        )
        """)
        
        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_template_points_template ON syllabus_template_points(template_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_template_points_sequence ON syllabus_template_points(template_id, sequence)")
        
        logger.info("Created syllabus_template_points table")

def create_subject_offerings_table(engine: Engine):
    """Create subject_offerings table with template support."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Subject reference
            subject_id INTEGER,  -- FK to subjects_catalog
            subject_code TEXT NOT NULL,
            
            -- Context
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            
            -- When (AY-Year-Term)
            ay_label TEXT NOT NULL,  -- e.g., '2024-25'
            year INTEGER NOT NULL,   -- Year of study: 1, 2, 3...
            term INTEGER NOT NULL,   -- Term within year: 1, 2, 3...
            
            -- Template link (THE KEY ADDITION)
            syllabus_template_id INTEGER,  -- NULL = no template (legacy or custom)
            syllabus_customized INTEGER NOT NULL DEFAULT 0,  -- Has overrides?
            
            -- Instructor assignment
            instructor_email TEXT,
            
            -- Status
            status TEXT NOT NULL DEFAULT 'draft',  -- draft, published, archived
            
            -- Audit
            created_by TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            updated_at DATETIME,
            
            -- Foreign keys
            FOREIGN KEY (subject_id) REFERENCES subjects_catalog(id) ON DELETE SET NULL,
            FOREIGN KEY (syllabus_template_id) REFERENCES syllabus_templates(id) ON DELETE SET NULL,
            
            -- Prevent duplicate offerings
            UNIQUE(subject_code, degree_code, program_code, branch_code, ay_label, year, term)
        )
        """)
        
        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_subject ON subject_offerings(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_ay ON subject_offerings(ay_label)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_template ON subject_offerings(syllabus_template_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offerings_context ON subject_offerings(degree_code, program_code, branch_code)")
        
        logger.info("Created subject_offerings table")

def create_syllabus_point_overrides_table(engine: Engine):
    """Create syllabus_point_overrides table for per-offering customizations."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_point_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Links to offering
            offering_id INTEGER NOT NULL,
            
            -- Which point is being overridden
            sequence INTEGER NOT NULL,
            
            -- Override type
            override_type TEXT NOT NULL DEFAULT 'replace',  
            -- 'replace': Full override
            -- 'append': Add to template content
            -- 'prepend': Add before template content
            -- 'hide': Don't show this point
            
            -- Overridden content (NULL = use template value)
            title TEXT,
            description TEXT,
            tags TEXT,
            resources TEXT,
            hours_weight REAL,
            learning_outcomes TEXT,
            assessment_methods TEXT,
            
            -- Why was this overridden?
            override_reason TEXT,
            
            -- Audit
            created_by TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            updated_at DATETIME,
            
            -- Foreign key
            FOREIGN KEY (offering_id) REFERENCES subject_offerings(id) ON DELETE CASCADE,
            
            -- One override per sequence per offering
            UNIQUE(offering_id, sequence)
        )
        """)
        
        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_overrides_offering ON syllabus_point_overrides(offering_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_overrides_type ON syllabus_point_overrides(override_type)")
        
        logger.info("Created syllabus_point_overrides table")

# ===========================================================================
# AUDIT TABLES
# ===========================================================================

def create_audit_tables(engine: Engine):
    """Create audit tables for tracking changes."""
    with engine.begin() as conn:
        # Template audit
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_templates_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            template_code TEXT NOT NULL,
            action TEXT NOT NULL,  -- create, update, deprecate, activate
            note TEXT,
            changed_fields TEXT,  -- JSON
            actor TEXT NOT NULL,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_template_audit_id ON syllabus_templates_audit(template_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_template_audit_at ON syllabus_templates_audit(at)")
        
        # Override audit
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS syllabus_overrides_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            override_id INTEGER NOT NULL,
            offering_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            action TEXT NOT NULL,  -- create, update, delete
            note TEXT,
            changed_fields TEXT,  -- JSON
            actor TEXT NOT NULL,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_override_audit_id ON syllabus_overrides_audit(override_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_override_audit_offering ON syllabus_overrides_audit(offering_id)")
        
        # Offering audit
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            offering_id INTEGER NOT NULL,
            subject_code TEXT NOT NULL,
            degree_code TEXT NOT NULL,
            ay_label TEXT NOT NULL,
            year INTEGER NOT NULL,
            term INTEGER NOT NULL,
            action TEXT NOT NULL,  -- create, update, publish, archive
            note TEXT,
            changed_fields TEXT,  -- JSON
            actor TEXT NOT NULL,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offering_audit_id ON subject_offerings_audit(offering_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_offering_audit_at ON subject_offerings_audit(at)")
        
        logger.info("Created audit tables")

# ===========================================================================
# SEED DATA
# ===========================================================================

def seed_example_templates(engine: Engine):
    """Seed some example templates for testing."""
    with engine.begin() as conn:
        # Check if already seeded
        existing = _exec(conn, """
            SELECT COUNT(*) FROM syllabus_templates
        """).fetchone()[0]
        
        if existing > 0:
            logger.info("Templates already exist, skipping seed")
            return
        
        # Example: Create a generic template
        _exec(conn, """
        INSERT INTO syllabus_templates (
            code, subject_code, version, version_number,
            name, description, is_current, created_by
        ) VALUES (
            'EXAMPLE_v1', 'EXAMPLE101', 'v1', 1,
            'Example Standard Syllabus', 
            'This is an example template - delete or modify as needed',
            1, 'system'
        )
        """)
        
        template_id = _exec(conn, "SELECT last_insert_rowid()").fetchone()[0]
        
        # Add example points
        example_points = [
            (1, "Introduction to the Subject", "Overview of key concepts and course objectives"),
            (2, "Fundamental Concepts", "Core principles and terminology"),
            (3, "Practical Applications", "Real-world examples and case studies"),
            (4, "Advanced Topics", "In-depth exploration of complex areas"),
            (5, "Review and Assessment", "Summary and evaluation methods")
        ]
        
        for seq, title, desc in example_points:
            _exec(conn, """
            INSERT INTO syllabus_template_points (
                template_id, sequence, title, description, created_by
            ) VALUES (
                :tid, :seq, :title, :desc, 'system'
            )
            """, {"tid": template_id, "seq": seq, "title": title, "desc": desc})
        
        logger.info("Seeded example template")

# ===========================================================================
# MASTER INSTALL FUNCTION
# ===========================================================================

def install_template_syllabus_schema(engine: Engine, seed_examples: bool = False):
    """
    Install complete template-based syllabus schema.
    Safe to run multiple times (uses IF NOT EXISTS).
    
    Args:
        engine: SQLAlchemy engine
        seed_examples: If True, create example templates for testing
    """
    logger.info("Installing template-based syllabus schema...")
    
    try:
        # Core tables
        create_syllabus_templates_table(engine)
        create_syllabus_template_points_table(engine)
        create_subject_offerings_table(engine)
        create_syllabus_point_overrides_table(engine)
        
        # Audit tables
        create_audit_tables(engine)
        
        # Optional: Seed examples
        if seed_examples:
            seed_example_templates(engine)
        
        logger.info("✅ Template-based syllabus schema installed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Schema installation failed: {e}", exc_info=True)
        return False

# ===========================================================================
# UTILITY QUERY: Get Effective Syllabus
# ===========================================================================

def get_effective_syllabus_for_offering(conn, offering_id: int) -> list[dict]:
    """
    Get the effective syllabus for an offering (template + overrides merged).
    This is THE key query function for the template architecture.
    
    Returns list of points with all fields populated (template + overrides).
    """
    # Get offering and its template
    offering = _exec(conn, """
        SELECT so.syllabus_template_id, so.syllabus_customized
        FROM subject_offerings so
        WHERE so.id = :oid
    """, {"oid": offering_id}).fetchone()
    
    if not offering or not offering[0]:
        # No template assigned - return empty (or could fall back to legacy system)
        return []
    
    template_id = offering[0]
    
    # Get template points
    template_points = _exec(conn, """
        SELECT tp.sequence, tp.title, tp.description, tp.tags, 
               tp.resources, tp.hours_weight, tp.learning_outcomes,
               tp.assessment_methods
        FROM syllabus_template_points tp
        WHERE tp.template_id = :tid
        ORDER BY tp.sequence
    """, {"tid": template_id}).fetchall()
    
    # Get overrides
    overrides = _exec(conn, """
        SELECT sequence, override_type, title, description, 
               tags, resources, hours_weight, learning_outcomes,
               assessment_methods
        FROM syllabus_point_overrides
        WHERE offering_id = :oid
    """, {"oid": offering_id}).fetchall()
    
    # Build override map
    override_map = {}
    for ov in overrides:
        override_map[ov[0]] = {
            "type": ov[1],
            "title": ov[2],
            "description": ov[3],
            "tags": ov[4],
            "resources": ov[5],
            "hours_weight": ov[6],
            "learning_outcomes": ov[7],
            "assessment_methods": ov[8]
        }
    
    # Merge template + overrides
    result = []
    for tp in template_points:
        seq = tp[0]
        
        if seq in override_map:
            ov = override_map[seq]
            
            if ov["type"] == "hide":
                continue  # Skip hidden points
            
            elif ov["type"] == "replace":
                # Use override values, fallback to template
                result.append({
                    "sequence": seq,
                    "title": ov["title"] or tp[1],
                    "description": ov["description"] or tp[2],
                    "tags": ov["tags"] or tp[3],
                    "resources": ov["resources"] or tp[4],
                    "hours_weight": ov["hours_weight"] if ov["hours_weight"] is not None else tp[5],
                    "learning_outcomes": ov["learning_outcomes"] or tp[6],
                    "assessment_methods": ov["assessment_methods"] or tp[7],
                    "is_overridden": True
                })
            
            elif ov["type"] == "append":
                # Append override to template
                result.append({
                    "sequence": seq,
                    "title": tp[1],
                    "description": (tp[2] or "") + "\n\n" + (ov["description"] or ""),
                    "tags": tp[3],
                    "resources": (tp[4] or "") + "\n" + (ov["resources"] or ""),
                    "hours_weight": tp[5],
                    "learning_outcomes": tp[6],
                    "assessment_methods": tp[7],
                    "is_overridden": True
                })
            
            elif ov["type"] == "prepend":
                # Prepend override to template
                result.append({
                    "sequence": seq,
                    "title": tp[1],
                    "description": (ov["description"] or "") + "\n\n" + (tp[2] or ""),
                    "tags": tp[3],
                    "resources": (ov["resources"] or "") + "\n" + (tp[4] or ""),
                    "hours_weight": tp[5],
                    "learning_outcomes": tp[6],
                    "assessment_methods": tp[7],
                    "is_overridden": True
                })
        else:
            # Use template as-is
            result.append({
                "sequence": seq,
                "title": tp[1],
                "description": tp[2],
                "tags": tp[3],
                "resources": tp[4],
                "hours_weight": tp[5],
                "learning_outcomes": tp[6],
                "assessment_methods": tp[7],
                "is_overridden": False
            })
    
    return result
