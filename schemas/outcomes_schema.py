# app/schemas/outcomes_schema.py
"""
Program Outcomes schema: PEOs, POs, PSOs
Manages educational objectives and outcomes at degree/program/branch scope.
Based on slide16_POS.yaml specification.
"""

from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from core.schema_registry import register


def _exec(conn, sql: str):
    """Execute SQL with SQLAlchemy text wrapper."""
    conn.execute(sa_text(sql))


def _has_column(conn, table: str, col: str) -> bool:
    """Check if a column exists in a SQLite table."""
    rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any(r[1].lower() == col.lower() for r in rows)


def create_outcomes_scope_config(engine: Engine):
    """
    Creates table to store scope configuration per degree.
    Determines whether outcomes are managed at degree/program/branch level.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS outcomes_scope_config(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL UNIQUE,
            scope_level TEXT NOT NULL CHECK(scope_level IN ('per_degree','per_program','per_branch')) DEFAULT 'per_program',
            changed_by TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            change_reason TEXT,
            FOREIGN KEY(degree_code) REFERENCES degrees(code) ON DELETE CASCADE
        )
        """)
        
        _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_osc_degree ON outcomes_scope_config(lower(degree_code))")
        
        # Audit table for scope changes
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS outcomes_scope_config_audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL,
            action TEXT NOT NULL,
            old_scope TEXT,
            new_scope TEXT,
            reason TEXT,
            actor TEXT,
            at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_osc_audit_degree ON outcomes_scope_config_audit(degree_code)")


def create_outcomes_sets(engine: Engine):
    """
    Creates main outcomes sets table.
    Each set contains multiple PEO/PO/PSO items for a given scope.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS outcomes_sets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            set_type TEXT NOT NULL CHECK(set_type IN ('peos','pos','psos')),
            status TEXT NOT NULL CHECK(status IN ('draft','published','archived')) DEFAULT 'draft',
            version INTEGER NOT NULL DEFAULT 1,
            is_current INTEGER NOT NULL DEFAULT 1,
            created_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            published_by TEXT,
            published_at TIMESTAMP,
            archived_by TEXT,
            archived_at TIMESTAMP,
            archive_reason TEXT,
            FOREIGN KEY(degree_code) REFERENCES degrees(code) ON DELETE CASCADE
        )
        """)
        
        # Indexes for efficient querying
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_os_degree ON outcomes_sets(lower(degree_code))")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_os_program ON outcomes_sets(lower(program_code))")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_os_branch ON outcomes_sets(lower(branch_code))")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_os_type_status ON outcomes_sets(set_type, status)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_os_current ON outcomes_sets(is_current)")
        
        # Composite index for scope resolution
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS ix_os_scope 
        ON outcomes_sets(degree_code, program_code, branch_code, set_type, status)
        """)


def create_outcomes_items(engine: Engine):
    """
    Creates table for individual outcome items (PEO/PO/PSO entries).
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS outcomes_items(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            title TEXT,
            description TEXT NOT NULL,
            bloom_level TEXT CHECK(bloom_level IN ('Remember','Understand','Apply','Analyze','Evaluate','Create')),
            timeline_years INTEGER CHECK(timeline_years >= 1 AND timeline_years <= 10),
            tags TEXT,
            sort_order INTEGER NOT NULL DEFAULT 100,
            created_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(set_id) REFERENCES outcomes_sets(id) ON DELETE CASCADE
        )
        """)
        
        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oi_set ON outcomes_items(set_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oi_code ON outcomes_items(lower(code))")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oi_sort ON outcomes_items(set_id, sort_order)")
        
        # Unique code within a set
        _exec(conn, """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_oi_set_code 
        ON outcomes_items(set_id, lower(code))
        """)


def create_outcomes_audit(engine: Engine):
    """
    Creates comprehensive audit table for all outcomes operations.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS outcomes_audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            actor_role TEXT,
            operation TEXT NOT NULL,
            scope_degree TEXT,
            scope_program TEXT,
            scope_branch TEXT,
            set_type TEXT,
            set_id INTEGER,
            item_id INTEGER,
            before_data TEXT,
            after_data TEXT,
            reason TEXT,
            source TEXT CHECK(source IN ('ui','import','api')) DEFAULT 'ui',
            correlation_id TEXT,
            step_up_performed INTEGER DEFAULT 0,
            occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Indexes for audit queries
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oa_event ON outcomes_audit(event_type)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oa_actor ON outcomes_audit(actor_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oa_scope ON outcomes_audit(scope_degree, scope_program, scope_branch)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oa_set ON outcomes_audit(set_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oa_occurred ON outcomes_audit(occurred_at)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oa_correlation ON outcomes_audit(correlation_id)")


def create_outcomes_versions(engine: Engine):
    """
    Creates version snapshots table for rollback capability.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS outcomes_versions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_id INTEGER NOT NULL,
            version INTEGER NOT NULL,
            snapshot_data TEXT NOT NULL,
            snapshot_reason TEXT NOT NULL,
            created_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(set_id) REFERENCES outcomes_sets(id) ON DELETE CASCADE
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_ov_set ON outcomes_versions(set_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_ov_version ON outcomes_versions(set_id, version)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_ov_created ON outcomes_versions(created_at)")
        
        # Unique version per set
        _exec(conn, """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ov_set_version 
        ON outcomes_versions(set_id, version)
        """)


def create_outcomes_mappings(engine: Engine):
    """
    Creates table to track where outcomes are mapped/referenced.
    Used to prevent breaking changes when outcomes are in use.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS outcomes_mappings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            outcome_set_id INTEGER NOT NULL,
            outcome_item_id INTEGER NOT NULL,
            mapped_to_type TEXT NOT NULL,
            mapped_to_id INTEGER NOT NULL,
            mapped_to_ref TEXT,
            created_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(outcome_set_id) REFERENCES outcomes_sets(id) ON DELETE CASCADE,
            FOREIGN KEY(outcome_item_id) REFERENCES outcomes_items(id) ON DELETE CASCADE
        )
        """)
        
        # Indexes for mapping lookups
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_om_set ON outcomes_mappings(outcome_set_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_om_item ON outcomes_mappings(outcome_item_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_om_target ON outcomes_mappings(mapped_to_type, mapped_to_id)")
        
        # Prevent duplicate mappings
        _exec(conn, """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_om_mapping 
        ON outcomes_mappings(outcome_item_id, mapped_to_type, mapped_to_id)
        """)


def create_outcomes_approvals_queue(engine: Engine):
    """
    Creates queue table for approval workflow integration (Slide 38).
    Stores pending approval requests for outcomes operations.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS outcomes_approvals_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL UNIQUE,
            operation_key TEXT NOT NULL,
            queue_name TEXT NOT NULL DEFAULT 'OUTCOMES',
            status TEXT NOT NULL CHECK(status IN ('pending','approved','rejected','escalated','cancelled')) DEFAULT 'pending',
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            set_type TEXT,
            submitted_by TEXT NOT NULL,
            submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            step_up_verified INTEGER DEFAULT 0,
            reason TEXT NOT NULL,
            payload_data TEXT NOT NULL,
            approver_roles TEXT NOT NULL,
            approved_by TEXT,
            approved_at TIMESTAMP,
            rejected_by TEXT,
            rejected_at TIMESTAMP,
            rejection_reason TEXT,
            sla_hours INTEGER NOT NULL,
            escalate_after_hours INTEGER NOT NULL,
            escalated_to TEXT,
            escalated_at TIMESTAMP,
            expires_at TIMESTAMP,
            completed_at TIMESTAMP
        )
        """)
        
        # Indexes for approval workflows
        _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_oaq_request ON outcomes_approvals_queue(request_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oaq_status ON outcomes_approvals_queue(status)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oaq_operation ON outcomes_approvals_queue(operation_key)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oaq_submitted ON outcomes_approvals_queue(submitted_by, submitted_at)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oaq_scope ON outcomes_approvals_queue(degree_code, program_code, branch_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_oaq_expires ON outcomes_approvals_queue(expires_at)")


def create_outcomes_import_sessions(engine: Engine):
    """
    Creates table to track import sessions and their results.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS outcomes_import_sessions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL UNIQUE,
            file_name TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            degree_code TEXT NOT NULL,
            uploaded_by TEXT NOT NULL,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            preview_status TEXT CHECK(preview_status IN ('pending','completed','failed')) DEFAULT 'pending',
            preview_errors TEXT,
            preview_warnings TEXT,
            import_status TEXT CHECK(import_status IN ('not_started','in_progress','completed','failed','rolled_back')),
            import_started_at TIMESTAMP,
            import_completed_at TIMESTAMP,
            records_total INTEGER DEFAULT 0,
            records_imported INTEGER DEFAULT 0,
            records_failed INTEGER DEFAULT 0,
            error_log TEXT,
            rollback_reason TEXT,
            rolled_back_at TIMESTAMP
        )
        """)
        
        _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_ois_session ON outcomes_import_sessions(session_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_ois_uploaded ON outcomes_import_sessions(uploaded_by, uploaded_at)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_ois_status ON outcomes_import_sessions(import_status)")


def verify_outcomes_schema(engine: Engine):
    """Verifies that all outcomes tables were created successfully."""
    with engine.begin() as conn:
        print("\n=== Verifying Outcomes Schema ===")
        
        expected_tables = [
            'outcomes_scope_config',
            'outcomes_scope_config_audit',
            'outcomes_sets',
            'outcomes_items',
            'outcomes_audit',
            'outcomes_versions',
            'outcomes_mappings',
            'outcomes_approvals_queue',
            'outcomes_import_sessions'
        ]
        
        result = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )).fetchall()
        
        existing_tables = {row[0] for row in result}
        
        all_present = True
        for table in expected_tables:
            if table in existing_tables:
                print(f"✅ {table}")
            else:
                print(f"❌ {table} - MISSING")
                all_present = False
        
        if all_present:
            print("\n✅ All outcomes tables created successfully!")
        else:
            print("\n⚠️  Some tables are missing!")
        
        print("=== Verification Complete ===\n")


@register
def ensure_outcomes_schema(engine: Engine):
    """
    Main entry point for outcomes schema initialization.
    Creates all tables, indexes, and constraints for the outcomes module.
    Registered for auto-discovery by the schema registry.
    """
    print("\n" + "="*60)
    print("OUTCOMES (PEOs/POs/PSOs) SCHEMA INITIALIZATION")
    print("="*60)
    
    # Create all tables in dependency order
    create_outcomes_scope_config(engine)
    create_outcomes_sets(engine)
    create_outcomes_items(engine)
    create_outcomes_audit(engine)
    create_outcomes_versions(engine)
    create_outcomes_mappings(engine)
    create_outcomes_approvals_queue(engine)
    create_outcomes_import_sessions(engine)
    
    # Verify everything was created
    verify_outcomes_schema(engine)
    
    print("✅ Outcomes schema initialization complete!")
    print("="*60 + "\n")


# Alternative entry point for manual execution
def run(engine: Engine):
    """Alternative entry point compatible with legacy schema runners."""
    ensure_outcomes_schema(engine)
