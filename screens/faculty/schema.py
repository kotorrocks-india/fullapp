# app/screens/faculty/schema.py
"""
Faculty Module Schema - Module-Specific Tables Only

This module manages tables that are NOT covered by dedicated *_schema.py files.
All faculty tables with @register decorators are handled by the schema registry.

Tables managed here (unique to this module):
- faculty_profile_custom_fields: Admin-defined custom profile fields
- faculty_profile_custom_data: Actual custom field values per faculty
- affiliation_edit_approvals: Approval workflow for affiliation changes

Tables managed by dedicated schema files (NOT here):
- faculty_profiles (faculty_profiles_schema.py)
- faculty_initial_credentials (faculty_initial_credentials_schema.py)
- faculty_affiliations (affiliations_schema.py)
- affiliation_types (affiliation_types_schema.py)
- designations (designations_schema.py)
- designation_degree_enables (designation_degree_enables_schema.py)
- faculty_credits_policy (credits_policy_schema.py)
- academic_admin_credits_policy (academic_admin_credits_policy_schema.py)
- faculty_audit (faculty_audit_schema.py)
- emergency_deletion_log (emergency_deletion_log_schema.py)
"""
from __future__ import annotations

from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine


def install_custom_profile_fields(engine: Engine) -> None:
    """
    Create custom profile fields tables.
    
    Allows admins to dynamically define additional fields for faculty profiles
    beyond the standard ones (e.g., "Office Location", "Research Interests").
    
    Tables:
    - faculty_profile_custom_fields: Metadata about custom fields
    - faculty_profile_custom_data: Actual values stored per faculty member
    """
    with engine.begin() as conn:
        # Custom field definitions
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS faculty_profile_custom_fields(
                field_name   TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                field_type   TEXT NOT NULL DEFAULT 'text',
                field_options TEXT,
                is_active    INTEGER NOT NULL DEFAULT 1,
                created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at   DATETIME
            )
        """))
        
        # Custom field values
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS faculty_profile_custom_data(
                email       TEXT NOT NULL,
                field_name  TEXT NOT NULL,
                value       TEXT,
                updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(email, field_name)
            )
        """))
        
        # Index for efficient email lookups
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_fp_custom_lower_email
            ON faculty_profile_custom_data(lower(email))
        """))


def install_affiliation_edit_approvals(engine: Engine) -> None:
    """
    Create affiliation edit approvals table.
    
    Supports an approval workflow for faculty affiliation changes.
    Currently defined but not actively used in the UI.
    
    Future enhancement: Could enable faculty to request affiliation changes
    that require admin approval before taking effect.
    """
    with engine.begin() as conn:
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS affiliation_edit_approvals(
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                affiliation_id  INTEGER NOT NULL,
                requester_email TEXT NOT NULL,
                reason_note     TEXT,
                status          TEXT NOT NULL DEFAULT 'pending',
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at      DATETIME
            )
        """))
        
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_aff_edit_approvals_aff
            ON affiliation_edit_approvals(affiliation_id, status)
        """))


def install_all(engine: Engine) -> None:
    """
    Install all faculty module-specific tables.
    
    Called by page.py during schema bootstrap.
    This function creates ONLY the tables unique to the faculty module.
    
    All other faculty tables are created by dedicated *_schema.py files
    that use the @register decorator and are executed via run_all(engine).
    
    Order of execution in page.py:
    1. Core tables (degrees, branches, users, etc.)
    2. run_all(engine) - All @register schema files
    3. install_all(engine) - THIS FUNCTION (module-specific tables)
    """
    install_custom_profile_fields(engine)
    install_affiliation_edit_approvals(engine)
