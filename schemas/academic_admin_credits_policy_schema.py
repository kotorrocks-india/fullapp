# schemas/academic_admin_credits_policy_schema.py
from __future__ import annotations
from sqlalchemy import text as T
from sqlalchemy.engine import Engine
from core.schema_registry import register

@register
def install_academic_admin_credits_policy(engine: Engine):
    """
    Create credits policy table for institution-level academic admins.
    
    This table stores credit requirements for immutable academic admin roles
    (Principal and Director) who are institution-wide and not tied to specific degrees.
    """
    with engine.begin() as c:
        # Create the table
        c.execute(T("""
        CREATE TABLE IF NOT EXISTS academic_admin_credits_policy(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixed_role TEXT NOT NULL UNIQUE,
            required_credits INTEGER NOT NULL DEFAULT 0,
            allowed_credit_override INTEGER NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME
        )
        """))
        
        # Pre-populate with the two immutable roles
        c.execute(T("""
        INSERT INTO academic_admin_credits_policy (fixed_role, required_credits, allowed_credit_override)
        VALUES 
            ('principal', 0, 0),
            ('director', 0, 0)
        ON CONFLICT(fixed_role) DO NOTHING
        """))
