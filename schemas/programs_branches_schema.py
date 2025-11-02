from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from core.schema_registry import register

def verify_branches_data(engine: Engine):
    """Verifies that all branches have proper degree_code values and shows data issues."""
    with engine.begin() as conn:
        print("\n=== Verifying Branches Data ===")
        
        # Check if branches table exists
        result = conn.execute(sa_text("SELECT name FROM sqlite_master WHERE type='table' AND name='branches'")).fetchone()
        if not result:
            print("❌ Branches table does not exist")
            return
        
        # Check if degree_code column exists
        result = conn.execute(sa_text("PRAGMA table_info(branches)")).fetchall()
        columns = [row[1] for row in result]
        
        if 'degree_code' not in columns:
            print("❌ degree_code column missing from branches table")
            return
        
        # Count total branches
        total_branches = conn.execute(sa_text("SELECT COUNT(*) FROM branches")).fetchone()[0]
        print(f"Total branches: {total_branches}")
        
        # Check branches with empty degree_code
        empty_degree = conn.execute(sa_text("SELECT COUNT(*) FROM branches WHERE degree_code = '' OR degree_code IS NULL")).fetchone()[0]
        print(f"Branches with empty degree_code: {empty_degree}")
        
        # Check branches with invalid program links
        invalid_links = conn.execute(sa_text("""
            SELECT COUNT(*) FROM branches b 
            WHERE b.program_id IS NOT NULL 
            AND NOT EXISTS (SELECT 1 FROM programs p WHERE p.id = b.program_id)
        """)).fetchone()[0]
        print(f"Branches with invalid program links: {invalid_links}")
        
        # Check branches where degree_code doesn't match parent program
        mismatched_degrees = conn.execute(sa_text("""
            SELECT COUNT(*) FROM branches b
            JOIN programs p ON b.program_id = p.id
            WHERE b.degree_code != p.degree_code
        """)).fetchone()[0]
        print(f"Branches with mismatched degree codes: {mismatched_degrees}")
        
        # Show sample of problematic data
        if empty_degree > 0 or invalid_links > 0 or mismatched_degrees > 0:
            print("\n⚠️  Data issues found. Sample of problematic records:")
            
            # Empty degree_code
            if empty_degree > 0:
                empty_records = conn.execute(sa_text("""
                    SELECT b.id, b.branch_code, b.branch_name, b.program_id, b.degree_code
                    FROM branches b 
                    WHERE b.degree_code = '' OR b.degree_code IS NULL
                    LIMIT 5
                """)).fetchall()
                print(f"\nEmpty degree_code samples:")
                for row in empty_records:
                    print(f"  - Branch {row[1]} (ID: {row[0]}) -> degree_code: '{row[4]}'")
            
            # Invalid program links
            if invalid_links > 0:
                invalid_records = conn.execute(sa_text("""
                    SELECT b.id, b.branch_code, b.branch_name, b.program_id
                    FROM branches b 
                    WHERE b.program_id IS NOT NULL 
                    AND NOT EXISTS (SELECT 1 FROM programs p WHERE p.id = b.program_id)
                    LIMIT 5
                """)).fetchall()
                print(f"\nInvalid program link samples:")
                for row in invalid_records:
                    print(f"  - Branch {row[1]} (ID: {row[0]}) -> program_id: {row[3]} (not found)")
            
            # Mismatched degrees
            if mismatched_degrees > 0:
                mismatch_records = conn.execute(sa_text("""
                    SELECT b.id, b.branch_code, b.degree_code, p.program_code, p.degree_code as program_degree
                    FROM branches b
                    JOIN programs p ON b.program_id = p.id
                    WHERE b.degree_code != p.degree_code
                    LIMIT 5
                """)).fetchall()
                print(f"\nMismatched degree samples:")
                for row in mismatch_records:
                    print(f"  - Branch {row[1]} -> branch.degree: '{row[2]}' vs program.degree: '{row[4]}'")
        
        if empty_degree == 0 and invalid_links == 0 and mismatched_degrees == 0:
            print("✅ All branches data verified and consistent!")
        
        print("=== Verification Complete ===\n")

def migrate_branches_degree_code(engine: Engine):
    """Ensure branches table has degree_code column and populate it correctly."""
    with engine.begin() as conn:
        # Check if column exists
        result = conn.execute(sa_text("PRAGMA table_info(branches)")).fetchall()
        columns = [row[1] for row in result]
        
        if 'degree_code' not in columns:
            print("🚀 Migrating: Adding degree_code column to branches table...")
            
            # Add the column
            conn.execute(sa_text("ALTER TABLE branches ADD COLUMN degree_code TEXT NOT NULL DEFAULT ''"))
            print("✅ Added degree_code column to branches table")
            
            # Populate with data from programs table
            result = conn.execute(sa_text("""
                UPDATE branches 
                SET degree_code = (
                    SELECT p.degree_code 
                    FROM programs p 
                    WHERE p.id = branches.program_id
                )
                WHERE degree_code = '' AND program_id IS NOT NULL
            """))
            
            updated_count = result.rowcount
            print(f"✅ Populated degree_code for {updated_count} branches from parent programs")
            
            # Handle branches without program_id (shouldn't happen, but just in case)
            orphaned_branches = conn.execute(sa_text("""
                SELECT COUNT(*) FROM branches 
                WHERE degree_code = '' AND program_id IS NULL
            """)).fetchone()[0]
            
            if orphaned_branches > 0:
                print(f"⚠️  Found {orphaned_branches} branches without program_id - these need manual review")
                
        else:
            print("✅ degree_code column already exists in branches table")
            
            # Even if column exists, verify data is consistent
            print("🔍 Verifying existing degree_code data consistency...")
            
            # Fix any branches with empty degree_code but valid program_id
            result = conn.execute(sa_text("""
                UPDATE branches 
                SET degree_code = (
                    SELECT p.degree_code 
                    FROM programs p 
                    WHERE p.id = branches.program_id
                )
                WHERE (degree_code = '' OR degree_code IS NULL) 
                AND program_id IS NOT NULL
            """))
            
            fixed_count = result.rowcount
            if fixed_count > 0:
                print(f"✅ Fixed degree_code for {fixed_count} branches that had empty values")
            
            # Fix any mismatched degree codes
            result = conn.execute(sa_text("""
                UPDATE branches 
                SET degree_code = (
                    SELECT p.degree_code 
                    FROM programs p 
                    WHERE p.id = branches.program_id
                )
                WHERE program_id IS NOT NULL
                AND degree_code != (
                    SELECT p.degree_code 
                    FROM programs p 
                    WHERE p.id = branches.program_id
                )
            """))
            
            mismatched_fixed = result.rowcount
            if mismatched_fixed > 0:
                print(f"✅ Fixed {mismatched_fixed} branches with mismatched degree codes")

def create_programs_and_branches(engine: Engine):
    """Creates tables for programs, branches, and their audit logs."""
    with engine.begin() as conn:
        # PROGRAMS
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS programs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL,
            program_code TEXT NOT NULL,
            program_name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 100,
            logo_file_name TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""))
        conn.execute(sa_text("CREATE UNIQUE INDEX IF NOT EXISTS uq_program_code ON programs(lower(program_code))"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_programs_degree ON programs(lower(degree_code))"))

        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS programs_audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            actor TEXT NOT NULL,
            note TEXT,
            degree_code TEXT,
            program_code TEXT,
            program_name TEXT,
            active INTEGER,
            sort_order INTEGER,
            logo_file_name TEXT,
            description TEXT,
            at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""))

        # BRANCHES
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS branches(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            program_id INTEGER,
            degree_code TEXT NOT NULL,
            branch_code TEXT NOT NULL,
            branch_name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 100,
            logo_file_name TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(program_id) REFERENCES programs(id) ON DELETE SET NULL
        )"""))
        
        # Now we can safely create all indexes since degree_code column exists
        conn.execute(sa_text("CREATE UNIQUE INDEX IF NOT EXISTS uq_branch_code ON branches(lower(branch_code))"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_branches_degree ON branches(lower(degree_code))"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_branches_program ON branches(program_id)"))

        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS branches_audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            actor TEXT NOT NULL,
            note TEXT,
            program_id INTEGER,
            degree_code TEXT,
            branch_code TEXT,
            branch_name TEXT,
            active INTEGER,
            sort_order INTEGER,
            logo_file_name TEXT,
            description TEXT,
            at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""))

def create_curriculum_groups(engine: Engine):
    """Creates tables for curriculum groups, links, and their audit logs."""
    with engine.begin() as conn:
        # CURRICULUM GROUPS
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS curriculum_groups(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL,
            group_code TEXT NOT NULL,
            group_name TEXT NOT NULL,
            kind TEXT NOT NULL CHECK(kind IN ('pseudo','cohort')) DEFAULT 'pseudo',
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 100,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""))
        conn.execute(sa_text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_cg_degree_code
            ON curriculum_groups(lower(degree_code), lower(group_code))
        """))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_cg_degree ON curriculum_groups(lower(degree_code))"))

        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS curriculum_groups_audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            actor TEXT NOT NULL,
            note TEXT,
            degree_code TEXT,
            group_code TEXT,
            group_name TEXT,
            kind TEXT,
            active INTEGER,
            sort_order INTEGER,
            description TEXT,
            at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""))

        # LINKS (attach groups to program and/or branch)
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS curriculum_group_links(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(group_id) REFERENCES curriculum_groups(id) ON DELETE CASCADE
        )"""))
        
        # Create a unique constraint without expressions
        conn.execute(sa_text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_cgl_composite 
            ON curriculum_group_links(group_id, program_code, branch_code)
        """))
        
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_cgl_group ON curriculum_group_links(group_id)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_cgl_degree ON curriculum_group_links(lower(degree_code))"))


# ================================================================= #
# ======================== CORRECTED LOGIC ======================== #
# ================================================================= #
@register
def ensure_programs_branches_schema(engine: Engine):
    """
    Initializes all schemas related to programs, branches, and curriculum.
    This function name is designed to be found by the auto-discovery system.
    """
    print("\n" + "="*60)
    print("PROGRAMS & BRANCHES SCHEMA INITIALIZATION")
    print("="*60)
    
    # 1. Create the tables first. This ensures they exist before we try to alter them.
    create_programs_and_branches(engine)
    create_curriculum_groups(engine)
    
    # 2. Run migration to add/populate columns on the now-existing tables.
    migrate_branches_degree_code(engine)
    
    # 3. Finally, run verification on the fully formed tables.
    verify_branches_data(engine)
    
    print("✅ Programs & Branches schema initialization complete!")
    print("="*60 + "\n")
