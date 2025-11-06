# schemas/students_schema.py
from __future__ import annotations

from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
import json
import logging

log = logging.getLogger(__name__)

def install_schema(engine: Engine) -> None:
    """
    Create (if missing) all student-related tables and indexes.
    Idempotent: safe to re-run.
    
    FIXED: This schema now matches what the application code expects:
    - student_enrollments.student_profile_id (not student_id)
    - student_enrollments.batch (not year_code)
    - student_enrollments.current_year (for year tracking)
    - student_initial_credentials.student_profile_id (not student_id)
    """
    ddl = [
      
        # 1) Core student profile
        """
        CREATE TABLE IF NOT EXISTS student_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email TEXT UNIQUE,
            student_id TEXT UNIQUE,
            phone TEXT,
            status TEXT DEFAULT 'active',
            username TEXT,
            first_login_pending INTEGER DEFAULT 1,
            password_export_available INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP
        )
        """,

        # 2) Enrollment records (ties a student to degree/program/branch/batch/year)
        """
        CREATE TABLE IF NOT EXISTS student_enrollments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_profile_id INTEGER NOT NULL,
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            batch TEXT NOT NULL,
            current_year INTEGER,
            enrollment_status TEXT DEFAULT 'active',
            is_primary INTEGER DEFAULT 1,
            enrolled_on DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY(student_profile_id) REFERENCES student_profiles(id)
        )
        """,

        # 3) First-time credentials (optional)
        """
        CREATE TABLE IF NOT EXISTS student_initial_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_profile_id INTEGER NOT NULL UNIQUE,
            username TEXT UNIQUE,
            plaintext TEXT,
            consumed INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(student_profile_id) REFERENCES student_profiles(id)
        )
        """,

        # 4) Custom profile fields (definitions)
        """
        CREATE TABLE IF NOT EXISTS student_custom_profile_fields (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            label TEXT NOT NULL,
            dtype TEXT NOT NULL,         
            -- 'text' | 'number' | 'date' | 'choice' | 'boolean'
            required INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP
        )
        """,

        # 5) Custom profile data (values)
        """
        CREATE TABLE IF NOT EXISTS student_custom_profile_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_profile_id INTEGER NOT NULL,
            field_code TEXT NOT NULL,
            value TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            UNIQUE(student_profile_id, field_code),
            FOREIGN KEY(student_profile_id) REFERENCES student_profiles(id),
            FOREIGN KEY(field_code) REFERENCES student_custom_profile_fields(code)
        )
        """,

        # --- NEW TABLES ADDED ---
        
        # 6) Formal batch hierarchy
        """
        CREATE TABLE IF NOT EXISTS degree_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL,
            batch_code TEXT NOT NULL,
            batch_name TEXT,
            start_date DATE NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(degree_code, batch_code)
        );
        """,

        # 7) Application settings
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """,
        
        # 8) Degree-to-Year scaffold
        """
        CREATE TABLE IF NOT EXISTS degree_year_scaffold (
            degree_code TEXT NOT NULL,
            year_num INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (degree_code, year_num)
        );
        """,

        # 9) Batch-to-Year scaffold
        """
        CREATE TABLE IF NOT EXISTS batch_year_scaffold (
            degree_code TEXT NOT NULL,
            batch TEXT NOT NULL,
            year_num INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (degree_code, batch, year_num)
        );
        """,
        
        # --- END OF ADDED TABLES ---

        # Helpful indexes
        "CREATE INDEX IF NOT EXISTS idx_student_profiles_email ON student_profiles(email)",
        "CREATE INDEX IF NOT EXISTS idx_student_profiles_sid   ON student_profiles(student_id)",
        "CREATE INDEX IF NOT EXISTS idx_student_profiles_username ON student_profiles(username)",
        "CREATE INDEX IF NOT EXISTS idx_enrollments_student    ON student_enrollments(student_profile_id)",
        "CREATE INDEX IF NOT EXISTS idx_enrollments_degree     ON student_enrollments(degree_code)",
        "CREATE INDEX IF NOT EXISTS idx_enrollments_batch      ON student_enrollments(batch)",
        "CREATE INDEX IF NOT EXISTS idx_enrollments_status     ON student_enrollments(enrollment_status)",
        "CREATE INDEX IF NOT EXISTS idx_custom_data_student    ON student_custom_profile_data(student_profile_id)"
    ]

    try:
        with engine.begin() as conn:
            for stmt in ddl:
                conn.execute(sa_text(stmt))
        log.info("✅ Student schema installed/verified successfully.")
    except Exception as e:
        log.error(f"❌ Failed to install student schema: {e}")
        raise


def migrate_existing_schema(engine: Engine) -> None:
    """
    Migration helper to update existing tables to match the new schema.
    Run this ONCE if you already have data in the old schema format.
    """
    with engine.begin() as conn:
        # Check if we're using the old schema
        old_schema_check = conn.execute(sa_text("""
            SELECT COUNT(*) 
            FROM pragma_table_info('student_enrollments') 
            WHERE name='student_id'
        """)).scalar()
        
        if old_schema_check > 0:
            print("🔄 Migrating from old schema to new schema...")
            
            # 1. Rename student_id to student_profile_id in enrollments
            conn.execute(sa_text("""
                ALTER TABLE student_enrollments 
                RENAME COLUMN student_id TO student_profile_id
            """))
            
            # 2. Add batch column if using year_code
            year_code_exists = conn.execute(sa_text("""
                SELECT COUNT(*) 
                FROM pragma_table_info('student_enrollments') 
                WHERE name='year_code'
            """)).scalar()
            
            if year_code_exists > 0:
                # Add batch column
                conn.execute(sa_text("""
                    ALTER TABLE student_enrollments 
                    ADD COLUMN batch TEXT
                """))
                
                # Copy year_code to batch
                conn.execute(sa_text("""
                    UPDATE student_enrollments 
                    SET batch = year_code
                """))
                
                print("✅ Migrated year_code -> batch")
            
            # 3. Add current_year if missing
            current_year_exists = conn.execute(sa_text("""
                SELECT COUNT(*) 
                FROM pragma_table_info('student_enrollments') 
                WHERE name='current_year'
            """)).scalar()
            
            if current_year_exists == 0:
                conn.execute(sa_text("""
                    ALTER TABLE student_enrollments 
                    ADD COLUMN current_year INTEGER
                """))
                print("✅ Added current_year column")
            
            # 4. Update student_initial_credentials FK
            creds_old_fk = conn.execute(sa_text("""
                SELECT COUNT(*) 
                FROM pragma_table_info('student_initial_credentials') 
                WHERE name='student_id'
            """)).scalar()
            
            if creds_old_fk > 0:
                conn.execute(sa_text("""
                    ALTER TABLE student_initial_credentials 
                    RENAME COLUMN student_id TO student_profile_id
                """))
                print("✅ Updated student_initial_credentials FK")
            
            # 5. Update custom data FK
            custom_old_fk = conn.execute(sa_text("""
                SELECT COUNT(*) 
                FROM pragma_table_info('student_custom_profile_data') 
                WHERE name='student_id'
            """)).scalar()
            
            if custom_old_fk > 0:
                conn.execute(sa_text("""
                    ALTER TABLE student_custom_profile_data 
                    RENAME COLUMN student_id TO student_profile_id
                """))
                print("✅ Updated student_custom_profile_data FK")
            
            # 6. Add missing columns to student_profiles
            for col, definition in [
                ('username', 'TEXT'),
                ('status', "TEXT DEFAULT 'active'"),
                ('first_login_pending', 'INTEGER DEFAULT 1'),
                ('password_export_available', 'INTEGER DEFAULT 1')
            ]:
                col_exists = conn.execute(sa_text(f"""
                    SELECT COUNT(*) 
                    FROM pragma_table_info('student_profiles') 
                    WHERE name='{col}'
                """)).scalar()
                
                if col_exists == 0:
                    conn.execute(sa_text(f"""
                        ALTER TABLE student_profiles 
                        ADD COLUMN {col} {definition}
                    """))
                    print(f"✅ Added {col} to student_profiles")
            
            # 7. Rename status to enrollment_status in enrollments
            status_check = conn.execute(sa_text("""
                SELECT COUNT(*) 
                FROM pragma_table_info('student_enrollments') 
                WHERE name='status'
            """)).scalar()
            
            enrollment_status_check = conn.execute(sa_text("""
                SELECT COUNT(*) 
                FROM pragma_table_info('student_enrollments') 
                WHERE name='enrollment_status'
            """)).scalar()
            
            if status_check > 0 and enrollment_status_check == 0:
                conn.execute(sa_text("""
                    ALTER TABLE student_enrollments 
                    RENAME COLUMN status TO enrollment_status
                """))
                print("✅ Renamed status -> enrollment_status")
            
            print("✅ Migration complete!")
        else:
            print("ℹ️  Schema is already up to date")
