# faculty_profiles_schema.py
from sqlalchemy import text as T
from core.schema_registry import register

# Import the helper from the core module
try:
    from core.schema_helpers import _ensure_column
except ImportError:
    # Fallback: define a minimal version
    def _ensure_column(conn, table, col, col_def):
        """Add column if it doesn't exist"""
        cols = conn.execute(T(f"PRAGMA table_info({table});")).fetchall()
        names = {str(c[1]).lower() for c in cols}
        if col.lower() in names:
            return
        conn.execute(T(f"ALTER TABLE {table} ADD COLUMN {col} {col_def};"))


@register
def install_faculty_profiles(engine):
    """Installs or migrates the faculty_profiles table."""
    print("🔧 [SCHEMA] Installing faculty_profiles table...")
    
    with engine.begin() as c:
        # Create table with all columns, including new ones
        c.execute(T("""
        CREATE TABLE IF NOT EXISTS faculty_profiles(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT NOT NULL UNIQUE COLLATE NOCASE,
          name TEXT NOT NULL,
          phone TEXT,
          employee_id TEXT UNIQUE,
          date_of_joining TEXT,
          highest_qualification TEXT,
          specialization TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          username TEXT UNIQUE COLLATE NOCASE,
          password_hash TEXT,
          first_login_pending INTEGER NOT NULL DEFAULT 1,
          password_export_available INTEGER NOT NULL DEFAULT 0,
          last_login_at DATETIME,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME
        )"""))
        
        print("✅ [SCHEMA] faculty_profiles table created/verified")
        
        # Ensure indexes exist
        c.execute(T("CREATE UNIQUE INDEX IF NOT EXISTS uq_faculty_email ON faculty_profiles(lower(email))"))
        c.execute(T("CREATE UNIQUE INDEX IF NOT EXISTS uq_faculty_username ON faculty_profiles(username) WHERE username IS NOT NULL"))
        c.execute(T("CREATE UNIQUE INDEX IF NOT EXISTS uq_faculty_emp_id ON faculty_profiles(employee_id) WHERE employee_id IS NOT NULL"))
        
        print("✅ [SCHEMA] faculty_profiles indexes created")

        # --- Migration for existing tables ---
        # Check if table already existed and add missing columns
        print("🔧 [SCHEMA] Checking for missing columns in faculty_profiles...")
        
        _ensure_column(c, "faculty_profiles", "username", "TEXT UNIQUE COLLATE NOCASE")
        _ensure_column(c, "faculty_profiles", "password_hash", "TEXT")
        _ensure_column(c, "faculty_profiles", "first_login_pending", "INTEGER NOT NULL DEFAULT 1")
        _ensure_column(c, "faculty_profiles", "password_export_available", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(c, "faculty_profiles", "last_login_at", "DATETIME")
        _ensure_column(c, "faculty_profiles", "created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP")
        _ensure_column(c, "faculty_profiles", "updated_at", "DATETIME")
        
        print("✅ [SCHEMA] faculty_profiles migration complete")
        
        # Verify the columns exist
        cols = c.execute(T("PRAGMA table_info(faculty_profiles);")).fetchall()
        col_names = [col[1] for col in cols]
        print(f"📊 [SCHEMA] faculty_profiles columns: {', '.join(col_names)}")
        
        # Check for critical columns
        critical_cols = ['username', 'password_hash', 'first_login_pending', 'password_export_available']
        missing = [col for col in critical_cols if col not in col_names]
        
        if missing:
            print(f"⚠️ [SCHEMA] WARNING: Missing columns: {', '.join(missing)}")
        else:
            print("✅ [SCHEMA] All critical columns present")


# Also export the function directly in case it's imported elsewhere
__all__ = ['install_faculty_profiles']
