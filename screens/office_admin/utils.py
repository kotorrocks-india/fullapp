# screens/office_admin/utils.py
from __future__ import annotations
import re
import hashlib
import secrets

def is_valid_email(s: str) -> bool:
    """Validate email format."""
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s or ""))

def generate_initial_password(full_name: str) -> str:
    """
    Generate initial password based on policy:
    {first5lower}{lastinitiallower}@{4digits}
    
    Example: "John Doe" -> "johnj@1234"
    """
    parts = full_name.strip().split()
    if not parts:
        return f"temp@{secrets.randbelow(10000):04d}"
    
    first_name = parts[0].lower()
    last_initial = parts[-1][0].lower() if len(parts) > 1 else ""
    
    # Take first 5 chars of first name
    first_part = first_name[:5]
    
    # Generate 4 random digits
    digits = f"{secrets.randbelow(10000):04d}"
    
    return f"{first_part}{last_initial}@{digits}"

def hash_password(password: str) -> str:
    """
    Hash a password using SHA-256.
    In production, use bcrypt or argon2.
    """
    return hashlib.sha256(password.encode()).hexdigest()

def validate_username(username: str) -> tuple[bool, str]:
    """
    Validate username against policy:
    - Pattern: ^[a-z][a-z0-9._-]{5,29}$
    - Reserved words: admin, superadmin, root, support, help, test, system
    
    Returns: (is_valid, error_message)
    """
    reserved = ["admin", "superadmin", "root", "support", "help", "test", "system"]
    
    if username.lower() in reserved:
        return False, f"Username '{username}' is reserved."
    
    pattern = r"^[a-z][a-z0-9._-]{5,29}$"
    if not re.match(pattern, username):
        return False, "Username must start with lowercase letter, 6-30 chars, only lowercase letters, digits, ., _, -"
    
    return True, ""

def mask_pii(value: str, mask_char: str = "*") -> str:
    """
    Mask PII data for display.
    Shows first 2 and last 2 characters.
    """
    if not value or len(value) <= 4:
        return mask_char * len(value) if value else ""
    
    return f"{value[:2]}{mask_char * (len(value) - 4)}{value[-2:]}"
