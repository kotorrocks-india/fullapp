# screens/academic_years/utils.py
from __future__ import annotations
import re, datetime, logging, json
import streamlit as st

logger = logging.getLogger(__name__)

# --- Fallback error handler (if faculty utils not available) ---
try:
    from screens.faculty.utils import _handle_error  # reuse central toast
except Exception:
    def _handle_error(e: Exception, user_message: str = "An error occurred."):
        logger.error(user_message, exc_info=True)
        st.error(user_message)

# UPDATED: Allows optional 'AY' prefix (case-insensitive) and '/' or '-' separator
AY_CODE_PATTERN = re.compile(r"^(?:[Aa][Yy])?\d{4}[-/]\d{2}$")

def is_valid_ay_code(ay_code: str) -> bool:
    return bool(ay_code and AY_CODE_PATTERN.match(ay_code))

def validate_date_format(date_str: str) -> bool:
    try:
        datetime.date.fromisoformat(date_str)
        return True
    except Exception:
        return False

def _get_year_from_ay_code(ay_code: str) -> int | None:
    """Extracts the 4-digit year from a valid AY code (e.g., 2025, AY2025/26)."""
    if not ay_code:
        return None
    # Find the first sequence of 4 digits
    match = re.search(r"(\d{4})", ay_code)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            return None
    return None

def get_next_ay_code(current_ay_code: str) -> str | None:
    """
    Generates the next AY code (e.g., 2026-27) from a given code.
    Always returns the standard 'YYYY-YY' format.
    """
    if not is_valid_ay_code(current_ay_code):
        return None
    
    # UPDATED: Use helper to get year robustly
    base = _get_year_from_ay_code(current_ay_code)
    if base is None:
        return None
        
    nxt = base + 1
    yy = (nxt + 1) % 100
    # Always return the normalized format
    return f"{nxt}-{yy:02d}"

def validate_ay_code_dates(ay_code: str, start_date: datetime.date) -> bool:
    """Check if the AY code's start year aligns logically with the start date's calendar year."""
    if not is_valid_ay_code(ay_code):
        return False

    # UPDATED: Use helper to get year robustly
    ay_start_year = _get_year_from_ay_code(ay_code)
    if ay_start_year is None:
        return False
        
    date_year = start_date.year
    # Check if the start date year is the AY start year OR the calendar year before it.
    return date_year in (ay_start_year, ay_start_year - 1)

def generate_ay_range(start_ay: str, num_years: int) -> list[str]:
    if not is_valid_ay_code(start_ay):
        return []
    out, cur = [], start_ay
    for _ in range(num_years):
        out.append(cur)
        # get_next_ay_code will handle parsing and normalization
        cur = get_next_ay_code(cur)
        if not cur: break
    return out

# ---------- Calendar profile helpers ----------

def _mmdd_to_date(ay_start_year: int, mmdd: str) -> datetime.date:
    """Map 'MM-DD' to a concrete date within AY span: (AY start year .. AY start year+1)."""
    mm, dd = map(int, mmdd.split("-"))
    # If month >= July (7) we consider it same AY-start year; else it is next calendar year.
    year = ay_start_year if mm >= 7 else ay_start_year + 1
    return datetime.date(year, mm, dd)

def compute_term_windows_for_ay(
    profile: dict,
    ay_code: str,
    shift_days: int = 0
) -> list[dict]:
    """
    Given a stored calendar profile (with JSON spec) and AY code, produce concrete
    term windows [{label, start_date, end_date}].
    """
    if not is_valid_ay_code(ay_code):
        raise ValueError("Invalid AY code.")
    if shift_days < -30 or shift_days > 30:
        raise ValueError("shift_days must be between -30 and +30.")

    spec = json.loads(profile.get("term_spec_json") or "[]")
    
    # UPDATED: Use helper to get year robustly
    ay_start_year = _get_year_from_ay_code(ay_code)
    if ay_start_year is None:
        raise ValueError("Invalid AY code format for year extraction.")

    results = []
    for idx, term in enumerate(spec):
        label = term.get("label") or f"Term {idx+1}"
        start_mmdd = term["start_mmdd"]
        end_mmdd   = term["end_mmdd"]
        start_dt = _mmdd_to_date(ay_start_year, start_mmdd)
        end_dt   = _mmdd_to_date(ay_start_year, end_mmdd)

        # If computed end < start (rare for same AY boundary), bump end one year
        if end_dt < start_dt:
            end_dt = datetime.date(end_dt.year + 1, end_dt.month, end_dt.day)

        if shift_days:
            start_dt += datetime.timedelta(days=shift_days)
            end_dt   += datetime.timedelta(days=shift_days)

        results.append({
            "label": label,
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat()
        })
    return results
