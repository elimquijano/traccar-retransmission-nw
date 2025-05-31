from datetime import datetime, timedelta, timezone
import logging
import pytz  # Para manejo de zonas horarias robusto
from app.config import (
    TIMEZONE_SYSTEM,
    DATETIME_OFFSET_HOURS,
)  # Asegúrate que TIMEZONE_SYSTEM y DATETIME_OFFSET_HOURS estén en config.py

logger = logging.getLogger(__name__)


def format_traccar_datetime_with_offset(
    traccar_datetime_str: str | None, hours_offset: int = DATETIME_OFFSET_HOURS
) -> str:
    if not traccar_datetime_str:
        return ""
    try:
        # Standardize input: ensure it ends with +00:00 if it's 'Z' or has no offset info
        # and handle potential milliseconds
        dt_obj_utc: datetime
        if "." in traccar_datetime_str and (
            "Z" in traccar_datetime_str
            or "+" in traccar_datetime_str
            or "-" in traccar_datetime_str[10:]
        ):
            # Contains milliseconds and timezone info
            dt_obj_utc = datetime.fromisoformat(
                traccar_datetime_str.replace("Z", "+00:00")
            )
        elif "Z" in traccar_datetime_str:  # ISO format with Z
            dt_obj_utc = datetime.fromisoformat(
                traccar_datetime_str.replace("Z", "+00:00")
            )
        elif (
            "+" in traccar_datetime_str[10:] or "-" in traccar_datetime_str[10:]
        ):  # ISO format with offset
            dt_obj_utc = datetime.fromisoformat(traccar_datetime_str)
        else:  # Assuming naive datetime string is UTC
            # Try to parse common formats, remove milliseconds if present
            base_time_str = traccar_datetime_str.split(".")[0]
            dt_obj_naive_utc = datetime.strptime(base_time_str, "%Y-%m-%dT%H:%M:%S")
            dt_obj_utc = dt_obj_naive_utc.replace(tzinfo=timezone.utc)

        # Ensure it's timezone-aware UTC
        if dt_obj_utc.tzinfo is None or dt_obj_utc.tzinfo.utcoffset(dt_obj_utc) is None:
            dt_obj_utc = dt_obj_utc.replace(tzinfo=timezone.utc)
        else:  # Convert to UTC if it had another offset
            dt_obj_utc = dt_obj_utc.astimezone(timezone.utc)

        dt_obj_offset = dt_obj_utc + timedelta(hours=hours_offset)
        return dt_obj_offset.strftime("%d/%m/%Y %H:%M:%S")
    except ValueError as ve:
        logger.error(
            f"Error parsing date '{traccar_datetime_str}' with ISO or common formats: {ve}. Returning original."
        )
        return traccar_datetime_str


def get_current_datetime_str_for_log() -> str:
    """Returns current datetime as 'YYYY-MM-DD HH:MM:SS' string in system timezone."""
    try:
        tz = pytz.timezone(TIMEZONE_SYSTEM)
        now_local = datetime.now(tz)
        return now_local.strftime("%Y-%m-%d %H:%M:%S")
    except pytz.exceptions.UnknownTimeZoneError:
        logger.warning(f"Unknown timezone: {TIMEZONE_SYSTEM}. Using UTC for logs.")
        now_utc = datetime.now(timezone.utc)
        return now_utc.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(
            f"Error getting current datetime for log: {e}. Using UTC as fallback."
        )
        now_utc = datetime.now(timezone.utc)
        return now_utc.strftime("%Y-%m-%d %H:%M:%S")
