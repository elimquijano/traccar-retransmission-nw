from datetime import datetime, timedelta, timezone
import logging

logger = logging.getLogger(__name__)

def format_traccar_datetime_with_offset(
    traccar_datetime_str: str | None, hours_offset: int = -5
) -> str:
    """
    Formats a Traccar datetime string (assumed to be ISO 8601 UTC)
    to 'dd/mm/YYYY HH:MM:SS' with a given hour offset.
    """
    if not traccar_datetime_str:
        return ""
    try:
        # Python's fromisoformat handles 'Z' as UTC and timezone-aware ISO strings
        dt_obj_utc = datetime.fromisoformat(traccar_datetime_str.replace("Z", "+00:00"))
        # Ensure it's timezone-aware UTC if it wasn't already
        if dt_obj_utc.tzinfo is None:
            dt_obj_utc = dt_obj_utc.replace(tzinfo=timezone.utc)
        else:
            dt_obj_utc = dt_obj_utc.astimezone(timezone.utc) # Convert to UTC if it had another offset

        dt_obj_offset = dt_obj_utc + timedelta(hours=hours_offset)
        return dt_obj_offset.strftime("%d/%m/%Y %H:%M:%S")
    except ValueError:
        # Fallback for non-standard or timezone-naive ISO formats, assuming UTC
        try:
            base_time_str = traccar_datetime_str.split(".")[0].split("+")[0].replace("Z", "")
            dt_obj_naive_utc = datetime.strptime(base_time_str, "%Y-%m-%dT%H:%M:%S")
            # Apply offset, still naive, but based on UTC assumption
            dt_obj_offset_naive = dt_obj_naive_utc + timedelta(hours=hours_offset)
            return dt_obj_offset_naive.strftime("%d/%m/%Y %H:%M:%S")
        except ValueError as ve_fallback:
            logger.error(
                f"Error parsing date '{traccar_datetime_str}': {ve_fallback}. Returning original."
            )
            return traccar_datetime_str