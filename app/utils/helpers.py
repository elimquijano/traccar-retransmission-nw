from datetime import datetime, timedelta, timezone
import logging
import pytz  # Para manejo de zonas horarias robusto
from app.config import (
    TIMEZONE_SYSTEM,  # Usado para el log y como fallback
    DATETIME_OFFSET_HOURS,  # Usado como offset por defecto para los handlers que lo requieran
)

logger = logging.getLogger(__name__)

# Estos deben coincidir con los HANDLER_ID definidos en tus clases de handler
HANDLER_ID_SEGURIDAD_CIUDADANA = "seguridad_ciudadana"
HANDLER_ID_PNP = "pnp"
HANDLER_ID_SUTRAN = "sutran"
HANDLER_ID_COMSATEL = "comsatel"
HANDLER_ID_OSINERGMIN = "osinergmin"


def format_traccar_datetime_for_handler(
    traccar_datetime_str: str | None,
    handler_id: str,  # Nuevo parámetro para identificar el handler
    default_hours_offset: int = DATETIME_OFFSET_HOURS,  # Offset por defecto si no se especifica uno para el handler
) -> str:
    """
    Formats a Traccar datetime string (assumed to be ISO 8601 UTC)
    to a specific format based on the handler_id.
    The input traccar_datetime_str is expected to be a UTC timestamp.
    """
    if not traccar_datetime_str:
        return ""

    try:
        # Paso 1: Parsear la fecha de Traccar a un objeto datetime UTC consciente de la zona horaria.
        # Esta lógica de parseo es la misma que tenías antes.
        dt_obj_utc: datetime
        if "." in traccar_datetime_str and (
            "Z" in traccar_datetime_str
            or "+" in traccar_datetime_str  # Busca '+' en cualquier parte
            or (  # Busca '-' solo si está en la parte de offset de tiempo
                "-" in traccar_datetime_str
                and len(traccar_datetime_str)
                > 10  # Asegura que no es el '-' de la fecha YYYY-MM-DD
                and "T"
                in traccar_datetime_str  # Más robusto para diferenciar de YYYY-MM-DD
                and traccar_datetime_str.rfind("-") > traccar_datetime_str.find("T")
            )
        ):
            dt_obj_utc = datetime.fromisoformat(
                traccar_datetime_str.replace("Z", "+00:00")
            )
        elif "Z" in traccar_datetime_str:
            dt_obj_utc = datetime.fromisoformat(
                traccar_datetime_str.replace("Z", "+00:00")
            )
        elif (
            "+" in traccar_datetime_str
            and traccar_datetime_str.rfind("+") > traccar_datetime_str.find("T")
        ) or (
            "-" in traccar_datetime_str
            and traccar_datetime_str.rfind("-") > traccar_datetime_str.find("T")
        ):
            dt_obj_utc = datetime.fromisoformat(traccar_datetime_str)
        else:
            base_time_str = traccar_datetime_str.split(".")[
                0
            ]  # Quitar milisegundos si los hay
            dt_obj_naive_utc = datetime.strptime(base_time_str, "%Y-%m-%dT%H:%M:%S")
            dt_obj_utc = dt_obj_naive_utc.replace(tzinfo=timezone.utc)

        # Asegurar que sea UTC consciente
        if dt_obj_utc.tzinfo is None or dt_obj_utc.tzinfo.utcoffset(dt_obj_utc) is None:
            dt_obj_utc = dt_obj_utc.replace(tzinfo=timezone.utc)
        elif dt_obj_utc.tzinfo != timezone.utc:
            dt_obj_utc = dt_obj_utc.astimezone(timezone.utc)

        # Paso 2: Aplicar offset y formatear según el handler_id
        # Por defecto, la mayoría usará el offset de DATETIME_OFFSET_HOURS (-5 para America/Lima)
        # pero algunos pueden requerir UTC (offset 0) o un formato específico.

        if handler_id in [HANDLER_ID_SEGURIDAD_CIUDADANA, HANDLER_ID_PNP]:
            # Formato: "dd/mm/YYYY HH:MM:SS" con offset (ej. America/Lima)
            dt_obj_with_offset = dt_obj_utc + timedelta(hours=default_hours_offset)
            return dt_obj_with_offset.strftime("%d/%m/%Y %H:%M:%S")

        elif handler_id == HANDLER_ID_OSINERGMIN:
            # Formato: "YYYY-MM-DDTHH:MM:SS.sssZ" (UTC, con milisegundos)
            # 'sss' son los primeros 3 dígitos de los microsegundos.
            # El '.%f' da microsegundos, lo truncamos a 3 dígitos.
            return (
                dt_obj_utc.strftime("%Y-%m-%dT%H:%M:%S.")
                + f"{dt_obj_utc.microsecond // 1000:03d}Z"
            )

        elif handler_id == HANDLER_ID_COMSATEL:
            # Formato: "YYYYMMDDHHMMSS" (con offset, ej. America/Lima)
            dt_obj_with_offset = dt_obj_utc + timedelta(hours=default_hours_offset)
            return dt_obj_with_offset.strftime("%Y%m%d%H%M%S")

        elif handler_id == HANDLER_ID_SUTRAN:
            # Formato: "YYYY-MM-DD HH:MM:SS" (con offset, ej. America/Lima)
            dt_obj_with_offset = dt_obj_utc + timedelta(hours=default_hours_offset)
            return dt_obj_with_offset.strftime("%Y-%m-%d %H:%M:%S")

        # Añade más casos para otros handlers aquí:
        # elif handler_id == "otro_handler_id":
        #     # Lógica de formato específica
        #     return ...

        else:
            # Fallback a un formato por defecto si el handler_id no es reconocido
            # O podrías lanzar un error. Aquí usamos el formato original de seguridad ciudadana.
            logger.warning(
                f"Handler ID '{handler_id}' no reconocido para formateo de fecha. "
                f"Usando formato por defecto dd/mm/YYYY HH:MM:SS con offset {default_hours_offset}."
            )
            dt_obj_with_offset = dt_obj_utc + timedelta(hours=default_hours_offset)
            return dt_obj_with_offset.strftime("%d/%m/%Y %H:%M:%S")

    except ValueError as ve:
        logger.error(
            f"Error parseando fecha '{traccar_datetime_str}' para handler '{handler_id}': {ve}. "
            f"Devolviendo cadena original."
        )
        return traccar_datetime_str
    except Exception as e:
        logger.error(
            f"Error inesperado formateando fecha '{traccar_datetime_str}' para handler '{handler_id}': {e}. "
            f"Devolviendo cadena original.",
            exc_info=True,
        )
        return traccar_datetime_str


def get_current_datetime_str_for_log(timezone_str: str = TIMEZONE_SYSTEM) -> str:
    """
    Returns current datetime as 'YYYY-MM-DD HH:MM:SS' string in the specified timezone.
    Defaults to TIMEZONE_SYSTEM from config.
    """
    try:
        tz = pytz.timezone(timezone_str)
        now_local = datetime.now(tz)
        return now_local.strftime("%Y-%m-%d %H:%M:%S")
    except pytz.exceptions.UnknownTimeZoneError:
        logger.warning(f"Unknown timezone: {timezone_str}. Using UTC for logs.")
        now_utc = datetime.now(timezone.utc)
        return now_utc.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:  # Captura cualquier otro error
        logger.error(
            f"Error getting current datetime for log with timezone '{timezone_str}': {e}. "
            f"Using UTC as fallback.",
            exc_info=True,
        )
        now_utc = datetime.now(timezone.utc)  # Fallback a UTC
        return now_utc.strftime("%Y-%m-%d %H:%M:%S")
