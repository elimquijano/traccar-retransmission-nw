from datetime import datetime, timedelta, timezone
import logging
import pytz  # Para manejo de zonas horarias robusto
from app.config import (
    TIMEZONE_SYSTEM,  # Usado para el log y como fallback
    DATETIME_OFFSET_HOURS,  # Usado como offset por defecto para los handlers que lo requieran
)

# Configuramos el logger para este módulo
logger = logging.getLogger(__name__)

# Constantes para los IDs de los handlers
# Estos deben coincidir con los HANDLER_ID definidos en las clases de handler
HANDLER_ID_SEGURIDAD_CIUDADANA = "seguridad_ciudadana"
HANDLER_ID_PNP = "pnp"
HANDLER_ID_SUTRAN = "sutran"
HANDLER_ID_COMSATEL = "comsatel"
HANDLER_ID_OSINERGMIN = "osinergmin"


def format_traccar_datetime_for_handler(
    traccar_datetime_str: str | None,
    handler_id: str,  # Identificador del handler
    default_hours_offset: int = DATETIME_OFFSET_HOURS,  # Offset por defecto si no se especifica uno para el handler
) -> str:
    """
    Formatea una cadena de fecha/hora de Traccar (asumida como ISO 8601 UTC)
    a un formato específico basado en el handler_id.

    La entrada traccar_datetime_str se espera que sea un timestamp UTC.

    Args:
        traccar_datetime_str: Cadena de fecha/hora en formato ISO 8601 desde Traccar.
        handler_id: Identificador del handler para determinar el formato de salida.
        default_hours_offset: Offset horario por defecto (en horas) para aplicar a la fecha.

    Returns:
        str: Cadena formateada según el formato requerido por el handler específico.
    """
    # Si no hay cadena de fecha, devolvemos cadena vacía
    if not traccar_datetime_str:
        return ""

    try:
        # Paso 1: Parsear la fecha de Traccar a un objeto datetime UTC consciente de la zona horaria
        dt_obj_utc: datetime

        # Manejo de diferentes formatos de fecha ISO 8601
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
            # Formato con milisegundos y zona horaria
            dt_obj_utc = datetime.fromisoformat(
                traccar_datetime_str.replace("Z", "+00:00")
            )
        elif "Z" in traccar_datetime_str:
            # Formato con Z pero sin milisegundos
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
            # Formato con offset de zona horaria
            dt_obj_utc = datetime.fromisoformat(traccar_datetime_str)
        else:
            # Formato sin zona horaria (asumimos UTC)
            base_time_str = traccar_datetime_str.split(".")[
                0
            ]  # Quitar milisegundos si los hay
            dt_obj_naive_utc = datetime.strptime(base_time_str, "%Y-%m-%dT%H:%M:%S")
            dt_obj_utc = dt_obj_naive_utc.replace(tzinfo=timezone.utc)

        # Aseguramos que sea UTC consciente
        if dt_obj_utc.tzinfo is None or dt_obj_utc.tzinfo.utcoffset(dt_obj_utc) is None:
            dt_obj_utc = dt_obj_utc.replace(tzinfo=timezone.utc)
        elif dt_obj_utc.tzinfo != timezone.utc:
            dt_obj_utc = dt_obj_utc.astimezone(timezone.utc)

        # Paso 2: Aplicar offset y formatear según el handler_id
        # Por defecto, la mayoría usará el offset de DATETIME_OFFSET_HOURS (-5 para America/Lima)
        # pero algunos pueden requerir UTC (offset 0) o un formato específico

        if handler_id in [HANDLER_ID_SEGURIDAD_CIUDADANA, HANDLER_ID_PNP]:
            # Formato para Seguridad Ciudadana y PNP: "dd/mm/YYYY HH:MM:SS" con offset
            dt_obj_with_offset = dt_obj_utc + timedelta(hours=default_hours_offset)
            return dt_obj_with_offset.strftime("%d/%m/%Y %H:%M:%S")

        elif handler_id == HANDLER_ID_OSINERGMIN:
            # Formato para OSINERGMIN: "YYYY-MM-DDTHH:MM:SS.sssZ" (UTC, con milisegundos)
            # 'sss' son los primeros 3 dígitos de los microsegundos
            return (
                dt_obj_utc.strftime("%Y-%m-%dT%H:%M:%S.")
                + f"{dt_obj_utc.microsecond // 1000:03d}Z"
            )

        elif handler_id == HANDLER_ID_COMSATEL:
            # Formato para COMSATEL: "YYYYMMDDHHMMSS" (con offset)
            dt_obj_with_offset = dt_obj_utc + timedelta(hours=default_hours_offset)
            return dt_obj_with_offset.strftime("%Y%m%d%H%M%S")

        elif handler_id == HANDLER_ID_SUTRAN:
            # Formato para SUTRAN: "YYYY-MM-DD HH:MM:SS" (con offset)
            dt_obj_with_offset = dt_obj_utc + timedelta(hours=default_hours_offset)
            return dt_obj_with_offset.strftime("%Y-%m-%d %H:%M:%S")

        else:
            # Fallback a un formato por defecto si el handler_id no es reconocido
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
    Devuelve la fecha y hora actual como cadena 'YYYY-MM-DD HH:MM:SS' en la zona horaria especificada.
    Por defecto usa TIMEZONE_SYSTEM de la configuración.

    Args:
        timezone_str: Cadena con la zona horaria (ej: 'America/Lima'). Por defecto TIMEZONE_SYSTEM.

    Returns:
        str: Fecha y hora actual formateada como 'YYYY-MM-DD HH:MM:SS'.
    """
    try:
        # Obtenemos la zona horaria
        tz = pytz.timezone(timezone_str)
        # Obtenemos la fecha y hora actual en esa zona horaria
        now_local = datetime.now(tz)
        return now_local.strftime("%Y-%m-%d %H:%M:%S")
    except pytz.exceptions.UnknownTimeZoneError:
        # Si la zona horaria no es reconocida, usamos UTC
        logger.warning(
            f"Zona horaria desconocida: {timezone_str}. Usando UTC para logs."
        )
        now_utc = datetime.now(timezone.utc)
        return now_utc.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        # Manejo de cualquier otro error
        logger.error(
            f"Error obteniendo fecha y hora actual para log con zona horaria '{timezone_str}': {e}. "
            f"Usando UTC como fallback.",
            exc_info=True,
        )
        now_utc = datetime.now(timezone.utc)  # Fallback a UTC
        return now_utc.strftime("%Y-%m-%d %H:%M:%S")
