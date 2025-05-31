import logging
from typing import Dict, Any
from app.config import DATETIME_OFFSET_HOURS
from utils.helpers import format_traccar_datetime_with_offset

logger = logging.getLogger(__name__)

def transform_position_to_seguridad_ciudadana_payload(
    traccar_position: Dict[str, Any],
    device_info: Dict[str, Any],
    retrans_config: Dict[str, Any],
) -> Dict[str, Any]:
    """Transforms Traccar position data to the 'Seguridad Ciudadana' payload format."""
    attributes = traccar_position.get("attributes", {})
    
    distancia_actual_mts = float(attributes.get("distance", 0.0))
    total_distancia_acum_mts = float(attributes.get("totalDistance", 0.0))
    
    # Traccar hours attribute is usually in milliseconds
    engine_hours_ms = attributes.get("hours")
    total_horas_motor_acum = 0.0
    if engine_hours_ms is not None:
        try:
            total_horas_motor_acum = float(engine_hours_ms) / (1000 * 60 * 60)
        except (TypeError, ValueError):
            logger.warning(f"Could not parse engine hours: {engine_hours_ms}")


    fecha_hora_final = format_traccar_datetime_with_offset(
        traccar_position.get("deviceTime"), hours_offset=DATETIME_OFFSET_HOURS
    )

    payload = {
        "alarma": str(attributes.get("alarm", "")),
        "altitud": float(traccar_position.get("altitude", 0.0)),
        "angulo": int(traccar_position.get("course", 0)),
        "distancia": round(distancia_actual_mts, 2),
        "fechaHora": fecha_hora_final,
        "horasMotor": 0.0,  # This field seems to be always 0.0 in original, confirm if needed
        "idMunicipalidad": str(retrans_config.get("id_municipalidad", "")),
        "ignition": bool(attributes.get("ignition", False)),
        "imei": str(retrans_config.get("imei", "")),
        "latitud": float(traccar_position.get("latitude", 0.0)),
        "longitud": float(traccar_position.get("longitude", 0.0)),
        "motion": bool(attributes.get("motion", False)),
        "placa": str(device_info.get("name", "")), # Assuming device name is the plate
        "totalDistancia": round(total_distancia_acum_mts, 2),
        "totalHorasMotor": round(total_horas_motor_acum, 2),
        "ubigeo": str(retrans_config.get("bypass", "")), # Assuming 'bypass' is 'ubigeo'
        "valid": bool(traccar_position.get("valid", True)), # Default to True if not present
        "velocidad": round(float(traccar_position.get("speed", 0.0)) * 1.852, 2), # Knots to km/h
    }
    return payload