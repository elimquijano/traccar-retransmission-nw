import logging
from typing import Dict, Any
from app.services.base_retransmission_handler import BaseRetransmissionHandler

# Import helpers/constants directly if BaseRetransmissionHandler doesn't re-export them conveniently
from app.utils.helpers import format_traccar_datetime_for_handler
from app.config import DATETIME_OFFSET_HOURS

logger = logging.getLogger(__name__)


class SeguridadCiudadanaHandler(BaseRetransmissionHandler):
    HANDLER_ID = (
        "seguridad_ciudadana"  # Must match a value in RETRANSMISSION_HANDLER_MAP
    )

    def get_handler_id(self) -> str:
        return self.HANDLER_ID

    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[str, Any],  # Full config from BD1
    ) -> Dict[str, Any]:
        attributes = traccar_position.get("attributes", {})

        distancia_actual_mts = float(attributes.get("distance", 0.0))
        total_distancia_acum_mts = float(attributes.get("totalDistance", 0.0))

        engine_hours_ms = attributes.get("hours")
        total_horas_motor_acum = 0.0
        if engine_hours_ms is not None:
            try:
                total_horas_motor_acum = float(engine_hours_ms) / (1000 * 60 * 60)
            except (TypeError, ValueError):
                logger.warning(
                    f"Could not parse engine hours '{engine_hours_ms}' for device {device_info.get('id')} "
                    f"(Handler: {self.HANDLER_ID})"
                )

        fecha_hora_final = format_traccar_datetime_for_handler(
            traccar_position.get("deviceTime"), self.HANDLER_ID, default_hours_offset=DATETIME_OFFSET_HOURS
        )

        # Ensure 'placa' is in the payload for logging by the base handler
        placa = str(device_info.get("name", ""))  # Assuming device name is the plate

        payload = {
            "alarma": str(attributes.get("alarm", "")),
            "altitud": float(traccar_position.get("altitude", 0.0)),
            "angulo": int(traccar_position.get("course", 0)),
            "distancia": round(distancia_actual_mts, 2),
            "fechaHora": fecha_hora_final,
            "horasMotor": 0.0,  # This field was always 0.0 in original code, confirm if still needed
            "idMunicipalidad": str(retrans_config_for_device.get("id_municipalidad", "")),
            "ignition": bool(attributes.get("ignition", False)),
            "imei": str(retrans_config_for_device.get("imei", "")),
            "latitud": float(traccar_position.get("latitude", 0.0)),
            "longitud": float(traccar_position.get("longitude", 0.0)),
            "motion": bool(attributes.get("motion", False)),
            "placa": placa,  # Crucial for logging in BaseRetransmissionHandler
            "totalDistancia": round(total_distancia_acum_mts, 2),
            "totalHorasMotor": round(total_horas_motor_acum, 2),
            "ubigeo": str(
                retrans_config_for_device.get("bypass", "")
            ),  # 'bypass' field used as ubigeo
            "valid": bool(traccar_position.get("valid", True)),  # Default to True
            "velocidad": round(
                float(traccar_position.get("speed", 0.0)) * 1.852, 2
            ),  # Knots to km/h
        }
        logger.debug(
            f"Payload transformed by {self.HANDLER_ID} for placa {payload['placa']}: {str(payload)[:500]}"
        )
        return payload
