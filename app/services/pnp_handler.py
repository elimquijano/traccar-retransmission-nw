import logging
from typing import Dict, Any
from app.services.base_retransmission_handler import BaseRetransmissionHandler

# from app.utils.helpers import format_traccar_datetime_with_offset # Si lo necesitas
# from app.config import DATETIME_OFFSET_HOURS # Si lo necesitas

logger = logging.getLogger(__name__)


class OtroTipoHandler(BaseRetransmissionHandler):
    HANDLER_ID = "otro_tipo"  # Must match a value in RETRANSMISSION_HANDLER_MAP

    def get_handler_id(self) -> str:
        return self.HANDLER_ID

    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Implementa la transformación específica para "otro_tipo"
        # Ejemplo: usa un campo 'custom_field_otro_tipo' de la config de BD1
        # custom_field = retrans_config_for_device.get("custom_field_for_otro_tipo", "default_value")

        placa = str(device_info.get("name", ""))  # Para el log en BaseHandler

        payload = {
            "traccarDeviceId": traccar_position.get("deviceId"),
            "deviceName": device_info.get("name"),
            "placa": placa,  # Para que BaseHandler pueda loguearlo
            "timestamp_utc_iso": traccar_position.get(
                "fixTime"
            ),  # Example: use fixTime
            "latitude": traccar_position.get("latitude"),
            "longitude": traccar_position.get("longitude"),
            "speed_kph": round(
                float(traccar_position.get("speed", 0.0)) * 1.852, 2
            ),  # Knots to km/h
            "attributes": traccar_position.get("attributes"),
            # "custom_info": custom_field # Ejemplo
        }
        logger.debug(
            f"Payload transformed by {self.HANDLER_ID} for device {device_info.get('name')}: {str(payload)[:500]}"
        )
        return payload
