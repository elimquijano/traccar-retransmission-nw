import logging
from typing import Dict, Any, Optional
from app.services.base_retransmission_handler import BaseRetransmissionHandler
from app.utils.helpers import format_traccar_datetime_for_handler
from app.config import DATETIME_OFFSET_HOURS

logger = logging.getLogger(__name__)


class SutranHandler(BaseRetransmissionHandler):
    HANDLER_ID = "sutran"

    def get_handler_id(self) -> str:
        return self.HANDLER_ID

    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[str, Any],
    ) -> Dict[str, Any]:
        fecha_hora_final = format_traccar_datetime_for_handler(
            traccar_position.get("deviceTime"),
            self.HANDLER_ID,
            default_hours_offset=DATETIME_OFFSET_HOURS,
        )
        placa = str(device_info.get("name", ""))

        payload = {
            "plate": placa,  # Sutran usa "plate"
            "placa": placa,  # Añadido para consistencia con el log del RetransmissionManager
            "geo": [
                float(traccar_position.get("latitude", 0.0)),
                float(traccar_position.get("longitude", 0.0)),
            ],
            "direction": int(
                traccar_position.get("course", 0)
            ),  # Sutran usa direction, course de Traccar
            "event": str(traccar_position.get("attributes", {}).get("alarm", "ER")),
            "speed": round(float(traccar_position.get("speed", 0.0)) * 1.852, 2),
            "time_device": fecha_hora_final,
            "imei": str(retrans_config_for_device.get("imei", "")),
        }
        logger.debug(
            f"Payload transformed by {self.HANDLER_ID} for placa {payload['plate']}: {str(payload)[:500]}"
        )
        return payload

    def get_custom_headers(
        self, retrans_config_for_device: Dict[str, Any]
    ) -> Optional[Dict[str, str]]:
        access_token = str(
            retrans_config_for_device.get("id_municipalidad", "")
        )  # "id_municipalidad" es el token
        if not access_token:
            logger.error(
                f"SutranHandler: No 'id_municipalidad' (Access-token) found in retrans_config for device."
            )
            return None  # No enviar si no hay token, probablemente cause un error 401/403 que es informativo.

        headers = {"Access-token": access_token}
        return headers
