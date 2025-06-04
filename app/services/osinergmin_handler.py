import logging
from typing import Dict, Any
from app.services.base_retransmission_handler import BaseRetransmissionHandler
from app.utils.helpers import format_traccar_datetime_for_handler
from app.config import DATETIME_OFFSET_HOURS

logger = logging.getLogger(__name__)


class OsinergminHandler(BaseRetransmissionHandler):
    HANDLER_ID = "osinergmin"  # Must match a value in RETRANSMISSION_HANDLER_MAP

    def get_handler_id(self) -> str:
        return self.HANDLER_ID

    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[str, Any],  # Full config from BD1
    ) -> Dict[str, Any]:
        attributes = traccar_position.get("attributes", {})

        fecha_hora_final = format_traccar_datetime_for_handler(
            traccar_position.get("deviceTime"), self.HANDLER_ID, default_hours_offset=DATETIME_OFFSET_HOURS
        )

        # Ensure 'placa' is in the payload for logging by the base handler
        placa = str(device_info.get("name", ""))  # Assuming device name is the plate

        payload = {
            "event": str(attributes.get("alarm", "")),
            "plate": placa,
            "speed": round(
                float(traccar_position.get("speed", 0.0)) * 1.852, 2
            ),  # Knots to km/h
            "position": {
                "latitude": float(traccar_position.get("latitude", 0.0)),
                "longitude": float(traccar_position.get("longitude", 0.0)),
                "altitude": float(traccar_position.get("altitude", 0.0)),
            },
            "gpsDate": fecha_hora_final,
            "tokenTrama": str(retrans_config_for_device.get("id_municipalidad", "")),
            "odometer": 0,
            "placa": placa,  # Ensure 'placa' is included in the payload
        }
        logger.debug(
            f"Payload transformed by {self.HANDLER_ID} for placa {payload['placa']}: {str(payload)[:500]}"
        )
        return payload
