import logging
from typing import Dict, Any, Optional
from app.services.base_retransmission_handler import BaseRetransmissionHandler
from app.utils.helpers import format_traccar_datetime_for_handler
from app.config import DATETIME_OFFSET_HOURS

logger = logging.getLogger(__name__)


class ComsatelHandler(BaseRetransmissionHandler):
    HANDLER_ID = "comsatel"  # Must match a value in RETRANSMISSION_HANDLER_MAP

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

        payload = {
            "positionId": int(traccar_position.get("id", 0)),
            "vehiculoId": str(retrans_config_for_device.get("imei", "")),
            "velocidad": round(
                float(traccar_position.get("speed", 0.0)) * 1.852, 2
            ),  # Knots to km/h
            "satelites": 7,
            "rumbo": int(traccar_position.get("course", 0)),
            "latitud": float(traccar_position.get("latitude", 0.0)),
            "longitud": float(traccar_position.get("longitude", 0.0)),
            "altitud": float(traccar_position.get("altitude", 0.0)),
            "gpsDateTime": fecha_hora_final,
            "sendDateTime": fecha_hora_final,
            "fechaHora": fecha_hora_final,
            "evento": 1,
            "ignition": bool(attributes.get("ignition", False)),
            "odometro": 0,
            "horometro": 0,
            "nivelBateria": 100,
            "valido": bool(traccar_position.get("valid", True)),  # Default to True
            "fuente": "N&W",
            "placa": str(device_info.get("name", "")),
        }
        logger.debug(
            f"Payload transformed by {self.HANDLER_ID} for placa {payload.get('placa', '')}: {str(payload)[:500]}"
        )
        return payload
    
    def get_custom_headers(
        self, retrans_config_for_device: Dict[str, Any]
    ) -> Optional[Dict[str, str]]:
        auth_token = str(
            retrans_config_for_device.get("id_municipalidad", "")
        )  # "id_municipalidad" es el token
        if not auth_token:
            logger.error(
                f"ComsatelHandler: No 'id_municipalidad' (auth_token) found in retrans_config for device."
            )
            # Si no hay token, podrías decidir no enviar el header "Authorization"
            # y solo "Aplicacion", o devolver None para que falle o use defaults.
            # Aquí, si no hay token, solo se envía "Aplicacion".
            return {"Aplicacion": "SERVICIO_RECOLECTOR"}

        headers = {"Authorization": auth_token, "Aplicacion": "SERVICIO_RECOLECTOR"}
        return headers
