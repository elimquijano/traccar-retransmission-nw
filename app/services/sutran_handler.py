import logging
from typing import Dict, Any, Optional
from app.services.base_retransmission_handler import BaseRetransmissionHandler
from app.utils.helpers import format_traccar_datetime_for_handler
from app.config import DATETIME_OFFSET_HOURS

# Configuramos el logger para esta clase
logger = logging.getLogger(__name__)


class SutranHandler(BaseRetransmissionHandler):
    """
    Manejador específico para la retransmisión de datos a SUTRAN (Superintendencia de Transporte Terrestre de Personas, Carga y Mercancías).
    Transforma los datos de posición de Traccar al formato requerido por el sistema de SUTRAN.

    Esta clase implementa los métodos abstractos de BaseRetransmissionHandler
    para adaptar los datos al formato específico que necesita el sistema de SUTRAN.
    """

    # Identificador único para este manejador
    # Debe coincidir con un valor en RETRANSMISSION_HANDLER_MAP de la configuración
    HANDLER_ID = "sutran"

    def get_handler_id(self) -> str:
        """
        Devuelve el identificador único para este manejador.

        Returns:
            str: El identificador 'sutran' que corresponde a este manejador.
        """
        return self.HANDLER_ID

    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Transforma los datos de posición de Traccar al formato específico requerido por SUTRAN.

        Args:
            traccar_position: Diccionario con los datos de posición del dispositivo desde Traccar.
            device_info: Información adicional sobre el dispositivo (incluye la placa).
            retrans_config_for_device: Configuración completa de retransmisión para este dispositivo.

        Returns:
            Dict[str, Any]: Un diccionario con los datos transformados en el formato esperado por SUTRAN.
        """
        # Formateamos la fecha y hora según el formato requerido por SUTRAN
        fecha_hora_final = format_traccar_datetime_for_handler(
            traccar_position.get("deviceTime"),
            self.HANDLER_ID,
            default_hours_offset=DATETIME_OFFSET_HOURS,
        )

        # Obtenemos la placa del vehículo (asumimos que el nombre del dispositivo es la placa)
        placa = str(device_info.get("name", ""))

        # Construimos el payload con la estructura requerida por SUTRAN
        payload = {
            # Placa del vehículo (SUTRAN usa "plate")
            "plate": placa,
            # También incluimos "placa" para consistencia con el logging del RetransmissionManager
            "placa": placa,
            # Coordenadas geográficas como un array [latitud, longitud]
            "geo": [
                float(traccar_position.get("latitude", 0.0)),
                float(traccar_position.get("longitude", 0.0)),
            ],
            # Dirección (SUTRAN usa "direction", que corresponde al "course" de Traccar)
            "direction": int(traccar_position.get("course", 0)),
            # Tipo de evento (usamos "alarm" de los atributos, por defecto "ER")
            "event": str(traccar_position.get("attributes", {}).get("alarm", "ER")),
            # Velocidad convertida de nudos a km/h y redondeada a 2 decimales
            "speed": round(float(traccar_position.get("speed", 0.0)) * 1.852, 2),
            # Fecha y hora del dispositivo
            "time_device": fecha_hora_final,
            # IMEI del dispositivo (obtenido de la configuración de retransmisión)
            "imei": str(retrans_config_for_device.get("imei", "")),
        }

        # Registramos información de depuración sobre el payload transformado
        logger.debug(
            f"Payload transformado por {self.HANDLER_ID} para placa {payload['plate']}: {str(payload)[:500]}"
        )

        return payload

    def get_custom_headers(
        self, retrans_config_for_device: Dict[str, Any]
    ) -> Optional[Dict[str, str]]:
        """
        Devuelve los headers HTTP personalizados necesarios para la autenticación con SUTRAN.

        Args:
            retrans_config_for_device: Configuración de retransmisión para el dispositivo actual.

        Returns:
            Optional[Dict[str, str]]: Un diccionario con los headers personalizados.
                                     Incluye el token de acceso. Devuelve None si no hay token.
        """
        # Obtenemos el token de acceso (almacenado como 'id_municipalidad' en la configuración)
        access_token = str(retrans_config_for_device.get("id_municipalidad", ""))

        # Si no hay token, registramos un error y devolvemos None
        if not access_token:
            logger.error(
                f"SutranHandler: No se encontró 'id_municipalidad' (Access-token) en la configuración de retransmisión para el dispositivo."
            )
            return None  # No enviar si no hay token, probablemente causará un error 401/403

        # Si hay token, creamos los headers con el token de acceso
        headers = {"Access-token": access_token}
        return headers
