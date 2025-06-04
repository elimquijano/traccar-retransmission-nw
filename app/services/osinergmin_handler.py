import logging
from typing import Dict, Any
from app.services.base_retransmission_handler import BaseRetransmissionHandler
from app.utils.helpers import format_traccar_datetime_for_handler
from app.config import DATETIME_OFFSET_HOURS

# Configuramos el logger para esta clase
logger = logging.getLogger(__name__)


class OsinergminHandler(BaseRetransmissionHandler):
    """
    Manejador específico para la retransmisión de datos a OSINERGMIN.
    Transforma los datos de posición de Traccar al formato requerido por OSINERGMIN.

    Esta clase implementa los métodos abstractos de BaseRetransmissionHandler
    para adaptar los datos al formato específico que necesita el sistema OSINERGMIN.
    """

    # Identificador único para este manejador
    # Debe coincidir con un valor en RETRANSMISSION_HANDLER_MAP de la configuración
    HANDLER_ID = "osinergmin"

    def get_handler_id(self) -> str:
        """
        Devuelve el identificador único para este manejador.

        Returns:
            str: El identificador 'osinergmin' que corresponde a este manejador.
        """
        return self.HANDLER_ID

    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Transforma los datos de posición de Traccar al formato específico requerido por OSINERGMIN.

        Args:
            traccar_position: Diccionario con los datos de posición del dispositivo desde Traccar.
            device_info: Información adicional sobre el dispositivo (incluye la placa).
            retrans_config_for_device: Configuración completa de retransmisión para este dispositivo.

        Returns:
            Dict[str, Any]: Un diccionario con los datos transformados en el formato esperado por OSINERGMIN.
        """
        # Obtenemos los atributos adicionales de la posición
        attributes = traccar_position.get("attributes", {})

        # Formateamos la fecha y hora según el formato requerido por OSINERGMIN
        fecha_hora_final = format_traccar_datetime_for_handler(
            traccar_position.get("deviceTime"),
            self.HANDLER_ID,
            default_hours_offset=DATETIME_OFFSET_HOURS,
        )

        # Obtenemos la placa del vehículo (asumimos que el nombre del dispositivo es la placa)
        placa = str(device_info.get("name", ""))

        # Creamos el payload con los datos transformados según el formato de OSINERGMIN
        payload = {
            # Tipo de evento (obtenido de los atributos, usando 'alarm' como evento)
            "event": str(attributes.get("alarm", "")),
            # Placa del vehículo (incluida dos veces: como 'plate' y 'placa')
            "plate": placa,
            "placa": placa,  # Se incluye explícitamente para el logging
            # Velocidad convertida de nudos a km/h y redondeada a 2 decimales
            "speed": round(float(traccar_position.get("speed", 0.0)) * 1.852, 2),
            # Posición geográfica con latitud, longitud y altitud
            "position": {
                "latitude": float(traccar_position.get("latitude", 0.0)),
                "longitude": float(traccar_position.get("longitude", 0.0)),
                "altitude": float(traccar_position.get("altitude", 0.0)),
            },
            # Fecha y hora del evento GPS
            "gpsDate": fecha_hora_final,
            # Token de autenticación (obtenido de la configuración)
            "tokenTrama": str(retrans_config_for_device.get("id_municipalidad", "")),
            # Odómetro (valor fijo ya que no está disponible en los datos de Traccar)
            "odometer": 0,
        }

        # Registramos información de depuración sobre el payload transformado
        logger.debug(
            f"Payload transformado por {self.HANDLER_ID} para placa {payload['placa']}: {str(payload)[:500]}"
        )

        return payload
