import logging
from typing import Dict, Any, Optional
from app.services.base_retransmission_handler import BaseRetransmissionHandler
from app.utils.helpers import format_traccar_datetime_for_handler
from app.config import DATETIME_OFFSET_HOURS

# Configuramos el logger para esta clase
logger = logging.getLogger(__name__)


class ComsatelHandler(BaseRetransmissionHandler):
    """
    Manejador específico para la retransmisión de datos a Comsatel.
    Transforma los datos de posición de Traccar al formato requerido por Comsatel.

    Esta clase implementa los métodos abstractos de BaseRetransmissionHandler
    para adaptar los datos al formato específico que necesita el sistema Comsatel.
    """

    # Identificador único para este manejador (debe coincidir con el valor en RETRANSMISSION_HANDLER_MAP)
    HANDLER_ID = "comsatel"

    def get_handler_id(self) -> str:
        """
        Devuelve el identificador único para este manejador.

        Returns:
            str: El identificador 'comsatel' que corresponde a este manejador.
        """
        return self.HANDLER_ID

    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Transforma los datos de posición de Traccar al formato específico requerido por Comsatel.

        Args:
            traccar_position: Diccionario con los datos de posición del dispositivo desde Traccar.
            device_info: Información adicional sobre el dispositivo (incluye la placa).
            retrans_config_for_device: Configuración completa de retransmisión para este dispositivo desde BD1.

        Returns:
            Dict[str, Any]: Un diccionario con los datos transformados en el formato esperado por Comsatel.
        """
        # Obtenemos los atributos adicionales de la posición
        attributes = traccar_position.get("attributes", {})

        # Formateamos la fecha y hora según el formato requerido por Comsatel
        fecha_hora_final = format_traccar_datetime_for_handler(
            traccar_position.get("deviceTime"),
            self.HANDLER_ID,
            default_hours_offset=DATETIME_OFFSET_HOURS,
        )

        # Creamos el payload con los datos transformados
        payload = {
            # ID de la posición (convertido a entero)
            "positionId": int(traccar_position.get("id", 0)),
            # ID del vehículo (usamos el IMEI de la configuración)
            "vehiculoId": str(retrans_config_for_device.get("imei", "")),
            # Velocidad convertida de nudos a km/h y redondeada a 2 decimales
            "velocidad": round(float(traccar_position.get("speed", 0.0)) * 1.852, 2),
            # Número de satélites (valor fijo ya que no viene en los datos de Traccar)
            "satelites": 7,
            # Rumbo (curso) del vehículo
            "rumbo": int(traccar_position.get("course", 0)),
            # Coordenadas geográficas
            "latitud": float(traccar_position.get("latitude", 0.0)),
            "longitud": float(traccar_position.get("longitude", 0.0)),
            "altitud": float(traccar_position.get("altitude", 0.0)),
            # Fechas y horas (todas con el mismo valor formateado)
            "gpsDateTime": fecha_hora_final,
            "sendDateTime": fecha_hora_final,
            "fechaHora": fecha_hora_final,
            # Tipo de evento (valor fijo)
            "evento": 1,
            # Estado del encendido (obtenido de los atributos)
            "ignition": bool(attributes.get("ignition", False)),
            # Valores fijos para odómetro y horómetro (no disponibles en los datos de Traccar)
            "odometro": 0,
            "horometro": 0,
            # Nivel de batería (valor fijo)
            "nivelBateria": 100,
            # Validez de la posición (por defecto True si no está especificado)
            "valido": bool(traccar_position.get("valid", True)),
            # Fuente de los datos (valor fijo)
            "fuente": "N&W",
            # Placa del vehículo (obtenida de la información del dispositivo)
            "placa": str(device_info.get("name", "")),
        }

        # Registramos información de depuración sobre el payload transformado
        logger.debug(
            f"Payload transformado por {self.HANDLER_ID} para placa {payload.get('placa', '')}: {str(payload)[:500]}"
        )

        return payload

    def get_custom_headers(
        self, retrans_config_for_device: Dict[str, Any]
    ) -> Optional[Dict[str, str]]:
        """
        Devuelve los headers HTTP personalizados necesarios para la autenticación con Comsatel.

        Args:
            retrans_config_for_device: Configuración de retransmisión para el dispositivo actual.

        Returns:
            Optional[Dict[str, str]]: Un diccionario con los headers personalizados.
                                     Incluye el token de autenticación y el nombre de la aplicación.
        """
        # Obtenemos el token de autenticación (almacenado como 'id_municipalidad' en la configuración)
        auth_token = str(retrans_config_for_device.get("id_municipalidad", ""))

        # Si no hay token, registramos un error
        if not auth_token:
            logger.error(
                f"ComsatelHandler: No se encontró 'id_municipalidad' (auth_token) en la configuración de retransmisión para el dispositivo."
            )
            # Si no hay token, solo enviamos el header "Aplicacion"
            return {"Aplicacion": "SERVICIO_RECOLECTOR"}

        # Si hay token, enviamos ambos headers
        headers = {"Authorization": auth_token, "Aplicacion": "SERVICIO_RECOLECTOR"}
        return headers
