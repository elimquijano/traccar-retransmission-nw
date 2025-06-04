import logging
from typing import Dict, Any
from app.services.base_retransmission_handler import BaseRetransmissionHandler

# Importamos utilidades y constantes necesarias
from app.utils.helpers import format_traccar_datetime_for_handler
from app.config import DATETIME_OFFSET_HOURS

# Configuramos el logger para esta clase
logger = logging.getLogger(__name__)


class SeguridadCiudadanaHandler(BaseRetransmissionHandler):
    """
    Manejador específico para la retransmisión de datos al sistema de Seguridad Ciudadana.
    Transforma los datos de posición de Traccar al formato requerido por el sistema de Seguridad Ciudadana.

    Esta clase implementa los métodos abstractos de BaseRetransmissionHandler
    para adaptar los datos al formato específico que necesita el sistema de Seguridad Ciudadana.
    """

    # Identificador único para este manejador
    # Debe coincidir con un valor en RETRANSMISSION_HANDLER_MAP de la configuración
    HANDLER_ID = "seguridad_ciudadana"

    def get_handler_id(self) -> str:
        """
        Devuelve el identificador único para este manejador.

        Returns:
            str: El identificador 'seguridad_ciudadana' que corresponde a este manejador.
        """
        return self.HANDLER_ID

    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Transforma los datos de posición de Traccar al formato específico requerido por Seguridad Ciudadana.

        Args:
            traccar_position: Diccionario con los datos de posición del dispositivo desde Traccar.
            device_info: Información adicional sobre el dispositivo (incluye la placa).
            retrans_config_for_device: Configuración completa de retransmisión para este dispositivo.

        Returns:
            Dict[str, Any]: Un diccionario con los datos transformados en el formato esperado por Seguridad Ciudadana.
        """
        # Obtenemos los atributos adicionales de la posición
        attributes = traccar_position.get("attributes", {})

        # Procesamos las distancias (en metros)
        # Distancia actual del último movimiento
        distancia_actual_mts = float(attributes.get("distance", 0.0))
        # Distancia total acumulada
        total_distancia_acum_mts = float(attributes.get("totalDistance", 0.0))

        # Procesamos las horas del motor (convertimos de milisegundos a horas)
        engine_hours_ms = attributes.get("hours")
        total_horas_motor_acum = 0.0

        if engine_hours_ms is not None:
            try:
                # Convertimos milisegundos a horas (1 hora = 1000 ms * 60 s * 60 min)
                total_horas_motor_acum = float(engine_hours_ms) / (1000 * 60 * 60)
            except (TypeError, ValueError):
                # Registramos un warning si no podemos convertir el valor
                logger.warning(
                    f"No se pudo parsear las horas del motor '{engine_hours_ms}' para el dispositivo {device_info.get('id')}"
                    f"(Handler: {self.HANDLER_ID})"
                )

        # Formateamos la fecha y hora según el formato requerido
        fecha_hora_final = format_traccar_datetime_for_handler(
            traccar_position.get("deviceTime"),
            self.HANDLER_ID,
            default_hours_offset=DATETIME_OFFSET_HOURS,
        )

        # Obtenemos la placa del vehículo (asumimos que el nombre del dispositivo es la placa)
        placa = str(device_info.get("name", ""))

        # Construimos el payload con la estructura requerida por Seguridad Ciudadana
        payload = {
            # Información de alarmas (obtenida de los atributos)
            "alarma": str(attributes.get("alarm", "")),
            # Datos de posición geográfica
            "altitud": float(traccar_position.get("altitude", 0.0)),
            "angulo": int(traccar_position.get("course", 0)),  # Rumbo o curso
            "latitud": float(traccar_position.get("latitude", 0.0)),
            "longitud": float(traccar_position.get("longitude", 0.0)),
            # Distancias (redondeadas a 2 decimales)
            "distancia": round(distancia_actual_mts, 2),  # Distancia actual
            "totalDistancia": round(
                total_distancia_acum_mts, 2
            ),  # Distancia total acumulada
            # Fecha y hora del evento
            "fechaHora": fecha_hora_final,
            # Horas del motor (este campo siempre era 0.0 en el código original)
            "horasMotor": 0.0,
            # Identificadores
            "idMunicipalidad": str(
                retrans_config_for_device.get("id_municipalidad", "")
            ),  # ID de la municipalidad
            "imei": str(
                retrans_config_for_device.get("imei", "")
            ),  # IMEI del dispositivo
            # Estado del vehículo
            "ignition": bool(attributes.get("ignition", False)),  # Estado del encendido
            "motion": bool(attributes.get("motion", False)),  # Estado de movimiento
            # Placa del vehículo (importante para el logging)
            "placa": placa,
            # Horas totales del motor (convertidas y redondeadas)
            "totalHorasMotor": round(total_horas_motor_acum, 2),
            # UBIGEO (usamos el campo 'bypass' de la configuración)
            "ubigeo": str(retrans_config_for_device.get("bypass", "")),
            # Validez de la posición (por defecto True)
            "valid": bool(traccar_position.get("valid", True)),
            # Velocidad convertida de nudos a km/h y redondeada
            "velocidad": round(float(traccar_position.get("speed", 0.0)) * 1.852, 2),
        }

        # Registramos información de depuración sobre el payload transformado
        logger.debug(
            f"Payload transformado por {self.HANDLER_ID} para placa {payload['placa']}: {str(payload)[:500]}"
        )

        return payload
