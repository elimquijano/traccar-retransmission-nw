# Importamos las clases necesarias para crear una clase abstracta
from abc import ABC, abstractmethod

# Importamos tipos para definir los diccionarios y valores opcionales
from typing import Dict, Any, Optional


class BaseRetransmissionHandler(ABC):
    """
    Clase base abstracta para manejadores de retransmisión.
    Todos los manejadores de retransmisión específicos deben heredar de esta clase
    e implementar sus métodos abstractos.

    Esta clase define la interfaz común que todos los manejadores de retransmisión deben seguir,
    lo que permite al sistema trabajar con diferentes tipos de retransmisión de manera uniforme.
    """

    @abstractmethod
    def get_handler_id(self) -> str:
        """
        Devuelve un identificador único para este tipo de manejador.

        Este ID debe coincidir con uno de los valores en RETRANSMISSION_HANDLER_MAP
        del archivo config.py. Se usa para seleccionar el manejador adecuado
        para cada tipo de retransmisión.

        Returns:
            str: Un identificador único para este manejador.
        """
        pass

    @abstractmethod
    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Transforma los datos de posición de Traccar al formato específico requerido por el destino.

        Este método es el corazón del manejador, donde se realiza la conversión de los datos
        del formato estándar de Traccar al formato específico que necesita el sistema destino.

        Args:
            traccar_position: Diccionario con los datos de posición del dispositivo desde Traccar.
            device_info: Información adicional sobre el dispositivo.
            retrans_config_for_device: Configuración específica de retransmisión para este dispositivo.

        Returns:
            Dict[str, Any]: Un diccionario con los datos transformados listos para ser enviados.
                           Se recomienda incluir un campo 'placa' (o similar) si ese valor específico
                           es deseado para el registro (logging).
        """
        pass

    def get_custom_headers(
        self, retrans_config_for_device: Dict[str, Any]
    ) -> Optional[Dict[str, str]]:
        """
        Opcionalmente puede ser sobrescrito por subclases para devolver headers HTTP personalizados.

        Si este método devuelve None o no es sobrescrito, el RetransmissionManager
        usará los headers por defecto. La configuración del dispositivo (de la base de datos)
        se pasa como parámetro para que se puedan extraer tokens u otra información necesaria
        para los headers.

        Args:
            retrans_config_for_device: Configuración de retransmisión para el dispositivo actual.

        Returns:
            Optional[Dict[str, str]]: Un diccionario con headers HTTP personalizados,
                                     o None si se deben usar los headers por defecto.
        """
        return None  # Por defecto, no hay headers personalizados
