from abc import ABC, abstractmethod
from typing import Dict, Any

# No se necesitan más imports aquí si esta clase solo define la interfaz
# para la transformación del payload y la obtención del ID del handler.


class BaseRetransmissionHandler(ABC):
    @abstractmethod
    def get_handler_id(self) -> str:
        """
        Returns a unique identifier for this handler type.
        This ID should match one of the values in RETRANSMISSION_HANDLER_MAP from config.py.
        """
        pass

    @abstractmethod
    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[
            str, Any
        ],  # Full config from BD1 for this device
    ) -> Dict[str, Any]:
        """
        Transforms Traccar position data into the specific payload format required by the target.
        It's recommended that the transformed payload includes a 'placa' field (or similar)
        if that specific value is desired for logging, as the RetransmissionManager will try
        to extract it from here for the log entry.
        """
        pass

    # El método que antes hacía el POST HTTP (retransmit o execute_http_post)
    # se elimina de esta clase base. El RetransmissionManager se encargará
    # de realizar la petición HTTP directamente en su método _execute_single_http_post.
