import requests
import websocket  # type: ignore
import json
import logging
from typing import Optional, Dict, Any, Callable
from requests.cookies import RequestsCookieJar

# Importamos configuraciones necesarias
from app.config import (
    TRACCAR_URL,
    TRACCAR_EMAIL,
    TRACCAR_PASSWORD,
    WS_PING_INTERVAL_SECONDS,
    WS_PING_TIMEOUT_SECONDS,
)

# Configuramos el logger para esta clase
logger = logging.getLogger(__name__)


class TraccarClient:
    """
    Cliente para interactuar con el servidor Traccar.
    Maneja la autenticación, obtención de dispositivos y conexión WebSocket.

    Esta clase proporciona métodos para:
    - Iniciar sesión en Traccar
    - Obtener dispositivos
    - Manejar la conexión WebSocket
    - Actualizar la caché de dispositivos
    """

    def __init__(
        self,
        message_callback: Callable[[websocket.WebSocketApp, str], None],
        open_callback: Callable[[websocket.WebSocketApp], None],
        close_callback: Callable[
            [websocket.WebSocketApp, Optional[int], Optional[str]], None
        ],
        error_callback: Callable[[websocket.WebSocketApp, Exception], None],
    ):
        """
        Inicializa el cliente Traccar con los callbacks necesarios.

        Args:
            message_callback: Función callback para manejar mensajes WebSocket
            open_callback: Función callback cuando se abre la conexión WebSocket
            close_callback: Función callback cuando se cierra la conexión WebSocket
            error_callback: Función callback cuando ocurre un error en WebSocket
        """
        # Cookies de sesión para autenticación
        self.session_cookies: Optional[RequestsCookieJar] = None
        # Aplicación WebSocket
        self.ws_app: Optional[websocket.WebSocketApp] = None
        # Callbacks para eventos WebSocket
        self._message_callback = message_callback
        self._open_callback = open_callback
        self._close_callback = close_callback
        self._error_callback = error_callback
        # Caché de dispositivos Traccar (device_id -> datos del dispositivo)
        self.traccar_devices_cache: Dict[int, Dict[str, Any]] = {}

    def login(self) -> bool:
        """
        Inicia sesión en Traccar y almacena las cookies de sesión.

        Returns:
            bool: True si el login fue exitoso, False en caso contrario.
        """
        session = requests.Session()
        login_url = f"{TRACCAR_URL}/api/session"
        login_data = {"email": TRACCAR_EMAIL, "password": TRACCAR_PASSWORD}

        try:
            logger.info(f"Intentando login en Traccar: {login_url}...")
            response = session.post(login_url, data=login_data, timeout=10)
            response.raise_for_status()  # Lanza excepción si hay error HTTP
            self.session_cookies = session.cookies
            logger.info("Login en Traccar exitoso!")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en login de Traccar: {e}")
            return False

    def fetch_devices(self) -> bool:
        """
        Obtiene todos los dispositivos de Traccar y pobla la caché local.

        Returns:
            bool: True si la obtención fue exitosa, False en caso contrario.
        """
        if not self.session_cookies:
            logger.warning(
                "No hay cookies de sesión para obtener dispositivos de Traccar."
            )
            return False

        devices_url = f"{TRACCAR_URL}/api/devices"

        try:
            logger.info(f"Obteniendo dispositivos de Traccar desde {devices_url}...")
            response = requests.get(
                devices_url, cookies=self.session_cookies, timeout=15
            )
            response.raise_for_status()
            devices_list = response.json()

            # Creamos una nueva caché con los dispositivos obtenidos
            new_cache: Dict[int, Dict[str, Any]] = {}
            for dev in devices_list:
                dev_id = dev.get("id")
                if dev_id is not None:
                    new_cache[int(dev_id)] = dev  # Aseguramos que el ID sea entero

            self.traccar_devices_cache = new_cache
            logger.info(
                f"Obtenidos {len(self.traccar_devices_cache)} dispositivos de Traccar."
            )
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error obteniendo dispositivos de Traccar: {e}")
        except json.JSONDecodeError as e:
            logger.error(
                f"Error decodificando respuesta de dispositivos de Traccar: {e}"
            )
        return False

    def update_device_cache_from_ws(self, device_updates: list):
        """
        Actualiza la caché local de dispositivos desde mensajes de actualización WebSocket.

        Args:
            device_updates: Lista de actualizaciones de dispositivos recibidas por WebSocket.
        """
        updated_count = 0

        for dev_update in device_updates:
            dev_id = dev_update.get("id")
            if dev_id is not None:
                try:
                    self.traccar_devices_cache[int(dev_id)] = (
                        dev_update  # Aseguramos clave entera
                    )
                    updated_count += 1
                except ValueError:
                    logger.warning(
                        f"ID de dispositivo inválido '{dev_id}' en actualización WebSocket. Saltando."
                    )

        if updated_count > 0:
            logger.info(
                f"{updated_count} dispositivos actualizados en caché desde WebSocket."
            )

    def connect_websocket(self):
        """
        Conecta al API WebSocket de Traccar. Esta llamada es bloqueante (run_forever).
        """
        if not self.session_cookies:
            logger.error(
                "No hay cookies de sesión. Abortando conexión WebSocket de Traccar."
            )
            return

        # Determinamos el esquema (ws o wss) basado en la URL de Traccar
        ws_scheme = "ws" if TRACCAR_URL.startswith("http:") else "wss"

        # Aseguramos que ws_url_base elimine correctamente http(s):// y cualquier barra final
        ws_url_base = (
            TRACCAR_URL.replace("http://", "", 1).replace("https://", "", 1).rstrip("/")
        )
        websocket_url = f"{ws_scheme}://{ws_url_base}/api/socket"

        # Creamos el header de Cookie para la conexión WebSocket
        cookie_header_value = "; ".join(
            [
                f"{name}={value}"
                for name, value in self.session_cookies.get_dict().items()
            ]
        )
        headers = {"Cookie": cookie_header_value}

        logger.info(f"Intentando conectar al WebSocket de Traccar: {websocket_url}")

        # Aseguramos que ws_app se reinicie si existía una conexión previa que falló
        if self.ws_app:
            try:
                self.ws_app.close()  # Intentamos cerrar cualquier aplicación existente
            except (
                Exception
            ):  # Ignoramos errores durante el cierre de la aplicación antigua
                pass
            self.ws_app = None

        # Configuramos la aplicación WebSocket con los callbacks
        self.ws_app = websocket.WebSocketApp(
            websocket_url,
            header=headers,
            on_open=self._on_open_ws,
            on_message=self._on_message_ws,
            on_error=self._on_error_ws,
            on_close=self._on_close_ws,
        )

        # run_forever bloqueará hasta que la conexión WebSocket se cierre o ocurra un error
        self.ws_app.run_forever(
            ping_interval=WS_PING_INTERVAL_SECONDS,
            ping_timeout=WS_PING_TIMEOUT_SECONDS,
            # ping_payload="PING" # Opcional: si el servidor necesita un formato específico de ping
        )

        logger.info("El bucle run_forever del WebSocket de Traccar ha terminado.")

        # Después de que run_forever termina, ws_app podría estar en un estado no usable o cerrado
        self.ws_app = None

    def _on_open_ws(self, ws_app_instance: websocket.WebSocketApp):
        """
        Callback llamado cuando se abre la conexión WebSocket.

        Args:
            ws_app_instance: Instancia de WebSocketApp que se abrió.
        """
        logger.info("--- Conexión WebSocket de Traccar abierta ---")
        if self._open_callback:
            self._open_callback(ws_app_instance)

    def _on_message_ws(self, ws_app_instance: websocket.WebSocketApp, message_str: str):
        """
        Callback llamado cuando se recibe un mensaje WebSocket.

        Args:
            ws_app_instance: Instancia de WebSocketApp.
            message_str: Mensaje recibido como string.
        """
        # Reenvía al callback designado (generalmente del retransmission_manager)
        if self._message_callback:
            self._message_callback(ws_app_instance, message_str)

    def _on_error_ws(self, ws_app_instance: websocket.WebSocketApp, error: Exception):
        """
        Callback llamado cuando ocurre un error en WebSocket.

        Args:
            ws_app_instance: Instancia de WebSocketApp.
            error: Excepción que ocurrió.
        """
        # Registramos tipo y representación del error para mejor diagnóstico
        logger.error(
            f"--- Error en WebSocket de Traccar ---: Tipo: {type(error)}, "
            f"Representación: {repr(error)}, Cadena: {str(error)}"
        )
        if self._error_callback:
            self._error_callback(ws_app_instance, error)

    def _on_close_ws(
        self,
        ws_app_instance: websocket.WebSocketApp,
        close_status_code: Optional[int],
        close_msg: Optional[str],
    ):
        """
        Callback llamado cuando se cierra la conexión WebSocket.

        Args:
            ws_app_instance: Instancia de WebSocketApp.
            close_status_code: Código de estado de cierre.
            close_msg: Mensaje de cierre.
        """
        logger.warning(
            f"--- Conexión WebSocket de Traccar cerrada --- Código: {close_status_code}, "
            f"Mensaje: {close_msg}"
        )
        if self._close_callback:
            self._close_callback(ws_app_instance, close_status_code, close_msg)

    def close_websocket(self):
        """
        Cierra la conexión WebSocket si está activa.
        """
        if self.ws_app:
            logger.info("Cerrando conexión WebSocket de Traccar explícitamente...")
            try:
                self.ws_app.close()
            except Exception as e:
                logger.error(f"Error durante cierre explícito de WebSocket: {e}")
            finally:
                self.ws_app = None  # Aseguramos que se establezca a None
        else:
            logger.info("No hay conexión WebSocket de Traccar activa para cerrar.")
