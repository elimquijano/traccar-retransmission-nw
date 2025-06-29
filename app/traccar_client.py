import requests
import websocket  # type: ignore
import json
import logging
from typing import Optional, Dict, Any, Callable

# Ya no es necesario importar RequestsCookieJar explícitamente

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
            message_callback: Función callback para manejar mensajes WebSocket.
            open_callback: Función callback cuando se abre la conexión WebSocket.
            close_callback: Función callback cuando se cierra la conexión WebSocket.
            error_callback: Función callback cuando ocurre un error en WebSocket.
        """
        # --- MODIFICACIÓN: Usar una única sesión de requests para toda la vida del cliente ---
        self._session = requests.Session()
        # Las cookies se almacenarán en self._session.cookies

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
        Inicia sesión en Traccar y almacena las cookies de sesión en el objeto de sesión.

        Returns:
            bool: True si el login fue exitoso, False en caso contrario.
        """
        login_url = f"{TRACCAR_URL}/api/session"
        login_data = {"email": TRACCAR_EMAIL, "password": TRACCAR_PASSWORD}

        try:
            logger.info(f"Intentando login en Traccar: {login_url}...")
            # Usar la sesión de la instancia, self._session, para la petición POST
            response = self._session.post(login_url, data=login_data, timeout=10)
            response.raise_for_status()  # Lanza excepción si hay error HTTP (4xx o 5xx)
            # Las cookies se almacenan automáticamente en self._session.cookies
            logger.info("Login en Traccar exitoso!")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en login de Traccar: {e}")
            return False

    def fetch_devices(self) -> bool:
        """
        Obtiene todos los dispositivos de Traccar y pobla la caché local usando la sesión activa.

        Returns:
            bool: True si la obtención fue exitosa, False en caso contrario.
        """
        if not self._session.cookies:
            logger.warning(
                "No hay cookies en la sesión para obtener dispositivos de Traccar."
            )
            return False

        devices_url = f"{TRACCAR_URL}/api/devices"

        try:
            logger.info(f"Obteniendo dispositivos de Traccar desde {devices_url}...")
            # Usar la sesión de la instancia, que ya contiene las cookies
            response = self._session.get(devices_url, timeout=15)
            response.raise_for_status()
            devices_list = response.json()

            new_cache: Dict[int, Dict[str, Any]] = {}
            for dev in devices_list:
                dev_id = dev.get("id")
                if dev_id is not None:
                    new_cache[int(dev_id)] = dev

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
                    self.traccar_devices_cache[int(dev_id)] = dev_update
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
        if not self._session.cookies:
            logger.error(
                "No hay cookies en la sesión. Abortando conexión WebSocket de Traccar."
            )
            return

        ws_scheme = "ws" if TRACCAR_URL.startswith("http:") else "wss"
        ws_url_base = (
            TRACCAR_URL.replace("http://", "", 1).replace("https://", "", 1).rstrip("/")
        )
        websocket_url = f"{ws_scheme}://{ws_url_base}/api/socket"

        # --- MODIFICACIÓN CLAVE: Generar el header de la cookie de forma robusta ---
        # Usamos una utilidad de `requests` para formatear correctamente el header `Cookie`
        # a partir del cookiejar de nuestra sesión.
        try:
            # Creamos una request ficticia para que get_cookie_header sepa para qué dominio son las cookies
            dummy_request = requests.Request("GET", TRACCAR_URL)
            cookie_header_value = requests.utils.get_cookie_header(
                self._session.cookies, dummy_request
            )
            if not cookie_header_value:
                logger.error(
                    "No se pudo generar el header de la cookie desde la sesión. Abortando WS."
                )
                return
        except Exception as e:
            logger.error(f"Error inesperado al generar el header de la cookie: {e}")
            return

        headers = {"Cookie": cookie_header_value}
        # --- FIN DE MODIFICACIÓN ---

        logger.info(f"Intentando conectar al WebSocket de Traccar: {websocket_url}")
        logger.debug(
            f"Usando headers para WebSocket: {headers}"
        )  # Log para depurar el header de la cookie

        if self.ws_app:
            try:
                self.ws_app.close()
            except Exception:
                pass
            self.ws_app = None

        self.ws_app = websocket.WebSocketApp(
            websocket_url,
            header=headers,
            on_open=self._on_open_ws,
            on_message=self._on_message_ws,
            on_error=self._on_error_ws,
            on_close=self._on_close_ws,
        )

        self.ws_app.run_forever(
            ping_interval=WS_PING_INTERVAL_SECONDS,
            ping_timeout=WS_PING_TIMEOUT_SECONDS,
        )

        logger.info("El bucle run_forever del WebSocket de Traccar ha terminado.")
        self.ws_app = None

    def _on_open_ws(self, ws_app_instance: websocket.WebSocketApp):
        logger.info("--- Conexión WebSocket de Traccar abierta ---")
        if self._open_callback:
            self._open_callback(ws_app_instance)

    def _on_message_ws(self, ws_app_instance: websocket.WebSocketApp, message_str: str):
        if self._message_callback:
            self._message_callback(ws_app_instance, message_str)

    def _on_error_ws(self, ws_app_instance: websocket.WebSocketApp, error: Exception):
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
        logger.warning(
            f"--- Conexión WebSocket de Traccar cerrada --- Código: {close_status_code}, "
            f"Mensaje: {close_msg}"
        )
        if self._close_callback:
            self._close_callback(ws_app_instance, close_status_code, close_msg)

    def close_websocket(self):
        if self.ws_app:
            logger.info("Cerrando conexión WebSocket de Traccar explícitamente...")
            try:
                self.ws_app.close()
            except Exception as e:
                logger.error(f"Error durante cierre explícito de WebSocket: {e}")
            finally:
                self.ws_app = None
        else:
            logger.info("No hay conexión WebSocket de Traccar activa para cerrar.")
