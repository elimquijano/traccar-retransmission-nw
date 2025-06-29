import requests
import websocket  # type: ignore
import json
import logging
import threading  # NUEVO: Importamos threading para el heartbeat
import time  # NUEVO: Importamos time para el heartbeat
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

    MODIFICADO: Se ha añadido un sistema de 'heartbeat' a nivel de aplicación
    para mantener la conexión WebSocket activa y evitar desconexiones por 'Idle Timeout'.
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

        # NUEVO: Atributos para el hilo de heartbeat
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.heartbeat_stop_event = threading.Event()

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
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            logger.error(
                f"Error obteniendo o decodificando dispositivos de Traccar: {e}"
            )
        return False

    def update_device_cache_from_ws(self, device_updates: list):
        """
        Actualiza la caché local de dispositivos desde mensajes de actualización WebSocket.
        """
        updated_count = 0
        for dev_update in device_updates:
            dev_id = dev_update.get("id")
            if dev_id is not None:
                self.traccar_devices_cache[int(dev_id)] = dev_update
                updated_count += 1
        if updated_count > 0:
            logger.info(
                f"{updated_count} dispositivos actualizados en caché desde WebSocket."
            )

    # NUEVO: Métodos para controlar el heartbeat
    def _start_heartbeat(self):
        """Inicia un hilo que envía un mensaje periódico para mantener la conexión activa."""
        self.heartbeat_stop_event.clear()

        def heartbeat_loop():
            logger.info("Hilo de heartbeat iniciado.")
            while not self.heartbeat_stop_event.wait(
                30
            ):  # Envía un mensaje cada 30 segundos
                if self.ws_app and self.ws_app.sock and self.ws_app.sock.connected:
                    try:
                        # Enviamos un JSON vacío. El servidor lo interpreta como actividad.
                        self.ws_app.send("{}")
                        logger.debug("Heartbeat de aplicación enviado al WebSocket.")
                    except Exception as e:
                        logger.error(f"Error al enviar heartbeat de aplicación: {e}")
                        break  # Salir del bucle si hay un error
                else:
                    logger.warning(
                        "Socket no conectado, deteniendo bucle de heartbeat."
                    )
                    break
            logger.info("Hilo de heartbeat detenido.")

        self.heartbeat_thread = threading.Thread(
            target=heartbeat_loop, name="WsHeartbeatThread", daemon=True
        )
        self.heartbeat_thread.start()

    def _stop_heartbeat(self):
        """Detiene el hilo de heartbeat de forma segura."""
        self.heartbeat_stop_event.set()
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=2)
            self.heartbeat_thread = None

    def connect_websocket(self):
        """
        Conecta al API WebSocket de Traccar. Esta llamada es bloqueante (run_forever).
        """
        if not self.session_cookies:
            logger.error(
                "No hay cookies de sesión. Abortando conexión WebSocket de Traccar."
            )
            return

        ws_scheme = "ws" if TRACCAR_URL.startswith("http:") else "wss"
        ws_url_base = (
            TRACCAR_URL.replace("http://", "", 1).replace("https://", "", 1).rstrip("/")
        )
        websocket_url = f"{ws_scheme}://{ws_url_base}/api/socket"

        cookie_header_value = "; ".join(
            [
                f"{name}={value}"
                for name, value in self.session_cookies.get_dict().items()
            ]
        )
        headers = {"Cookie": cookie_header_value}

        logger.info(f"Intentando conectar al WebSocket de Traccar: {websocket_url}")

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
        self._stop_heartbeat()  # Aseguramos que el heartbeat se detenga al salir.
        self.ws_app = None

    def _on_open_ws(self, ws_app_instance: websocket.WebSocketApp):
        logger.info("--- Conexión WebSocket de Traccar abierta ---")
        self._start_heartbeat()  # MODIFICADO: Iniciar el heartbeat al abrir la conexión
        if self._open_callback:
            self._open_callback(ws_app_instance)

    def _on_message_ws(self, ws_app_instance: websocket.WebSocketApp, message_str: str):
        if self._message_callback:
            self._message_callback(ws_app_instance, message_str)

    def _on_error_ws(self, ws_app_instance: websocket.WebSocketApp, error: Exception):
        logger.error(
            f"--- Error en WebSocket de Traccar ---: Tipo: {type(error)}, "
            f"Representación: {repr(error)}"
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
        self._stop_heartbeat()  # MODIFICADO: Detener el heartbeat al cerrar la conexión
        if self._close_callback:
            self._close_callback(ws_app_instance, close_status_code, close_msg)

    def close_websocket(self):
        """Cierra la conexión WebSocket si está activa."""
        if self.ws_app:
            logger.info("Cerrando conexión WebSocket de Traccar explícitamente...")
            try:
                # El cierre de ws_app llamará a _on_close_ws, que detendrá el heartbeat.
                self.ws_app.close()
            except Exception as e:
                logger.error(f"Error durante cierre explícito de WebSocket: {e}")
            finally:
                self.ws_app = None
        else:
            logger.info("No hay conexión WebSocket de Traccar activa para cerrar.")
