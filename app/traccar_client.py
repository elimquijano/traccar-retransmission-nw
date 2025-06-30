# app/traccar_client.py
import requests
import websocket  # type: ignore
import json
import logging
from typing import Optional, Dict, Any, Callable

# Importamos configuraciones necesarias
from app.config import (
    TRACCAR_URL,
    TRACCAR_EMAIL,
    TRACCAR_PASSWORD,
    WS_PING_INTERVAL_SECONDS,
    WS_PING_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)


class TraccarClient:
    def __init__(
        self,
        message_callback: Callable[[websocket.WebSocketApp, str], None],
        open_callback: Callable[[websocket.WebSocketApp], None],
        close_callback: Callable[
            [websocket.WebSocketApp, Optional[int], Optional[str]], None
        ],
        error_callback: Callable[[websocket.WebSocketApp, Exception], None],
    ):
        self._session = requests.Session()
        self.ws_app: Optional[websocket.WebSocketApp] = None
        self._message_callback = message_callback
        self._open_callback = open_callback
        self._close_callback = close_callback
        self._error_callback = error_callback
        self.traccar_devices_cache: Dict[int, Dict[str, Any]] = {}

    def login(self) -> bool:
        login_url = f"{TRACCAR_URL}/api/session"
        login_data = {"email": TRACCAR_EMAIL, "password": TRACCAR_PASSWORD}
        try:
            logger.info(f"Intentando login en Traccar: {login_url}...")
            response = self._session.post(login_url, data=login_data, timeout=10)
            response.raise_for_status()
            logger.info("Login en Traccar exitoso!")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en login de Traccar: {e}")
            return False

    def fetch_devices(self) -> bool:
        if not self._session.cookies:
            logger.warning("No hay cookies en la sesión para obtener dispositivos.")
            return False
        devices_url = f"{TRACCAR_URL}/api/devices"
        try:
            logger.info(f"Obteniendo dispositivos de Traccar desde {devices_url}...")
            response = self._session.get(devices_url, timeout=15)
            response.raise_for_status()
            devices_list = response.json()
            new_cache: Dict[int, Dict[str, Any]] = {
                int(dev["id"]): dev for dev in devices_list if dev.get("id") is not None
            }
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
        updated_count = 0
        for dev_update in device_updates:
            dev_id = dev_update.get("id")
            if dev_id is not None:
                try:
                    self.traccar_devices_cache[int(dev_id)] = dev_update
                    updated_count += 1
                except ValueError:
                    logger.warning(
                        f"ID de dispositivo inválido '{dev_id}' en actualización WebSocket."
                    )
        if updated_count > 0:
            logger.info(
                f"{updated_count} dispositivos actualizados en caché desde WebSocket."
            )

    def connect_websocket(self):
        if not self._session.cookies:
            logger.error("No hay cookies en la sesión. Abortando conexión WebSocket.")
            return

        ws_scheme = "ws" if TRACCAR_URL.startswith("http:") else "wss"
        ws_url_base = (
            TRACCAR_URL.replace("http://", "", 1).replace("https://", "", 1).rstrip("/")
        )
        websocket_url = f"{ws_scheme}://{ws_url_base}/api/socket"

        try:
            cookies_dict = self._session.cookies.get_dict()
            if not cookies_dict:
                logger.error(
                    "No se encontraron cookies en la sesión para el WebSocket. Abortando."
                )
                return
            cookie_header_value = "; ".join(
                [f"{name}={value}" for name, value in cookies_dict.items()]
            )
            headers = {"Cookie": cookie_header_value}
        except Exception as e:
            logger.error(
                f"Error inesperado al construir el header de la cookie: {e}",
                exc_info=True,
            )
            return

        logger.info(f"Intentando conectar al WebSocket de Traccar: {websocket_url}")
        logger.debug(f"Usando headers para WebSocket: {headers}")

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
            ping_interval=WS_PING_INTERVAL_SECONDS, ping_timeout=WS_PING_TIMEOUT_SECONDS
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
        logger.error(f"--- Error en WebSocket de Traccar ---: {error}", exc_info=True)
        if self._error_callback:
            self._error_callback(ws_app_instance, error)

    def _on_close_ws(
        self,
        ws_app_instance: websocket.WebSocketApp,
        code: Optional[int],
        msg: Optional[str],
    ):
        logger.warning(
            f"--- Conexión WebSocket de Traccar cerrada --- Código: {code}, Mensaje: {msg}"
        )
        if self._close_callback:
            self._close_callback(ws_app_instance, code, msg)

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
