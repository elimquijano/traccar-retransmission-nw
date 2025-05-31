import requests
import websocket  # type: ignore
import json
import logging
from typing import Optional, Dict, Any, Callable  # Added Callable for callbacks
from requests.cookies import RequestsCookieJar

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
        self.session_cookies: Optional[RequestsCookieJar] = None
        self.ws_app: Optional[websocket.WebSocketApp] = None
        self._message_callback = message_callback
        self._open_callback = open_callback
        self._close_callback = close_callback
        self._error_callback = error_callback
        self.traccar_devices_cache: Dict[int, Dict[str, Any]] = (
            {}
        )  # device_id (int) -> device_data

    def login(self) -> bool:
        """Logs into Traccar and stores session cookies."""
        session = requests.Session()
        login_url = f"{TRACCAR_URL}/api/session"
        login_data = {"email": TRACCAR_EMAIL, "password": TRACCAR_PASSWORD}
        try:
            logger.info(f"Attempting Traccar login: {login_url}...")
            response = session.post(login_url, data=login_data, timeout=10)
            response.raise_for_status()
            self.session_cookies = session.cookies
            logger.info("Traccar login successful!")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Traccar login error: {e}")
        return False

    def fetch_devices(self) -> bool:
        """Fetches all devices from Traccar and populates the cache."""
        if not self.session_cookies:
            logger.warning("No session cookies to fetch Traccar devices.")
            return False

        devices_url = f"{TRACCAR_URL}/api/devices"
        try:
            logger.info(f"Fetching Traccar devices from {devices_url}...")
            response = requests.get(
                devices_url, cookies=self.session_cookies, timeout=15
            )
            response.raise_for_status()
            devices_list = response.json()

            new_cache: Dict[int, Dict[str, Any]] = {}
            for dev in devices_list:
                dev_id = dev.get("id")
                if dev_id is not None:
                    new_cache[int(dev_id)] = dev  # Ensure device ID is int
            self.traccar_devices_cache = new_cache
            logger.info(f"Fetched {len(self.traccar_devices_cache)} Traccar devices.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Traccar devices: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding Traccar devices response: {e}")
        return False

    def update_device_cache_from_ws(self, device_updates: list):
        """Updates the local device cache from WebSocket device update messages."""
        updated_count = 0
        # This method might be called from a different thread (WebSocket callback)
        # if traccar_devices_cache can be read by another thread concurrently,
        # a lock might be needed here for thread safety.
        # For now, assuming TraccarClient methods are called from the main app thread
        # or the RetransmissionManager handles external access to its TraccarClient instance's cache safely.
        for dev_update in device_updates:
            dev_id = dev_update.get("id")
            if dev_id is not None:
                try:
                    self.traccar_devices_cache[int(dev_id)] = (
                        dev_update  # Ensure int key
                    )
                    updated_count += 1
                except ValueError:
                    logger.warning(
                        f"Invalid device ID '{dev_id}' in WebSocket device update. Skipping."
                    )
        if updated_count > 0:
            logger.info(f"{updated_count} devices updated in cache from WebSocket.")

    def connect_websocket(self):
        """Connects to the Traccar WebSocket API. This call is blocking via run_forever."""
        if not self.session_cookies:
            logger.error("No session cookies. Aborting Traccar WebSocket connection.")
            return

        ws_scheme = "ws" if TRACCAR_URL.startswith("http:") else "wss"
        # Ensure ws_url_base correctly removes http(s):// and any trailing slashes
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

        logger.info(f"Attempting to connect to Traccar WebSocket: {websocket_url}")
        # Ensure ws_app is reset if a previous connection existed and failed uncleanly
        if self.ws_app:
            try:
                self.ws_app.close()  # Attempt to close any existing stale app
            except Exception:  # Ignore errors during close of stale app
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
        # run_forever will block until the WebSocket connection is closed or an error occurs
        # that stops the loop.
        # Consider adding `ping_payload` if Traccar expects a specific format.
        # `sslopt={"cert_reqs": ssl.CERT_NONE}` can be added if facing SSL certificate validation issues (not recommended for production).
        self.ws_app.run_forever(
            ping_interval=WS_PING_INTERVAL_SECONDS,
            ping_timeout=WS_PING_TIMEOUT_SECONDS,
            # ping_payload="PING" # Optional: if server needs specific ping
        )
        logger.info("Traccar WebSocket run_forever loop has ended.")
        # After run_forever ends, ws_app might be in an unusable state or closed.
        # It's good practice to nullify it so a fresh one is created on next connect_websocket call.
        self.ws_app = None

    def _on_open_ws(
        self, ws_app_instance: websocket.WebSocketApp
    ):  # Argument is the WebSocketApp instance
        logger.info("--- Traccar WebSocket Connection Opened ---")
        if self._open_callback:
            self._open_callback(ws_app_instance)

    def _on_message_ws(self, ws_app_instance: websocket.WebSocketApp, message_str: str):
        # Forward to the retransmission_manager's handler or other designated callback
        if self._message_callback:
            self._message_callback(ws_app_instance, message_str)

    def _on_error_ws(self, ws_app_instance: websocket.WebSocketApp, error: Exception):
        # Log type and representation of error for better diagnosis
        # Common errors: ConnectionRefusedError, WebSocketConnectionClosedException, socket.timeout
        logger.error(
            f"--- Traccar WebSocket Error ---: Type: {type(error)}, Repr: {repr(error)}, Str: {str(error)}"
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
            f"--- Traccar WebSocket Connection Closed --- Code: {close_status_code}, Msg: {close_msg}"
        )
        if self._close_callback:
            self._close_callback(ws_app_instance, close_status_code, close_msg)

    def close_websocket(self):
        """Closes the WebSocket connection if it's active."""
        if self.ws_app:
            logger.info("Closing Traccar WebSocket connection explicitly...")
            try:
                self.ws_app.close()
            except Exception as e:
                logger.error(f"Error during explicit WebSocket close: {e}")
            finally:
                self.ws_app = None  # Ensure it's nullified
        else:
            logger.info("No active Traccar WebSocket connection to close.")
