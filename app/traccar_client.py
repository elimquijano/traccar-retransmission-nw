import requests
import websocket  # type: ignore
import json
import logging
from typing import Optional, Dict, Any
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
    def __init__(self, message_callback, open_callback, close_callback, error_callback):
        self.session_cookies: Optional[RequestsCookieJar] = None
        self.ws_app: Optional[websocket.WebSocketApp] = None
        self._message_callback = message_callback
        self._open_callback = open_callback
        self._close_callback = close_callback
        self._error_callback = error_callback
        self.traccar_devices_cache: Dict[int, Dict[str, Any]] = {}

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

            # Ensure IDs are integers for consistent keying
            self.traccar_devices_cache = {int(dev["id"]): dev for dev in devices_list}
            logger.info(f"Fetched {len(self.traccar_devices_cache)} Traccar devices.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Traccar devices: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding Traccar devices response: {e}")
        return False

    def update_device_cache_from_ws(self, device_updates: list):
        updated_count = 0
        for dev_update in device_updates:
            dev_id = dev_update.get("id")
            if dev_id is not None:  # Check for None explicitly
                self.traccar_devices_cache[int(dev_id)] = dev_update  # Ensure int key
                updated_count += 1
        if updated_count > 0:
            logger.info(f"{updated_count} devices updated in cache from WebSocket.")

    def connect_websocket(self):
        """Connects to the Traccar WebSocket API."""
        if not self.session_cookies:
            logger.error("No session cookies. Aborting Traccar WebSocket connection.")
            return

        ws_scheme = "ws" if TRACCAR_URL.startswith("http:") else "wss"
        ws_url_base = TRACCAR_URL.replace("http://", "").replace("https://", "")
        websocket_url = f"{ws_scheme}://{ws_url_base}/api/socket"

        cookie_header_value = "; ".join(
            [
                f"{name}={value}"
                for name, value in self.session_cookies.get_dict().items()
            ]
        )
        headers = {"Cookie": cookie_header_value}

        logger.info(f"Attempting to connect to Traccar WebSocket: {websocket_url}")
        self.ws_app = websocket.WebSocketApp(
            websocket_url,
            header=headers,
            on_open=self._on_open_ws,
            on_message=self._on_message_ws,
            on_error=self._on_error_ws,
            on_close=self._on_close_ws,
        )
        # Keep_running is important for run_forever in a thread
        # dispatcher is for potential advanced scenarios, not strictly needed here
        self.ws_app.run_forever(
            ping_interval=WS_PING_INTERVAL_SECONDS,
            ping_timeout=WS_PING_TIMEOUT_SECONDS,
            # sslopt={"cert_reqs": ssl.CERT_NONE} # If SSL issues, uncomment
        )
        logger.info(
            "Traccar WebSocket run_forever loop has ended."
        )  # Should ideally not be reached often

    def _on_open_ws(self, ws):
        logger.info("--- Traccar WebSocket Connection Opened ---")
        if self._open_callback:
            self._open_callback(ws)

    def _on_message_ws(self, ws, message_str):
        # Forward to the retransmitter_service's handler
        if self._message_callback:
            self._message_callback(ws, message_str)

    def _on_error_ws(self, ws, error):
        # CRITICAL: Log type and representation of error for better diagnosis
        logger.error(
            f"--- Traccar WebSocket Error ---: Type: {type(error)}, Repr: {repr(error)}, Str: {error}"
        )
        if self._error_callback:
            self._error_callback(ws, error)

    def _on_close_ws(self, ws, close_status_code, close_msg):
        # Log details of the close
        logger.warning(
            f"--- Traccar WebSocket Connection Closed --- Code: {close_status_code}, Msg: {close_msg}"
        )
        if self._close_callback:
            self._close_callback(ws, close_status_code, close_msg)

    def close_websocket(self):
        if self.ws_app:
            logger.info("Closing Traccar WebSocket connection...")
            self.ws_app.close()
            self.ws_app = None  # Important to allow re-creation if needed
