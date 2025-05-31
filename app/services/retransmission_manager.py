import logging
import threading
import time
import json
from collections import deque
from typing import Deque, Set, Dict, Any, Optional, List, Type

from app.config import (
    RETRANSMIT_INTERVAL_SECONDS,
    MAX_QUEUE_SIZE_BEFORE_WARN,
    MAX_PROCESSED_IDS_SIZE,
    RETRANSMISSION_HANDLER_MAP,
)

# from app.constants import DEFAULT_RETRANSMIT_FALLBACK_URL # Si decides usar un fallback
from app.services.base_retransmission_handler import BaseRetransmissionHandler

# Importa tus clases de Handler aquí
from app.services.seguridad_ciudadana_handler import SeguridadCiudadanaHandler
from app.services.pnp_handler import OtroTipoHandler

# from app.services.un_tercer_tipo_handler import UnTercerTipoHandler # Si creas más
from app.traccar_client import TraccarClient  # For type hinting

logger = logging.getLogger(__name__)


class RetransmissionManager:
    def __init__(self):
        self.position_queue: Deque[Dict[str, Any]] = deque()
        self.processed_position_ids: Set[str] = set()
        self.stop_event: threading.Event = threading.Event()
        self.queue_lock: threading.Lock = threading.Lock()

        self.traccar_client: Optional[TraccarClient] = None
        self.retransmission_configs_db1: Dict[int, Dict[str, Any]] = {}  # Config de BD1

        self.worker_thread: Optional[threading.Thread] = None
        self._handler_instances: Dict[str, BaseRetransmissionHandler] = (
            {}
        )  # handler_id -> instance
        self._register_handlers()

    def _register_handlers(self):
        """Registers and instantiates all available retransmission handlers."""
        # Mapeo de ID de handler a la clase del Handler
        # Los IDs deben coincidir con los valores en config.RETRANSMISSION_HANDLER_MAP
        available_handler_classes: Dict[str, Type[BaseRetransmissionHandler]] = {
            SeguridadCiudadanaHandler.HANDLER_ID: SeguridadCiudadanaHandler,
            OtroTipoHandler.HANDLER_ID: OtroTipoHandler,
            # UnTercerTipoHandler.HANDLER_ID: UnTercerTipoHandler, # Añade más aquí
        }

        registered_handler_ids_from_config = set(RETRANSMISSION_HANDLER_MAP.values())

        for handler_id_in_code, HandlerClass in available_handler_classes.items():
            if handler_id_in_code not in registered_handler_ids_from_config:
                logger.warning(
                    f"Handler class {HandlerClass.__name__} (ID: {handler_id_in_code}) is defined in code "
                    f"but its ID is not found as a value in RETRANSMISSION_HANDLER_MAP in config. "
                    "This handler will not be used unless config is updated."
                )
                continue  # Skip if not in config map values

            try:
                self._handler_instances[handler_id_in_code] = HandlerClass()
                logger.info(
                    f"Registered and instantiated handler: {handler_id_in_code} ({HandlerClass.__name__})"
                )
            except Exception as e:
                logger.error(
                    f"Failed to instantiate handler {handler_id_in_code} ({HandlerClass.__name__}): {e}",
                    exc_info=True,
                )

        if not self._handler_instances:
            logger.warning(
                "No retransmission handlers were successfully registered and mapped from config!"
            )

    def set_traccar_client(self, client: TraccarClient):
        self.traccar_client = client

    def update_retransmission_configs_from_db1(
        self, configs_from_db1: Dict[int, Dict[str, Any]]
    ):
        with self.queue_lock:  # Protect shared cache
            self.retransmission_configs_db1 = configs_from_db1
        logger.info(
            f"Retransmission configs (BD1) cache updated with {len(configs_from_db1)} entries."
        )

    def handle_traccar_websocket_message(self, ws_app, message_str: str):
        if not self.traccar_client:
            logger.error(
                "Traccar client not set in RetransmissionManager. Cannot process WebSocket message."
            )
            return

        try:
            data = json.loads(message_str)
            if "positions" in data:
                self._enqueue_traccar_positions(data["positions"])
            elif "devices" in data:
                # Update device cache directly in traccar_client
                self.traccar_client.update_device_cache_from_ws(data["devices"])
        except json.JSONDecodeError:
            if (
                message_str.strip() and message_str.strip() != "{}"
            ):  # Avoid logging empty heartbeats
                logger.warning(
                    f"Non-JSON message received from Traccar WebSocket: {message_str[:200]}"
                )
        except Exception as e:
            logger.error(
                f"Error processing Traccar WebSocket message: {e} - Message: {message_str[:200]}",
                exc_info=True,
            )

    def _get_handler_instance_for_url(
        self, target_host_url: str
    ) -> Optional[BaseRetransmissionHandler]:
        """Finds the appropriate handler instance based on the target_host_url from RETRANSMISSION_HANDLER_MAP."""
        handler_id = RETRANSMISSION_HANDLER_MAP.get(target_host_url)
        if handler_id:
            handler_instance = self._handler_instances.get(handler_id)
            if not handler_instance:
                logger.warning(
                    f"Handler ID '{handler_id}' (for URL '{target_host_url}') found in config map, "
                    f"but no corresponding handler class was registered/instantiated."
                )
            return handler_instance

        # logger.debug(f"No specific handler_id found in RETRANSMISSION_HANDLER_MAP for URL: {target_host_url}")
        return None

    def _enqueue_traccar_positions(self, positions_data: list):
        if not self.traccar_client:
            logger.error("Cannot enqueue positions: Traccar client not available.")
            return

        new_positions_added = 0
        with self.queue_lock:  # Thread-safe operations on queue and caches
            for pos_data in positions_data:
                pos_id_traccar = str(
                    pos_data.get("id", "UNKNOWN_POS_ID")
                )  # Ensure string for set

                device_id_traccar_orig = pos_data.get("deviceId")
                if device_id_traccar_orig is None:
                    logger.debug(f"Position {pos_id_traccar} lacks deviceId. Skipping.")
                    continue

                try:
                    device_id_traccar_int = int(device_id_traccar_orig)
                except ValueError:
                    logger.warning(
                        f"Invalid deviceId format '{device_id_traccar_orig}' for pos {pos_id_traccar}. Skipping."
                    )
                    continue

                if pos_id_traccar in self.processed_position_ids:
                    # logger.debug(f"Position ID {pos_id_traccar} already processed, skipping.")
                    continue

                device_info = self.traccar_client.traccar_devices_cache.get(
                    device_id_traccar_int
                )
                # Config específica para este deviceId desde BD1 (retransmission_configs_db1)
                retrans_config_for_device_bd1 = self.retransmission_configs_db1.get(
                    device_id_traccar_int
                )

                if not device_info:
                    logger.warning(
                        f"No Traccar device info for deviceId {device_id_traccar_int} in cache. Position {pos_id_traccar} omitted."
                    )
                    continue

                if not retrans_config_for_device_bd1:
                    # This is normal if a device is in Traccar but not configured for retransmission in BD1.
                    # logger.debug(f"No retransmission config (BD1) for deviceId {device_id_traccar_int}. Position {pos_id_traccar} not queued.")
                    continue

                target_host_url_from_db = retrans_config_for_device_bd1.get("host_url")
                if not target_host_url_from_db:
                    logger.warning(
                        f"No 'host_url' in retransmission config (BD1) for deviceId {device_id_traccar_int}. "
                        f"Position {pos_id_traccar} skipped."
                    )
                    continue

                # Encontrar la instancia del handler basado en la URL del dispositivo de BD1
                handler_instance_to_use = self._get_handler_instance_for_url(
                    target_host_url_from_db
                )

                if not handler_instance_to_use:
                    logger.warning(
                        f"No registered/mapped handler found for host_url '{target_host_url_from_db}' "
                        f"(configured for deviceId {device_id_traccar_int}). Position {pos_id_traccar} skipped."
                    )
                    # Consider fallback if DEFAULT_RETRANSMIT_FALLBACK_URL is set
                    continue

                queue_item = {
                    "traccar_position": pos_data,
                    "device_info": device_info,
                    "retransmission_config_bd1": retrans_config_for_device_bd1,  # Contiene el host_url y otros datos de BD1
                    "handler_instance": handler_instance_to_use,  # La instancia del handler a usar
                    "target_url_for_retransmission": target_host_url_from_db,  # Para conveniencia en el worker
                }
                self.position_queue.append(queue_item)
                new_positions_added += 1
                self.processed_position_ids.add(pos_id_traccar)

                # Manage processed_position_ids size (same logic as before)
                if len(self.processed_position_ids) > MAX_PROCESSED_IDS_SIZE:
                    num_to_remove = (
                        len(self.processed_position_ids)
                        - MAX_PROCESSED_IDS_SIZE
                        + (MAX_PROCESSED_IDS_SIZE // 10)
                    )  # remove 10% extra
                    oldest_ids_sample = list(self.processed_position_ids)[
                        :num_to_remove
                    ]
                    for old_id in oldest_ids_sample:
                        self.processed_position_ids.discard(old_id)
                    # logger.debug(f"Trimmed processed_position_ids. New size: {len(self.processed_position_ids)}")

            if new_positions_added > 0:
                logger.info(
                    f"{new_positions_added} new positions added to queue. Queue size: {len(self.position_queue)}"
                )
            if len(self.position_queue) > MAX_QUEUE_SIZE_BEFORE_WARN:
                logger.warning(
                    f"WARNING: Retransmission queue has {len(self.position_queue)} items."
                )

    def _retransmission_worker_loop(self):
        logger.info("Retransmission worker loop started.")
        while not self.stop_event.is_set():
            queue_item_to_send: Optional[Dict[str, Any]] = None

            with self.queue_lock:
                if self.position_queue:
                    queue_item_to_send = self.position_queue.popleft()

            if queue_item_to_send:
                self._process_and_retransmit_item(queue_item_to_send)
            else:
                # Sleep briefly if queue is empty to avoid busy-waiting.
                # The loop continues if stop_event is set.
                # Use wait on the event for a short period.
                self.stop_event.wait(
                    RETRANSMIT_INTERVAL_SECONDS / 4
                    if RETRANSMIT_INTERVAL_SECONDS > 0.2
                    else 0.05
                )

            # Small delay to cede control, especially if many items are processed quickly
            # or if the queue was empty, to not consume CPU unnecessarily.
            # This also helps if stop_event is set while queue is empty.
            if not self.stop_event.is_set():  # Only sleep if not trying to stop
                time.sleep(0.01)

        logger.info("Retransmission worker loop stopped.")

    def _process_and_retransmit_item(self, queue_item: Dict[str, Any]):
        traccar_pos_data = queue_item["traccar_position"]
        device_info_data = queue_item["device_info"]
        retrans_cfg_bd1_data = queue_item["retransmission_config_bd1"]
        handler_to_use: BaseRetransmissionHandler = queue_item["handler_instance"]
        actual_target_url = queue_item["target_url_for_retransmission"]

        pos_id_display = traccar_pos_data.get("id", "N/A")
        dev_id_display = traccar_pos_data.get("deviceId", "N/A")

        try:
            transformed_payload = handler_to_use.transform_payload(
                traccar_pos_data, device_info_data, retrans_cfg_bd1_data
            )
        except Exception as e:
            logger.error(
                f"Error transforming payload with handler {handler_to_use.get_handler_id()} for device {dev_id_display}, "
                f"pos {pos_id_display}: {e}. Item dropped from queue.",
                exc_info=True,
            )
            return  # No re-encolar si la transformación falla catastróficamente

        # Retransmission and logging to BD2 are handled by the handler's retransmit method
        success, response_message, http_status_code = handler_to_use.retransmit(
            actual_target_url, transformed_payload, device_info_data
        )

        if not success:
            # If retransmission failed, re-queue the original item for retry.
            # The handler.retransmit method already logged the specific error.
            logger.warning(
                f"Failed to retransmit pos {pos_id_display} (dev {dev_id_display}) via handler "
                f"{handler_to_use.get_handler_id()} to {actual_target_url}. "
                f"Status: {http_status_code}, Response: {response_message[:200]}. Re-queuing."
            )

            # Basic retry logic: avoid retrying unrecoverable client errors (4xx)
            # except for common transient ones like 408 (Timeout) or 429 (Too Many Requests).
            # 5xx server errors and connection issues are generally worth retrying.
            should_retry = True
            if http_status_code:
                if 400 <= http_status_code < 500 and http_status_code not in [408, 429]:
                    logger.error(
                        f"Client error {http_status_code} for pos {pos_id_display}. Not re-queuing permanently."
                    )
                    should_retry = False

            if should_retry:
                with self.queue_lock:
                    self.position_queue.appendleft(queue_item)  # Re-queue at the front
                # Backoff to avoid hammering the target server
                time.sleep(RETRANSMIT_INTERVAL_SECONDS)  # Simple backoff
            # If not retrying, item is effectively dropped.

    def start_retransmission_worker(self):
        if self.worker_thread and self.worker_thread.is_alive():
            logger.info("Retransmission worker is already running.")
            return

        logger.info("Starting retransmission worker...")
        self.stop_event.clear()
        self.worker_thread = threading.Thread(
            target=self._retransmission_worker_loop,
            name="RetransmissionWorker",
            daemon=True,  # Exits when the main thread exits
        )
        self.worker_thread.start()

    def stop_retransmission_worker(self):
        logger.info("Signaling retransmission worker to stop...")
        self.stop_event.set()
        if self.worker_thread and self.worker_thread.is_alive():
            logger.info(
                f"Waiting for retransmission worker ({self.worker_thread.name}) to join..."
            )
            # Give the worker time to finish its current item and check the stop_event
            self.worker_thread.join(timeout=RETRANSMIT_INTERVAL_SECONDS + 5)
            if self.worker_thread.is_alive():
                logger.warning(
                    f"Retransmission worker ({self.worker_thread.name}) did not terminate in time."
                )
            else:
                logger.info(
                    f"Retransmission worker ({self.worker_thread.name}) joined successfully."
                )
        self.worker_thread = None
