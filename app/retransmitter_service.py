import requests
import json
import threading
import time
import logging
from collections import deque
from typing import Deque, Set, Dict, Any, Optional

from app.config import (
    RETRANSMIT_INTERVAL_SECONDS,
    MAX_QUEUE_SIZE_BEFORE_WARN,
    MAX_PROCESSED_IDS_SIZE,
    DEFAULT_TARGET_RETRANSMIT_URL,
)
from app.payload_transformer import transform_position_to_seguridad_ciudadana_payload
from app.traccar_client import TraccarClient  # For type hinting

logger = logging.getLogger(__name__)


class RetransmitterService:
    def __init__(self):
        self.position_queue: Deque[Dict[str, Any]] = deque()
        self.processed_position_ids: Set[str] = set()
        self.retransmit_thread_stop_event: threading.Event = threading.Event()
        self.queue_lock: threading.Lock = threading.Lock()

        # Caches to be populated by the main application logic
        self.traccar_client: Optional[TraccarClient] = None  # Will be set from main
        self.retransmission_config_cache: Dict[int, Dict[str, Any]] = {}

        self.retransmit_worker_thread: Optional[threading.Thread] = None

    def set_traccar_client(self, client: TraccarClient):
        self.traccar_client = client

    def set_retransmission_config_cache(self, config_cache: Dict[int, Dict[str, Any]]):
        with self.queue_lock:  # Protect shared cache
            self.retransmission_config_cache = config_cache

    def handle_traccar_message(self, ws, message_str: str):
        """Handles incoming messages from Traccar WebSocket."""
        if not self.traccar_client:
            logger.error(
                "Traccar client not set in RetransmitterService. Cannot process message."
            )
            return

        try:
            data = json.loads(message_str)

            if "positions" in data:
                self._process_positions(data["positions"])
            elif "devices" in data:
                # Update device cache directly in traccar_client
                with self.queue_lock:  # Lock access to traccar_client's cache
                    self.traccar_client.update_device_cache_from_ws(data["devices"])

        except json.JSONDecodeError:
            # Avoid logging empty heartbeats or keep-alive messages if Traccar sends them
            if message_str.strip() and message_str.strip() != "{}":
                logger.warning(
                    f"Non-JSON message received: {message_str[:200]}"
                )  # Log truncated
        except Exception as e:
            logger.error(
                f"Error processing Traccar message: {e} - Message: {message_str[:200]}",
                exc_info=True,
            )

    def _process_positions(self, positions_data: list):
        if not self.traccar_client:
            return  # Should not happen if called internally

        new_positions_added = 0
        with self.queue_lock:  # Ensure thread-safe operations on queue and caches
            for pos in positions_data:
                pos_id_traccar = str(pos.get("id"))  # Ensure string for set
                device_id_traccar = pos.get("deviceId")

                if not device_id_traccar:
                    logger.debug(
                        f"Position without deviceId, skipping: {pos.get('id')}"
                    )
                    continue

                device_id_traccar = int(device_id_traccar)  # Ensure int for cache keys

                if pos_id_traccar in self.processed_position_ids:
                    # logger.debug(f"Position ID {pos_id_traccar} already processed, skipping.")
                    continue

                device_info = self.traccar_client.traccar_devices_cache.get(
                    device_id_traccar
                )
                retrans_config_for_device = self.retransmission_config_cache.get(
                    device_id_traccar
                )

                if not device_info:
                    logger.warning(
                        f"No device info for deviceId {device_id_traccar} in cache. Position {pos_id_traccar} omitted."
                    )
                    continue

                if not retrans_config_for_device:
                    # This is not an error, device might not be configured for retransmission
                    # logger.debug(f"No retransmission config for deviceId {device_id_traccar}. Position {pos_id_traccar} not queued.")
                    continue

                queue_item = {
                    "traccar_position": pos,
                    "device_info": device_info,
                    "retransmission_config": retrans_config_for_device,
                }
                self.position_queue.append(queue_item)
                new_positions_added += 1
                self.processed_position_ids.add(pos_id_traccar)

                # Manage processed_position_ids size
                if len(self.processed_position_ids) > MAX_PROCESSED_IDS_SIZE:
                    # Simple strategy: remove a chunk of the oldest (not strictly LRU)
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
                    logger.info(
                        f"Trimmed processed_position_ids set. New size: {len(self.processed_position_ids)}"
                    )

            if new_positions_added > 0:
                logger.info(
                    f"{new_positions_added} new positions added to queue. Queue size: {len(self.position_queue)}"
                )
            if len(self.position_queue) > MAX_QUEUE_SIZE_BEFORE_WARN:
                logger.warning(
                    f"WARNING: Retransmission queue has {len(self.position_queue)} items."
                )

    def _retransmit_position_worker(self):
        logger.info("Retransmission worker thread started.")
        while not self.retransmit_thread_stop_event.is_set():
            queue_item_to_send: Optional[Dict[str, Any]] = None

            with self.queue_lock:
                if self.position_queue:
                    queue_item_to_send = self.position_queue.popleft()

            if queue_item_to_send:
                self._send_item(queue_item_to_send)
            else:
                # Sleep briefly if queue is empty
                time.sleep(RETRANSMIT_INTERVAL_SECONDS / 4)  # Shorter sleep when idle

            # Small delay to prevent tight loop if many items are processed quickly
            # or to allow other threads to acquire the lock if queue was empty.
            time.sleep(0.05)

        logger.info("Retransmission worker thread stopped.")

    def _send_item(self, queue_item: Dict[str, Any]):
        traccar_pos = queue_item["traccar_position"]
        dev_info = queue_item["device_info"]
        retrans_cfg = queue_item["retransmission_config"]

        # Target URL from DB config, fallback to default if not specified
        target_url = retrans_cfg.get("host_url") or DEFAULT_TARGET_RETRANSMIT_URL

        if not target_url:
            logger.error(
                f"No host_url for deviceId {traccar_pos.get('deviceId')}. Discarding position {traccar_pos.get('id')}."
            )
            return

        transformed_payload = transform_position_to_seguridad_ciudadana_payload(
            traccar_pos, dev_info, retrans_cfg
        )
        pos_id_display = traccar_pos.get("id", "N/A")
        dev_id_display = traccar_pos.get("deviceId", "N/A")
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(
                target_url, json=transformed_payload, headers=headers, timeout=10
            )
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            logger.info(
                f"Successfully retransmitted pos {pos_id_display} (dev {dev_id_display}) to {target_url}. Status: {response.status_code}"
            )
        except requests.exceptions.HTTPError as http_err:
            logger.error(
                f"HTTP error retransmitting pos {pos_id_display} (dev {dev_id_display}) to {target_url}: {http_err}. Status: {http_err.response.status_code}. Re-queuing."
            )
            with self.queue_lock:
                self.position_queue.appendleft(queue_item)  # Re-queue at the front
            time.sleep(RETRANSMIT_INTERVAL_SECONDS)  # Backoff
        except requests.exceptions.ConnectionError as conn_err:
            logger.error(
                f"Connection error retransmitting pos {pos_id_display} (dev {dev_id_display}) to {target_url}: {conn_err}. Re-queuing."
            )
            with self.queue_lock:
                self.position_queue.appendleft(queue_item)
            time.sleep(
                RETRANSMIT_INTERVAL_SECONDS * 2
            )  # Longer backoff for connection errors
        except requests.exceptions.Timeout as timeout_err:
            logger.error(
                f"Timeout retransmitting pos {pos_id_display} (dev {dev_id_display}) to {target_url}: {timeout_err}. Re-queuing."
            )
            with self.queue_lock:
                self.position_queue.appendleft(queue_item)
            time.sleep(RETRANSMIT_INTERVAL_SECONDS)
        except (
            requests.exceptions.RequestException
        ) as req_err:  # Catch other request-related errors
            logger.error(
                f"Request exception retransmitting pos {pos_id_display} (dev {dev_id_display}) to {target_url}: {req_err}. Re-queuing."
            )
            with self.queue_lock:
                self.position_queue.appendleft(queue_item)
            time.sleep(RETRANSMIT_INTERVAL_SECONDS)
        except Exception as e:  # Catch-all for other unexpected errors during send
            logger.critical(
                f"Unexpected error retransmitting pos {pos_id_display} (dev {dev_id_display}) to {target_url}: {e}. Re-queuing.",
                exc_info=True,
            )
            with self.queue_lock:
                self.position_queue.appendleft(queue_item)
            time.sleep(RETRANSMIT_INTERVAL_SECONDS)

    def start_retransmission_worker(self):
        if self.retransmit_worker_thread and self.retransmit_worker_thread.is_alive():
            logger.info("Retransmission worker thread is already running.")
            return

        logger.info("Starting retransmission worker thread...")
        self.retransmit_thread_stop_event.clear()
        self.retransmit_worker_thread = threading.Thread(
            target=self._retransmit_position_worker,
            name="RetransmitWorker",
            daemon=True,  # Daemon thread will exit when main program exits
        )
        self.retransmit_worker_thread.start()

    def stop_retransmission_worker(self):
        logger.info("Signaling retransmission worker thread to stop...")
        self.retransmit_thread_stop_event.set()
        if self.retransmit_worker_thread and self.retransmit_worker_thread.is_alive():
            logger.info(
                f"Waiting for retransmission worker thread ({self.retransmit_worker_thread.name}) to join..."
            )
            self.retransmit_worker_thread.join(
                timeout=RETRANSMIT_INTERVAL_SECONDS + 5
            )  # Wait a bit longer than interval
            if self.retransmit_worker_thread.is_alive():
                logger.warning(
                    f"Retransmission worker thread ({self.retransmit_worker_thread.name}) did not terminate in time."
                )
            else:
                logger.info(
                    f"Retransmission worker thread ({self.retransmit_worker_thread.name}) joined successfully."
                )
        self.retransmit_worker_thread = None
