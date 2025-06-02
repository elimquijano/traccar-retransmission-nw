import logging
import threading
import time  # Aún necesario para el sleep general del worker si la cola está vacía
import json
import requests
import warnings
from collections import deque
from typing import Deque, Set, Dict, Any, Optional, Tuple, Type

from app.config import (
    RETRANSMIT_INTERVAL_SECONDS,
    MAX_QUEUE_SIZE_BEFORE_WARN,
    MAX_PROCESSED_IDS_SIZE,
    RETRANSMISSION_HANDLER_MAP,
    MAX_RETRANSMISSION_ATTEMPTS,
)
from app.services.base_retransmission_handler import BaseRetransmissionHandler
from app.services.seguridad_ciudadana_handler import SeguridadCiudadanaHandler
from app.services.pnp_handler import OtroTipoHandler
from app.traccar_client import TraccarClient
from app.persistence.log_writer_db import log_writer_db_instance
from app.constants import (
    LOG_ORIGIN_RETRANSMISSION_GPS,
    LOG_LEVEL_DB_INFO,
    LOG_LEVEL_DB_ERROR,
)

logger = logging.getLogger(__name__)


class RetransmissionManager:
    def __init__(self):
        self.position_queue: Deque[Dict[str, Any]] = deque()
        self.processed_position_ids: Set[str] = set()
        self.stop_event: threading.Event = threading.Event()
        self.queue_lock: threading.Lock = threading.Lock()

        self.traccar_client: Optional[TraccarClient] = None
        self.retransmission_configs_db1: Dict[int, Dict[str, Any]] = {}

        self.worker_thread: Optional[threading.Thread] = None
        self._handler_instances: Dict[str, BaseRetransmissionHandler] = {}
        self._register_handlers()

    # _register_handlers, set_traccar_client, update_retransmission_configs_from_db1,
    # handle_traccar_websocket_message, _get_handler_instance_for_url, _enqueue_traccar_positions
    # permanecen IGUALES que en la versión anterior. Solo copio los métodos que cambian o son clave.

    def _register_handlers(self):  # Sin cambios respecto a la última versión
        available_handler_classes: Dict[str, Type[BaseRetransmissionHandler]] = {
            SeguridadCiudadanaHandler.HANDLER_ID: SeguridadCiudadanaHandler,
            OtroTipoHandler.HANDLER_ID: OtroTipoHandler,
        }
        registered_handler_ids_from_config = set(RETRANSMISSION_HANDLER_MAP.values())
        for handler_id_in_code, HandlerClass in available_handler_classes.items():
            if handler_id_in_code not in registered_handler_ids_from_config:
                logger.warning(
                    f"Handler class {HandlerClass.__name__} (ID: {handler_id_in_code}) defined but not mapped in config."
                )
                continue
            try:
                self._handler_instances[handler_id_in_code] = HandlerClass()
                logger.info(f"Registered handler: {handler_id_in_code}")
            except Exception as e:
                logger.error(
                    f"Failed to instantiate handler {handler_id_in_code}: {e}",
                    exc_info=True,
                )
        if not self._handler_instances:
            logger.warning("No retransmission handlers registered/mapped!")

    def set_traccar_client(self, client: TraccarClient):  # Sin cambios
        self.traccar_client = client

    def update_retransmission_configs_from_db1(
        self, configs_from_db1: Dict[int, Dict[str, Any]]
    ):  # Sin cambios
        with self.queue_lock:
            self.retransmission_configs_db1 = configs_from_db1
        logger.info(
            f"Retransmission configs (BD1) cache updated with {len(configs_from_db1)} entries."
        )

    def handle_traccar_websocket_message(self, ws_app, message_str: str):  # Sin cambios
        if not self.traccar_client:
            logger.error("Traccar client not set. Cannot process WebSocket message.")
            return
        try:
            data = json.loads(message_str)
            if "positions" in data:
                self._enqueue_traccar_positions(data["positions"])
            elif "devices" in data:
                self.traccar_client.update_device_cache_from_ws(data["devices"])
        except json.JSONDecodeError:
            if message_str.strip() and message_str.strip() != "{}":
                logger.warning(f"Non-JSON message from Traccar WS: {message_str[:200]}")
        except Exception as e:
            logger.error(
                f"Error processing Traccar WS message: {e} - Msg: {message_str[:200]}",
                exc_info=True,
            )

    def _get_handler_instance_for_url(
        self, target_host_url: str
    ) -> Optional[BaseRetransmissionHandler]:  # Sin cambios
        handler_id = RETRANSMISSION_HANDLER_MAP.get(target_host_url)
        if handler_id:
            handler_instance = self._handler_instances.get(handler_id)
            if not handler_instance:
                logger.warning(
                    f"Handler ID '{handler_id}' (URL '{target_host_url}') mapped in config, but no class registered."
                )
            return handler_instance
        return None

    def _enqueue_traccar_positions(self, positions_data: list):  # Sin cambios
        if not self.traccar_client:
            logger.error("Cannot enqueue: Traccar client not available.")
            return
        new_positions_added = 0
        with self.queue_lock:
            for pos_data in positions_data:
                pos_id_traccar = str(pos_data.get("id", "UNKNOWN_POS_ID"))
                device_id_traccar_orig = pos_data.get("deviceId")
                if device_id_traccar_orig is None:
                    continue
                try:
                    device_id_traccar_int = int(device_id_traccar_orig)
                except ValueError:
                    logger.warning(
                        f"Invalid deviceId '{device_id_traccar_orig}' for pos {pos_id_traccar}. Skipping."
                    )
                    continue
                if pos_id_traccar in self.processed_position_ids:
                    continue
                device_info = self.traccar_client.traccar_devices_cache.get(
                    device_id_traccar_int
                )
                retrans_config_for_device_bd1 = self.retransmission_configs_db1.get(
                    device_id_traccar_int
                )
                if not device_info or not retrans_config_for_device_bd1:
                    continue
                target_host_url_from_db = retrans_config_for_device_bd1.get("host_url")
                if not target_host_url_from_db:
                    continue
                handler_instance_to_use = self._get_handler_instance_for_url(
                    target_host_url_from_db
                )
                if not handler_instance_to_use:
                    continue
                queue_item = {
                    "traccar_position": pos_data,
                    "device_info": device_info,
                    "retransmission_config_bd1": retrans_config_for_device_bd1,
                    "handler_instance": handler_instance_to_use,
                    "target_url_for_retransmission": target_host_url_from_db,
                }
                self.position_queue.append(queue_item)
                new_positions_added += 1
                self.processed_position_ids.add(pos_id_traccar)
                if len(self.processed_position_ids) > MAX_PROCESSED_IDS_SIZE:
                    num_to_remove = (
                        len(self.processed_position_ids)
                        - MAX_PROCESSED_IDS_SIZE
                        + (MAX_PROCESSED_IDS_SIZE // 10)
                    )
                    oldest_ids_sample = list(self.processed_position_ids)[
                        :num_to_remove
                    ]
                    for old_id in oldest_ids_sample:
                        self.processed_position_ids.discard(old_id)
            if new_positions_added > 0:
                logger.info(
                    f"{new_positions_added} new positions added to retransmission queue. Size: {len(self.position_queue)}"
                )
            if len(self.position_queue) > MAX_QUEUE_SIZE_BEFORE_WARN:
                logger.warning(
                    f"WARNING: Retransmission queue size: {len(self.position_queue)}"
                )

    def _retransmission_worker_loop(self):
        logger.info("Retransmission worker loop started.")
        while not self.stop_event.is_set():
            queue_item_to_process: Optional[Dict[str, Any]] = None

            with self.queue_lock:
                if self.position_queue:
                    queue_item_to_process = self.position_queue.popleft()

            if queue_item_to_process:
                self._attempt_item_retransmission_with_retries(queue_item_to_process)
            else:
                # Esperar si la cola está vacía
                self.stop_event.wait(
                    RETRANSMIT_INTERVAL_SECONDS / 2
                    if RETRANSMIT_INTERVAL_SECONDS > 0.1
                    else 0.05
                )

            # ELIMINADO EL time.sleep(0.01) general del worker si no quieres ningún respiro.
            # Si la cola está vacía, el wait anterior ya introduce una pausa.
            # Si la cola está llena, queremos procesar lo más rápido posible.

        logger.info("Retransmission worker loop stopped.")

    def _attempt_item_retransmission_with_retries(self, queue_item: Dict[str, Any]):
        traccar_pos_data = queue_item["traccar_position"]
        device_info_data = queue_item["device_info"]
        # retrans_cfg_bd1_data = queue_item["retransmission_config_bd1"] # No se usa si el handler no lo necesita explícitamente
        handler_to_use: BaseRetransmissionHandler = queue_item["handler_instance"]
        actual_target_url = queue_item["target_url_for_retransmission"]

        pos_id_display = traccar_pos_data.get("id", "N/A")
        dev_id_display = traccar_pos_data.get("deviceId", "N/A")
        placa_for_log = str(device_info_data.get("name", "N/A_dev_name"))

        final_success = False
        final_response_text = (
            f"All {MAX_RETRANSMISSION_ATTEMPTS} attempts failed or unrecoverable error."
        )
        final_status_code = None
        transformed_payload_dict: Optional[Dict[str, Any]] = None
        json_send_str_for_log = "{}"

        try:
            transformed_payload_dict = handler_to_use.transform_payload(
                traccar_pos_data,
                device_info_data,
                queue_item[
                    "retransmission_config_bd1"
                ],  # Pasar config al transformador
            )
            json_send_str_for_log = json.dumps(transformed_payload_dict)
            placa_for_log = str(transformed_payload_dict.get("placa", placa_for_log))
        except Exception as e:
            logger.error(
                f"CRITICAL_TRANSFORM_FAIL: Handler {handler_to_use.get_handler_id()} "
                f"for dev {dev_id_display}, pos {pos_id_display}: {e}. Item dropped.",
                exc_info=True,
            )
            return  # No se puede retransmitir ni loguear

        for attempt in range(1, MAX_RETRANSMISSION_ATTEMPTS + 1):
            if self.stop_event.is_set():
                logger.info(
                    f"Stop event during retransmission attempt {attempt} for pos {pos_id_display}. Aborting."
                )
                return  # No loguear a BD

            # logger.info( # Log más conciso para cada intento
            #     f"Attempt {attempt}/{MAX_RETRANSMISSION_ATTEMPTS} for pos {pos_id_display} (placa {placa_for_log})."
            # )

            current_success, current_response_text, current_status_code = (
                self._execute_single_http_post(actual_target_url, json_send_str_for_log)
            )  # Pasar string JSON

            final_response_text = current_response_text
            final_status_code = current_status_code

            if current_success:
                logger.info(  # Log de éxito del intento (no el final aún)
                    f"Attempt {attempt} SUCCESS for pos {pos_id_display} (placa {placa_for_log}). Status: {current_status_code}."
                )
                final_success = True
                break  # Salir del bucle de reintentos
            else:  # Fallo en el intento actual
                logger.warning(
                    f"Attempt {attempt} FAIL for pos {pos_id_display} (placa {placa_for_log}). "
                    f"Status: {current_status_code}, Resp: {current_response_text[:100]}."
                )
                if attempt < MAX_RETRANSMISSION_ATTEMPTS:
                    if (
                        current_status_code
                        and 400 <= current_status_code < 500
                        and current_status_code not in [408, 429]
                    ):
                        logger.error(
                            f"Client error {current_status_code} for pos {pos_id_display}. No further retries."
                        )
                        final_response_text = (
                            f"{current_response_text}"  # Guardar este error
                        )
                        break

                    # SIN RESPIRO ENTRE REINTENTOS
                    # if self.stop_event.wait(RETRANSMISSION_RETRY_DELAY_SECONDS): # ELIMINADO
                    #     logger.info(f"Stop event during retry delay for pos {pos_id_display}. Aborting.")
                    #     return
                else:  # Último intento y falló
                    logger.error(
                        f"All {MAX_RETRANSMISSION_ATTEMPTS} attempts failed for pos {pos_id_display} (placa {placa_for_log})."
                    )

        # Loguear el resultado final UNA SOLA VEZ
        if log_writer_db_instance:
            log_level_db = LOG_LEVEL_DB_INFO if final_success else LOG_LEVEL_DB_ERROR
            log_writer_db_instance.add_log_entry_data(
                response=str(final_response_text),
                level=log_level_db,
                placa=placa_for_log,
                host=actual_target_url,
                json_send=json_send_str_for_log,
                origen=LOG_ORIGIN_RETRANSMISSION_GPS,
            )
        else:
            logger.warning(
                f"LogWriterDB not available. Final result for pos {pos_id_display} (placa {placa_for_log}) not logged to DB."
            )

    def _execute_single_http_post(
        self, target_url: str, json_payload_str: str, timeout: int = 10
    ) -> Tuple[bool, str, Optional[int]]:
        headers = {"Content-Type": "application/json"}
        success = False
        response_text = "No response"
        status_code = None
        try:
            with warnings.catch_warnings():
                warnings.simplefilter(
                    "ignore",
                    requests.packages.urllib3.exceptions.InsecureRequestWarning,
                )
                response = requests.post(
                    target_url,
                    data=json_payload_str,
                    headers=headers,
                    timeout=timeout,
                    verify=False,
                )
            status_code = response.status_code
            try:
                response_text = "Se ha registrado de forma exitosa el punto gps"
            except Exception:
                response_text = "Could not decode response text."
            response.raise_for_status()
            success = True
        except requests.exceptions.RequestException as req_err:
            response_text = f"{type(req_err).__name__}: {req_err}"
            if (
                isinstance(req_err, requests.exceptions.HTTPError)
                and req_err.response is not None
            ):
                status_code = req_err.response.status_code
                response_text = (
                    f"HTTPError {status_code}: {req_err.response.text[:200]}"
                    if hasattr(req_err.response, "text")
                    else f"HTTPError {status_code}"
                )
        except Exception as e:
            response_text = f"UnexpectedErrorInPostAttempt: {e}"
        return success, response_text, status_code

    def start_retransmission_worker(self):  # Sin cambios
        if self.worker_thread and self.worker_thread.is_alive():
            logger.info("Retransmission worker is already running.")
            return
        logger.info("Starting retransmission worker...")
        self.stop_event.clear()
        self.worker_thread = threading.Thread(
            target=self._retransmission_worker_loop,
            name="RetransmissionWorker",
            daemon=True,
        )
        self.worker_thread.start()

    def stop_retransmission_worker(
        self,
    ):  # Sin cambios (timeout de join podría ajustarse si es necesario)
        logger.info("Signaling retransmission worker to stop...")
        self.stop_event.set()
        if self.worker_thread and self.worker_thread.is_alive():
            join_timeout = (
                RETRANSMIT_INTERVAL_SECONDS + 2 + (MAX_RETRANSMISSION_ATTEMPTS * 0.1)
            )  # Ajustado ya que no hay delay de reintento
            logger.info(
                f"Waiting for retransmission worker ({self.worker_thread.name}) to join (timeout: {join_timeout:.2f}s)..."
            )
            self.worker_thread.join(timeout=join_timeout)
            if self.worker_thread.is_alive():
                logger.warning(
                    f"Retransmission worker ({self.worker_thread.name}) did not terminate in time."
                )
            else:
                logger.info(
                    f"Retransmission worker ({self.worker_thread.name}) joined successfully."
                )
        self.worker_thread = None
