# app/services/retransmission_manager.py
import logging
import asyncio
import aiohttp
import json
import certifi
from collections import deque
from typing import Deque, Dict, Any, Optional, Tuple, Type, Set

from app.config import (
    MAX_QUEUE_SIZE_BEFORE_WARN,
    MAX_PROCESSED_IDS_SIZE,
    RETRANSMISSION_HANDLER_MAP,
    MAX_RETRANSMISSION_ATTEMPTS,
    RETRANSMISSION_RETRY_DELAY_SECONDS,
    MAX_CONCURRENT_RETRANSMISSIONS,
    AIOHTTP_TOTAL_TIMEOUT_SECONDS,
    AIOHTTP_CONNECT_TIMEOUT_SECONDS,
)

# ... (otros imports como los tenías) ...
from app.services.base_retransmission_handler import BaseRetransmissionHandler
from app.services.seguridad_ciudadana_handler import SeguridadCiudadanaHandler
from app.services.pnp_handler import PnpHandler
from app.services.comsatel_handler import ComsatelHandler
from app.services.osinergmin_handler import OsinergminHandler
from app.services.sutran_handler import SutranHandler
from app.traccar_client import TraccarClient
from app.persistence.log_writer_db import log_writer_db_instance
from app.constants import (
    LOG_ORIGIN_RETRANSMISSION_GPS,
    LOG_LEVEL_DB_INFO,
    LOG_LEVEL_DB_ERROR,
)

logger = logging.getLogger(__name__)


class RetransmissionManager:
    def __init__(self, loop: asyncio.AbstractEventLoop):  # <--- ACEPTAR EL LOOP AQUÍ
        self.loop = loop  # <--- ALMACENAR EL LOOP
        self.async_position_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(
            maxsize=MAX_QUEUE_SIZE_BEFORE_WARN * 2
        )
        self.processed_position_ids: Deque[str] = deque(maxlen=MAX_PROCESSED_IDS_SIZE)
        self.stop_event: asyncio.Event = asyncio.Event()

        self.traccar_client: Optional[TraccarClient] = None
        self.retransmission_configs_db1: Dict[int, Dict[str, Any]] = {}

        self.worker_task: Optional[asyncio.Task] = None
        self._handler_instances: Dict[str, BaseRetransmissionHandler] = {}
        self._register_handlers()
        self._aiohttp_session: Optional[aiohttp.ClientSession] = None
        self._retransmission_semaphore = asyncio.Semaphore(
            MAX_CONCURRENT_RETRANSMISSIONS
        )

    # ... (_register_handlers, set_traccar_client, update_retransmission_configs_from_db1 sin cambios)
    def _register_handlers(self):
        available_handler_classes: Dict[str, Type[BaseRetransmissionHandler]] = {
            SeguridadCiudadanaHandler.HANDLER_ID: SeguridadCiudadanaHandler,
            PnpHandler.HANDLER_ID: PnpHandler,
            ComsatelHandler.HANDLER_ID: ComsatelHandler,
            OsinergminHandler.HANDLER_ID: OsinergminHandler,
            SutranHandler.HANDLER_ID: SutranHandler,
        }
        registered_handler_ids_from_config = set(RETRANSMISSION_HANDLER_MAP.values())
        for handler_id_in_code, HandlerClass in available_handler_classes.items():
            if handler_id_in_code not in registered_handler_ids_from_config:
                logger.warning(
                    f"Handler class {HandlerClass.__name__} (ID: {handler_id_in_code}) defined but its ID "
                    f"is not found as a value in RETRANSMISSION_HANDLER_MAP in config. "
                    "This handler will not be used unless config (RETRANSMISSION_URL_*) is updated."
                )
                continue
            try:
                self._handler_instances[handler_id_in_code] = HandlerClass()
                logger.info(
                    f"Registered handler: {handler_id_in_code} ({HandlerClass.__name__})"
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
        self.retransmission_configs_db1 = configs_from_db1
        logger.info(
            f"Retransmission configs (BD1) cache updated with {len(configs_from_db1)} entries."
        )

    def handle_traccar_websocket_message(self, ws_app, message_str: str):
        if not self.traccar_client:
            logger.error("Traccar client not set. Cannot process WebSocket message.")
            return
        try:
            data = json.loads(message_str)
            if "positions" in data:
                self._enqueue_traccar_positions_from_sync_thread(data["positions"])
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

    def _enqueue_traccar_positions_from_sync_thread(self, positions_data: list):
        if not self.traccar_client:
            return
        new_positions_added_to_async_queue = 0

        # Usar el loop almacenado en self.loop
        if not self.loop:  # Comprobación de seguridad
            logger.error(
                "Asyncio event loop not set in RetransmissionManager. Cannot queue items from sync thread."
            )
            return

        for pos_data in positions_data:
            pos_id_traccar = str(pos_data.get("id", "UNKNOWN_POS_ID"))
            if pos_id_traccar in self.processed_position_ids:
                continue

            device_id_traccar_orig = pos_data.get("deviceId")
            if device_id_traccar_orig is None:
                continue
            try:
                device_id_traccar_int = int(device_id_traccar_orig)
            except ValueError:
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

            try:
                self.loop.call_soon_threadsafe(
                    self.async_position_queue.put_nowait, queue_item
                )
                self.processed_position_ids.append(pos_id_traccar)
                new_positions_added_to_async_queue += 1
            except asyncio.QueueFull:
                logger.warning(
                    f"Async position queue is full! Discarding position {pos_id_traccar}."
                )
            except Exception as e_put:
                logger.error(f"Error putting item to async_position_queue: {e_put}")

        if new_positions_added_to_async_queue > 0:
            q_size = self.async_position_queue.qsize()
            logger.info(
                f"{new_positions_added_to_async_queue} new positions offered to async queue. Approx. size: {q_size}"
            )
            if q_size > MAX_QUEUE_SIZE_BEFORE_WARN:
                logger.warning(
                    f"WARNING: Async Retransmission queue size approaching limit: {q_size}"
                )

    # ... (_get_handler_instance_for_url, _retransmission_worker_loop_async,
    #      _process_item_async_with_retries, _execute_single_aiohttp_post,
    #      _log_to_db_from_async, start_retransmission_worker_async,
    #      stop_retransmission_worker_async SIN CAMBIOS respecto a la última versión que te di)
    # Pego estos métodos sin cambios para que el archivo esté completo, pero la única
    # modificación importante está arriba en __init__ y _enqueue_traccar_positions_from_sync_thread.

    def _get_handler_instance_for_url(
        self, target_host_url: str
    ) -> Optional[BaseRetransmissionHandler]:
        handler_id = RETRANSMISSION_HANDLER_MAP.get(target_host_url)
        if handler_id:
            handler_instance = self._handler_instances.get(handler_id)
            if not handler_instance:
                logger.warning(
                    f"Handler ID '{handler_id}' (URL '{target_host_url}') mapped in config, but no class registered for this ID."
                )
            return handler_instance
        return None

    async def _retransmission_worker_loop_async(self):
        logger.info("Async Retransmission worker loop started.")

        timeout_config = aiohttp.ClientTimeout(
            total=AIOHTTP_TOTAL_TIMEOUT_SECONDS, connect=AIOHTTP_CONNECT_TIMEOUT_SECONDS
        )
        connector = aiohttp.TCPConnector(
            limit_per_host=max(1, MAX_CONCURRENT_RETRANSMISSIONS // 5),
            limit=MAX_CONCURRENT_RETRANSMISSIONS,
            ssl=False,  # Ver nota sobre SSL y certifi en _execute_single_aiohttp_post
        )
        self._aiohttp_session = aiohttp.ClientSession(
            connector=connector, timeout=timeout_config
        )

        active_tasks: Set[asyncio.Task] = set()

        try:
            while not self.stop_event.is_set():
                try:
                    queue_item_to_process = await asyncio.wait_for(
                        self.async_position_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    logger.info("Async worker's queue.get() cancelled.")
                    break

                if queue_item_to_process:
                    await self._retransmission_semaphore.acquire()

                    task = asyncio.create_task(
                        self._process_item_async_with_retries(queue_item_to_process)
                    )
                    active_tasks.add(task)
                    task.add_done_callback(
                        lambda t: (
                            self._retransmission_semaphore.release(),
                            active_tasks.discard(
                                t
                            ),  # Remover la tarea del set cuando termina
                        )
                    )
                    self.async_position_queue.task_done()

            if active_tasks:  # Al salir del bucle por stop_event
                logger.info(
                    f"Shutdown: Waiting for {len(active_tasks)} active retransmission tasks to complete..."
                )
                # Dar un tiempo razonable para que las tareas en curso terminen
                # El timeout aquí debe ser suficiente para el AIOHTTP_TOTAL_TIMEOUT_SECONDS
                await asyncio.wait(
                    active_tasks, timeout=AIOHTTP_TOTAL_TIMEOUT_SECONDS + 10
                )
        except asyncio.CancelledError:
            logger.info(
                "Async Retransmission worker loop was cancelled during main execution."
            )
            if active_tasks:
                logger.info(f"Cancelling {len(active_tasks)} outstanding tasks...")
                for task_to_cancel in active_tasks:
                    task_to_cancel.cancel()
                await asyncio.gather(*active_tasks, return_exceptions=True)
        finally:
            if self._aiohttp_session and not self._aiohttp_session.closed:
                await self._aiohttp_session.close()
                self._aiohttp_session = None
            logger.info("Async Retransmission worker loop elegantly stopped.")

    async def _process_item_async_with_retries(self, queue_item: Dict[str, Any]):
        traccar_pos_data = queue_item["traccar_position"]
        device_info_data = queue_item["device_info"]
        retrans_cfg_bd1_data = queue_item["retransmission_config_bd1"]
        handler_to_use: BaseRetransmissionHandler = queue_item["handler_instance"]
        actual_target_url = queue_item["target_url_for_retransmission"]

        pos_id_display = traccar_pos_data.get("id", "N/A")
        dev_id_display = traccar_pos_data.get("deviceId", "N/A")
        placa_for_log = str(device_info_data.get("name", "N/A_dev_name"))

        transformed_payload_dict: Optional[Dict[str, Any]] = None
        json_send_str_for_log = "{}"

        try:
            transformed_payload_dict = handler_to_use.transform_payload(
                traccar_pos_data, device_info_data, retrans_cfg_bd1_data
            )
            placa_for_log = str(
                transformed_payload_dict.get(
                    "placa", transformed_payload_dict.get("plate", placa_for_log)
                )
            )
            json_send_str_for_log = json.dumps(transformed_payload_dict)
        except Exception as e_transform:
            logger.error(
                f"CRITICAL_TRANSFORM_FAIL: Handler {handler_to_use.get_handler_id()} for dev {dev_id_display} (placa {placa_for_log}), pos {pos_id_display}: {e_transform}. Item dropped.",
                exc_info=True,
            )
            self._log_to_db_from_async(
                f"Transform payload failed: {e_transform}",
                LOG_LEVEL_DB_ERROR,
                placa_for_log,
                actual_target_url,
                json.dumps(traccar_pos_data),
            )
            return

        if transformed_payload_dict is None:
            logger.error(
                f"PAYLOAD_IS_NONE: Handler {handler_to_use.get_handler_id()} for dev {dev_id_display} (placa {placa_for_log}), pos {pos_id_display} returned None. Dropped."
            )
            self._log_to_db_from_async(
                "Payload transform returned None",
                LOG_LEVEL_DB_ERROR,
                placa_for_log,
                actual_target_url,
                json_send_str_for_log,
            )
            return

        custom_headers = handler_to_use.get_custom_headers(retrans_cfg_bd1_data)
        http_headers: Dict[str, str] = {"Content-Type": "application/json"}
        if custom_headers:
            http_headers.update(custom_headers)

        final_success = False
        final_response_text = f"All {MAX_RETRANSMISSION_ATTEMPTS} attempts failed."
        final_status_code = None

        for attempt in range(1, MAX_RETRANSMISSION_ATTEMPTS + 1):
            # Chequeo de stop_event global (del RetransmissionManager)
            if self.stop_event.is_set():
                logger.info(
                    f"Global stop event detected during retransmission task for pos {pos_id_display}. Task aborting."
                )
                return
            # Chequeo de cancelación de la tarea asyncio actual
            if asyncio.current_task().cancelled():  # type: ignore
                logger.info(f"Retransmission task for pos {pos_id_display} cancelled.")
                return

            current_success, current_response_text, current_status_code = (
                await self._execute_single_aiohttp_post(
                    actual_target_url,
                    json_send_str_for_log,
                    http_headers,
                    pos_id_display,
                    placa_for_log,
                    attempt,
                )
            )

            final_response_text = current_response_text
            final_status_code = current_status_code

            if current_success:
                final_success = True
                break
            else:
                if attempt < MAX_RETRANSMISSION_ATTEMPTS:
                    if (
                        current_status_code
                        and 400 <= current_status_code < 500
                        and current_status_code not in [408, 429]
                    ):
                        logger.error(
                            f"Client error {current_status_code} on attempt {attempt} for pos {pos_id_display}. No further retries for this item."
                        )
                        break

                    if (
                        RETRANSMISSION_RETRY_DELAY_SECONDS > 0
                    ):  # Es 0, así que no hay delay
                        try:
                            await asyncio.sleep(RETRANSMISSION_RETRY_DELAY_SECONDS)
                        except asyncio.CancelledError:
                            logger.info(
                                f"Retry delay sleep cancelled for pos {pos_id_display}. Task aborting."
                            )
                            return
                else:
                    logger.error(
                        f"All {MAX_RETRANSMISSION_ATTEMPTS} async attempts failed for pos {pos_id_display} (placa {placa_for_log}). Final Status: {final_status_code}, Resp: {final_response_text[:100]}"
                    )

        self._log_to_db_from_async(
            final_response_text,
            LOG_LEVEL_DB_INFO if final_success else LOG_LEVEL_DB_ERROR,
            placa_for_log,
            actual_target_url,
            json_send_str_for_log,
        )

    async def _execute_single_aiohttp_post(
        self,
        target_url: str,
        json_payload_str: str,
        http_headers: Dict[str, str],
        pos_id_display: str,
        placa_for_log: str,
        attempt_num: int,
    ) -> Tuple[bool, str, Optional[int]]:
        success = False
        response_text = "No response"
        status_code = None

        if not self._aiohttp_session or self._aiohttp_session.closed:
            logger.error(
                f"AIOHTTP session not available for pos {pos_id_display} (attempt {attempt_num})."
            )
            return False, "AIOHTTP session closed", None

        # Para SSL con aiohttp, por defecto verifica. Para usar certifi:
        import ssl

        ssl_context_with_certifi = ssl.create_default_context(cafile=certifi.where())
        # Si el servidor tiene un cert autofirmado o inválido y QUIERES ignorar (NO RECOMENDADO EN PROD):
        # ssl_context_no_verify = ssl.SSLContext()
        # ssl_context_no_verify.check_hostname = False
        # ssl_context_no_verify.verify_mode = ssl.CERT_NONE

        try:
            logger.debug(
                f"AIOHTTP POST (intento {attempt_num}) a {target_url} para pos {pos_id_display} (placa {placa_for_log}) con headers {http_headers}"
            )
            async with self._aiohttp_session.post(
                target_url,
                data=json_payload_str,
                headers=http_headers,
                ssl=ssl_context_with_certifi,  # Usar certifi
                # ssl=False # Alternativa para DESHABILITAR verificación SSL (NO RECOMENDADO)
            ) as response:
                status_code = response.status
                try:
                    response_body = await response.text(
                        encoding="utf-8", errors="replace"
                    )
                    response_text = response_body[:1000]
                except Exception as e_read_body:
                    response_text = f"Could not read response body: {e_read_body}"
                    logger.warning(
                        f"Failed to read response body from {target_url} for pos {pos_id_display} (attempt {attempt_num}): {e_read_body}"
                    )

                if 200 <= status_code < 300:
                    success_msg_detail = (
                        f" (Body: {response_text[:60]})"
                        if response_text
                        and response_text
                        != "Se ha registrado de forma exitosa el punto gps"
                        else ""
                    )
                    # Tu lógica específica para el mensaje de éxito
                    if status_code in [200, 201] and target_url.startswith(
                        "https://seguridadciudadana.mininter.gob.pe"
                    ):
                        response_text = "Se ha registrado de forma exitosa el punto gps"
                    logger.info(
                        f"AIOHTTP POST Success (intento {attempt_num}): Status {status_code} para pos {pos_id_display}, placa {placa_for_log}"
                    )
                    success = True
                else:  # Error HTTP 4xx/5xx
                    logger.warning(
                        f"AIOHTTP POST Fail (intento {attempt_num}): Status {status_code} para pos {pos_id_display}, placa {placa_for_log}. Response: {response_text}"
                    )
                    success = False

        except aiohttp.ClientSSLError as ssl_err:
            response_text = f"AIOHTTP_SSLError: {ssl_err}"
            logger.error(
                f"AIOHTTP SSL error (Attempt {attempt_num}) for {target_url}, pos {pos_id_display}: {ssl_err}",
                exc_info=False,
            )
        except (
            aiohttp.ClientConnectorError
        ) as conn_err:  # Incluye errores de conexión como DNS, refused, etc.
            response_text = f"AIOHTTP_ConnectionError: {conn_err}"
            logger.warning(
                f"AIOHTTP Connection error (Attempt {attempt_num}) for {target_url}, pos {pos_id_display}: {conn_err}"
            )
        except asyncio.TimeoutError:  # Captura el timeout configurado en ClientSession
            response_text = "AIOHTTP_Timeout"
            logger.warning(
                f"AIOHTTP Timeout (Attempt {attempt_num}) for {target_url}, pos {pos_id_display}"
            )
        except aiohttp.ClientError as client_err:  # Otros errores genéricos de aiohttp
            response_text = f"AIOHTTP_ClientError: {client_err}"
            if hasattr(client_err, "status") and client_err.status:  # type: ignore
                status_code = client_err.status  # type: ignore
            logger.warning(
                f"AIOHTTP Client error (Attempt {attempt_num}) for {target_url}, pos {pos_id_display}: {client_err}"
            )
        except Exception as e:
            response_text = f"UnexpectedErrorInAiohttpAttempt: {e}"
            logger.error(
                f"Unexpected error (Attempt {attempt_num}) during aiohttp POST to {target_url}, pos {pos_id_display}: {e}",
                exc_info=True,
            )

        return success, response_text, status_code

    def _log_to_db_from_async(
        self, response: str, level: str, placa: str, host: str, json_send: str
    ):
        if log_writer_db_instance:
            try:
                log_writer_db_instance.add_log_entry_data(
                    response,
                    level,
                    placa,
                    host,
                    json_send,
                    LOG_ORIGIN_RETRANSMISSION_GPS,
                )
            except Exception as e:
                logger.error(
                    f"Failed to submit log to LogWriterDB from async context: {e}"
                )
        else:
            logger.warning(
                "LogWriterDB not available. Log not sent to DB from async context."
            )

    async def start_retransmission_worker_async(self):
        if self.worker_task and not self.worker_task.done():
            logger.info("Async Retransmission worker is already running.")
            return
        logger.info("Starting Async Retransmission worker...")
        self.stop_event.clear()
        self.worker_task = asyncio.create_task(self._retransmission_worker_loop_async())

    async def stop_retransmission_worker_async(self):
        logger.info("Signaling Async Retransmission worker to stop...")
        self.stop_event.set()

        if self.worker_task and not self.worker_task.done():
            logger.info(
                f"Waiting for Async Retransmission worker task to complete/cancel (timeout: {AIOHTTP_TOTAL_TIMEOUT_SECONDS + 15}s)..."
            )
            try:
                await asyncio.wait_for(
                    self.worker_task, timeout=AIOHTTP_TOTAL_TIMEOUT_SECONDS + 15
                )
                logger.info("Async Retransmission worker task finished normally.")
            except asyncio.TimeoutError:
                logger.warning(
                    "Async Retransmission worker task did not terminate in time after stop signal. Attempting forceful cancellation."
                )
                self.worker_task.cancel()
                try:
                    await self.worker_task
                except asyncio.CancelledError:
                    logger.info(
                        "Async Retransmission worker task successfully cancelled after timeout."
                    )
                except Exception as e_final_stop:
                    logger.error(
                        f"Exception while awaiting forcefully cancelled worker task: {e_final_stop}"
                    )
            except asyncio.CancelledError:
                logger.info(
                    "Async Retransmission worker task was cancelled by other means."
                )
            except Exception as e_stop_worker:
                logger.error(
                    f"Exception during async worker stop: {e_stop_worker}",
                    exc_info=True,
                )
        self.worker_task = None
