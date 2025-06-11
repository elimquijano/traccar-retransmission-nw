# app/services/retransmission_manager.py
import logging
import asyncio
import aiohttp  # Para peticiones HTTP asíncronas
import json
import certifi  # Para el path de CAs si se decide usar verificación SSL explícita
from collections import deque
from typing import Deque, Dict, Any, Optional, Tuple, Type, Set
import threading  # Para el hilo de refresco bajo demanda
import time  # Para el cooldown del refresco bajo demanda
import socket  # Para AF_INET

# Importamos configuraciones necesarias
from app.config import (
    MAX_QUEUE_SIZE_BEFORE_WARN,
    MAX_PROCESSED_IDS_SIZE,
    RETRANSMISSION_HANDLER_MAP,
    MAX_RETRANSMISSION_ATTEMPTS,
    RETRANSMISSION_RETRY_DELAY_SECONDS,
    MAX_CONCURRENT_RETRANSMISSIONS,
    AIOHTTP_TOTAL_TIMEOUT_SECONDS,
    AIOHTTP_CONNECT_TIMEOUT_SECONDS,
    # AIOHTTP_SESSION_RECREATE_INTERVAL_SECONDS, # Ya no se usan con sesión por tarea
    # AIOHTTP_SESSION_RECREATE_AFTER_REQUESTS,   # Ya no se usan con sesión por tarea
)

# Importamos los manejadores específicos y otras dependencias
from app.services.base_retransmission_handler import BaseRetransmissionHandler
from app.services.seguridad_ciudadana_handler import SeguridadCiudadanaHandler
from app.services.pnp_handler import PnpHandler
from app.services.comsatel_handler import ComsatelHandler
from app.services.osinergmin_handler import OsinergminHandler
from app.services.sutran_handler import SutranHandler
from app.traccar_client import TraccarClient
from app.persistence.db_config_loader import load_retransmission_configs_from_db1
from app.persistence.log_writer_db import log_writer_db_instance
from app.constants import (
    LOG_ORIGIN_RETRANSMISSION_GPS,
    LOG_LEVEL_DB_INFO,
    LOG_LEVEL_DB_ERROR,
)

logger = logging.getLogger(__name__)


class RetransmissionManager:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
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
        # _aiohttp_session ya no es un atributo de instancia del Manager, se crea por tarea
        self._retransmission_semaphore = asyncio.Semaphore(
            MAX_CONCURRENT_RETRANSMISSIONS
        )

        self._on_demand_refresh_thread: Optional[threading.Thread] = None
        self._on_demand_refresh_lock = threading.Lock()
        self._last_on_demand_refresh_trigger_time: float = 0
        self._ON_DEMAND_REFRESH_COOLDOWN_SECONDS: int = 60

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
                    f"La clase manejadora {HandlerClass.__name__} (ID: {handler_id_in_code}) está definida "
                    f"pero su ID no se encuentra como valor en RETRANSMISSION_HANDLER_MAP en la configuración. "
                    "Este manejador no se usará a menos que se actualice la configuración (RETRANSMISSION_URL_*)."
                )
                continue
            try:
                self._handler_instances[handler_id_in_code] = HandlerClass()
                logger.info(
                    f"Manejador registrado: {handler_id_in_code} ({HandlerClass.__name__})"
                )
            except Exception as e:
                logger.error(
                    f"Error al instanciar el manejador {handler_id_in_code} ({HandlerClass.__name__}): {e}",
                    exc_info=True,
                )
        if not self._handler_instances:
            logger.warning(
                "¡No se registraron manejadores de retransmisión correctamente desde la configuración!"
            )

    def set_traccar_client(self, client: TraccarClient):
        self.traccar_client = client

    def update_retransmission_configs_from_db1(
        self, configs_from_db1: Dict[int, Dict[str, Any]]
    ):
        self.retransmission_configs_db1 = configs_from_db1
        logger.info(
            f"Cache de configuraciones de retransmisión (BD1) actualizado con {len(configs_from_db1)} entradas."
        )

    def handle_traccar_websocket_message(self, ws_app, message_str: str):
        if not self.traccar_client:
            logger.error(
                "Cliente Traccar no establecido. No se puede procesar el mensaje WebSocket."
            )
            return
        try:
            data = json.loads(message_str)
            if "positions" in data:
                self._enqueue_traccar_positions_from_sync_thread(data["positions"])
            elif "devices" in data:
                self.traccar_client.update_device_cache_from_ws(data["devices"])
        except json.JSONDecodeError:
            if message_str.strip() and message_str.strip() != "{}":
                logger.warning(f"Mensaje no JSON desde Traccar WS: {message_str[:200]}")
        except Exception as e:
            logger.error(
                f"Error procesando mensaje WS de Traccar: {e} - Msg: {message_str[:200]}",
                exc_info=True,
            )

    def _trigger_on_demand_cache_refresh_non_blocking(self):
        if not self.traccar_client:
            logger.warning(
                "No se puede disparar refresco bajo demanda: Traccar client no establecido."
            )
            return
        current_time = time.time()
        if (
            current_time - self._last_on_demand_refresh_trigger_time
            < self._ON_DEMAND_REFRESH_COOLDOWN_SECONDS
        ):
            logger.debug(
                f"Cooldown de refresco bajo demanda activo ({self._ON_DEMAND_REFRESH_COOLDOWN_SECONDS}s). Saltando disparo de refresco."
            )
            return
        if self._on_demand_refresh_lock.acquire(blocking=False):
            try:
                if (
                    self._on_demand_refresh_thread
                    and self._on_demand_refresh_thread.is_alive()
                ):
                    logger.debug("Hilo de refresco bajo demanda ya está activo.")
                    self._on_demand_refresh_lock.release()
                    return
                self._last_on_demand_refresh_trigger_time = current_time
                logger.info(
                    "Disparando refresco de caché bajo demanda en un hilo separado..."
                )
                self._on_demand_refresh_thread = threading.Thread(
                    target=self._perform_cache_refresh_task_sync,
                    name="OnDemandCacheRefreshThread",
                    daemon=True,
                )
                self._on_demand_refresh_thread.start()
            except Exception as e_thread_start:
                logger.error(
                    f"Fallo al iniciar hilo de refresco de caché bajo demanda: {e_thread_start}"
                )
                self._on_demand_refresh_lock.release()
        else:
            logger.debug(
                "No se pudo adquirir lock para refresco bajo demanda; otro refresco probablemente en curso."
            )

    def _perform_cache_refresh_task_sync(self):
        if not self.traccar_client:
            logger.error(
                "[OnDemandRefreshThread] Cliente Traccar no disponible. Abortando refresco."
            )
            if self._on_demand_refresh_lock.locked():
                self._on_demand_refresh_lock.release()
            return
        logger.info(
            "[OnDemandRefreshThread] Iniciando proceso de refresco de caché bajo demanda..."
        )
        try:
            logger.info("[OnDemandRefreshThread] Obteniendo dispositivos de Traccar...")
            if self.traccar_client.fetch_devices():
                logger.info(
                    f"[OnDemandRefreshThread] {len(self.traccar_client.traccar_devices_cache)} Dispositivos de Traccar actualizados."
                )
            else:
                logger.error(
                    "[OnDemandRefreshThread] Fallo al actualizar dispositivos de Traccar."
                )
            logger.info(
                "[OnDemandRefreshThread] Cargando configuraciones de retransmisión desde BD..."
            )
            db_configs = load_retransmission_configs_from_db1()
            if db_configs is not None:
                self.update_retransmission_configs_from_db1(db_configs)
                logger.info(
                    f"[OnDemandRefreshThread] {len(db_configs)} Configuraciones de retransmisión actualizadas."
                )
            else:
                logger.error(
                    "[OnDemandRefreshThread] Fallo al actualizar configuraciones de retransmisión desde BD."
                )
            logger.info(
                "[OnDemandRefreshThread] Proceso de refresco de caché bajo demanda completado."
            )
        except Exception as e_refresh:
            logger.error(
                f"[OnDemandRefreshThread] Excepción durante tarea de refresco de caché: {e_refresh}",
                exc_info=True,
            )
        finally:
            if self._on_demand_refresh_lock.locked():
                self._on_demand_refresh_lock.release()
            logger.debug(
                "[OnDemandRefreshThread] Lock de refresco liberado (si fue adquirido por este hilo)."
            )

    def _enqueue_traccar_positions_from_sync_thread(self, positions_data: list):
        if not self.traccar_client:
            return
        new_positions_added = 0
        trigger_refresh = False
        if not self.loop:
            logger.error("Asyncio loop not set in Manager for enqueuing.")
            return
        for pos_data in positions_data:
            pos_id = str(pos_data.get("id", "?"))
            dev_id_orig = pos_data.get("deviceId")
            if pos_id in self.processed_position_ids or dev_id_orig is None:
                continue
            try:
                dev_id = int(dev_id_orig)
            except ValueError:
                continue
            dev_info = self.traccar_client.traccar_devices_cache.get(dev_id)
            retrans_cfg = self.retransmission_configs_db1.get(dev_id)
            if not dev_info or not retrans_cfg:
                logger.info(
                    f"Datos no encontrados para deviceId {dev_id} (pos {pos_id}). Disparando refresco."
                )
                trigger_refresh = True
                self.processed_position_ids.append(pos_id)
                logger.debug(
                    f"Descartando pos {pos_id} del dispositivo {dev_id} (caché faltante)."
                )
                continue
            host_url = retrans_cfg.get("host_url")
            handler = self._get_handler_instance_for_url(host_url) if host_url else None
            if not handler:
                continue
            item = {
                "traccar_position": pos_data,
                "device_info": dev_info,
                "retransmission_config_bd1": retrans_cfg,
                "handler_instance": handler,
                "target_url_for_retransmission": host_url,
            }
            try:
                self.loop.call_soon_threadsafe(
                    self.async_position_queue.put_nowait, item
                )
                self.processed_position_ids.append(pos_id)
                new_positions_added += 1
            except asyncio.QueueFull:
                logger.warning(f"Async queue full! Discarding pos {pos_id}.")
            except Exception as e:
                logger.error(f"Error putting to async_queue: {e}")
        if trigger_refresh:
            self._trigger_on_demand_cache_refresh_non_blocking()
        if new_positions_added > 0:
            q_size = self.async_position_queue.qsize()
            logger.info(
                f"{new_positions_added} nuevas posiciones a cola async. Tamaño: {q_size}"
            )
            if q_size > MAX_QUEUE_SIZE_BEFORE_WARN:
                logger.warning(f"WARNING: Cola async: {q_size}")

    def _get_handler_instance_for_url(
        self, target_host_url: str
    ) -> Optional[BaseRetransmissionHandler]:
        handler_id = RETRANSMISSION_HANDLER_MAP.get(target_host_url)
        if handler_id:
            handler_instance = self._handler_instances.get(handler_id)
            if not handler_instance:
                logger.warning(
                    f"Handler ID '{handler_id}' (URL '{target_host_url}') mapped but no class registered."
                )
            return handler_instance
        return None

    async def _retransmission_worker_loop_async(self):
        logger.info("Async Retransmission worker loop started.")
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
                    # La sesión y conector se crean DENTRO de _process_item_async_with_retries
                    task = asyncio.create_task(
                        self._process_item_async_with_retries(queue_item_to_process)
                    )
                    active_tasks.add(task)
                    task.add_done_callback(
                        lambda t: (
                            self._retransmission_semaphore.release(),
                            active_tasks.discard(t),
                        )
                    )
                    self.async_position_queue.task_done()

            if active_tasks:
                logger.info(
                    f"Shutdown: Waiting for {len(active_tasks)} active tasks to complete..."
                )
                await asyncio.wait(
                    active_tasks, timeout=AIOHTTP_TOTAL_TIMEOUT_SECONDS + 10
                )
        except asyncio.CancelledError:
            logger.info("Async Retransmission worker loop was cancelled.")
            if active_tasks:
                logger.info(f"Cancelling {len(active_tasks)} outstanding tasks...")
                for task_to_cancel in active_tasks:
                    task_to_cancel.cancel()
                await asyncio.gather(*active_tasks, return_exceptions=True)
        finally:
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
                f"CRITICAL_TRANSFORM_FAIL: Handler {handler_to_use.get_handler_id()} for dev {dev_id_display}, pos {pos_id_display}: {e_transform}. Item dropped.",
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
                f"PAYLOAD_IS_NONE: Handler {handler_to_use.get_handler_id()} for dev {dev_id_display}, pos {pos_id_display} returned None. Dropped."
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

        # --- MODIFICACIÓN: Crear Conector y Sesión aiohttp aquí, por tarea ---
        timeout_config_per_task = aiohttp.ClientTimeout(
            total=AIOHTTP_TOTAL_TIMEOUT_SECONDS, connect=AIOHTTP_CONNECT_TIMEOUT_SECONDS
        )
        # force_close=True en el conector es la medida más drástica para el FD error
        connector_per_task = aiohttp.TCPConnector(
            family=socket.AF_INET,  # Forzar IPv4
            enable_cleanup_closed=True,  # Buena práctica
            force_close=True,  # <--- CLAVE: No reutilizar conexiones TCP
            ssl=False,  # <--- SSL deshabilitado a nivel de conector,
            #      ya que session.post(ssl=False) lo anularía de todos modos.
            #      Si algún endpoint SÍ necesita SSL válido, esta estrategia
            #      global de ssl=False en el conector NO funcionará para ellos.
            #      En ese caso, ssl aquí debería ser un contexto SSL válido,
            #      y ssl=False se pasaría condicionalmente a session.post().
        )

        async with aiohttp.ClientSession(
            connector=connector_per_task, timeout=timeout_config_per_task
        ) as task_specific_session:
            for attempt in range(1, MAX_RETRANSMISSION_ATTEMPTS + 1):
                if self.stop_event.is_set():
                    logger.info(
                        f"Global stop event for pos {pos_id_display}. Task aborting."
                    )
                    return
                if asyncio.current_task().cancelled():
                    logger.info(f"Task for pos {pos_id_display} cancelled.")
                    return  # type: ignore

                logger.info(
                    f"Attempt {attempt}/{MAX_RETRANSMISSION_ATTEMPTS} for pos {pos_id_display} (placa {placa_for_log}) to {actual_target_url} via {handler_to_use.get_handler_id()}."
                )

                current_success, current_response_text, current_status_code = (
                    await self._execute_single_aiohttp_post(
                        task_specific_session,  # <--- Pasar la sesión de esta tarea
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
                    logger.info(
                        f"Attempt {attempt} SUCCESS for pos {pos_id_display}. Status: {current_status_code}."
                    )
                    final_success = True
                    break
                else:
                    logger.warning(
                        f"Attempt {attempt} FAIL for pos {pos_id_display}. Status: {current_status_code}, Resp: {current_response_text[:100]}."
                    )
                    if attempt < MAX_RETRANSMISSION_ATTEMPTS:
                        if (
                            current_status_code
                            and 400 <= current_status_code < 500
                            and current_status_code not in [408, 429]
                        ):
                            logger.error(
                                f"Client error {current_status_code} on attempt {attempt} for pos {pos_id_display}. No further retries."
                            )
                            break
                        if RETRANSMISSION_RETRY_DELAY_SECONDS > 0:  # Es 0
                            try:
                                await asyncio.sleep(RETRANSMISSION_RETRY_DELAY_SECONDS)
                            except asyncio.CancelledError:
                                logger.info(
                                    f"Retry delay sleep cancelled for pos {pos_id_display}."
                                )
                                return
                    else:
                        logger.error(
                            f"All {MAX_RETRANSMISSION_ATTEMPTS} attempts failed for pos {pos_id_display}. Final: {final_status_code}, {final_response_text[:100]}"
                        )

        # task_specific_session y connector_per_task se cierran al salir del 'async with'
        self._log_to_db_from_async(
            final_response_text,
            LOG_LEVEL_DB_INFO if final_success else LOG_LEVEL_DB_ERROR,
            placa_for_log,
            actual_target_url,
            json_send_str_for_log,
        )

    async def _execute_single_aiohttp_post(  # Aceptar `session`
        self,
        session: aiohttp.ClientSession,  # Usar la sesión pasada
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

        if session.closed:
            logger.error(
                f"AIOHTTP session passed is closed for pos {pos_id_display} (attempt {attempt_num})."
            )
            return False, "AIOHTTP session (passed) closed", None

        # El parámetro ssl en session.post() anula el del conector.
        # Si el conector tiene ssl=False, no es estrictamente necesario aquí.
        # Pero para ser explícito si esa es tu intención:
        ssl_param_for_this_post: Any = False

        try:
            logger.debug(
                f"AIOHTTP POST (Attempt {attempt_num}) to {target_url} for pos {pos_id_display} ..."
            )
            async with session.post(
                target_url,
                data=json_payload_str,
                headers=http_headers,
                ssl=ssl_param_for_this_post,  # Para este POST específico
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
                    f"Failed to read response body from {target_url} (attempt {attempt_num}): {e_read_body}"
                )

                if 200 <= status_code < 300:
                    if status_code in [200, 201] and target_url.startswith(
                        "https://seguridadciudadana.mininter.gob.pe"
                    ):
                        response_text = "Se ha registrado de forma exitosa el punto gps"
                    success = True
                else:
                    success = False
        except aiohttp.ClientSSLError as ssl_err:
            response_text = f"AIOHTTP_SSLError: {ssl_err}"
        except aiohttp.ClientConnectorError as conn_err:
            response_text = f"AIOHTTP_ConnectionError: {conn_err}"
        except asyncio.TimeoutError:
            response_text = "AIOHTTP_Timeout"
        except aiohttp.ClientError as client_err:
            response_text = f"AIOHTTP_ClientError: {client_err}"
            if hasattr(client_err, "status") and client_err.status:
                status_code = client_err.status  # type: ignore
        except Exception as e:
            response_text = f"UnexpectedErrorInAiohttpAttempt: {e}"
            logger.error(
                f"Unexpected error (Attempt {attempt_num}) during aiohttp POST to {target_url}, pos {pos_id_display}: {e}",
                exc_info=True,
            )

        # Los logs de éxito/fallo detallados del intento se hacen en _process_item_async_with_retries
        return success, response_text, status_code

    def _log_to_db_from_async(
        self, response: str, level: str, placa: str, host: str, json_send: str
    ):  # Sin cambios
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
                    f"Error al enviar log a LogWriterDB desde contexto asíncrono: {e}"
                )
        else:
            logger.warning(
                "LogWriterDB no disponible. Log no enviado a BD desde contexto asíncrono."
            )

    async def start_retransmission_worker_async(self):  # Sin cambios
        if self.worker_task and not self.worker_task.done():
            logger.info("El worker de retransmisión asíncrono ya está ejecutándose.")
            return
        logger.info("Iniciando worker de retransmisión asíncrono...")
        self.stop_event.clear()
        self.worker_task = asyncio.create_task(self._retransmission_worker_loop_async())

    async def stop_retransmission_worker_async(self):  # Sin cambios
        logger.info(
            "Señalizando al worker de retransmisión asíncrono para detenerse..."
        )
        self.stop_event.set()
        if self.worker_task and not self.worker_task.done():
            logger.info(
                f"Esperando a que la tarea del worker de retransmisión asíncrono se complete/cancele (timeout: {AIOHTTP_TOTAL_TIMEOUT_SECONDS + 15}s)..."
            )
            try:
                await asyncio.wait_for(
                    self.worker_task, timeout=AIOHTTP_TOTAL_TIMEOUT_SECONDS + 15
                )
                logger.info(
                    "Tarea del worker de retransmisión asíncrono finalizada normalmente."
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "La tarea del worker de retransmisión asíncrono no terminó a tiempo. Cancelación forzada."
                )
                self.worker_task.cancel()
                try:
                    await self.worker_task
                except asyncio.CancelledError:
                    logger.info(
                        "Tarea del worker de retransmisión asíncrono cancelada exitosamente después del timeout."
                    )
                except Exception as e_final_stop:
                    logger.error(
                        f"Excepción mientras se esperaba la tarea del worker cancelada forzadamente: {e_final_stop}"
                    )
            except asyncio.CancelledError:
                logger.info(
                    "La tarea del worker de retransmisión asíncrono fue cancelada por otros medios."
                )
            except Exception as e_stop_worker:
                logger.error(
                    f"Excepción durante la parada del worker asíncrono: {e_stop_worker}",
                    exc_info=True,
                )
        self.worker_task = None
