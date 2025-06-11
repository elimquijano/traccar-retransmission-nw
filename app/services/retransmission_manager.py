import logging
import asyncio
import aiohttp
import json
import threading
import time
from collections import deque
from typing import Deque, Dict, Any, Optional, Tuple, Type, Set
from app.persistence.db_config_loader import load_retransmission_configs_from_db1
import socket

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
    AIOHTTP_SESSION_RECREATE_INTERVAL_SECONDS,
    AIOHTTP_SESSION_RECREATE_AFTER_REQUESTS,
)

# Importamos los manejadores específicos y otras dependencias
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

# Configuramos el logger para esta clase
logger = logging.getLogger(__name__)


class RetransmissionManager:
    """
    Gestor de retransmisión de datos GPS a diferentes sistemas externos.
    Esta clase maneja la cola de posiciones GPS, las transforma según el destino
    y las envía a los sistemas correspondientes usando HTTP asíncrono.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop):
        """
        Inicializa el RetransmissionManager con el loop de eventos asíncrono.

        Args:
            loop: El loop de eventos asíncrono que se usará para las operaciones asíncronas.
        """
        self.loop = loop  # Guardamos el loop de eventos
        # Cola asíncrona para las posiciones GPS (con tamaño máximo definido)
        self.async_position_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(
            maxsize=MAX_QUEUE_SIZE_BEFORE_WARN * 2
        )
        # Cola de IDs de posiciones procesadas (para evitar duplicados)
        self.processed_position_ids: Deque[str] = deque(maxlen=MAX_PROCESSED_IDS_SIZE)
        # Evento para controlar la parada del worker
        self.stop_event: asyncio.Event = asyncio.Event()

        # Cliente Traccar (se establecerá más tarde)
        self.traccar_client: Optional[TraccarClient] = None
        # Configuraciones de retransmisión cargadas desde BD1
        self.retransmission_configs_db1: Dict[int, Dict[str, Any]] = {}

        # Tarea del worker asíncrono
        self.worker_task: Optional[asyncio.Task] = None
        # Instancias de los manejadores de retransmisión
        self._handler_instances: Dict[str, BaseRetransmissionHandler] = {}
        # Registramos los manejadores disponibles
        self._register_handlers()
        # Sesión aiohttp para las solicitudes HTTP
        self._aiohttp_session: Optional[aiohttp.ClientSession] = None
        # Semáforo para limitar las retransmisiones concurrentes
        self._retransmission_semaphore = asyncio.Semaphore(
            MAX_CONCURRENT_RETRANSMISSIONS
        )
        self._on_demand_refresh_thread: Optional[threading.Thread] = None
        self._on_demand_refresh_lock = threading.Lock()
        self._last_on_demand_refresh_trigger_time: float = 0
        self._ON_DEMAND_REFRESH_COOLDOWN_SECONDS: int = 60  # Segundos de cooldown
        self._aiohttp_session_creation_time: float = 0
        self._aiohttp_session_request_count: int = 0
        self._aiohttp_session_lock = asyncio.Lock()

    def _register_handlers(self):
        """
        Registra las clases manejadoras disponibles según la configuración.
        Compara los manejadores definidos en el código con los configurados en RETRANSMISSION_HANDLER_MAP.
        """
        # Diccionario de clases manejadoras disponibles
        available_handler_classes: Dict[str, Type[BaseRetransmissionHandler]] = {
            SeguridadCiudadanaHandler.HANDLER_ID: SeguridadCiudadanaHandler,
            PnpHandler.HANDLER_ID: PnpHandler,
            ComsatelHandler.HANDLER_ID: ComsatelHandler,
            OsinergminHandler.HANDLER_ID: OsinergminHandler,
            SutranHandler.HANDLER_ID: SutranHandler,
        }

        # Obtenemos los IDs de manejadores configurados
        registered_handler_ids_from_config = set(RETRANSMISSION_HANDLER_MAP.values())

        # Registramos cada manejador que esté configurado
        for handler_id_in_code, HandlerClass in available_handler_classes.items():
            if handler_id_in_code not in registered_handler_ids_from_config:
                logger.warning(
                    f"La clase manejadora {HandlerClass.__name__} (ID: {handler_id_in_code}) está definida "
                    f"pero su ID no se encuentra como valor en RETRANSMISSION_HANDLER_MAP en la configuración. "
                    "Este manejador no se usará a menos que se actualice la configuración (RETRANSMISSION_URL_*)."
                )
                continue

            try:
                # Creamos una instancia del manejador y lo registramos
                self._handler_instances[handler_id_in_code] = HandlerClass()
                logger.info(
                    f"Manejador registrado: {handler_id_in_code} ({HandlerClass.__name__})"
                )
            except Exception as e:
                logger.error(
                    f"Error al instanciar el manejador {handler_id_in_code} ({HandlerClass.__name__}): {e}",
                    exc_info=True,
                )

        # Advertencia si no se registró ningún manejador
        if not self._handler_instances:
            logger.warning(
                "¡No se registraron manejadores de retransmisión correctamente desde la configuración!"
            )

    def set_traccar_client(self, client: TraccarClient):
        """
        Establece el cliente Traccar para este gestor.

        Args:
            client: Instancia de TraccarClient para obtener datos de dispositivos.
        """
        self.traccar_client = client

    def update_retransmission_configs_from_db1(
        self, configs_from_db1: Dict[int, Dict[str, Any]]
    ):
        """
        Actualiza las configuraciones de retransmisión desde la base de datos BD1.

        Args:
            configs_from_db1: Diccionario con las configuraciones de retransmisión.
        """
        self.retransmission_configs_db1 = configs_from_db1
        logger.info(
            f"Cache de configuraciones de retransmisión (BD1) actualizado con {len(configs_from_db1)} entradas."
        )

    def handle_traccar_websocket_message(self, ws_app, message_str: str):
        """
        Maneja los mensajes recibidos del WebSocket de Traccar.

        Args:
            ws_app: Aplicación WebSocket (no utilizada directamente).
            message_str: Mensaje recibido como string.
        """
        if not self.traccar_client:
            logger.error(
                "Cliente Traccar no establecido. No se puede procesar el mensaje WebSocket."
            )
            return

        try:
            # Parseamos el mensaje JSON
            data = json.loads(message_str)

            # Procesamos posiciones o actualizamos información de dispositivos
            if "positions" in data:
                self._enqueue_traccar_positions_from_sync_thread(data["positions"])
            elif "devices" in data:
                self.traccar_client.update_device_cache_from_ws(data["devices"])

        except json.JSONDecodeError:
            # Ignoramos mensajes vacíos o no JSON
            if message_str.strip() and message_str.strip() != "{}":
                logger.warning(f"Mensaje no JSON desde Traccar WS: {message_str[:200]}")
        except Exception as e:
            logger.error(
                f"Error procesando mensaje WS de Traccar: {e} - Msg: {message_str[:200]}",
                exc_info=True,
            )

    def _trigger_on_demand_cache_refresh_non_blocking(self):
        """
        Intenta disparar el refresco de cachés en un hilo separado.
        Usa un lock para asegurar que solo un hilo de refresco se ejecute a la vez.
        Usa un cooldown para evitar disparos demasiado frecuentes.
        """
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
        """Tarea síncrona ejecutada en un hilo para refrescar los cachés."""
        if not self.traccar_client:
            logger.error(
                "[OnDemandRefreshThread] Cliente Traccar no disponible. Abortando refresco."
            )
            if (
                self._on_demand_refresh_lock.locked()
            ):  # Asegurarse de liberar si se adquirió
                self._on_demand_refresh_lock.release()
            return

        logger.info(
            "[OnDemandRefreshThread] Iniciando proceso de refresco de caché bajo demanda..."
        )
        try:
            logger.info("[OnDemandRefreshThread] Obteniendo dispositivos de Traccar...")
            if (
                self.traccar_client.fetch_devices()
            ):  # Actualiza el cache en TraccarClient
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
                self.update_retransmission_configs_from_db1(
                    db_configs
                )  # Actualiza el cache aquí
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
            if (
                self._on_demand_refresh_lock.locked()
            ):  # Solo liberar si este hilo lo posee
                self._on_demand_refresh_lock.release()
            logger.debug(
                "[OnDemandRefreshThread] Lock de refresco liberado (si fue adquirido por este hilo)."
            )

    def _enqueue_traccar_positions_from_sync_thread(self, positions_data: list):
        """
        Encola posiciones de Traccar desde un hilo síncrono.

        Args:
            positions_data: Lista de datos de posiciones recibidas de Traccar.
        """
        if not self.traccar_client:
            return

        new_positions_added_to_async_queue = 0
        trigger_refresh_needed_for_batch = False

        # Verificamos que el loop de eventos esté disponible
        if not self.loop:
            logger.error(
                "Loop de eventos asíncrono no establecido en RetransmissionManager. No se pueden encolar elementos desde hilo síncrono."
            )
            return

        # Procesamos cada posición
        for pos_data in positions_data:
            # Obtenemos el ID de la posición
            pos_id_traccar = str(pos_data.get("id", "UNKNOWN_POS_ID"))

            # Saltamos posiciones ya procesadas
            if pos_id_traccar in self.processed_position_ids:
                continue

            # Obtenemos el ID del dispositivo
            device_id_traccar_orig = pos_data.get("deviceId")
            if device_id_traccar_orig is None:
                continue

            try:
                device_id_traccar_int = int(device_id_traccar_orig)
            except ValueError:
                continue

            # Obtenemos información del dispositivo y su configuración
            device_info = self.traccar_client.traccar_devices_cache.get(
                device_id_traccar_int
            )
            retrans_config_for_device_bd1 = self.retransmission_configs_db1.get(
                device_id_traccar_int
            )

            if not device_info or not retrans_config_for_device_bd1:
                logger.info(
                    f"Datos no encontrados para deviceId {device_id_traccar_int} (pos {pos_id_traccar}). "
                    f"Cache de dispositivo: {'OK' if device_info else 'FALTA'}, "
                    f"Cache de config: {'OK' if retrans_config_for_device_bd1 else 'FALTA'}. "
                    "Se marcará para refresco de caché."
                )
                trigger_refresh_needed_for_batch = True
                # Descartar esta posición actual. Las siguientes se beneficiarán del refresco.
                self.processed_position_ids.append(
                    pos_id_traccar
                )  # Marcar como "procesada" (descartada)
                logger.debug(
                    f"Descartando posición {pos_id_traccar} del dispositivo {device_id_traccar_int} que no está en caché."
                )
                continue  # No encolar esta posición específica

            # Obtenemos la URL de destino
            target_host_url_from_db = retrans_config_for_device_bd1.get("host_url")
            if not target_host_url_from_db:
                continue

            # Obtenemos el manejador adecuado para esta URL
            handler_instance_to_use = self._get_handler_instance_for_url(
                target_host_url_from_db
            )
            if not handler_instance_to_use:
                continue

            # Creamos el elemento para la cola
            queue_item = {
                "traccar_position": pos_data,
                "device_info": device_info,
                "retransmission_config_bd1": retrans_config_for_device_bd1,
                "handler_instance": handler_instance_to_use,
                "target_url_for_retransmission": target_host_url_from_db,
            }

            try:
                # Añadimos el elemento a la cola de manera segura desde otro hilo
                self.loop.call_soon_threadsafe(
                    self.async_position_queue.put_nowait, queue_item
                )
                self.processed_position_ids.append(pos_id_traccar)
                new_positions_added_to_async_queue += 1
            except asyncio.QueueFull:
                logger.warning(
                    f"¡Cola de posiciones asíncrona está llena! Descartando posición {pos_id_traccar}."
                )
            except Exception as e_put:
                logger.error(
                    f"Error al añadir elemento a async_position_queue: {e_put}"
                )

        if (
            trigger_refresh_needed_for_batch
        ):  # Si se detectó algún dispositivo desconocido en el lote
            self._trigger_on_demand_cache_refresh_non_blocking()

        # Registramos información sobre las nuevas posiciones encoladas
        if new_positions_added_to_async_queue > 0:
            q_size = self.async_position_queue.qsize()
            logger.info(
                f"{new_positions_added_to_async_queue} NUEVAS POSICIONES AÑADIDAS A LA COLA DE RETRANSMISIÓN ASÍNCRONA. Tamaño aproximado: {q_size}"
            )
            if q_size > MAX_QUEUE_SIZE_BEFORE_WARN:
                logger.warning(
                    f"ADVERTENCIA: Tamaño de la cola de retransmisión asíncrona acercándose al límite: {q_size}"
                )

    async def _get_or_create_aiohttp_session(self) -> aiohttp.ClientSession:
        """
        Obtiene la sesión aiohttp existente o crea/recrea una nueva si es necesario.
        Este método es una corutina y debe ser llamado con await.
        Usa un asyncio.Lock para asegurar que la creación/recreación sea atómica.
        """
        async with self._aiohttp_session_lock:  # Lock para proteger la creación de sesión
            current_time = time.time()

            # Condiciones para recrear la sesión
            session_expired_by_time = (
                AIOHTTP_SESSION_RECREATE_INTERVAL_SECONDS > 0
                and self._aiohttp_session_creation_time
                > 0  # Asegurar que ya se creó una vez
                and (current_time - self._aiohttp_session_creation_time)
                > AIOHTTP_SESSION_RECREATE_INTERVAL_SECONDS
            )
            session_expired_by_count = (
                AIOHTTP_SESSION_RECREATE_AFTER_REQUESTS > 0
                and self._aiohttp_session_request_count
                >= AIOHTTP_SESSION_RECREATE_AFTER_REQUESTS
            )

            if (
                not self._aiohttp_session
                or self._aiohttp_session.closed
                or session_expired_by_time
                or session_expired_by_count
            ):
                if self._aiohttp_session and not self._aiohttp_session.closed:
                    logger.info(
                        f"Recreando ClientSession aiohttp. Razon: "
                        f"{'tiempo' if session_expired_by_time else ''}"
                        f"{' y ' if session_expired_by_time and session_expired_by_count else ''}"
                        f"{'conteo' if session_expired_by_count else ''}"
                        f"{' o primera creación/cierre' if not (session_expired_by_time or session_expired_by_count) else ''}."
                        f" Tiempo activo: {current_time - self._aiohttp_session_creation_time:.0f}s, "
                        f"Peticiones: {self._aiohttp_session_request_count}."
                    )
                    await self._aiohttp_session.close()

                logger.info("Creando nueva ClientSession aiohttp...")
                timeout_config = aiohttp.ClientTimeout(
                    total=AIOHTTP_TOTAL_TIMEOUT_SECONDS,
                    connect=AIOHTTP_CONNECT_TIMEOUT_SECONDS,
                )

                # No se pasa contexto SSL al conector si se va a usar ssl=False en cada post
                # o si se quiere usar la verificación por defecto del sistema en el conector.
                # Si se quisiera usar certifi globalmente:
                # import ssl
                # ssl_context = ssl.create_default_context(cafile=certifi.where())
                # connector_ssl_param = ssl_context
                connector_ssl_param = (
                    None  # Usará el default del sistema si ssl= no se pasa a post()
                )

                connector = aiohttp.TCPConnector(
                    limit_per_host=max(1, MAX_CONCURRENT_RETRANSMISSIONS // 5),
                    limit=MAX_CONCURRENT_RETRANSMISSIONS,
                    family=socket.AF_INET,  # <--- Forzar IPv4
                    ssl=connector_ssl_param,  # <--- Usar None o un contexto SSL
                    enable_cleanup_closed=True,  # <--- Intentar limpieza más agresiva
                    force_close=True
                )
                self._aiohttp_session = aiohttp.ClientSession(
                    connector=connector, timeout=timeout_config
                )
                self._aiohttp_session_creation_time = current_time
                self._aiohttp_session_request_count = 0

            if not self._aiohttp_session:  # Fallback por si algo muy raro pasa
                raise RuntimeError("No se pudo obtener o crear la sesión aiohttp.")
            return self._aiohttp_session

    def _get_handler_instance_for_url(
        self, target_host_url: str
    ) -> Optional[BaseRetransmissionHandler]:
        """
        Obtiene la instancia del manejador adecuado para una URL de destino.

        Args:
            target_host_url: URL de destino para la retransmisión.

        Returns:
            Optional[BaseRetransmissionHandler]: Instancia del manejador correspondiente o None si no se encuentra.
        """
        # Obtenemos el ID del manejador desde la configuración
        handler_id = RETRANSMISSION_HANDLER_MAP.get(target_host_url)

        if handler_id:
            # Buscamos la instancia del manejador registrado
            handler_instance = self._handler_instances.get(handler_id)
            if not handler_instance:
                logger.warning(
                    f"ID de manejador '{handler_id}' (URL '{target_host_url}') mapeado en configuración, pero no hay clase registrada para este ID."
                )
            return handler_instance

        return None

    async def _retransmission_worker_loop_async(self):
        """
        Bucle principal del worker asíncrono que procesa la cola de retransmisión.
        """
        logger.info("Bucle del worker de retransmisión asíncrono iniciado.")

        """ # Configuramos timeouts para las solicitudes HTTP
        timeout_config = aiohttp.ClientTimeout(
            total=AIOHTTP_TOTAL_TIMEOUT_SECONDS, connect=AIOHTTP_CONNECT_TIMEOUT_SECONDS
        )

        # Configuramos el conector TCP para aiohttp
        connector = aiohttp.TCPConnector(
            limit_per_host=max(1, MAX_CONCURRENT_RETRANSMISSIONS // 5),
            limit=MAX_CONCURRENT_RETRANSMISSIONS,
            ssl=False,  # Ver nota sobre SSL y certifi en _execute_single_aiohttp_post
        )

        # Creamos la sesión aiohttp
        self._aiohttp_session = aiohttp.ClientSession(
            connector=connector, timeout=timeout_config
        ) """

        # Conjunto para llevar seguimiento de las tareas activas
        active_tasks: Set[asyncio.Task] = set()

        try:
            # Bucle principal del worker
            while not self.stop_event.is_set():
                try:
                    # Esperamos un elemento de la cola con timeout
                    queue_item_to_process = await asyncio.wait_for(
                        self.async_position_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    logger.info(
                        "La operación queue.get() del worker asíncrono fue cancelada."
                    )
                    break

                if queue_item_to_process:
                    # Adquirimos el semáforo para limitar las retransmisiones concurrentes
                    await self._retransmission_semaphore.acquire()

                    try:
                        current_session = await self._get_or_create_aiohttp_session()
                    except RuntimeError as e_session:
                        logger.error(
                            f"Fallo crítico al obtener/crear sesión aiohttp: {e_session}. Reintentando en el próximo ciclo."
                        )
                        self._retransmission_semaphore.release()  # Liberar semáforo si no se puede crear tarea
                        await asyncio.sleep(
                            1
                        )  # Pequeña pausa antes de reintentar el bucle principal
                        continue  # Saltar este ítem y reintentar el bucle para obtener sesión

                    # Creamos una tarea para procesar el elemento
                    task = asyncio.create_task(
                        self._process_item_async_with_retries(
                            queue_item_to_process, current_session
                        )
                    )
                    active_tasks.add(task)

                    # Configuramos un callback para cuando la tarea termine
                    task.add_done_callback(
                        lambda t: (
                            self._retransmission_semaphore.release(),
                            active_tasks.discard(
                                t
                            ),  # Removemos la tarea del conjunto cuando termina
                        )
                    )
                    self.async_position_queue.task_done()

            # Al salir del bucle por stop_event, esperamos a que las tareas activas terminen
            if active_tasks:
                logger.info(
                    f"Apagado: Esperando a que {len(active_tasks)} tareas activas de retransmisión se completen..."
                )
                # Esperamos con un timeout razonable
                await asyncio.wait(
                    active_tasks, timeout=AIOHTTP_TOTAL_TIMEOUT_SECONDS + 10
                )

        except asyncio.CancelledError:
            logger.info(
                "El bucle del worker de retransmisión asíncrono fue cancelado durante la ejecución principal."
            )
            if active_tasks:
                logger.info(f"Cancelando {len(active_tasks)} tareas pendientes...")
                for task_to_cancel in active_tasks:
                    task_to_cancel.cancel()
                await asyncio.gather(*active_tasks, return_exceptions=True)

        finally:
            # Cerramos la sesión aiohttp
            if self._aiohttp_session and not self._aiohttp_session.closed:
                logger.info("Cerrando la sesión aiohttp final al detener el worker.")
                await self._aiohttp_session.close()
                self._aiohttp_session = None
            logger.info(
                "Bucle del worker de retransmisión asíncrono detenido elegantemente."
            )

    async def _process_item_async_with_retries(
        self, queue_item: Dict[str, Any], session: aiohttp.ClientSession
    ):
        """
        Procesa un elemento de la cola con reintentos en caso de fallo.

        Args:
            queue_item: Elemento de la cola que contiene los datos de la posición y la configuración.
        """
        # Extraemos los datos del elemento de la cola
        traccar_pos_data = queue_item["traccar_position"]
        device_info_data = queue_item["device_info"]
        retrans_cfg_bd1_data = queue_item["retransmission_config_bd1"]
        handler_to_use: BaseRetransmissionHandler = queue_item["handler_instance"]
        actual_target_url = queue_item["target_url_for_retransmission"]

        # Datos para logging
        pos_id_display = traccar_pos_data.get("id", "N/A")
        dev_id_display = traccar_pos_data.get("deviceId", "N/A")
        placa_for_log = str(device_info_data.get("name", "N/A_dev_name"))

        # Variables para el payload transformado
        transformed_payload_dict: Optional[Dict[str, Any]] = None
        json_send_str_for_log = "{}"

        try:
            # Transformamos el payload usando el manejador adecuado
            transformed_payload_dict = handler_to_use.transform_payload(
                traccar_pos_data, device_info_data, retrans_cfg_bd1_data
            )

            # Obtenemos la placa del payload transformado
            placa_for_log = str(
                transformed_payload_dict.get(
                    "placa", transformed_payload_dict.get("plate", placa_for_log)
                )
            )
            json_send_str_for_log = json.dumps(transformed_payload_dict)

        except Exception as e_transform:
            # Manejo de errores en la transformación del payload
            logger.error(
                f"FALLO_CRÍTICO_EN_TRANSFORMACIÓN: Manejador {handler_to_use.get_handler_id()} para dispositivo {dev_id_display} (placa {placa_for_log}), posición {pos_id_display}: {e_transform}. Elemento descartado.",
                exc_info=True,
            )
            self._log_to_db_from_async(
                f"Fallo en transformación del payload: {e_transform}",
                LOG_LEVEL_DB_ERROR,
                placa_for_log,
                actual_target_url,
                json.dumps(traccar_pos_data),
            )
            return

        # Verificamos que el payload no sea None
        if transformed_payload_dict is None:
            logger.error(
                f"PAYLOAD_ES_NONE: Manejador {handler_to_use.get_handler_id()} para dispositivo {dev_id_display} (placa {placa_for_log}), posición {pos_id_display} devolvió None. Descartado."
            )
            self._log_to_db_from_async(
                "La transformación del payload devolvió None",
                LOG_LEVEL_DB_ERROR,
                placa_for_log,
                actual_target_url,
                json_send_str_for_log,
            )
            return

        # Obtenemos headers personalizados si los hay
        custom_headers = handler_to_use.get_custom_headers(retrans_cfg_bd1_data)
        http_headers: Dict[str, str] = {"Content-Type": "application/json"}
        if custom_headers:
            http_headers.update(custom_headers)

        # Variables para el resultado final
        final_success = False
        final_response_text = (
            f"Todos los {MAX_RETRANSMISSION_ATTEMPTS} intentos fallaron."
        )
        final_status_code = None

        # Intentamos la retransmisión varias veces
        for attempt in range(1, MAX_RETRANSMISSION_ATTEMPTS + 1):
            # Verificamos si se ha solicitado parada
            if self.stop_event.is_set():
                logger.info(
                    f"Evento de parada global detectado durante tarea de retransmisión para posición {pos_id_display}. Tarea abortando."
                )
                return

            # Verificamos si la tarea actual fue cancelada
            if asyncio.current_task().cancelled():  # type: ignore
                logger.info(
                    f"Tarea de retransmisión para posición {pos_id_display} cancelada."
                )
                return

            # Ejecutamos un intento de retransmisión
            current_success, current_response_text, current_status_code = (
                await self._execute_single_aiohttp_post(
                    session,
                    actual_target_url,
                    json_send_str_for_log,
                    http_headers,
                    pos_id_display,
                    placa_for_log,
                    attempt,
                )
            )

            # Guardamos el resultado del intento
            final_response_text = current_response_text
            final_status_code = current_status_code

            if current_success:
                final_success = True
                break
            else:
                # Lógica para decidir si reintentar
                if attempt < MAX_RETRANSMISSION_ATTEMPTS:
                    # No reintentamos para ciertos códigos de error 4xx
                    if (
                        current_status_code
                        and 400 <= current_status_code < 500
                        and current_status_code not in [408, 429]
                    ):
                        logger.error(
                            f"Error de cliente {current_status_code} en intento {attempt} para posición {pos_id_display}. No más reintentos para este elemento."
                        )
                        break

                    # Esperamos antes de reintentar si está configurado
                    if RETRANSMISSION_RETRY_DELAY_SECONDS > 0:
                        try:
                            await asyncio.sleep(RETRANSMISSION_RETRY_DELAY_SECONDS)
                        except asyncio.CancelledError:
                            logger.info(
                                f"Espera para reintento cancelada para posición {pos_id_display}. Tarea abortando."
                            )
                            return
                else:
                    logger.error(
                        f"Todos los {MAX_RETRANSMISSION_ATTEMPTS} intentos asíncronos fallaron para posición {pos_id_display} (placa {placa_for_log}). Estado final: {final_status_code}, Respuesta: {final_response_text[:100]}"
                    )

        async with self._aiohttp_session_lock:  # Proteger acceso a _aiohttp_session_request_count
            self._aiohttp_session_request_count += 1

        # Registramos el resultado final en la base de datos
        self._log_to_db_from_async(
            final_response_text,
            LOG_LEVEL_DB_INFO if final_success else LOG_LEVEL_DB_ERROR,
            placa_for_log,
            actual_target_url,
            json_send_str_for_log,
        )

    async def _execute_single_aiohttp_post(
        self,
        session: aiohttp.ClientSession,
        target_url: str,
        json_payload_str: str,
        http_headers: Dict[str, str],
        pos_id_display: str,
        placa_for_log: str,
        attempt_num: int,
    ) -> Tuple[bool, str, Optional[int]]:
        """
        Ejecuta una solicitud POST HTTP individual usando aiohttp.

        Args:
            target_url: URL de destino.
            json_payload_str: Payload en formato JSON como string.
            http_headers: Headers HTTP para la solicitud.
            pos_id_display: ID de la posición para logging.
            placa_for_log: Placa del vehículo para logging.
            attempt_num: Número de intento actual.

        Returns:
            Tuple[bool, str, Optional[int]]: Tupla con (éxito, texto de respuesta, código de estado).
        """
        success = False
        response_text = "Sin respuesta"
        status_code = None

        # Verificamos que la sesión aiohttp esté disponible
        if session.closed:
            logger.error(
                f"AIOHTTP session passed to _execute_single_aiohttp_post is closed for pos {pos_id_display} (attempt {attempt_num})."
            )
            return False, "AIOHTTP session (passed) closed", None

        try:
            logger.debug(
                f"POST AIOHTTP (intento {attempt_num}) a {target_url} para posición {pos_id_display} (placa {placa_for_log}) con headers {http_headers}"
            )

            # Ejecutamos la solicitud POST
            async with session.post(
                target_url, data=json_payload_str, headers=http_headers, ssl=False
            ) as response:
                status_code = response.status

                try:
                    # Leemos el cuerpo de la respuesta
                    response_body = await response.text(
                        encoding="utf-8", errors="replace"
                    )
                    response_text = response_body[:1000]
                except Exception as e_read_body:
                    response_text = (
                        f"No se pudo leer el cuerpo de la respuesta: {e_read_body}"
                    )
                    logger.warning(
                        f"Error al leer el cuerpo de la respuesta desde {target_url} para posición {pos_id_display} (intento {attempt_num}): {e_read_body}"
                    )

                # Procesamos la respuesta
                if 200 <= status_code < 300:
                    # Éxito
                    success_msg_detail = (
                        f" (Cuerpo: {response_text[:60]})"
                        if response_text
                        and response_text
                        != "Se ha registrado de forma exitosa el punto gps"
                        else ""
                    )

                    # Lógica específica para mensajes de éxito
                    if status_code in [200, 201] and target_url.startswith(
                        "https://seguridadciudadana.mininter.gob.pe"
                    ):
                        response_text = "Se ha registrado de forma exitosa el punto gps"

                    logger.info(
                        f"POST AIOHTTP Éxito (intento {attempt_num}): Estado {status_code} para posición {pos_id_display}, placa {placa_for_log}"
                    )
                    success = True
                else:
                    # Error HTTP
                    logger.warning(
                        f"POST AIOHTTP Fallo (intento {attempt_num}): Estado {status_code} para posición {pos_id_display}, placa {placa_for_log}. Respuesta: {response_text}"
                    )
                    success = False

        except aiohttp.ClientSSLError as ssl_err:
            response_text = f"Error SSL de AIOHTTP: {ssl_err}"
            logger.error(
                f"Error SSL de AIOHTTP (Intento {attempt_num}) para {target_url}, posición {pos_id_display}: {ssl_err}",
                exc_info=False,
            )
        except aiohttp.ClientConnectorError as conn_err:
            response_text = f"Error de conexión de AIOHTTP: {conn_err}"
            logger.warning(
                f"Error de conexión de AIOHTTP (Intento {attempt_num}) para {target_url}, posición {pos_id_display}: {conn_err}"
            )
        except asyncio.TimeoutError:
            response_text = "Timeout de AIOHTTP"
            logger.warning(
                f"Timeout de AIOHTTP (Intento {attempt_num}) para {target_url}, posición {pos_id_display}"
            )
        except aiohttp.ClientError as client_err:
            response_text = f"Error de cliente de AIOHTTP: {client_err}"
            if hasattr(client_err, "status") and client_err.status:
                status_code = client_err.status
            logger.warning(
                f"Error de cliente de AIOHTTP (Intento {attempt_num}) para {target_url}, posición {pos_id_display}: {client_err}"
            )
        except Exception as e:
            response_text = f"Error inesperado en intento de AIOHTTP: {e}"
            logger.error(
                f"Error inesperado (Intento {attempt_num}) durante POST aiohttp a {target_url}, posición {pos_id_display}: {e}",
                exc_info=True,
            )

        return success, response_text, status_code

    def _log_to_db_from_async(
        self, response: str, level: str, placa: str, host: str, json_send: str
    ):
        """
        Registra un log en la base de datos desde un contexto asíncrono.

        Args:
            response: Texto de respuesta.
            level: Nivel del log (INFO, ERROR, etc.).
            placa: Placa del vehículo.
            host: Host de destino.
            json_send: JSON enviado.
        """
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

    async def start_retransmission_worker_async(self):
        """
        Inicia el worker asíncrono de retransmisión.
        """
        if self.worker_task and not self.worker_task.done():
            logger.info("El worker de retransmisión asíncrono ya está ejecutándose.")
            return

        logger.info("Iniciando worker de retransmisión asíncrono...")
        self.stop_event.clear()
        self.worker_task = asyncio.create_task(self._retransmission_worker_loop_async())

    async def stop_retransmission_worker_async(self):
        """
        Detiene el worker asíncrono de retransmisión.
        """
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
                    "La tarea del worker de retransmisión asíncrono no terminó a tiempo después de la señal de parada. Intentando cancelación forzada."
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
