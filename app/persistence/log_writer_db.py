import mysql.connector
import mysql.connector.pooling
import logging
import threading
import time
import json
import os
from collections import Counter
from queue import Queue, Empty, Full
from typing import List, Tuple, Any, Optional, Dict, Set

# Importamos configuraciones y constantes desde otros archivos
from app.config import (
    DB_CONN_PARAMS,  # Parámetros de conexión a la base de datos
    LOG_WRITER_DB_ENABLED,  # Si el LogWriter está habilitado
    LOG_WRITER_QUEUE_MAX_SIZE,  # Tamaño máximo de la cola
    LOG_WRITER_BATCH_SIZE,  # Tamaño de los lotes para inserción
    LOG_WRITER_FLUSH_INTERVAL_SECONDS,  # Intervalo de tiempo para vaciar la cola
    DB_LOG_BACKUP_FILE_PATH,  # Ruta del archivo de respaldo
    MAX_DB_CONNECTION_FAILURES_BEFORE_BACKUP,  # Máximo de fallos antes de hacer backup
)
from app.utils.helpers import (
    get_current_datetime_str_for_log,
)  # Para obtener la fecha y hora actual
from app.constants import LOG_LEVEL_DB_INFO, LOG_LEVEL_DB_ERROR  # Niveles de log

# Configuramos el registro de mensajes (logging)
logger = logging.getLogger(__name__)

# Definimos un tipo personalizado para los datos de log
# Es una tupla con: fecha_hora, response, level, placa, host, json_send, origen
LogEntryData = Tuple[str, str, str, str, str, str, str]

# Constante para identificar señales de apagado
SHUTDOWN_FLUSH_SIGNAL_ORIGIN = "SHUTDOWN_FLUSH_SIGNAL"


class LogWriterDB:
    """
    Clase para escribir logs en la base de datos de manera eficiente.
    Usa un patrón Singleton para tener solo una instancia.
    Maneja una cola de logs y los escribe en lotes a la base de datos.
    """

    _instance: Optional["LogWriterDB"] = None  # La única instancia de la clase
    _lock = threading.Lock()  # Para controlar el acceso a la instancia

    def __new__(cls, *args, **kwargs) -> Optional["LogWriterDB"]:
        """
        Implementación del patrón Singleton.
        Se asegura de que solo exista una instancia de LogWriterDB.
        """
        # Si el LogWriter está deshabilitado, no creamos instancia
        if not LOG_WRITER_DB_ENABLED:
            if not hasattr(cls, "_disabled_logged_once"):
                logger.warning(
                    f"LogWriterDB está deshabilitado. Archivo de respaldo: {DB_LOG_BACKUP_FILE_PATH}"
                )
                cls._disabled_logged_once = True
            return None

        # Usamos un bloqueo para evitar problemas con múltiples hilos
        with cls._lock:
            if cls._instance is None:  # Si no hay instancia aún
                logger.debug("Creando nueva instancia de LogWriterDB.")
                # Creamos la nueva instancia
                cls._instance = super().__new__(cls)
                # Inicializamos los atributos básicos
                cls._instance._initialized = False
                # Creamos la cola con tamaño máximo definido en la configuración
                cls._instance.log_queue: Queue[LogEntryData] = Queue(maxsize=LOG_WRITER_QUEUE_MAX_SIZE)  # type: ignore [attr-defined]
                # Evento para controlar el apagado
                cls._instance.stop_event = threading.Event()
                # Hilo de trabajo
                cls._instance.worker_thread: Optional[threading.Thread] = None  # type: ignore [attr-defined]
                # Pool de conexiones a la base de datos
                cls._instance._db_pool: Optional[mysql.connector.pooling.MySQLConnectionPool] = None  # type: ignore [attr-defined]
                # Contador de fallos consecutivos de conexión
                cls._instance._consecutive_db_connection_failures: int = 0  # type: ignore [attr-defined]

                # Intentamos crear un pool de conexiones si está configurado
                if DB_CONN_PARAMS.get("pool_name"):
                    try:
                        pool_creation_params = {
                            "pool_name": DB_CONN_PARAMS["pool_name"],
                            "pool_size": DB_CONN_PARAMS.get("pool_size", 5),
                            "host": DB_CONN_PARAMS["host"],
                            "user": DB_CONN_PARAMS["user"],
                            "password": DB_CONN_PARAMS["password"],
                            "database": DB_CONN_PARAMS["database"],
                        }
                        # Creamos el pool de conexiones
                        cls._instance._db_pool = (
                            mysql.connector.pooling.MySQLConnectionPool(
                                **pool_creation_params
                            )
                        )
                        logger.info(
                            f"LogWriterDB: Pool de conexiones MySQL '{pool_creation_params['pool_name']}' creado."
                        )
                    except mysql.connector.Error as err:
                        logger.error(
                            f"LogWriterDB: Falló al crear el pool de conexiones MySQL: {err}."
                        )
                        cls._instance._db_pool = None
                    except Exception as e:
                        logger.error(
                            f"LogWriterDB: Error inesperado al crear el pool MySQL: {e}."
                        )
                        cls._instance._db_pool = None
            return cls._instance

    def __init__(self):
        """
        Inicializa la instancia de LogWriterDB.
        Solo se ejecuta si el LogWriter está habilitado.
        """
        if not LOG_WRITER_DB_ENABLED:
            return

        # Evitamos inicializar dos veces
        if hasattr(self, "_initialized") and self._initialized:
            return

        logger.debug(f"Inicializando instancia de LogWriterDB {id(self)}...")
        # Iniciamos el hilo de trabajo
        self._start_worker()
        self._initialized = True
        logger.info("LogWriterDB inicializado y hilo de trabajo iniciado.")

    def _get_connection(self) -> Optional[mysql.connector.MySQLConnection]:
        """
        Obtiene una conexión a la base de datos.
        Usa el pool si está disponible, o crea una nueva conexión si no.

        Returns:
            Una conexión a MySQL o None si falla.
        """
        try:
            conn: Optional[mysql.connector.MySQLConnection] = None

            # Si tenemos un pool de conexiones, usamos eso
            if self._db_pool:
                conn = self._db_pool.get_connection()
            else:
                # Si no, creamos una conexión directa
                conn_params_no_pool = {
                    k: v
                    for k, v in DB_CONN_PARAMS.items()
                    if k not in ["pool_name", "pool_size"]
                }
                conn = mysql.connector.connect(**conn_params_no_pool)

            # Si habíamos tenido fallos antes, registramos que ahora sí funcionó
            if self._consecutive_db_connection_failures > 0:
                logger.info(
                    "LogWriterDB: Conexión a la base de datos exitosa después de fallos previos."
                )
            # Reiniciamos el contador de fallos
            self._consecutive_db_connection_failures = 0
            return conn

        except mysql.connector.Error as err:
            # Si falla, incrementamos el contador de fallos
            self._consecutive_db_connection_failures += 1
            logger.error(
                f"LogWriterDB: Falló al obtener/crear conexión a la base de datos (intento {self._consecutive_db_connection_failures}): {err}"
            )
            return None

        except Exception as e:
            self._consecutive_db_connection_failures += 1
            logger.error(
                f"LogWriterDB: Error inesperado al obtener/crear conexión a la base de datos (intento {self._consecutive_db_connection_failures}): {e}",
                exc_info=True,
            )
            return None

    def _start_worker(self):
        """
        Inicia el hilo de trabajo que procesa la cola de logs.
        """
        # Si ya hay un hilo activo, no hacemos nada
        if self.worker_thread and self.worker_thread.is_alive():
            return

        # Limpiamos el evento de parada
        self.stop_event.clear()

        # Creamos y empezamos el hilo de trabajo
        self.worker_thread = threading.Thread(
            target=self._process_queue_loop,
            name="LogWriterDBThread",
            daemon=True,  # Hilo demonio que se cierra cuando el programa principal termina
        )
        self.worker_thread.start()
        logger.info("Hilo de trabajo de LogWriterDB iniciado.")

    def add_log_entry_data(
        self,
        response: str,
        level: str,
        placa: str,
        host: str,
        json_send: str,
        origen: str,
    ):
        """
        Añade una entrada de log a la cola para ser procesada.

        Args:
            response: Respuesta o mensaje del log
            level: Nivel del log (INFO, ERROR, etc.)
            placa: Placa del vehículo asociado
            host: Host destino
            json_send: Datos JSON enviados
            origen: Origen del log
        """
        # Si no está inicializado o se ha solicitado parada, no hacemos nada
        if not self._initialized or self.stop_event.is_set():
            return

        # Obtenemos la fecha y hora actual
        fecha_hora = get_current_datetime_str_for_log()

        # Creamos la tupla con los datos del log
        log_item: LogEntryData = (
            fecha_hora,
            str(response)[:2048],  # Limitamos a 2048 caracteres
            str(level),
            str(placa),
            str(host),
            str(json_send),
            str(origen),
        )

        try:
            # Intentamos añadir el log a la cola
            self.log_queue.put(log_item, block=True, timeout=0.5)
        except Full:
            # Si la cola está llena, registramos un warning
            logger.warning(
                f"Cola de LogWriterDB llena. Log para {placa} a {host} descartado."
            )
        except Exception as e:
            # Si hay otro error, lo registramos
            logger.error(
                f"LogWriterDB: Error al añadir log a la cola: {e}", exc_info=True
            )

    def _process_queue_loop(self):
        """
        Bucle principal que procesa la cola de logs.
        Toma elementos de la cola y los inserta en la base de datos en lotes.
        """
        logger.info("Bucle de procesamiento de LogWriterDB iniciado.")
        last_flush_time = time.time()  # Tiempo del último vaciado

        # El bucle continúa mientras no se haya solicitado parada o haya elementos en la cola
        while not self.stop_event.is_set() or not self.log_queue.empty():
            batch: List[LogEntryData] = []  # Lote de logs a insertar
            current_time = time.time()  # Tiempo actual

            try:
                # Procesamos hasta completar un lote
                while len(batch) < LOG_WRITER_BATCH_SIZE:
                    # Calculamos el tiempo hasta el próximo vaciado automático
                    time_to_next_flush = (
                        last_flush_time + LOG_WRITER_FLUSH_INTERVAL_SECONDS
                    ) - current_time
                    # Tiempo de espera para obtener el próximo elemento
                    get_timeout = max(0.01, min(1.0, time_to_next_flush))

                    # Si se ha solicitado parada y la cola está vacía, salimos
                    if self.stop_event.is_set() and self.log_queue.empty():
                        break

                    try:
                        # Intentamos obtener un elemento de la cola
                        item = self.log_queue.get(block=True, timeout=get_timeout)

                        # Si es una señal de apagado, la ignoramos
                        if len(item) == 7 and item[6] == SHUTDOWN_FLUSH_SIGNAL_ORIGIN:
                            logger.debug(
                                "LogWriterDB: Descartando elemento SHUTDOWN_FLUSH_SIGNAL de la cola."
                            )
                            self.log_queue.task_done()
                            continue

                        # Añadimos el elemento al lote
                        batch.append(item)
                        self.log_queue.task_done()

                    except Empty:
                        # Si la cola está vacía, salimos del bucle interno
                        break

                # Si tenemos un lote, lo insertamos en la base de datos
                if batch:
                    self._insert_batch_to_db(batch, from_backup_processing=False)
                    last_flush_time = (
                        time.time()
                    )  # Actualizamos el tiempo del último vaciado

            except Exception as e:
                # Si hay un error, lo registramos
                logger.error(
                    f"LogWriterDB: Error en el bucle de procesamiento: {e}",
                    exc_info=True,
                )
                # Si no se ha solicitado parada, esperamos un poco antes de continuar
                if not self.stop_event.is_set():
                    time.sleep(1)

            # Si se ha solicitado parada y la cola está vacía, salimos del bucle principal
            if self.stop_event.is_set() and self.log_queue.empty():
                break

        logger.info("Bucle de procesamiento de LogWriterDB finalizado.")

    def _insert_batch_to_db(
        self, batch: List[LogEntryData], from_backup_processing: bool = False
    ):
        """
        Inserta un lote de logs en la base de datos.

        Args:
            batch: Lista de tuplas con los datos de log
            from_backup_processing: Si estamos procesando desde el archivo de respaldo
        """
        if not batch:
            return

        # Contadores para el resumen
        info_count = 0
        error_count = 0
        other_count = 0

        # Conjuntos para almacenar placas únicas
        info_placas_set: Set[str] = set()
        error_placas_set: Set[str] = set()
        other_placas_set: Set[str] = set()

        # Contamos los diferentes tipos de logs
        for log_entry in batch:
            level = log_entry[2]
            placa = log_entry[3]

            if placa == "SYSTEM":
                pass
            elif level == LOG_LEVEL_DB_INFO:
                info_count += 1
                info_placas_set.add(placa)
            elif level == LOG_LEVEL_DB_ERROR:
                error_count += 1
                error_placas_set.add(placa)
            else:
                other_count += 1
                other_placas_set.add(placa)

        # Obtenemos una conexión a la base de datos
        conn = self._get_connection()

        # Si no podemos conectar a la base de datos
        if not conn:
            # Si hemos superado el máximo de fallos, escribimos en el archivo de respaldo
            if (
                not from_backup_processing
                and self._consecutive_db_connection_failures
                >= MAX_DB_CONNECTION_FAILURES_BEFORE_BACKUP
            ):
                logger.error(
                    f"LogWriterDB: Fallo de conexión a la base de datos {self._consecutive_db_connection_failures} veces. Escribiendo lote en respaldo: {DB_LOG_BACKUP_FILE_PATH}"
                )
                self._write_batch_to_backup_file(batch)
            elif from_backup_processing:
                logger.error(
                    f"LogWriterDB: Fallo de conexión a la base de datos durante procesamiento de respaldo. Lote de {len(batch)} elementos re-escrito en respaldo."
                )
                self._write_batch_to_backup_file(batch)
            else:
                logger.warning(
                    f"LogWriterDB: Fallo de conexión a la base de datos. Lote no insertado (fallos: {self._consecutive_db_connection_failures}/{MAX_DB_CONNECTION_FAILURES_BEFORE_BACKUP})."
                )
            return

        cursor = None
        # Consulta SQL para insertar los logs
        query = "INSERT INTO logs (fecha_hora, response, nivel, placa, host, jsonSend, origen) VALUES (%s, %s, %s, %s, %s, %s, %s)"

        try:
            # Creamos un cursor y ejecutamos la inserción
            cursor = conn.cursor()
            cursor.executemany(query, batch)
            conn.commit()

            # Creamos un resumen de lo que se insertó
            summary_parts = [f"LogWriterDB: Lote insertado con {len(batch)} entradas."]

            if info_count > 0:
                summary_parts.append(f"INFOs: {info_count}")
                if info_placas_set:
                    placas_str = ", ".join(sorted(list(info_placas_set)))
                    summary_parts.append(f"(Placas INFO: {placas_str})")

            if error_count > 0:
                summary_parts.append(f"ERRORs: {error_count}")
                if error_placas_set:
                    placas_str = ", ".join(sorted(list(error_placas_set)))
                    summary_parts.append(f"(Placas ERROR: {placas_str})")

            if other_count > 0:
                summary_parts.append(f"OTHERs: {other_count}")
                if other_placas_set:
                    placas_str = ", ".join(sorted(list(other_placas_set)))
                    summary_parts.append(f"(Placas OTHER: {placas_str})")

            # Registramos el resumen
            logger.info(" ".join(summary_parts))
            self._consecutive_db_connection_failures = 0

        except mysql.connector.Error as err:
            # Si hay un error de MySQL, lo registramos
            logger.error(f"LogWriterDB: Error al insertar lote de logs: {err}.")

            # Intentamos hacer rollback
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    logger.error(f"LogWriterDB: Error durante rollback: {rb_err}")

            # Escribimos el lote fallido en el archivo de respaldo
            if not from_backup_processing:
                logger.info("Escribiendo lote fallido en archivo de respaldo.")
                self._write_batch_to_backup_file(batch)
            else:
                logger.warning(
                    "Re-escribiendo lote en archivo de respaldo durante fallo en inserción de procesamiento de respaldo."
                )
                self._write_batch_to_backup_file(batch)

        except Exception as e:
            # Si hay otro error inesperado, lo registramos
            logger.error(
                f"LogWriterDB: Error inesperado al insertar lote de logs: {e}.",
                exc_info=True,
            )

            # Intentamos hacer rollback
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    logger.error(
                        f"LogWriterDB: Error inesperado durante rollback: {rb_err}"
                    )

            # Escribimos el lote fallido en el archivo de respaldo
            if not from_backup_processing:
                logger.info(
                    "Escribiendo lote fallido (error inesperado) en archivo de respaldo."
                )
                self._write_batch_to_backup_file(batch)
            else:
                logger.warning(
                    "Re-escribiendo lote en archivo de respaldo durante procesamiento de respaldo (error inesperado)."
                )
                self._write_batch_to_backup_file(batch)

        finally:
            # Cerramos el cursor y la conexión
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _write_batch_to_backup_file(self, batch: List[LogEntryData]):
        """
        Escribe un lote de logs en el archivo de respaldo.

        Args:
            batch: Lote de logs a escribir
        """
        try:
            # Creamos el directorio si no existe
            backup_dir = os.path.dirname(DB_LOG_BACKUP_FILE_PATH)
            if backup_dir and not os.path.exists(backup_dir):
                os.makedirs(backup_dir, exist_ok=True)

            # Escribimos cada log como una línea JSON en el archivo
            with open(DB_LOG_BACKUP_FILE_PATH, "a", encoding="utf-8") as f:
                for log_entry_tuple in batch:
                    log_entry_dict = {
                        "fecha_hora": log_entry_tuple[0],
                        "response": log_entry_tuple[1],
                        "nivel": log_entry_tuple[2],
                        "placa": log_entry_tuple[3],
                        "host": log_entry_tuple[4],
                        "jsonSend": log_entry_tuple[5],
                        "origen": log_entry_tuple[6],
                    }
                    f.write(json.dumps(log_entry_dict) + "\n")

            logger.info(
                f"LogWriterDB: {len(batch)} entradas escritas en archivo de respaldo {DB_LOG_BACKUP_FILE_PATH}"
            )

        except Exception as e:
            # Si hay un error crítico al escribir en el respaldo, lo registramos
            logger.error(
                f"LogWriterDB: CRÍTICO - Falló al escribir en archivo de respaldo {DB_LOG_BACKUP_FILE_PATH}: {e}",
                exc_info=True,
            )

    def process_pending_logs_from_backup(self) -> bool:
        """
        Procesa logs pendientes desde el archivo de respaldo.

        Returns:
            True si se procesaron todos los logs correctamente, False si hubo errores.
        """
        # Si no existe el archivo o está vacío, no hay nada que procesar
        if (
            not os.path.exists(DB_LOG_BACKUP_FILE_PATH)
            or os.path.getsize(DB_LOG_BACKUP_FILE_PATH) == 0
        ):
            logger.info("No se encontraron logs pendientes en el archivo de respaldo.")
            return True

        logger.info(
            f"Procesando logs pendientes desde archivo de respaldo: {DB_LOG_BACKUP_FILE_PATH}"
        )
        processed_logs_successfully = True
        temp_backup_file = DB_LOG_BACKUP_FILE_PATH + ".processing"

        try:
            # Renombramos el archivo para indicar que lo estamos procesando
            if os.path.exists(DB_LOG_BACKUP_FILE_PATH):
                os.rename(DB_LOG_BACKUP_FILE_PATH, temp_backup_file)
            elif os.path.exists(temp_backup_file):
                logger.info(
                    f"Procesando archivo previamente iniciado: {temp_backup_file}"
                )
            else:
                logger.info("No se encontró archivo de respaldo o archivo .processing.")
                return True

            # Listas para almacenar los logs a insertar y los problemáticos
            pending_logs_to_insert: List[LogEntryData] = []
            remaining_logs_after_processing: List[Dict[str, Any]] = []

            # Leemos el archivo línea por línea
            with open(temp_backup_file, "r", encoding="utf-8") as f:
                for line_number, line in enumerate(f, 1):
                    try:
                        # Intentamos parsear cada línea como JSON
                        log_entry_dict = json.loads(line.strip())
                        log_entry_tuple: LogEntryData = (
                            log_entry_dict.get("fecha_hora", ""),
                            log_entry_dict.get("response", ""),
                            log_entry_dict.get("nivel", "UNKNOWN"),
                            log_entry_dict.get("placa", "UNKNOWN"),
                            log_entry_dict.get("host", "UNKNOWN"),
                            log_entry_dict.get("jsonSend", "{}"),
                            log_entry_dict.get("origen", "BACKUP_RECOVERY"),
                        )
                        pending_logs_to_insert.append(log_entry_tuple)
                    except json.JSONDecodeError as json_err:
                        # Si hay un error al parsear el JSON, lo registramos
                        logger.error(
                            f"Saltando línea no parseable #{line_number} en respaldo: '{line.strip()}'. Error: {json_err}"
                        )
                        remaining_logs_after_processing.append(
                            {
                                "raw_line": line.strip(),
                                "error": "JSONDecodeError",
                                "line": line_number,
                            }
                        )
                    except Exception as e_line:
                        # Si hay otro error, también lo registramos
                        logger.error(
                            f"Saltando línea problemática #{line_number} en respaldo: '{line.strip()}'. Error: {e_line}"
                        )
                        remaining_logs_after_processing.append(
                            {
                                "raw_line": line.strip(),
                                "error": str(e_line),
                                "line": line_number,
                            }
                        )

            # Si hay logs pendientes para insertar
            if pending_logs_to_insert:
                logger.info(
                    f"Intentando insertar {len(pending_logs_to_insert)} logs pendientes parseados desde el respaldo."
                )
                all_batches_inserted_ok_from_pending = True

                # Procesamos en lotes
                for i in range(0, len(pending_logs_to_insert), LOG_WRITER_BATCH_SIZE):
                    batch_to_insert = pending_logs_to_insert[
                        i : i + LOG_WRITER_BATCH_SIZE
                    ]
                    self._insert_batch_to_db(
                        batch_to_insert, from_backup_processing=True
                    )
                    # Si aparece el archivo de respaldo durante el procesamiento, algo salió mal
                    if (
                        os.path.exists(DB_LOG_BACKUP_FILE_PATH)
                        and os.path.getsize(DB_LOG_BACKUP_FILE_PATH) > 0
                    ):
                        all_batches_inserted_ok_from_pending = False
                        break

                if not all_batches_inserted_ok_from_pending:
                    processed_logs_successfully = False

            # Si el archivo de respaldo existe y tiene contenido, algo salió mal
            if (
                os.path.exists(DB_LOG_BACKUP_FILE_PATH)
                and os.path.getsize(DB_LOG_BACKUP_FILE_PATH) > 0
            ):
                logger.warning(
                    f"Algunos logs pendientes no pudieron ser re-insertados y permanecen en {DB_LOG_BACKUP_FILE_PATH}."
                )
                processed_logs_successfully = False
                if os.path.exists(temp_backup_file):
                    try:
                        os.remove(temp_backup_file)
                    except OSError:
                        logger.error(
                            f"No se pudo eliminar el archivo temporal {temp_backup_file} después de procesamiento parcial del respaldo."
                        )
            # Si hay líneas no parseables, las guardamos en el archivo de respaldo
            elif remaining_logs_after_processing:
                logger.warning(
                    f"{len(remaining_logs_after_processing)} líneas del respaldo no eran parseables y fueron re-guardadas en {DB_LOG_BACKUP_FILE_PATH}."
                )
                with open(DB_LOG_BACKUP_FILE_PATH, "a", encoding="utf-8") as f_rebackup:
                    for entry in remaining_logs_after_processing:
                        f_rebackup.write(json.dumps(entry) + "\n")
                processed_logs_successfully = False
                if os.path.exists(temp_backup_file):
                    try:
                        os.remove(temp_backup_file)
                    except OSError:
                        logger.error(
                            f"No se pudo eliminar el archivo temporal {temp_backup_file} después de guardar líneas no parseables."
                        )
            # Si todo salió bien, eliminamos el archivo temporal
            elif os.path.exists(temp_backup_file):
                try:
                    os.remove(temp_backup_file)
                    logger.info(
                        f"Logs pendientes procesados correctamente. Archivo de respaldo {temp_backup_file} eliminado."
                    )
                except OSError as e_rem:
                    logger.error(
                        f"Error al eliminar el archivo temporal de procesamiento {temp_backup_file}: {e_rem}"
                    )
            else:
                logger.info(
                    f"Logs pendientes procesados. No quedan más logs en los archivos de respaldo."
                )

            return processed_logs_successfully

        except FileNotFoundError:
            logger.info(
                f"Archivo de respaldo para logs pendientes ({DB_LOG_BACKUP_FILE_PATH} o {temp_backup_file}) no encontrado. Nada que procesar."
            )
            return True

        except Exception as e:
            # Si hay un error crítico durante el procesamiento, lo registramos
            logger.error(
                f"CRÍTICO: Error durante el procesamiento de logs pendientes: {e}",
                exc_info=True,
            )
            # Intentamos restaurar el archivo original si existe el temporal
            if os.path.exists(temp_backup_file) and not os.path.exists(
                DB_LOG_BACKUP_FILE_PATH
            ):
                try:
                    os.rename(temp_backup_file, DB_LOG_BACKUP_FILE_PATH)
                    logger.info(
                        f"Restaurado {temp_backup_file} a {DB_LOG_BACKUP_FILE_PATH} después de error en procesamiento."
                    )
                except Exception as rename_err:
                    logger.error(
                        f"Falló al restaurar archivo de respaldo {temp_backup_file} después de error: {rename_err}"
                    )
            return False

    def shutdown(self):
        """
        Apaga el LogWriterDB de manera ordenada.
        Vacía la cola y cierra todos los recursos.
        """
        # Si no está inicializado, no hacemos nada
        if not hasattr(self, "_initialized") or not self._initialized:
            return

        if not LOG_WRITER_DB_ENABLED:
            return

        logger.info("LogWriterDB iniciando apagado...")

        # Indicamos que queremos parar
        self.stop_event.set()

        try:
            # Creamos un elemento especial para indicar el apagado
            dummy_datetime_str = get_current_datetime_str_for_log()
            shutdown_item: LogEntryData = (
                dummy_datetime_str,
                "Señal de apagado",
                "DEBUG",
                "SYSTEM",
                "LogWriterDB",
                "{}",
                SHUTDOWN_FLUSH_SIGNAL_ORIGIN,
            )
            # Lo añadimos a la cola sin esperar
            self.log_queue.put_nowait(shutdown_item)
        except Full:
            logger.debug("Cola de LogWriterDB llena durante señal de apagado.")
        except Exception as e:
            logger.warning(
                f"LogWriterDB: Error al poner elemento de vaciado de apagado: {e}"
            )

        # Esperamos a que el hilo de trabajo termine
        if self.worker_thread and self.worker_thread.is_alive():
            logger.info("LogWriterDB: Esperando a que el hilo de trabajo termine...")
            self.worker_thread.join(timeout=LOG_WRITER_FLUSH_INTERVAL_SECONDS + 10)
            if self.worker_thread.is_alive():
                logger.warning("El hilo de trabajo de LogWriterDB no terminó a tiempo.")
            else:
                logger.info("Hilo de trabajo de LogWriterDB unido correctamente.")

        # Limpiamos el pool de conexiones
        if self._db_pool:
            self._db_pool = None

        logger.info("Apagado de LogWriterDB completo.")

        # Limpiamos la instancia singleton
        with LogWriterDB._lock:
            LogWriterDB._instance = None
            if hasattr(LogWriterDB, "_disabled_logged_once"):
                delattr(LogWriterDB, "_disabled_logged_once")


# Instancia global singleton
log_writer_db_instance: Optional[LogWriterDB] = LogWriterDB()
