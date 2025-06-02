import mysql.connector
import mysql.connector.pooling
import logging
import threading
import time
from queue import Queue, Empty, Full
from typing import List, Tuple, Any, Optional, Dict  # Asegúrate que Dict esté importado
from app.config import (
    DB_CONN_PARAMS,
    LOG_WRITER_DB_ENABLED,
    LOG_WRITER_QUEUE_MAX_SIZE,
    LOG_WRITER_BATCH_SIZE,
    LOG_WRITER_FLUSH_INTERVAL_SECONDS,
)
from app.utils.helpers import get_current_datetime_str_for_log

logger = logging.getLogger(__name__)

LogEntryData = Tuple[str, str, str, str, str, str, str]

# Constante para el mensaje de shutdown en la cola
SHUTDOWN_FLUSH_SIGNAL_ORIGIN = "SHUTDOWN_FLUSH_SIGNAL"


class LogWriterDB:
    _instance: Optional["LogWriterDB"] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs) -> Optional["LogWriterDB"]:
        if not LOG_WRITER_DB_ENABLED:
            if not hasattr(cls, "_disabled_logged_once"):
                logger.warning(
                    "LogWriterDB is disabled as DB_CONN_PARAMS are incomplete or LOG_WRITER_DB_ENABLED is False."
                )
                cls._disabled_logged_once = True  # type: ignore
            return None

        with cls._lock:
            if cls._instance is None:
                logger.debug("Creating new LogWriterDB instance.")
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False  # type: ignore
                cls._instance.log_queue = Queue(maxsize=LOG_WRITER_QUEUE_MAX_SIZE)  # type: ignore
                cls._instance.stop_event = threading.Event()  # type: ignore
                cls._instance.worker_thread = None  # type: ignore
                cls._instance._db_pool = None  # type: ignore

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
                        cls._instance._db_pool = mysql.connector.pooling.MySQLConnectionPool(**pool_creation_params)  # type: ignore
                        logger.info(
                            f"LogWriterDB: MySQL connection pool '{pool_creation_params['pool_name']}' created using DB_CONN_PARAMS."
                        )
                    except mysql.connector.Error as err:
                        logger.error(
                            f"LogWriterDB: Failed to create MySQL connection pool: {err}. Will use individual connections."
                        )
                        cls._instance._db_pool = None  # type: ignore
                    except Exception as e:
                        logger.error(
                            f"LogWriterDB: Unexpected error creating MySQL connection pool: {e}. Will use individual connections."
                        )
                        cls._instance._db_pool = None  # type: ignore
            return cls._instance

    def __init__(self):
        if not LOG_WRITER_DB_ENABLED:
            return
        if hasattr(self, "_initialized") and self._initialized:  # type: ignore
            return

        logger.debug(f"LogWriterDB instance {id(self)} initializing...")
        self._start_worker()
        self._initialized = True
        logger.info("LogWriterDB initialized and worker started.")

    def _get_connection(self) -> Optional[mysql.connector.MySQLConnection]:
        try:
            if self._db_pool:  # type: ignore
                return self._db_pool.get_connection()  # type: ignore
            else:
                conn_params_no_pool = {
                    k: v
                    for k, v in DB_CONN_PARAMS.items()
                    if k not in ["pool_name", "pool_size"]
                }
                return mysql.connector.connect(**conn_params_no_pool)
        except mysql.connector.Error as err:
            logger.error(f"LogWriterDB: Failed to get/create DB connection: {err}")
            return None
        except Exception as e:
            logger.error(
                f"LogWriterDB: Unexpected error getting/creating DB connection: {e}",
                exc_info=True,
            )
            return None

    def _start_worker(self):
        if self.worker_thread and self.worker_thread.is_alive():  # type: ignore
            logger.debug("LogWriterDB worker thread already running.")
            return
        self.stop_event.clear()  # type: ignore
        self.worker_thread = threading.Thread(  # type: ignore
            target=self._process_queue_loop, name="LogWriterDBThread", daemon=True
        )
        self.worker_thread.start()  # type: ignore
        logger.info("LogWriterDB worker thread started.")

    def add_log_entry_data(
        self,
        response: str,
        level: str,
        placa: str,
        host: str,
        json_send: str,
        origen: str,
    ):
        if not self._initialized or self.stop_event.is_set():  # type: ignore
            logger.debug("LogWriterDB not initialized or stopping, log discarded.")
            return

        # La fecha y hora se captura aquí, justo antes de encolar el log real.
        fecha_hora = get_current_datetime_str_for_log()
        log_item: LogEntryData = (
            fecha_hora,
            str(response),
            str(level),
            str(placa),
            str(host),
            str(json_send),
            str(origen),
        )
        try:
            self.log_queue.put(log_item, block=True, timeout=0.5)  # type: ignore
        except Full:
            logger.warning(
                f"LogWriterDB queue is full (size: {self.log_queue.qsize()}). "  # type: ignore
                f"Log entry for {placa} to {host} discarded."
            )
        except Exception as e:
            logger.error(
                f"LogWriterDB: Error agregando entrada de log a la cola: {e}", exc_info=True
            )

    def _process_queue_loop(self):
        logger.info("Bucle de procesamiento de LogWriterDB iniciado.")
        last_flush_time = time.time()

        while not self.stop_event.is_set() or not self.log_queue.empty():  # type: ignore [attr-defined]
            batch: List[LogEntryData] = []
            current_time = time.time()

            try:
                while len(batch) < LOG_WRITER_BATCH_SIZE:
                    time_to_next_flush = (
                        last_flush_time + LOG_WRITER_FLUSH_INTERVAL_SECONDS
                    ) - current_time
                    get_timeout = max(0.01, min(1.0, time_to_next_flush))

                    if self.stop_event.is_set() and self.log_queue.empty():  # type: ignore [attr-defined]
                        break

                    try:
                        item = self.log_queue.get(block=True, timeout=get_timeout)  # type: ignore [attr-defined]

                        # ---- MODIFICACIÓN AQUÍ para el ítem de SHUTDOWN ----
                        # El ítem de shutdown tendrá SHUTDOWN_FLUSH_SIGNAL_ORIGIN como origen.
                        if (
                            len(item) == 7 and item[6] == SHUTDOWN_FLUSH_SIGNAL_ORIGIN
                        ):  # item[6] es 'origen'
                            logger.debug(
                                "LogWriterDB: Descartando el elemento SHUTDOWN_FLUSH_SIGNAL de la cola."
                            )
                            self.log_queue.task_done()  # type: ignore [attr-defined]
                            continue  # Saltar al siguiente ítem de la cola
                        # ---- FIN DE MODIFICACIÓN ----

                        batch.append(item)
                        self.log_queue.task_done()  # type: ignore [attr-defined]
                    except Empty:
                        break

                if batch:
                    self._insert_batch_to_db(batch)
                    last_flush_time = time.time()
            except Exception as e:
                logger.error(
                    f"LogWriterDB: Error inesperado en el bucle de procesamiento: {e}",
                    exc_info=True,
                )
                if not self.stop_event.is_set():
                    time.sleep(1)

            if self.stop_event.is_set() and self.log_queue.empty():  # type: ignore [attr-defined]
                logger.debug(
                    "LogWriterDB: evento de parada establecido y cola vacía, saliendo del bucle de procesamiento."
                )
                break

        logger.info("LogWriterDB: procesamiento de bucle finalizado.")

    def _insert_batch_to_db(self, batch: List[LogEntryData]):
        if not batch:
            return

        conn = None
        cursor = None
        query = """
            INSERT INTO logs (fecha_hora, response, nivel, placa, host, jsonSend, origen) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            conn = self._get_connection()
            if not conn:
                logger.error(
                    "LogWriterDB: No se puede insertar el lote, no hay conexión a la base de datos disponible."
                )
                return

            cursor = conn.cursor()
            cursor.executemany(query, batch)
            conn.commit()
            logger.debug(
                f"LogWriterDB: Successfully inserted {len(batch)} log entries."
            )
        except mysql.connector.Error as err:
            # Loguear el error y también el primer item del batch para depuración
            first_item_str = str(batch[0]) if batch else "N/A"
            logger.error(
                f"LogWriterDB: Error inserting log batch: {err}. First item: {first_item_str}. Batch may be lost."
            )
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    logger.error(f"LogWriterDB: Error during rollback: {rb_err}")
        except Exception as e:
            first_item_str = str(batch[0]) if batch else "N/A"
            logger.error(
                f"LogWriterDB: Unexpected error inserting log batch: {e}. First item: {first_item_str}. Batch may be lost.",
                exc_info=True,
            )
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    logger.error(
                        f"LogWriterDB: Unexpected error during rollback: {rb_err}"
                    )
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def shutdown(self):
        if not hasattr(self, "_initialized") or not self._initialized:  # type: ignore
            logger.debug("LogWriterDB shutdown called but not initialized.")
            return
        if not LOG_WRITER_DB_ENABLED:
            logger.debug("LogWriterDB shutdown called but DB logging is disabled.")
            return

        logger.info("LogWriterDB initiating shutdown...")
        self.stop_event.set()  # type: ignore

        try:
            # El ítem de shutdown debe tener una estructura válida de LogEntryData
            # aunque sus valores no sean "reales", excepto el origen para identificarlo.
            # La fecha_hora no importa mucho aquí porque se descartará.
            dummy_datetime_str = (
                get_current_datetime_str_for_log()
            )  # O una cadena fija si prefieres
            shutdown_item: LogEntryData = (
                dummy_datetime_str,  # fecha_hora (no se usará)
                "Shutdown signal",  # response
                "DEBUG",  # nivel
                "SYSTEM",  # placa
                "LogWriterDB",  # host
                "{}",  # jsonSend
                SHUTDOWN_FLUSH_SIGNAL_ORIGIN,  # origen (para identificarlo)
            )
            self.log_queue.put_nowait(shutdown_item)  # type: ignore
        except Full:
            logger.debug(
                "LogWriterDB queue full during shutdown signal, worker should pick up stop_event."
            )
        except Exception as e:
            logger.warning(f"LogWriterDB: Error trying to put shutdown flush item: {e}")

        if self.worker_thread and self.worker_thread.is_alive():  # type: ignore
            logger.info(
                "LogWriterDB: Waiting for worker thread to process remaining queue and finish..."
            )
            self.worker_thread.join(timeout=LOG_WRITER_FLUSH_INTERVAL_SECONDS + 10)  # type: ignore
            if self.worker_thread.is_alive():  # type: ignore
                logger.warning("LogWriterDB worker thread did not terminate in time.")
            else:
                logger.info("LogWriterDB worker thread joined successfully.")

        if self._db_pool:  # type: ignore
            logger.info(
                "LogWriterDB: MySQL connection pool connections will be managed by the connector library."
            )
            self._db_pool = None  # type: ignore

        logger.info("LogWriterDB shutdown complete.")
        with LogWriterDB._lock:
            LogWriterDB._instance = None
            if hasattr(LogWriterDB, "_disabled_logged_once"):
                delattr(LogWriterDB, "_disabled_logged_once")


# Global singleton instance
log_writer_db_instance: Optional[LogWriterDB] = LogWriterDB()
