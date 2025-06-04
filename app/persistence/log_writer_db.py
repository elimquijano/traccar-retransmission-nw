import mysql.connector
import mysql.connector.pooling
import logging
import threading
import time
import json
import os
from collections import (
    Counter,
)  # Para contar (no se usa directamente, pero útil para otras agregaciones)
from queue import Queue, Empty, Full
from typing import List, Tuple, Any, Optional, Dict, Set  # <<< AÑADIDO Set AQUÍ

# from typing import Counter as CounterType # No se usa CounterType directamente
from app.config import (
    DB_CONN_PARAMS,
    LOG_WRITER_DB_ENABLED,
    LOG_WRITER_QUEUE_MAX_SIZE,
    LOG_WRITER_BATCH_SIZE,
    LOG_WRITER_FLUSH_INTERVAL_SECONDS,
    DB_LOG_BACKUP_FILE_PATH,
    MAX_DB_CONNECTION_FAILURES_BEFORE_BACKUP,
)
from app.utils.helpers import get_current_datetime_str_for_log

# Importar las constantes de nivel de log
from app.constants import LOG_LEVEL_DB_INFO, LOG_LEVEL_DB_ERROR

logger = logging.getLogger(__name__)

LogEntryData = Tuple[str, str, str, str, str, str, str]
SHUTDOWN_FLUSH_SIGNAL_ORIGIN = "SHUTDOWN_FLUSH_SIGNAL"


class LogWriterDB:
    _instance: Optional["LogWriterDB"] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs) -> Optional["LogWriterDB"]:
        if not LOG_WRITER_DB_ENABLED:
            if not hasattr(cls, "_disabled_logged_once"):
                logger.warning(
                    f"LogWriterDB is disabled. DB Backup File: {DB_LOG_BACKUP_FILE_PATH}"
                )
                cls._disabled_logged_once = True  # type: ignore
            return None
        with cls._lock:
            if cls._instance is None:
                logger.debug("Creating new LogWriterDB instance.")
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False  # type: ignore [attr-defined]
                cls._instance.log_queue: Queue[LogEntryData] = Queue(maxsize=LOG_WRITER_QUEUE_MAX_SIZE)  # type: ignore [attr-defined]
                cls._instance.stop_event = threading.Event()  # type: ignore [attr-defined]
                cls._instance.worker_thread: Optional[threading.Thread] = None  # type: ignore [attr-defined]
                cls._instance._db_pool: Optional[mysql.connector.pooling.MySQLConnectionPool] = None  # type: ignore [attr-defined]
                cls._instance._consecutive_db_connection_failures: int = 0  # type: ignore [attr-defined]

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
                        cls._instance._db_pool = mysql.connector.pooling.MySQLConnectionPool(**pool_creation_params)  # type: ignore [attr-defined]
                        logger.info(
                            f"LogWriterDB: MySQL connection pool '{pool_creation_params['pool_name']}' created."
                        )
                    except mysql.connector.Error as err:
                        logger.error(
                            f"LogWriterDB: Failed to create MySQL connection pool: {err}."
                        )
                        cls._instance._db_pool = None  # type: ignore [attr-defined]
                    except Exception as e:
                        logger.error(
                            f"LogWriterDB: Unexpected error creating MySQL pool: {e}."
                        )
                        cls._instance._db_pool = None  # type: ignore [attr-defined]
            return cls._instance

    def __init__(self):
        if not LOG_WRITER_DB_ENABLED:
            return
        if hasattr(self, "_initialized") and self._initialized:  # type: ignore [attr-defined]
            return
        logger.debug(f"LogWriterDB instance {id(self)} initializing...")
        self._start_worker()
        self._initialized = True  # type: ignore [attr-defined]
        logger.info("LogWriterDB initialized and worker started.")

    def _get_connection(self) -> Optional[mysql.connector.MySQLConnection]:
        try:
            conn: Optional[mysql.connector.MySQLConnection] = None
            if self._db_pool:  # type: ignore [attr-defined]
                conn = self._db_pool.get_connection()  # type: ignore [attr-defined]
            else:
                conn_params_no_pool = {
                    k: v
                    for k, v in DB_CONN_PARAMS.items()
                    if k not in ["pool_name", "pool_size"]
                }
                conn = mysql.connector.connect(**conn_params_no_pool)
            if self._consecutive_db_connection_failures > 0:  # type: ignore [attr-defined]
                logger.info(
                    "LogWriterDB: DB connection successful after previous failures."
                )
            self._consecutive_db_connection_failures = 0  # type: ignore [attr-defined]
            return conn
        except mysql.connector.Error as err:
            self._consecutive_db_connection_failures += 1  # type: ignore [attr-defined]
            logger.error(
                f"LogWriterDB: Failed to get/create DB connection (attempt {self._consecutive_db_connection_failures}): {err}"
            )
            return None
        except Exception as e:
            self._consecutive_db_connection_failures += 1  # type: ignore [attr-defined]
            logger.error(
                f"LogWriterDB: Unexpected error getting/creating DB connection (attempt {self._consecutive_db_connection_failures}): {e}",
                exc_info=True,
            )
            return None

    def _start_worker(self):
        if self.worker_thread and self.worker_thread.is_alive():  # type: ignore [attr-defined]
            return
        self.stop_event.clear()  # type: ignore [attr-defined]
        self.worker_thread = threading.Thread(  # type: ignore [attr-defined]
            target=self._process_queue_loop, name="LogWriterDBThread", daemon=True
        )
        self.worker_thread.start()  # type: ignore [attr-defined]
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
        if not self._initialized or self.stop_event.is_set():  # type: ignore [attr-defined]
            return
        fecha_hora = get_current_datetime_str_for_log()
        log_item: LogEntryData = (
            fecha_hora,
            str(response)[:2048],
            str(level),
            str(placa),
            str(host),
            str(json_send),
            str(origen),
        )
        try:
            self.log_queue.put(log_item, block=True, timeout=0.5)  # type: ignore [attr-defined]
        except Full:
            logger.warning(
                f"LogWriterDB queue full. Log for {placa} to {host} discarded."
            )
        except Exception as e:
            logger.error(f"LogWriterDB: Error adding log to queue: {e}", exc_info=True)

    def _process_queue_loop(self):
        logger.info("LogWriterDB processing loop started.")
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
                    if self.stop_event.is_set() and self.log_queue.empty():
                        break  # type: ignore [attr-defined]
                    try:
                        item = self.log_queue.get(block=True, timeout=get_timeout)  # type: ignore [attr-defined]
                        if len(item) == 7 and item[6] == SHUTDOWN_FLUSH_SIGNAL_ORIGIN:
                            logger.debug(
                                "LogWriterDB: Discarding SHUTDOWN_FLUSH_SIGNAL item from queue."
                            )
                            self.log_queue.task_done()  # type: ignore [attr-defined]
                            continue
                        batch.append(item)
                        self.log_queue.task_done()  # type: ignore [attr-defined]
                    except Empty:
                        break
                if batch:
                    self._insert_batch_to_db(batch, from_backup_processing=False)
                    last_flush_time = time.time()
            except Exception as e:
                logger.error(
                    f"LogWriterDB: Error in processing loop: {e}", exc_info=True
                )
                if not self.stop_event.is_set():
                    time.sleep(1)
            if self.stop_event.is_set() and self.log_queue.empty():
                break  # type: ignore [attr-defined]
        logger.info("LogWriterDB processing loop finished.")

    def _insert_batch_to_db(
        self, batch: List[LogEntryData], from_backup_processing: bool = False
    ):
        if not batch:
            return

        info_count = 0
        error_count = 0
        other_count = 0

        info_placas_set: Set[str] = set()  # Tipado correcto ahora
        error_placas_set: Set[str] = set()  # Tipado correcto ahora
        other_placas_set: Set[str] = set()  # Tipado correcto ahora

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

        conn = self._get_connection()

        if not conn:
            if not from_backup_processing and self._consecutive_db_connection_failures >= MAX_DB_CONNECTION_FAILURES_BEFORE_BACKUP:  # type: ignore [attr-defined]
                logger.error(f"LogWriterDB: DB conn failed {self._consecutive_db_connection_failures} times. Writing batch to backup: {DB_LOG_BACKUP_FILE_PATH}")  # type: ignore [attr-defined]
                self._write_batch_to_backup_file(batch)
            elif from_backup_processing:
                logger.error(
                    f"LogWriterDB: DB conn failed during backup processing. Batch of {len(batch)} items re-written to backup."
                )
                self._write_batch_to_backup_file(batch)
            else:
                logger.warning(f"LogWriterDB: DB conn failed. Batch not inserted (failures: {self._consecutive_db_connection_failures}/{MAX_DB_CONNECTION_FAILURES_BEFORE_BACKUP}).")  # type: ignore [attr-defined]
            return

        cursor = None
        query = "INSERT INTO logs (fecha_hora, response, nivel, placa, host, jsonSend, origen) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        try:
            cursor = conn.cursor()
            cursor.executemany(query, batch)
            conn.commit()

            summary_parts = [f"LogWriterDB: Inserted batch of {len(batch)} entries."]
            if info_count > 0:
                summary_parts.append(f"INFOs: {info_count}")
                if info_placas_set:
                    placas_str = ", ".join(sorted(list(info_placas_set)))
                    summary_parts.append(
                        f"(Placas INFO: {placas_str})"
                    )
            if error_count > 0:
                summary_parts.append(f"ERRORs: {error_count}")
                if error_placas_set:
                    placas_str = ", ".join(sorted(list(error_placas_set)))
                    summary_parts.append(
                        f"(Placas ERROR: {placas_str})"
                    )
            if other_count > 0:
                summary_parts.append(f"OTHERs: {other_count}")
                if other_placas_set:
                    placas_str = ", ".join(sorted(list(other_placas_set)))
                    summary_parts.append(
                        f"(Placas OTHER: {placas_str})"
                    )
            logger.info(" ".join(summary_parts))
            self._consecutive_db_connection_failures = 0  # type: ignore [attr-defined]
        except mysql.connector.Error as err:
            logger.error(f"LogWriterDB: Error inserting log batch: {err}.")
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    logger.error(f"LogWriterDB: Error during rollback: {rb_err}")
            if not from_backup_processing:
                logger.info("Writing failed insert batch to backup file.")
                self._write_batch_to_backup_file(batch)
            else:
                logger.warning(
                    "Re-writing batch to backup file during backup processing insert failure."
                )
                self._write_batch_to_backup_file(batch)
        except Exception as e:
            logger.error(
                f"LogWriterDB: Unexpected error inserting log batch: {e}.",
                exc_info=True,
            )
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    logger.error(
                        f"LogWriterDB: Unexpected error during rollback: {rb_err}"
                    )
            if not from_backup_processing:
                logger.info("Writing failed batch (unexpected error) to backup file.")
                self._write_batch_to_backup_file(batch)
            else:
                logger.warning(
                    "Re-writing batch to backup file during backup processing (unexpected error)."
                )
                self._write_batch_to_backup_file(batch)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _write_batch_to_backup_file(self, batch: List[LogEntryData]):
        try:
            backup_dir = os.path.dirname(DB_LOG_BACKUP_FILE_PATH)
            if backup_dir and not os.path.exists(backup_dir):
                os.makedirs(backup_dir, exist_ok=True)
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
                f"LogWriterDB: Wrote {len(batch)} entries to backup file {DB_LOG_BACKUP_FILE_PATH}"
            )
        except Exception as e:
            logger.error(
                f"LogWriterDB: CRITICAL - Failed to write to backup file {DB_LOG_BACKUP_FILE_PATH}: {e}",
                exc_info=True,
            )

    def process_pending_logs_from_backup(self) -> bool:
        if (
            not os.path.exists(DB_LOG_BACKUP_FILE_PATH)
            or os.path.getsize(DB_LOG_BACKUP_FILE_PATH) == 0
        ):
            logger.info("No pending logs found in backup file.")
            return True
        logger.info(
            f"Processing pending logs from backup file: {DB_LOG_BACKUP_FILE_PATH}"
        )
        processed_logs_successfully = True
        temp_backup_file = DB_LOG_BACKUP_FILE_PATH + ".processing"
        try:
            if os.path.exists(DB_LOG_BACKUP_FILE_PATH):
                os.rename(DB_LOG_BACKUP_FILE_PATH, temp_backup_file)
            elif os.path.exists(temp_backup_file):
                logger.info(f"Processing previously started file: {temp_backup_file}")
            else:
                logger.info("No backup file or .processing file found.")
                return True
            pending_logs_to_insert: List[LogEntryData] = []
            remaining_logs_after_processing: List[Dict[str, Any]] = []
            with open(temp_backup_file, "r", encoding="utf-8") as f:
                for line_number, line in enumerate(f, 1):
                    try:
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
                        logger.error(
                            f"Skipping unparseable line #{line_number} in backup: '{line.strip()}'. Error: {json_err}"
                        )
                        remaining_logs_after_processing.append(
                            {
                                "raw_line": line.strip(),
                                "error": "JSONDecodeError",
                                "line": line_number,
                            }
                        )
                    except Exception as e_line:
                        logger.error(
                            f"Skipping problematic line #{line_number} in backup: '{line.strip()}'. Error: {e_line}"
                        )
                        remaining_logs_after_processing.append(
                            {
                                "raw_line": line.strip(),
                                "error": str(e_line),
                                "line": line_number,
                            }
                        )
            if pending_logs_to_insert:
                logger.info(
                    f"Attempting to insert {len(pending_logs_to_insert)} parsed pending logs from backup."
                )
                all_batches_inserted_ok_from_pending = True
                for i in range(0, len(pending_logs_to_insert), LOG_WRITER_BATCH_SIZE):
                    batch_to_insert = pending_logs_to_insert[
                        i : i + LOG_WRITER_BATCH_SIZE
                    ]
                    self._insert_batch_to_db(
                        batch_to_insert, from_backup_processing=True
                    )
                    if (
                        os.path.exists(DB_LOG_BACKUP_FILE_PATH)
                        and os.path.getsize(DB_LOG_BACKUP_FILE_PATH) > 0
                    ):
                        all_batches_inserted_ok_from_pending = False
                        break
                if not all_batches_inserted_ok_from_pending:
                    processed_logs_successfully = False
            if (
                os.path.exists(DB_LOG_BACKUP_FILE_PATH)
                and os.path.getsize(DB_LOG_BACKUP_FILE_PATH) > 0
            ):
                logger.warning(
                    f"Some pending logs could not be re-inserted and remain in {DB_LOG_BACKUP_FILE_PATH}."
                )
                processed_logs_successfully = False
                if os.path.exists(temp_backup_file):
                    try:
                        os.remove(temp_backup_file)
                    except OSError:
                        logger.error(
                            f"Could not remove temp file {temp_backup_file} after partial backup processing."
                        )
            elif remaining_logs_after_processing:
                logger.warning(
                    f"{len(remaining_logs_after_processing)} lines from backup were unparseable and were re-saved to {DB_LOG_BACKUP_FILE_PATH}."
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
                            f"Could not remove temp file {temp_backup_file} after saving unparseable lines."
                        )
            elif os.path.exists(temp_backup_file):
                try:
                    os.remove(temp_backup_file)
                    logger.info(
                        f"Successfully processed and cleared pending logs. Backup file {temp_backup_file} removed."
                    )
                except OSError as e_rem:
                    logger.error(
                        f"Error removing temp processing file {temp_backup_file}: {e_rem}"
                    )
            else:
                logger.info(
                    f"Pending logs processed. No further logs remain in backup files."
                )
            return processed_logs_successfully
        except FileNotFoundError:
            logger.info(
                f"Backup file for pending logs ({DB_LOG_BACKUP_FILE_PATH} or {temp_backup_file}) not found. Nothing to process."
            )
            return True
        except Exception as e:
            logger.error(
                f"CRITICAL: Error during pending log processing: {e}", exc_info=True
            )
            if os.path.exists(temp_backup_file) and not os.path.exists(
                DB_LOG_BACKUP_FILE_PATH
            ):
                try:
                    os.rename(temp_backup_file, DB_LOG_BACKUP_FILE_PATH)
                    logger.info(
                        f"Restored {temp_backup_file} to {DB_LOG_BACKUP_FILE_PATH} after processing error."
                    )
                except Exception as rename_err:
                    logger.error(
                        f"Failed to restore backup file {temp_backup_file} after error: {rename_err}"
                    )
            return False

    def shutdown(self):
        if not hasattr(self, "_initialized") or not self._initialized:
            return  # type: ignore
        if not LOG_WRITER_DB_ENABLED:
            return
        logger.info("LogWriterDB initiating shutdown...")
        self.stop_event.set()  # type: ignore
        try:
            dummy_datetime_str = get_current_datetime_str_for_log()
            shutdown_item: LogEntryData = (
                dummy_datetime_str,
                "Shutdown signal",
                "DEBUG",
                "SYSTEM",
                "LogWriterDB",
                "{}",
                SHUTDOWN_FLUSH_SIGNAL_ORIGIN,
            )
            self.log_queue.put_nowait(shutdown_item)  # type: ignore
        except Full:
            logger.debug("LogWriterDB queue full during shutdown signal.")
        except Exception as e:
            logger.warning(f"LogWriterDB: Error putting shutdown flush item: {e}")
        if self.worker_thread and self.worker_thread.is_alive():  # type: ignore
            logger.info("LogWriterDB: Waiting for worker thread to finish...")
            self.worker_thread.join(timeout=LOG_WRITER_FLUSH_INTERVAL_SECONDS + 10)  # type: ignore
            if self.worker_thread.is_alive():  # type: ignore
                logger.warning("LogWriterDB worker thread did not terminate in time.")
            else:
                logger.info("LogWriterDB worker thread joined successfully.")
        if self._db_pool:
            self._db_pool = None  # type: ignore
        logger.info("LogWriterDB shutdown complete.")
        with LogWriterDB._lock:
            LogWriterDB._instance = None
            if hasattr(LogWriterDB, "_disabled_logged_once"):
                delattr(LogWriterDB, "_disabled_logged_once")


# Global singleton instance
log_writer_db_instance: Optional[LogWriterDB] = LogWriterDB()
