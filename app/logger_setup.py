import logging
import sys
import os
import datetime
import threading
from app.config import LOG_LEVEL, LOGS_DIR_NAME

# Variable global para mantener el path del archivo de log actual
# Esto es para que un posible reconfigurador sepa qué handler quitar
current_log_file_path = None
file_handler_instance = None  # Mantener una referencia al handler


def get_log_file_path_for_current_date():
    """Calcula el path del archivo de log para la fecha actual."""
    current_date_str = datetime.datetime.now().strftime("%Y_%m_%d")
    log_file_name = f"log_{current_date_str}.log"
    return os.path.join(LOGS_DIR_NAME, log_file_name)


def setup_logging():
    global current_log_file_path, file_handler_instance  # Acceder a las variables globales

    new_log_file_path = get_log_file_path_for_current_date()

    # Si el path del log ha cambiado (cambio de día), o si es la primera vez
    if new_log_file_path != current_log_file_path or file_handler_instance is None:
        logger_for_setup = logging.getLogger(
            __name__
        )  # Logger temporal para este setup
        if file_handler_instance:
            logger_for_setup.info(
                f"Día cambiado. Cambiando archivo de log de '{current_log_file_path}' a '{new_log_file_path}'."
            )
            root_logger = logging.getLogger()
            root_logger.removeHandler(file_handler_instance)
            file_handler_instance.close()

        current_log_file_path = new_log_file_path  # Actualizar path global

        if not os.path.exists(LOGS_DIR_NAME):
            try:
                os.makedirs(LOGS_DIR_NAME)
                print(f"INFO: Directorio de logs '{LOGS_DIR_NAME}' creado.")
            except OSError as e:
                print(
                    f"ADVERTENCIA: No se pudo crear el directorio de logs '{LOGS_DIR_NAME}': {e}. El logging a archivo podría fallar."
                )
                # Configurar solo consola si falla creación de dir
                log_format_console_only = "%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s"
                logging.basicConfig(
                    level=LOG_LEVEL,
                    format=log_format_console_only,
                    handlers=[logging.StreamHandler(sys.stdout)],
                )
                temp_logger = logging.getLogger(__name__)
                temp_logger.warning(
                    f"Logging a archivo deshabilitado por fallo en creación de dir '{LOGS_DIR_NAME}'."
                )
                return temp_logger

        log_format = "%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s"
        log_formatter = logging.Formatter(log_format)

        # Crear el nuevo FileHandler para el día actual
        # Usamos FileHandler simple, y la "rotación" se maneja llamando a setup_logging() de nuevo.
        file_handler_instance = logging.FileHandler(
            filename=current_log_file_path,
            encoding="utf-8",
            delay=False,
        )
        file_handler_instance.setFormatter(log_formatter)
        file_handler_instance.setLevel(LOG_LEVEL)

        root_logger = logging.getLogger()
        # Limpiar TODOS los handlers antes de añadir los nuevos (consola + nuevo archivo)
        # Esto es importante si setup_logging se llama para reconfigurar.
        if root_logger.hasHandlers():
            for h in root_logger.handlers[:]:
                h.close()
                root_logger.removeHandler(h)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(LOG_LEVEL)

        root_logger.setLevel(LOG_LEVEL)
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler_instance)

        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("websocket").setLevel(logging.INFO)
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)

        logger_for_setup.info(
            f"Logger raíz reconfigurado. Nivel: {LOG_LEVEL}. Logueando a archivo: '{current_log_file_path}'"
        )

    return logging.getLogger(
        __name__
    )  # Devuelve un logger normal para el módulo que llama


# --- Hilo para chequear y rotar el log diariamente ---
# Esta parte es la que manejaría la rotación si la app corre continuamente.
_log_rotation_stop_event = threading.Event()
_log_rotation_thread = None


def _daily_log_rotation_check():
    """Chequea si es necesario rotar el log y llama a setup_logging() para reconfigurar."""
    while not _log_rotation_stop_event.wait(60):  # Chequear cada 60 segundos
        # No es necesario hacer nada explícito aquí si setup_logging() se llama
        # desde algún punto que detecte el cambio de día.
        # Alternativamente, podemos forzar una llamada a setup_logging() para que
        # recalcule el nombre del archivo.
        logger_rot_check = logging.getLogger("LogRotationCheck")  # Logger específico
        logger_rot_check.debug("Chequeando necesidad de rotación de log...")
        # Forzar re-evaluación del nombre del archivo y posible reconfiguración del handler
        # Al llamar a setup_logging(), si la fecha cambió, recreará el handler de archivo.
        # Esto es un poco "forzado" y podría tener implicaciones si otros módulos
        # mantienen referencias a los handlers antiguos.
        # La solución con TimedRotatingFileHandler es más limpia para rotación automática.
        # PERO, si quieres que el archivo ACTIVO tenga la fecha, esto es una vía.

        # Si el nombre del archivo actual calculado es diferente al global, reconfigurar.
        if get_log_file_path_for_current_date() != current_log_file_path:
            logger_rot_check.info(
                "Detectado cambio de día. Reconfigurando logger para nuevo archivo."
            )
            setup_logging()  # Esto reconfigurará los handlers


def start_log_rotation_thread():
    global _log_rotation_thread
    if _log_rotation_thread is None or not _log_rotation_thread.is_alive():
        _log_rotation_stop_event.clear()
        _log_rotation_thread = threading.Thread(
            target=_daily_log_rotation_check, name="LogRotationChecker", daemon=True
        )
        _log_rotation_thread.start()
        logging.getLogger(__name__).info(
            "Hilo de chequeo de rotación de logs iniciado."
        )


def stop_log_rotation_thread():
    global _log_rotation_thread
    if _log_rotation_thread and _log_rotation_thread.is_alive():
        logging.getLogger(__name__).info(
            "Deteniendo hilo de chequeo de rotación de logs..."
        )
        _log_rotation_stop_event.set()
        _log_rotation_thread.join(timeout=5)
        if _log_rotation_thread.is_alive():
            logging.getLogger(__name__).warning(
                "Hilo de chequeo de rotación de logs no terminó a tiempo."
            )
        else:
            logging.getLogger(__name__).info(
                "Hilo de chequeo de rotación de logs detenido."
            )
        _log_rotation_thread = None
