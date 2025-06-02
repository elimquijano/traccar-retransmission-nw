import logging
import sys
import os
import datetime  # Para obtener la fecha actual
from app.config import LOG_LEVEL, LOGS_DIR_NAME  # LOGS_DIR_NAME viene de config.py


def setup_logging():
    """
    Configures the root logger with console output and file output
    to a file named log_YYYY_MM_DD.log for the current day.
    """

    # Crear el directorio de logs si no existe
    if not os.path.exists(LOGS_DIR_NAME):
        try:
            os.makedirs(LOGS_DIR_NAME)
            # Usar print aquí porque el logger aún no está configurado si esto falla.
            print(f"INFO: Logs directory '{LOGS_DIR_NAME}' created.")
        except OSError as e:
            print(
                f"WARNING: Could not create logs directory '{LOGS_DIR_NAME}': {e}. File logging will be disabled."
            )
            # Configurar solo StreamHandler si la creación del directorio falla
            log_format_console_only = "%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s"
            logging.basicConfig(
                level=LOG_LEVEL,
                format=log_format_console_only,
                handlers=[logging.StreamHandler(sys.stdout)],
            )
            # Obtener un logger DESPUÉS de basicConfig
            logger = logging.getLogger(__name__)
            logger.warning(
                f"File logging disabled due to directory creation failure for '{LOGS_DIR_NAME}'."
            )
            return logger

    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s"

    # Configurar el handler de consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format))

    # --- Calcular el nombre del archivo de log para el día actual ---
    current_date_str = datetime.datetime.now().strftime("%Y_%m_%d")
    log_file_name = f"log_{current_date_str}.log"  # Formato: log_YYYY_MM_DD.log
    log_file_path = os.path.join(LOGS_DIR_NAME, log_file_name)
    # --- Fin del cálculo del nombre ---

    # Usar FileHandler. Este handler escribirá en el archivo calculado.
    # Si la aplicación se reinicia (ej. por PM2 diariamente), se calculará un nuevo nombre de archivo.
    # No hay rotación automática de archivos antiguos con este handler; se acumularán.
    file_handler = logging.FileHandler(
        filename=log_file_path,
        encoding="utf-8",
        delay=False,  # Abrir el archivo inmediatamente (True lo abre en la primera emisión)
    )
    file_handler.setFormatter(logging.Formatter(log_format))

    # Configurar el logger raíz
    root_logger = logging.getLogger()
    # Limpiar handlers existentes si setup_logging se llama múltiples veces (defensivo)
    if root_logger.hasHandlers():
        for h in root_logger.handlers[
            :
        ]:  # Iterar sobre una copia para modificar la lista original
            root_logger.removeHandler(h)
            h.close()  # Cerrar el handler antes de removerlo

    root_logger.setLevel(LOG_LEVEL)  # Establecer nivel en el logger raíz
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)  # Añadir el FileHandler

    # Silenciar logs de librerías verbosas
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websocket").setLevel(logging.INFO)  # O WARNING según necesidad

    logger = logging.getLogger(__name__)  # Obtener un logger para este módulo
    logger.info(
        f"Root logger configured. Console and File level: {LOG_LEVEL}. Logging to file: '{log_file_path}'"
    )
    return logger
