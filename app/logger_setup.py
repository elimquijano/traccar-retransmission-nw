import logging
import sys
import os
import datetime
from app.config import LOG_LEVEL, LOGS_DIR_NAME


def setup_logging():
    """
    Configura el logger raíz con salida a consola y a archivo.
    Crea un archivo de log con el nombre log_YYYY_MM_DD.log para el día actual.

    Esta función:
    1. Crea el directorio de logs si no existe
    2. Configura handlers para consola y archivo
    3. Establece el formato de los logs
    4. Configura el nivel de logging
    5. Silencia logs de librerías verbosas
    6. Devuelve un logger configurado

    Returns:
        logging.Logger: Instancia del logger configurado
    """
    # Crear el directorio de logs si no existe
    if not os.path.exists(LOGS_DIR_NAME):
        try:
            os.makedirs(LOGS_DIR_NAME)
            # Usamos print aquí porque el logger aún no está configurado si esto falla
            print(f"INFO: Directorio de logs '{LOGS_DIR_NAME}' creado.")
        except OSError as e:
            print(
                f"ADVERTENCIA: No se pudo crear el directorio de logs '{LOGS_DIR_NAME}': {e}. "
                "El logging a archivo será deshabilitado."
            )
            # Configuramos solo StreamHandler si la creación del directorio falla
            log_format_console_only = "%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s"
            logging.basicConfig(
                level=LOG_LEVEL,
                format=log_format_console_only,
                handlers=[logging.StreamHandler(sys.stdout)],
            )
            # Obtenemos un logger DESPUÉS de basicConfig
            logger = logging.getLogger(__name__)
            logger.warning(
                f"Logging a archivo deshabilitado debido a fallo en la creación del directorio '{LOGS_DIR_NAME}'."
            )
            return logger

    # Formato de los logs: fecha - nivel - nombre - archivo:línea - mensaje
    log_format = (
        "%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s"
    )

    # Configuramos el handler de consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format))

    # Calculamos el nombre del archivo de log para el día actual
    current_date_str = datetime.datetime.now().strftime("%Y_%m_%d")
    log_file_name = f"log_{current_date_str}.log"  # Formato: log_AAAA_MM_DD.log
    log_file_path = os.path.join(LOGS_DIR_NAME, log_file_name)

    # Configuramos el FileHandler para escribir en el archivo calculado
    file_handler = logging.FileHandler(
        filename=log_file_path,
        encoding="utf-8",
        delay=False,  # Abrir el archivo inmediatamente (True lo abriría en la primera emisión)
    )
    file_handler.setFormatter(logging.Formatter(log_format))

    # Configuramos el logger raíz
    root_logger = logging.getLogger()

    # Limpiamos handlers existentes si setup_logging se llama múltiples veces (programación defensiva)
    if root_logger.hasHandlers():
        for h in root_logger.handlers[
            :
        ]:  # Iteramos sobre una copia para modificar la lista original
            root_logger.removeHandler(h)
            h.close()  # Cerramos el handler antes de removerlo

    # Establecemos el nivel en el logger raíz
    root_logger.setLevel(LOG_LEVEL)
    # Añadimos los handlers
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Silenciamos logs de librerías verbosas
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websocket").setLevel(logging.INFO)  # O WARNING según necesidad

    # Obtenemos un logger para este módulo
    logger = logging.getLogger(__name__)
    logger.info(
        f"Logger raíz configurado. Nivel de logging para consola y archivo: {LOG_LEVEL}. "
        f"Logging a archivo: '{log_file_path}'"
    )
    return logger
