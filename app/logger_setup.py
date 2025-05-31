import logging
import sys
from app.config import LOG_LEVEL

def setup_logging():
    """Configures the root logger."""
    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s"
    logging.basicConfig(
        level=LOG_LEVEL,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
            # Puedes añadir FileHandler si quieres guardar logs en archivos
            # logging.FileHandler("retransmitter.log")
        ]
    )
    # Silenciar logs de librerías muy verbosas si es necesario
    # logging.getLogger("requests").setLevel(logging.WARNING)
    # logging.getLogger("urllib3").setLevel(logging.WARNING)
    # logging.getLogger("websocket").setLevel(logging.WARNING) # O INFO para ver sus mensajes

    logger = logging.getLogger(__name__)
    logger.info("Logging configured.")
    return logger

# Configura el logger al importar el módulo para que esté disponible globalmente
# si se desea, aunque es mejor obtener el logger específico en cada módulo.
# setup_logging()