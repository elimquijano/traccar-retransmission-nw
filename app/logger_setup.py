import logging
import sys
from app.config import LOG_LEVEL # Asegúrate que config.py exista y LOG_LEVEL esté definido

def setup_logging():
    """Configures the root logger."""
    log_format = "%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s"
    logging.basicConfig(
        level=LOG_LEVEL,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
            # logging.FileHandler("retransmitter.log", mode='a') # Opcional: log a archivo
        ]
    )
    # Silenciar logs de librerías muy verbosas si es necesario
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websocket").setLevel(logging.INFO) # O WARNING si es mucho

    # Obtener un logger para este módulo y loguear que la configuración se ha aplicado
    logger = logging.getLogger(__name__) # Obtiene un logger con el nombre del módulo actual
    logger.info(f"Registrador raíz configurado con nivel: {LOG_LEVEL}")
    return logger # Devuelve el logger de este módulo, pero la configuración es global