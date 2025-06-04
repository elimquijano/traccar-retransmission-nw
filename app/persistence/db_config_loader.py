# Importamos las bibliotecas que necesitamos
import mysql.connector  # Para conectarnos a MySQL
import logging  # Para registrar mensajes y errores
from typing import Dict, Any, Optional  # Para definir tipos de datos
from app.config import DB_CONN_PARAMS  # Parámetros de conexión a la base de datos

# Configuramos el registro de mensajes (logging)
logger = logging.getLogger(__name__)


def load_retransmission_configs_from_db1() -> Optional[Dict[int, Dict[str, Any]]]:
    """
    Carga las configuraciones de retransmisión desde la base de datos.

    Esta función:
    1. Se conecta a la base de datos MySQL
    2. Ejecuta una consulta para obtener configuraciones
    3. Procesa los resultados
    4. Devuelve un diccionario con las configuraciones o None si hay error

    Returns:
        Un diccionario donde:
        - Las claves son IDs de dispositivos (números enteros)
        - Los valores son diccionarios con la configuración de cada dispositivo
        O None si ocurre un error.
    """
    # Verificamos si tenemos todos los parámetros de conexión
    if not DB_CONN_PARAMS or not all(DB_CONN_PARAMS.values()):
        logger.error("Faltan parámetros de conexión a la base de datos.")
        return None

    logger.info("Cargando configuraciones desde la base de datos...")

    # Creamos un diccionario vacío para guardar los resultados
    db_retransmission_config_cache = {}
    conn = None  # Inicialmente no hay conexión
    cursor = None  # Inicialmente no hay cursor

    # Definimos la consulta SQL que vamos a ejecutar
    query = """
        SELECT gr.Id_device, gr.imei, gh.Bypass, gh.Host, gh.Token
        FROM g_retransmission AS gr
        JOIN g_host_retransmission AS gh ON gr.Id_host = gh.Id;
    """

    try:
        # Preparamos los parámetros de conexión
        temp_conn_params = DB_CONN_PARAMS.copy()
        temp_conn_params.pop("pool_name", None)  # Eliminamos parámetros no necesarios
        temp_conn_params.pop("pool_size", None)

        # Nos conectamos a la base de datos
        conn = mysql.connector.connect(**temp_conn_params)

        # Creamos un cursor que devuelve los resultados como diccionarios
        cursor = conn.cursor(dictionary=True)

        # Ejecutamos la consulta
        cursor.execute(query)

        # Obtenemos todos los resultados
        results = cursor.fetchall()

        # Procesamos cada fila de resultados
        for row in results:
            try:
                # Convertimos el ID a entero y guardamos la configuración
                traccar_device_id = int(row["Id_device"])
                db_retransmission_config_cache[traccar_device_id] = {
                    "host_url": row.get("Host"),  # URL del host
                    "id_municipalidad": row.get("Token"),  # Token de identificación
                    "imei": row.get("imei"),  # Número IMEI del dispositivo
                    "bypass": row.get("Bypass"),  # Configuración de bypass
                }
            except (TypeError, ValueError) as e:
                # Si hay un error en los datos, lo registramos y continuamos
                logger.error(f"Error en los datos de la fila: {row}. Error: {e}")
                continue

        # Registramos cuántas configuraciones se cargaron
        logger.info(
            f"Se cargaron {len(db_retransmission_config_cache)} configuraciones."
        )
        return db_retransmission_config_cache

    except mysql.connector.Error as err:
        # Manejo de errores específicos de MySQL
        logger.error(f"Error al conectar o consultar la base de datos: {err}")
        return None

    except Exception as e:
        # Manejo de cualquier otro error inesperado
        logger.error(f"Error inesperado al cargar configuraciones: {e}", exc_info=True)
        return None

    finally:
        # Este bloque siempre se ejecuta, haya error o no
        if cursor:
            cursor.close()  # Cerramos el cursor
        if conn and conn.is_connected():
            conn.close()  # Cerramos la conexión
            logger.debug("Conexión a la base de datos cerrada.")
