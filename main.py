import asyncio
import uvloop

uvloop.install()
import logging
import threading
import signal
import os

# Importamos la configuración del logging
from app.logger_setup import (
    setup_logging,
    start_log_rotation_thread,
    stop_log_rotation_thread,
)

# Configuramos el logging
logger = setup_logging()

# Importamos configuraciones necesarias
from app.config import (
    RECONNECT_DELAY_SECONDS,
    INITIAL_LOAD_RETRY_DELAY_SECONDS,
    LOG_WRITER_DB_ENABLED,
)

# Importamos los componentes necesarios
from app.traccar_client import TraccarClient
from app.services.retransmission_manager import RetransmissionManager
from app.persistence.db_config_loader import load_retransmission_configs_from_db1
from app.persistence.log_writer_db import log_writer_db_instance

# Evento global para manejar el apagado
global_shutdown_event = threading.Event()


def os_signal_handler(signum, frame):
    """
    Manejador de señales del sistema operativo para apagado ordenado.

    Args:
        signum: Número de la señal recibida
        frame: Frame actual de ejecución
    """
    # Obtenemos el nombre de la señal
    signal_name = (
        signal.Signals(signum).name if hasattr(signal, "Signals") else f"Señal {signum}"
    )
    logger.info(f"Señal {signal_name} recibida. Iniciando apagado ordenado...")
    if not global_shutdown_event.is_set():
        global_shutdown_event.set()
        # Intentar cancelar la tarea principal de asyncio si está corriendo


def on_traccar_ws_open(ws_app_instance):
    """
    Callback llamado cuando se abre la conexión WebSocket de Traccar.

    Args:
        ws_app_instance: Instancia de WebSocketApp que se abrió
    """
    logger.info("Callback: Conexión WebSocket de Traccar abierta exitosamente.")


def on_traccar_ws_close(ws_app_instance, close_status_code, close_msg):
    """
    Callback llamado cuando se cierra la conexión WebSocket de Traccar.

    Args:
        ws_app_instance: Instancia de WebSocketApp
        close_status_code: Código de estado de cierre
        close_msg: Mensaje de cierre
    """
    logger.warning(
        f"Callback: WebSocket de Traccar cerrado. Código: {close_status_code}, "
        f"Mensaje: {close_msg}"
    )


def on_traccar_ws_error(ws_app_instance, error):
    """
    Callback llamado cuando ocurre un error en el WebSocket de Traccar.

    Args:
        ws_app_instance: Instancia de WebSocketApp
        error: Excepción que ocurrió
    """
    logger.error(f"Callback: Error recibido desde WebSocket de Traccar: {error}")


async def run_application_async(loop: asyncio.AbstractEventLoop):
    """
    Función principal asíncrona que ejecuta la aplicación.

    Args:
        loop: Loop de eventos de asyncio
    """
    logger.info("Núcleo de la aplicación asíncrona iniciando...")
    start_log_rotation_thread()  # Iniciamos el hilo de chequeo de rotación de logs

    # Procesamos logs pendientes si está habilitado el LogWriter
    if LOG_WRITER_DB_ENABLED and log_writer_db_instance:
        logger.info("Verificando logs pendientes de ejecuciones previas...")
        try:
            log_writer_db_instance.process_pending_logs_from_backup()
        except Exception as e_backup:
            logger.error(
                f"Error procesando logs de respaldo: {e_backup}", exc_info=True
            )

    # Inicializamos el gestor de retransmisión
    retransmission_mgr = RetransmissionManager(loop=loop)

    # Inicializamos el cliente Traccar con los callbacks necesarios
    traccar_cli = TraccarClient(
        message_callback=retransmission_mgr.handle_traccar_websocket_message,
        open_callback=on_traccar_ws_open,
        close_callback=on_traccar_ws_close,
        error_callback=on_traccar_ws_error,
    )
    retransmission_mgr.set_traccar_client(traccar_cli)

    # Iniciamos el worker de retransmisión asíncrono
    await retransmission_mgr.start_retransmission_worker_async()

    # Evento para detener el hilo de operaciones síncronas de Traccar
    traccar_logic_thread_stop_event = threading.Event()

    def traccar_sync_operations_loop():
        """
        Bucle de operaciones síncronas para Traccar.
        Maneja el login, obtención de dispositivos y conexión WebSocket.
        """
        logger.info("Hilo de operaciones síncronas de Traccar iniciado.")
        while (
            not global_shutdown_event.is_set()
            and not traccar_logic_thread_stop_event.is_set()
        ):
            try:
                # --- CORRECCIÓN AQUÍ ---
                # Se verifica traccar_cli._session.cookies en lugar de traccar_cli.session_cookies
                if not traccar_cli._session.cookies:
                    logger.info("Intentando login en Traccar (desde hilo síncrono)...")
                    if not traccar_cli.login():
                        logger.error(
                            f"Login en Traccar fallido. Reintentando en {INITIAL_LOAD_RETRY_DELAY_SECONDS}s..."
                        )
                        if (
                            global_shutdown_event.wait(INITIAL_LOAD_RETRY_DELAY_SECONDS)
                            or traccar_logic_thread_stop_event.is_set()
                        ):
                            break  # Salimos si se solicita parada
                        continue

                # El resto de la lógica permanece igual
                if not traccar_cli.traccar_devices_cache:
                    logger.info(
                        "Obteniendo dispositivos de Traccar (desde hilo síncrono)..."
                    )
                    if not traccar_cli.fetch_devices():
                        logger.error(
                            f"Fallo al obtener dispositivos de Traccar. Reintentando en {INITIAL_LOAD_RETRY_DELAY_SECONDS}s..."
                        )
                        if (
                            global_shutdown_event.wait(INITIAL_LOAD_RETRY_DELAY_SECONDS)
                            or traccar_logic_thread_stop_event.is_set()
                        ):
                            break
                        continue

                if not retransmission_mgr.retransmission_configs_db1:
                    logger.info(
                        "Cargando configuraciones de retransmisión desde BD (desde hilo síncrono)..."
                    )
                    current_db1_configs = load_retransmission_configs_from_db1()
                    if current_db1_configs is not None:
                        retransmission_mgr.update_retransmission_configs_from_db1(
                            current_db1_configs
                        )
                    else:
                        logger.error(
                            f"Fallo al obtener configuraciones de retransmisión. Reintentando en {INITIAL_LOAD_RETRY_DELAY_SECONDS}s..."
                        )
                        if (
                            global_shutdown_event.wait(INITIAL_LOAD_RETRY_DELAY_SECONDS)
                            or traccar_logic_thread_stop_event.is_set()
                        ):
                            break
                        continue

                logger.info(
                    "Intentando conexión WebSocket de Traccar (desde hilo síncrono)..."
                )
                traccar_cli.connect_websocket()

                if (
                    global_shutdown_event.is_set()
                    or traccar_logic_thread_stop_event.is_set()
                ):
                    break

                logger.warning(
                    "WebSocket de Traccar desconectado. Reconectando en bucle síncrono..."
                )
                if (
                    global_shutdown_event.wait(RECONNECT_DELAY_SECONDS)
                    or traccar_logic_thread_stop_event.is_set()
                ):
                    break

            except Exception as e_sync:
                logger.critical(
                    f"Excepción en bucle de operaciones síncronas de Traccar: {e_sync}",
                    exc_info=True,
                )
                if (
                    global_shutdown_event.is_set()
                    or traccar_logic_thread_stop_event.is_set()
                ):
                    break
                if (
                    global_shutdown_event.wait(RECONNECT_DELAY_SECONDS)
                    or traccar_logic_thread_stop_event.is_set()
                ):
                    break

        logger.info("Hilo de operaciones síncronas de Traccar finalizado.")

    # Iniciamos el hilo para operaciones síncronas de Traccar
    traccar_thread = threading.Thread(
        target=traccar_sync_operations_loop, name="TraccarSyncThread", daemon=True
    )
    traccar_thread.start()

    # Bucle principal de la aplicación
    try:
        while not global_shutdown_event.is_set():
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("run_application_async fue cancelado.")
    finally:
        logger.info("Bucle de ejecución asíncrona ha finalizado o sido interrumpido.")
        logger.info("Iniciando apagado de servicios de la aplicación...")

        # Señalizamos al hilo síncrono de Traccar para que se detenga
        logger.info(
            "Señalizando al bucle de operaciones síncronas de Traccar para detenerse..."
        )
        traccar_logic_thread_stop_event.set()

        # Cerramos la conexión WebSocket si existe
        if traccar_cli:
            traccar_cli.close_websocket()

        # Esperamos a que el hilo síncrono termine
        if traccar_thread.is_alive():
            logger.info("Esperando a que el hilo síncrono se una...")
            traccar_thread.join(timeout=RECONNECT_DELAY_SECONDS + 5)
            if traccar_thread.is_alive():
                logger.warning("El hilo síncrono no se unió correctamente.")
            else:
                logger.info("Hilo síncrono unido.")

        # Detenemos el worker de retransmisión
        if retransmission_mgr:
            await retransmission_mgr.stop_retransmission_worker_async()

        # Apagamos el LogWriter si está habilitado
        if LOG_WRITER_DB_ENABLED and log_writer_db_instance:
            logger.info("Solicitando apagado de LogWriterDB...")
            log_writer_db_instance.shutdown()
            logger.info(
                "Secuencia de apagado de LogWriterDB completada por la aplicación principal."
            )

        stop_log_rotation_thread()
        logger.info(
            "Todos los servicios de la aplicación han sido señalizados para detenerse o han completado su apagado."
        )


if __name__ == "__main__":
    logger.info(
        f"Iniciando Aplicación de Retransmisión Traccar (PID: {os.getpid()})..."
    )

    # Configuramos manejadores de señales para apagado ordenado
    signal.signal(signal.SIGINT, os_signal_handler)
    signal.signal(signal.SIGTERM, os_signal_handler)

    # Creamos y configuramos el loop de eventos de asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Creamos la tarea principal de la aplicación
    main_task = loop.create_task(run_application_async(loop))

    try:
        # Ejecutamos hasta que la tarea principal complete
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        logger.info(
            "KeyboardInterrupt recibido en __main__ (asyncio). Señalizando apagado..."
        )
        if not global_shutdown_event.is_set():
            global_shutdown_event.set()
        if not main_task.done():
            logger.info("Cancelando tarea principal de la aplicación...")
            main_task.cancel()
            try:
                loop.run_until_complete(main_task)
            except asyncio.CancelledError:
                logger.info("Tarea principal de la aplicación cancelada exitosamente.")
            except Exception as e_final_cancel:
                logger.error(
                    f"Excepción durante cancelación de tarea principal: {e_final_cancel}"
                )
    except Exception as e_main_async:
        logger.critical(
            f"Excepción no manejada en ejecución asíncrona de __main__: {e_main_async}",
            exc_info=True,
        )
        if not global_shutdown_event.is_set():
            global_shutdown_event.set()
        if not main_task.done():
            main_task.cancel()
            try:
                loop.run_until_complete(main_task)
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
    finally:
        logger.info("Hilo principal (__main__): Finalizando loop de asyncio...")
        current_loop = (
            asyncio.get_event_loop()
        )  # Obtenemos el loop actual para limpieza

        try:
            # Cancelamos tareas pendientes
            pending_tasks = [
                task for task in asyncio.all_tasks(loop=current_loop) if not task.done()
            ]
            if pending_tasks:
                logger.info(
                    f"Cancelando {len(pending_tasks)} tareas pendientes de asyncio..."
                )
                for task_to_cancel in pending_tasks:
                    task_to_cancel.cancel()
                current_loop.run_until_complete(
                    asyncio.gather(*pending_tasks, return_exceptions=True)
                )
                logger.info("Tareas pendientes de asyncio procesadas.")
        except RuntimeError:
            logger.warning(
                "Loop de asyncio ya cerrado al intentar cancelar tareas pendientes."
            )
        except Exception as e_cleanup:
            logger.error(f"Error durante limpieza de tareas de asyncio: {e_cleanup}")

        # Cerramos el loop de asyncio si no está cerrado
        if not current_loop.is_closed():
            current_loop.close()
            logger.info("Loop de asyncio cerrado.")

        # Apagamos el sistema de logging
        logging.shutdown()
        print("Programa finalizado.")
