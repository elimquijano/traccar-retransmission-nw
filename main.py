# main.py
import asyncio
import uvloop

uvloop.install()
import logging
import threading
import signal
import os
import time

from app.logger_setup import (
    setup_logging,
    start_log_rotation_thread,
    stop_log_rotation_thread,
)

logger = setup_logging()

from app.config import (
    RECONNECT_DELAY_SECONDS,
    INITIAL_LOAD_RETRY_DELAY_SECONDS,
    LOG_WRITER_DB_ENABLED,
    REFRESH_DEVICES_CONFIG_INTERVAL_SECONDS,  # Asumiendo que quieres mantener el refresco periódico de fondo
)
from app.traccar_client import TraccarClient
from app.services.retransmission_manager import RetransmissionManager
from app.persistence.db_config_loader import load_retransmission_configs_from_db1
from app.persistence.log_writer_db import log_writer_db_instance

global_shutdown_event = threading.Event()


def os_signal_handler(signum, frame):
    signal_name = (
        signal.Signals(signum).name
        if hasattr(signal, "Signals")
        else f"Signal {signum}"
    )
    logger.info(f"{signal_name} recibida. Iniciando apagado ordenado...")
    if not global_shutdown_event.is_set():
        global_shutdown_event.set()


def on_traccar_ws_open(ws_app_instance):
    logger.info("Callback: Conexión WebSocket de Traccar abierta exitosamente.")


def on_traccar_ws_close(ws_app_instance, close_status_code, close_msg):
    logger.warning(
        f"Callback: WebSocket de Traccar cerrado. Código: {close_status_code}, Mensaje: {close_msg}"
    )


def on_traccar_ws_error(ws_app_instance, error):
    logger.error(f"Callback: Error recibido desde WebSocket de Traccar: {error}")


async def run_application_async(loop: asyncio.AbstractEventLoop):
    logger.info("Núcleo de la aplicación asíncrona iniciando...")
    start_log_rotation_thread()

    if LOG_WRITER_DB_ENABLED and log_writer_db_instance:
        logger.info("Verificando logs pendientes de ejecuciones previas...")
        try:
            log_writer_db_instance.process_pending_logs_from_backup()
        except Exception as e_backup:
            logger.error(
                f"Error procesando logs de respaldo: {e_backup}", exc_info=True
            )

    retransmission_mgr = RetransmissionManager(loop=loop)
    traccar_cli = TraccarClient(
        message_callback=retransmission_mgr.handle_traccar_websocket_message,
        open_callback=on_traccar_ws_open,
        close_callback=on_traccar_ws_close,
        error_callback=on_traccar_ws_error,
    )
    retransmission_mgr.set_traccar_client(traccar_cli)
    await retransmission_mgr.start_retransmission_worker_async()

    traccar_logic_thread_stop_event = threading.Event()

    def traccar_sync_operations_loop():
        logger.info("Hilo de operaciones síncronas de Traccar iniciado.")
        last_refresh_time = 0
        while (
            not global_shutdown_event.is_set()
            and not traccar_logic_thread_stop_event.is_set()
        ):
            current_time = time.time()
            perform_periodic_refresh = (
                REFRESH_DEVICES_CONFIG_INTERVAL_SECONDS > 0
                and current_time - last_refresh_time
                > REFRESH_DEVICES_CONFIG_INTERVAL_SECONDS
            )
            try:
                # --- CORRECCIÓN DEFINITIVA ---
                # Verificar si hay cookies en el objeto de sesión de TraccarClient
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
                            break
                        continue
                    else:  # Forzar refresco después de un login exitoso
                        perform_periodic_refresh = True

                # Carga/Refresco de datos inicial o periódico
                if (
                    not traccar_cli.traccar_devices_cache
                    or not retransmission_mgr.retransmission_configs_db1
                    or perform_periodic_refresh
                ):
                    logger.info(
                        f"Refrescando cachés (Periódico: {perform_periodic_refresh}, Inicial: {not traccar_cli.traccar_devices_cache or not retransmission_mgr.retransmission_configs_db1})..."
                    )
                    if traccar_cli.fetch_devices():
                        db_configs = load_retransmission_configs_from_db1()
                        if db_configs is not None:
                            retransmission_mgr.update_retransmission_configs_from_db1(
                                db_configs
                            )
                            last_refresh_time = current_time  # Actualizar tiempo del último refresco exitoso
                        else:
                            logger.error("Fallo al refrescar configuraciones de BD.")
                    else:
                        logger.error("Fallo al refrescar dispositivos de Traccar.")

                # Conexión WebSocket
                if not (
                    traccar_cli.ws_app
                    and traccar_cli.ws_app.sock
                    and traccar_cli.ws_app.sock.connected
                ):
                    logger.info(
                        "Intentando conexión WebSocket de Traccar (desde hilo síncrono)..."
                    )
                    traccar_cli.connect_websocket()  # Bloqueante para este hilo
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
                else:  # Si el WS ya está conectado, esperar un tiempo antes de la siguiente iteración
                    wait_time = min(
                        60.0,
                        max(
                            1,
                            REFRESH_DEVICES_CONFIG_INTERVAL_SECONDS
                            - (current_time - last_refresh_time),
                        ),
                    )
                    if (
                        global_shutdown_event.wait(wait_time)
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

    traccar_thread = threading.Thread(
        target=traccar_sync_operations_loop, name="TraccarSyncThread", daemon=True
    )
    traccar_thread.start()

    try:
        while not global_shutdown_event.is_set():
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("run_application_async fue cancelado.")
    finally:
        logger.info("Bucle de ejecución asíncrona ha finalizado o sido interrumpido.")
        logger.info("Iniciando apagado de servicios de la aplicación...")
        logger.info(
            "Señalizando al bucle de operaciones síncronas de Traccar para detenerse..."
        )
        traccar_logic_thread_stop_event.set()
        if traccar_cli:
            traccar_cli.close_websocket()
        if traccar_thread.is_alive():
            logger.info("Esperando a que el hilo síncrono se una...")
            traccar_thread.join(timeout=RECONNECT_DELAY_SECONDS + 5)
            if traccar_thread.is_alive():
                logger.warning("El hilo síncrono no se unió correctamente.")
            else:
                logger.info("Hilo síncrono unido.")
        if retransmission_mgr:
            await retransmission_mgr.stop_retransmission_worker_async()
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
    signal.signal(signal.SIGINT, os_signal_handler)
    signal.signal(signal.SIGTERM, os_signal_handler)

    loop = asyncio.get_event_loop()  # uvloop.install() ya lo reemplazó

    main_task = loop.create_task(run_application_async(loop))

    try:
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
        current_loop = asyncio.get_running_loop()
        try:
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
        if not current_loop.is_closed():
            current_loop.close()
            logger.info("Loop de asyncio cerrado.")

        logging.shutdown()
        print("Programa finalizado.")
