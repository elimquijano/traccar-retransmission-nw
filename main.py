# main.py
import logging
import asyncio
import threading
import signal
import os

from app.logger_setup import setup_logging

logger = setup_logging()

from app.config import (
    RECONNECT_DELAY_SECONDS,
    INITIAL_LOAD_RETRY_DELAY_SECONDS,
    LOG_WRITER_DB_ENABLED,
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
    logger.info(f"{signal_name} received. Initiating graceful shutdown...")
    if not global_shutdown_event.is_set():
        global_shutdown_event.set()
        # Intentar cancelar la tarea principal de asyncio si está corriendo
        # Esto es un poco más complejo porque el loop puede no estar accesible directamente aquí
        # La tarea asyncio principal debe chequear global_shutdown_event.


def on_traccar_ws_open(ws_app_instance):
    logger.info("Callback: Traccar WebSocket successfully opened.")


def on_traccar_ws_close(ws_app_instance, close_status_code, close_msg):
    logger.warning(
        f"Callback: Traccar WebSocket closed. Code: {close_status_code}, Msg: {close_msg}"
    )


def on_traccar_ws_error(ws_app_instance, error):
    logger.error(f"Callback: Received error from Traccar WebSocket: {error}")


async def run_application_async(loop: asyncio.AbstractEventLoop):  # Aceptar el loop
    logger.info("Async Application core starting...")

    if LOG_WRITER_DB_ENABLED and log_writer_db_instance:
        logger.info("Checking for pending logs from previous runs...")
        try:
            log_writer_db_instance.process_pending_logs_from_backup()
        except Exception as e_backup:
            logger.error(f"Error processing backup logs: {e_backup}", exc_info=True)

    retransmission_mgr = RetransmissionManager(loop=loop)  # <--- PASAR EL LOOP

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
        logger.info("Traccar sync operations loop thread started.")
        while (
            not global_shutdown_event.is_set()
            and not traccar_logic_thread_stop_event.is_set()
        ):
            try:
                if not traccar_cli.session_cookies:
                    logger.info("Attempting Traccar login (from sync thread)...")
                    if not traccar_cli.login():
                        logger.error(
                            f"Traccar login failed. Retrying in {INITIAL_LOAD_RETRY_DELAY_SECONDS}s..."
                        )
                        if (
                            global_shutdown_event.wait(INITIAL_LOAD_RETRY_DELAY_SECONDS)
                            or traccar_logic_thread_stop_event.is_set()
                        ):
                            break  # Salir si se detiene
                        continue

                if not traccar_cli.traccar_devices_cache:
                    logger.info("Fetching Traccar devices (from sync thread)...")
                    if not traccar_cli.fetch_devices():
                        logger.error(
                            f"Failed to fetch Traccar devices. Retrying in {INITIAL_LOAD_RETRY_DELAY_SECONDS}s..."
                        )
                        if (
                            global_shutdown_event.wait(INITIAL_LOAD_RETRY_DELAY_SECONDS)
                            or traccar_logic_thread_stop_event.is_set()
                        ):
                            break
                        continue

                if not retransmission_mgr.retransmission_configs_db1:
                    logger.info(
                        "Loading retransmission configurations from DB (from sync thread)..."
                    )
                    current_db1_configs = load_retransmission_configs_from_db1()
                    if current_db1_configs is not None:
                        retransmission_mgr.update_retransmission_configs_from_db1(
                            current_db1_configs
                        )
                    else:
                        logger.error(
                            f"Failed to fetch retransmission configs. Retrying in {INITIAL_LOAD_RETRY_DELAY_SECONDS}s..."
                        )
                        if (
                            global_shutdown_event.wait(INITIAL_LOAD_RETRY_DELAY_SECONDS)
                            or traccar_logic_thread_stop_event.is_set()
                        ):
                            break
                        continue

                logger.info(
                    "Attempting Traccar WebSocket connection (from sync thread)..."
                )
                traccar_cli.connect_websocket()

                if (
                    global_shutdown_event.is_set()
                    or traccar_logic_thread_stop_event.is_set()
                ):
                    break
                logger.warning(
                    "Traccar WebSocket disconnected. Reconnecting in sync loop..."
                )
                if (
                    global_shutdown_event.wait(RECONNECT_DELAY_SECONDS)
                    or traccar_logic_thread_stop_event.is_set()
                ):
                    break

            except Exception as e_sync:
                logger.critical(
                    f"Exception in Traccar sync operations loop: {e_sync}",
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
        logger.info("Traccar sync operations loop thread finished.")

    traccar_thread = threading.Thread(
        target=traccar_sync_operations_loop, name="TraccarSyncThread", daemon=True
    )
    traccar_thread.start()

    try:
        while not global_shutdown_event.is_set():
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("run_application_async was cancelled.")
    finally:
        logger.info("Async Application run loop has finished or been interrupted.")
        logger.info("Initiating shutdown of application services...")

        logger.info("Signaling Traccar sync operations loop to stop...")
        traccar_logic_thread_stop_event.set()
        if traccar_cli:
            traccar_cli.close_websocket()
        if traccar_thread.is_alive():
            logger.info("Waiting for Traccar sync thread to join...")
            traccar_thread.join(timeout=RECONNECT_DELAY_SECONDS + 5)
            if traccar_thread.is_alive():
                logger.warning("Traccar sync thread did not join cleanly.")
            else:
                logger.info("Traccar sync thread joined.")

        if retransmission_mgr:
            await retransmission_mgr.stop_retransmission_worker_async()

        if LOG_WRITER_DB_ENABLED and log_writer_db_instance:
            logger.info("Requesting LogWriterDB shutdown...")
            log_writer_db_instance.shutdown()
            logger.info("LogWriterDB shutdown sequence completed by main app.")

        logger.info(
            "All application services have been signaled to stop or completed shutdown."
        )


if __name__ == "__main__":
    logger.info(f"Starting Traccar Retransmitter Application (PID: {os.getpid()})...")

    signal.signal(signal.SIGINT, os_signal_handler)
    signal.signal(signal.SIGTERM, os_signal_handler)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Pasar el loop a run_application_async
    main_task = loop.create_task(run_application_async(loop))  # <--- PASAR EL LOOP

    try:
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        logger.info(
            "KeyboardInterrupt received in __main__ (asyncio). Signaling shutdown..."
        )
        if not global_shutdown_event.is_set():
            global_shutdown_event.set()
        if not main_task.done():
            logger.info("Cancelling main application task...")
            main_task.cancel()
            try:
                loop.run_until_complete(main_task)
            except asyncio.CancelledError:
                logger.info("Main application task successfully cancelled.")
            except Exception as e_final_cancel:
                logger.error(
                    f"Exception during main task cancellation: {e_final_cancel}"
                )
    except Exception as e_main_async:
        logger.critical(
            f"Unhandled exception in __main__ async execution: {e_main_async}",
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
        logger.info("Main (__main__) thread: Finalizing asyncio loop...")
        current_loop = (
            asyncio.get_event_loop()
        )  # Obtener el loop actual para el cleanup
        try:
            pending_tasks = [
                task for task in asyncio.all_tasks(loop=current_loop) if not task.done()
            ]
            if pending_tasks:
                logger.info(
                    f"Cancelling {len(pending_tasks)} outstanding asyncio tasks..."
                )
                for task_to_cancel in pending_tasks:
                    task_to_cancel.cancel()
                current_loop.run_until_complete(
                    asyncio.gather(*pending_tasks, return_exceptions=True)
                )
                logger.info("Outstanding asyncio tasks processed.")
        except RuntimeError:
            logger.warning(
                "Asyncio loop already closed when trying to cancel pending tasks."
            )
        except Exception as e_cleanup:
            logger.error(f"Error during asyncio task cleanup: {e_cleanup}")

        if not current_loop.is_closed():
            current_loop.close()
            logger.info("Asyncio loop closed.")

        logging.shutdown()
        print("Program finalized.")
