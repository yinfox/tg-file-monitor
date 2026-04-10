import os

# Keep a single worker so monitor subprocess ownership stays in one process.
bind = os.getenv("GUNICORN_BIND", "0.0.0.0:5001")
workers = 1
worker_class = "gthread"
threads = int(os.getenv("GUNICORN_THREADS", "8"))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "120"))
graceful_timeout = int(os.getenv("GUNICORN_GRACEFUL_TIMEOUT", "30"))
accesslog = "-"
errorlog = "-"
capture_output = True
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")
preload_app = False


def _is_truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def post_worker_init(worker):
    if _is_truthy(os.getenv("TG_DISABLE_MONITORS", "0")):
        worker.log.info("TG_DISABLE_MONITORS enabled; skip monitor startup")
        return

    from app.app import (
        start_monitor_process,
        start_file_monitor_process,
        start_bot_monitor_process,
        start_drama_scheduler,
        start_hdhive_cookie_monitor,
        start_hdhive_checkin_scheduler,
    )

    start_monitor_process()
    start_file_monitor_process()
    start_bot_monitor_process()
    start_drama_scheduler()
    start_hdhive_cookie_monitor()
    start_hdhive_checkin_scheduler()
    worker.log.info("Background monitor tasks started in worker pid=%s", worker.pid)


def worker_exit(server, worker):
    try:
        from app.app import (
            tg_monitor_mgr,
            file_monitor_mgr,
            bot_monitor_mgr,
            _DRAMA_SCHEDULER_STOP_EVENT,
            _HDHIVE_COOKIE_MONITOR_STOP_EVENT,
            _HDHIVE_CHECKIN_STOP_EVENT,
        )

        tg_monitor_mgr.stop()
        file_monitor_mgr.stop()
        bot_monitor_mgr.stop()
        _DRAMA_SCHEDULER_STOP_EVENT.set()
        _HDHIVE_COOKIE_MONITOR_STOP_EVENT.set()
        _HDHIVE_CHECKIN_STOP_EVENT.set()
    except Exception as exc:
        worker.log.warning("Worker exit cleanup failed: %s", exc)
