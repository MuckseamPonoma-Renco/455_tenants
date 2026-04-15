import os
from redis import Redis
from rq import Queue


def _inline_enabled() -> bool:
    return os.environ.get("PROCESS_INLINE", "0").strip().lower() in {"1", "true", "yes", "on"}


def _queue_or_none():
    if _inline_enabled():
        return None
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    try:
        conn = Redis.from_url(redis_url)
        conn.ping()
        return Queue("default", connection=conn)
    except Exception:
        return None


def enqueue_process_message(message_id: str, *, sync_sheets: bool = True):
    from packages.worker_jobs import process_message

    queue = _queue_or_none()
    if queue is None:
        process_message(message_id, sync_sheets=sync_sheets)
        return f"inline-process-{message_id[:8]}"
    return queue.enqueue(process_message, message_id, sync_sheets=sync_sheets).id


def enqueue_full_resync():
    from packages.worker_jobs import full_resync_sheets

    queue = _queue_or_none()
    if queue is None:
        full_resync_sheets()
        return "inline-resync"
    return queue.enqueue(full_resync_sheets).id


def enqueue_reprocess_last_n(n: int):
    from packages.worker_jobs import reprocess_last_n

    queue = _queue_or_none()
    if queue is None:
        reprocess_last_n(n)
        return f"inline-reprocess-{n}"
    return queue.enqueue(reprocess_last_n, n).id


def enqueue_queue_311_jobs():
    from packages.worker_jobs import queue_311_jobs

    queue = _queue_or_none()
    if queue is None:
        queue_311_jobs()
        return "inline-queue-311"
    return queue.enqueue(queue_311_jobs).id


def enqueue_sync_311_statuses():
    from packages.worker_jobs import sync_311_statuses

    queue = _queue_or_none()
    if queue is None:
        sync_311_statuses()
        return "inline-sync-311"
    return queue.enqueue(sync_311_statuses).id


def enqueue_export_legal_bundle():
    from packages.worker_jobs import export_legal_bundle

    queue = _queue_or_none()
    if queue is None:
        result = export_legal_bundle()
        return f"inline-export-{result.get('markdown','bundle')}"
    return queue.enqueue(export_legal_bundle).id
