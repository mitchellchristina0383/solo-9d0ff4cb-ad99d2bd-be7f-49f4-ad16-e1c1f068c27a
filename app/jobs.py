"""Job orchestration for the reconciliation trial page.

Each upload becomes a job directory::

    <DATA_DIR>/jobs/<job_id>/
        debit.csv          uploaded debit rows
        receipts.csv       uploaded receipt rows
        result.csv         only exists once the join completed successfully
        work/              scratch space for the operator (always emptied)

A job runs in a background thread and publishes a snapshot of the operator
statistics.  The download route refuses to serve anything until the operator
has atomically published ``result.csv``.
"""

from __future__ import annotations

import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from . import external_join

MAX_UPLOAD_BYTES = 512 * 1024 * 1024
JOB_RETENTION_SECONDS = 24 * 3600


class JobNotFound(KeyError):
    pass


@dataclass
class Job:
    job_id: str
    limit: int
    fault_after_writes: int | None
    root: Path
    state: str = "queued"          # queued | running | done | failed
    error: str = ""
    stats: dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def debit_path(self) -> Path:
        return self.root / "debit.csv"

    @property
    def receipt_path(self) -> Path:
        return self.root / "receipts.csv"

    @property
    def result_path(self) -> Path:
        return self.root / "result.csv"

    @property
    def work_root(self) -> Path:
        return self.root / "work"

    def snapshot(self) -> dict:
        with self._lock:
            payload = {
                "job_id": self.job_id,
                "state": self.state,
                "error": self.error,
                "limit": self.limit,
                "fault_after_writes": self.fault_after_writes,
                "stats": dict(self.stats),
                "created_at": self.created_at,
                "finished_at": self.finished_at,
            }
        payload["result_available"] = (
            payload["state"] == "done" and self.result_path.is_file()
        )
        payload["temp_files_left"] = self._count_leftovers()
        return payload

    def _count_leftovers(self) -> int:
        count = 0
        if self.work_root.exists():
            try:
                count += sum(1 for _ in self.work_root.rglob("*"))
            except FileNotFoundError:
                # The operator removed its workspace between the two calls.
                pass
        part = self.result_path.with_name(self.result_path.name + ".part")
        try:
            if part.exists():
                count += 1
        except FileNotFoundError:
            pass
        return count


class JobManager:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.jobs_dir = self.data_dir / "jobs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._cleanup_stale()

    # -- lifecycle ---------------------------------------------------------
    def _cleanup_stale(self) -> None:
        cutoff = time.time() - JOB_RETENTION_SECONDS
        for entry in self.jobs_dir.iterdir():
            try:
                if entry.is_dir() and entry.stat().st_mtime < cutoff:
                    shutil.rmtree(entry, ignore_errors=True)
            except OSError:
                continue

    def create_job(self, limit: int, fault_after_writes: int | None) -> Job:
        job_id = uuid.uuid4().hex
        root = self.jobs_dir / job_id
        (root / "work").mkdir(parents=True)
        job = Job(job_id=job_id, limit=limit, fault_after_writes=fault_after_writes, root=root)
        with self._lock:
            self._jobs[job_id] = job
        return job

    def get(self, job_id: str) -> Job:
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            # A job can outlive a process restart only as leftover files.
            raise JobNotFound(job_id)
        return job

    def list_recent(self, n: int = 10) -> list[Job]:
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)
        return jobs[:n]

    # -- execution ---------------------------------------------------------
    def start(self, job: Job) -> None:
        thread = threading.Thread(
            target=self._run, args=(job,), name=f"recon-{job.job_id}", daemon=True
        )
        thread.start()

    def _run(self, job: Job) -> None:
        with job._lock:
            job.state = "running"
            job.stats = {"phase": "starting", "buffer_limit": job.limit,
                         "peak_cached_records": 0}
        try:
            stats = external_join.reconcile(
                debit_path=job.debit_path,
                receipt_path=job.receipt_path,
                output_path=job.result_path,
                limit=job.limit,
                work_root=job.work_root,
                progress=self._progress_writer(job),
                fault_after_writes=job.fault_after_writes,
            )
        except external_join.OperatorError as exc:
            self._fail(job, str(exc))
        except Exception as exc:  # pragma: no cover - defensive
            self._fail(job, f"未预期的错误：{exc.__class__.__name__}: {exc}")
        else:
            with job._lock:
                job.stats = stats.as_dict()
                job.state = "done"
                job.finished_at = time.time()

    @staticmethod
    def _progress_writer(job: Job):
        def write(payload: dict) -> None:
            with job._lock:
                if job.state == "running":
                    job.stats = payload
        return write

    @staticmethod
    def _fail(job: Job, message: str) -> None:
        # The operator publishes its final stats (including the failure) via
        # the progress callback and has already removed its workspace and the
        # .part result file.  Make sure that final snapshot survived, since a
        # progress call may have been throttled away.
        with job._lock:
            job.state = "failed"
            job.error = message
            job.finished_at = time.time()
            stats = dict(job.stats)
            stats["phase"] = "failed"
            stats.setdefault("error", message)
            job.stats = stats


def save_upload(file_storage, dest: Path) -> int:
    """Stream an upload to disk, enforcing the size cap."""
    total = 0
    try:
        with dest.open("wb") as out:
            while True:
                chunk = file_storage.stream.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_UPLOAD_BYTES:
                    raise ValueError(
                        f"文件超过上限 {MAX_UPLOAD_BYTES // (1024 * 1024)} MiB"
                    )
                out.write(chunk)
    except OSError as exc:
        raise ValueError(f"保存上传文件失败：{exc}") from exc
    return total
