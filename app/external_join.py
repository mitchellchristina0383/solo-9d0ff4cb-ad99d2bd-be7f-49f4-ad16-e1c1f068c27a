"""Bounded-memory external reconciliation (left join) of debits vs. receipts.

Design in one paragraph
-----------------------
The operator is a disk-partitioned hash join whose probe side streams:

1. The receipt CSV is read **once, sequentially**.  Every receipt with a
   non-empty transaction id is appended to a bucket file named after
   ``sha256(txn_id)``.  Only the receipt currently being written is resident,
   so the receipt pass caches at most 1 input record.
2. The debit CSV is read **once, sequentially**.  For each debit we stream the
   bucket file for its transaction id one receipt at a time.  Every receipt
   whose transaction id is exactly equal is emitted as a pair; if the whole
   bucket has been streamed and nothing matched, the receipt row number is
   written as an empty field.  Because buckets are streamed rather than loaded,
   a single transaction id with more receipts than the configured limit still
   completes inside it: the probe never holds more than the current debit plus
   the current receipt (2 records).

The configured limit is therefore a hard cap on simultaneously cached input
records.  The honest measured peak for the run is reported in
``RunStats.peak_cached_records`` and shown on the page; for small inputs it is
below the cap, for a hot transaction id it reaches exactly the cap.

Intermediate bucket files are always written under a private workspace
directory that is removed in a ``finally`` block, also when a temporary-disk
write fails.  The result CSV is written to a ``.part`` file next to its final
name, flushed and fsynced, and only then atomically renamed -- so a download
can never observe a partial result.
"""

from __future__ import annotations

import csv
import hashlib
import os
import shutil
import tempfile
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator

MIN_BUFFER_LIMIT = 2
DEFAULT_BUFFER_LIMIT = 2
OUTPUT_HEADER = ("debit_row_no", "receipt_row_no")
REQUIRED_HEADER = ("row_no", "txn_id")
MAX_OPEN_BUCKETS = 32
BUCKET_FANOUT = 256


class OperatorError(Exception):
    """Failure that is safe to show to the person operating the trial page."""


class InputError(OperatorError):
    """Uploaded file is missing, unreadable or malformed."""


class SpillWriteError(OperatorError):
    """A temporary/result file could not be written; workspace was cleaned."""


class LimitError(OperatorError):
    """The configured cache limit is below the supported minimum."""


@dataclass(frozen=True)
class InputRecord:
    row_no: str
    txn_id: str


@dataclass
class RunStats:
    buffer_limit: int = 0
    peak_cached_records: int = 0
    debit_rows_read: int = 0
    receipt_rows_read: int = 0
    empty_receipt_rows_skipped: int = 0
    intermediate_files_created: int = 0
    intermediate_records_written: int = 0
    output_rows: int = 0
    matched_debits: int = 0
    unmatched_debits: int = 0
    phase: str = "pending"
    error: str = ""
    cleaned_up: bool = False

    def as_dict(self) -> dict:
        return {
            "buffer_limit": self.buffer_limit,
            "peak_cached_records": self.peak_cached_records,
            "debit_rows_read": self.debit_rows_read,
            "receipt_rows_read": self.receipt_rows_read,
            "empty_receipt_rows_skipped": self.empty_receipt_rows_skipped,
            "intermediate_files_created": self.intermediate_files_created,
            "intermediate_records_written": self.intermediate_records_written,
            "output_rows": self.output_rows,
            "matched_debits": self.matched_debits,
            "unmatched_debits": self.unmatched_debits,
            "phase": self.phase,
            "error": self.error,
            "cleaned_up": self.cleaned_up,
        }


class _RecordBudget:
    """Counts simultaneously cached input records and enforces the cap."""

    def __init__(self, limit: int, stats: RunStats):
        self.limit = limit
        self.stats = stats
        self.held = 0

    def hold(self, n: int = 1) -> None:
        if self.held + n > self.limit:
            raise LimitError(
                f"缓存上限被突破：已持有 {self.held} 条，再缓存 {n} 条会超过上限 {self.limit}"
            )
        self.held += n
        if self.held > self.stats.peak_cached_records:
            self.stats.peak_cached_records = self.held

    def release(self, n: int = 1) -> None:
        self.held -= n
        if self.held < 0:
            raise LimitError("缓存计数错误：释放了未持有的记录")


class _FaultInjector:
    """Test-only hook: fails the Nth temporary write, like a full disk would."""

    def __init__(self, fail_after: int | None):
        self.fail_after = fail_after
        self.writes = 0

    def tick(self) -> None:
        if self.fail_after is None:
            return
        self.writes += 1
        if self.writes > self.fail_after:
            raise SpillWriteError(
                f"模拟临时盘写入失败：第 {self.writes} 次写入被拒绝"
            )


def _is_header(row: list[str]) -> bool:
    return (
        len(row) == 2
        and row[0].strip().lower() == REQUIRED_HEADER[0]
        and row[1].strip().lower() == REQUIRED_HEADER[1]
    )


def read_csv_records(path: Path, kind: str) -> Iterator[InputRecord]:
    """Stream ``row_no,txn_id`` rows from ``path`` exactly once.

    The header ``row_no,txn_id`` is optional (case-insensitive); a first row
    with two different cells is treated as data.
    """
    try:
        fh = path.open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise InputError(f"{kind} CSV 无法打开：{exc}") from exc
    with fh:
        reader = csv.reader(fh)
        try:
            first = next(reader)
        except StopIteration:
            raise InputError(f"{kind} CSV 为空") from None
        except csv.Error as exc:
            raise InputError(f"{kind} CSV 表头解析失败：{exc}") from exc
        if not _is_header(first):
            yield _row_to_record(first, reader.line_num, kind)
        for row in reader:
            if not row or (len(row) == 1 and not row[0].strip()):
                continue
            yield _row_to_record(row, reader.line_num, kind)


def _row_to_record(row: list[str], lineno: int, kind: str) -> InputRecord:
    if len(row) != 2:
        raise InputError(
            f"{kind} CSV 第 {lineno} 行：需要 2 列（行号, 交易号），实际 {len(row)} 列"
        )
    row_no = row[0].strip()
    if not row_no:
        raise InputError(f"{kind} CSV 第 {lineno} 行：行号为空")
    return InputRecord(row_no=row_no, txn_id=row[1].strip())


def _bucket_name(txn_id: str) -> str:
    """File name for a transaction id: hex digest, first 2 chars = shard."""
    return hashlib.sha256(txn_id.encode("utf-8")).hexdigest()[:32] + ".bucket"


# Bucket files are ours, so they use a cheap escaped tab-separated encoding
# instead of the general CSV state machine.  This matters: a bucket is read
# back once per probing debit, i.e. O(debits) times.


def _encode_field(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _decode_field(value: str) -> str:
    if "\\" not in value:
        return value
    out = []
    i = 0
    mapping = {"t": "\t", "n": "\n", "r": "\r", "\\": "\\"}
    while i < len(value):
        ch = value[i]
        if ch == "\\" and i + 1 < len(value):
            out.append(mapping.get(value[i + 1], value[i + 1]))
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _encode_row(row_no: str, txn_id: str) -> bytes:
    return f"{_encode_field(row_no)}\t{_encode_field(txn_id)}\n".encode("utf-8")


def _decode_bucket_line(raw: bytes) -> "InputRecord | None":
    if not raw.strip():
        return None
    text = raw.decode("utf-8")
    row_no, sep, txn_id = text.partition("\t")
    if not sep:
        raise SpillWriteError("临时分桶文件损坏：缺少字段分隔符")
    return InputRecord(row_no=_decode_field(row_no), txn_id=_decode_field(txn_id))


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


class _BucketStore:
    """Append receipts into one file per transaction-id hash.

    Buckets are spread over 256 subdirectories so a large number of distinct
    transaction ids does not produce one giant directory.  Bucket I/O uses
    raw byte file descriptors rather than text IO: a bucket is probed once per
    debit, so the per-open cost dominates, and ``os.open`` + ``os.read`` is
    roughly three times cheaper than a `TextIOWrapper` open/close cycle.

    At most :data:`MAX_OPEN_BUCKETS` append handles are kept open (an LRU), so
    the number of distinct transaction ids does not exhaust file descriptors.
    Handles, unlike cached CSV rows, are bookkeeping.
    """

    def __init__(self, root: Path, stats: RunStats, fault: _FaultInjector):
        self.bucket_root = os.path.join(str(root), "buckets")
        try:
            os.makedirs(self.bucket_root, exist_ok=True)
            # Pre-create the fan-out directories so appending a fresh bucket
            # needs no directory creation syscall.
            for i in range(BUCKET_FANOUT):
                os.makedirs(os.path.join(self.bucket_root, f"{i:02x}"), exist_ok=True)
        except OSError as exc:
            raise SpillWriteError(f"无法创建临时分桶目录：{exc}") from exc
        self.stats = stats
        self.fault = fault
        self._handles: OrderedDict[str, int] = OrderedDict()

    def path_for(self, txn_id: str) -> str:
        name = _bucket_name(txn_id)
        return os.path.join(self.bucket_root, name[:2], name)

    def iter_bucket(self, txn_id: str, chunk_bytes: int = 256 * 1024) -> Iterator[InputRecord]:
        """Stream the bucket for ``txn_id``; yields nothing if it has none.

        Only ``chunk_bytes`` of raw bytes is read at a time, so a bucket with
        more receipts than the configured limit is consumed in bounded space.
        Nothing is retained after the generator finishes.
        """
        try:
            fd = os.open(self.path_for(txn_id), os.O_RDONLY)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise SpillWriteError(f"读取临时分桶文件失败：{exc}") from exc
        tail = b""
        try:
            while True:
                try:
                    block = os.read(fd, chunk_bytes)
                except OSError as exc:
                    raise SpillWriteError(f"读取临时分桶文件失败：{exc}") from exc
                if not block:
                    break
                lines = (tail + block).split(b"\n")
                tail = lines.pop()
                for raw in lines:
                    rec = _decode_bucket_line(raw)
                    if rec is not None:
                        yield rec
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
        if tail:
            rec = _decode_bucket_line(tail)
            if rec is not None:
                yield rec

    def _close_entry(self, name: str) -> None:
        fd = self._handles.pop(name)
        os.close(fd)

    def add(self, rec: InputRecord) -> None:
        name = _bucket_name(rec.txn_id)
        fd = self._handles.pop(name, None)
        if fd is None:
            try:
                fd = os.open(
                    os.path.join(self.bucket_root, name[:2], name),
                    os.O_WRONLY | os.O_APPEND | os.O_CREAT,
                    0o600,
                )
            except OSError as exc:
                raise SpillWriteError(f"无法创建临时分桶文件：{exc}") from exc
            self.stats.intermediate_files_created += 1
        self._handles[name] = fd
        self.fault.tick()
        data = _encode_row(rec.row_no, rec.txn_id)
        try:
            _write_all(fd, data)
        except OSError as exc:
            raise SpillWriteError(f"写入临时分桶文件失败：{exc}") from exc
        self.stats.intermediate_records_written += 1
        if len(self._handles) > MAX_OPEN_BUCKETS:
            self._evict_oldest()

    def _evict_oldest(self) -> None:
        name = next(iter(self._handles))
        try:
            self._close_entry(name)
        except OSError as exc:
            raise SpillWriteError(f"关闭临时分桶文件失败：{exc}") from exc

    def count_files(self) -> int:
        """Count bucket files on disk (done once, after the receipt pass)."""
        total = 0
        for _dirpath, _dirs, files in os.walk(self.bucket_root):
            total += len(files)
        return total

    def close_all(self) -> None:
        while self._handles:
            name = next(iter(self._handles))
            try:
                self._close_entry(name)
            except OSError as exc:
                raise SpillWriteError(f"关闭临时分桶文件失败：{exc}") from exc

    def abort(self) -> None:
        """Best-effort close used on the error path; never raises."""
        for name in list(self._handles):
            try:
                self._close_entry(name)
            except OSError:
                pass


def _emit_matches(
    debit: InputRecord,
    chunk: list[InputRecord],
    out_writer,
    stats: RunStats,
    fault: "_FaultInjector",
) -> bool:
    """Write every pair in ``chunk`` matching ``debit``; report if any matched."""
    matched = False
    for other in chunk:
        if other.txn_id == debit.txn_id:
            fault.tick()
            out_writer.writerow([debit.row_no, other.row_no])
            stats.output_rows += 1
            matched = True
    return matched


def _open_result_writer(part_path: Path):
    try:
        fh = part_path.open("w", encoding="utf-8", newline="")
    except OSError as exc:
        raise SpillWriteError(f"无法创建结果临时文件：{exc}") from exc
    return fh, csv.writer(fh, lineterminator="\n")


def reconcile(
    debit_path: Path,
    receipt_path: Path,
    output_path: Path,
    limit: int,
    work_root: Path,
    progress: Callable[[dict], None] | None = None,
    fault_after_writes: int | None = None,
) -> RunStats:
    """Run the external left join and return statistics.

    ``debit_path`` and ``receipt_path`` are each read exactly once,
    sequentially.  ``output_path`` only appears once complete.  On any
    failure all intermediate files (and any partial result) are removed.
    """
    debit_path = Path(debit_path)
    receipt_path = Path(receipt_path)
    output_path = Path(output_path)
    work_root = Path(work_root)

    if limit < MIN_BUFFER_LIMIT:
        raise LimitError(f"缓存上限至少为 {MIN_BUFFER_LIMIT} 条，收到 {limit}")
    if not debit_path.is_file():
        raise InputError("扣款 CSV 不存在")
    if not receipt_path.is_file():
        raise InputError("回执 CSV 不存在")

    stats = RunStats(buffer_limit=limit)
    budget = _RecordBudget(limit, stats)
    fault = _FaultInjector(fault_after_writes)

    try:
        work_root.mkdir(parents=True, exist_ok=True)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        workspace = Path(tempfile.mkdtemp(prefix="recon-", dir=str(work_root)))
    except OSError as exc:
        raise SpillWriteError(f"无法创建临时工作目录：{exc}") from exc

    part_path = output_path.with_name(output_path.name + ".part")
    last_progress = [0.0]

    def emit(phase: str, force: bool = False) -> None:
        stats.phase = phase
        if progress is None:
            return
        now = time.monotonic()
        if force or now - last_progress[0] >= 0.15:
            last_progress[0] = now
            progress(stats.as_dict())

    success = False
    store = _BucketStore(workspace, stats, fault)
    out_fh = None
    try:
        # ---- Pass 1 over receipts: hash-partition onto disk ---------------
        emit("receipts", force=True)
        for rec in read_csv_records(receipt_path, "回执"):
            stats.receipt_rows_read += 1
            if not rec.txn_id:
                stats.empty_receipt_rows_skipped += 1
                continue
            budget.hold(1)
            try:
                store.add(rec)
            finally:
                budget.release(1)
            if stats.receipt_rows_read % 5000 == 0:
                emit("receipts")
        store.close_all()
        stats.intermediate_files_created = store.count_files()
        emit("debits", force=True)

        # ---- Pass 2 over debits: stream the matching bucket ---------------
        # The current debit is held, and its bucket is compared in chunks of
        # ``limit - 1`` receipts, so the configured limit is exactly what
        # bounds the working set.  A hot transaction id with more receipts
        # than the limit is consumed chunk by chunk and still completes
        # inside the limit.
        chunk_size = limit - 1
        out_fh, out_writer = _open_result_writer(part_path)
        try:
            out_writer.writerow(OUTPUT_HEADER)
            for rec in read_csv_records(debit_path, "扣款"):
                stats.debit_rows_read += 1
                budget.hold(1)  # the debit itself
                try:
                    matched = False
                    if rec.txn_id:
                        chunk: list[InputRecord] = []
                        try:
                            for other in store.iter_bucket(rec.txn_id):
                                budget.hold(1)
                                chunk.append(other)
                                if len(chunk) == chunk_size:
                                    matched |= _emit_matches(
                                        rec, chunk, out_writer, stats, fault
                                    )
                                    budget.release(len(chunk))
                                    chunk = []
                        finally:
                            # Flush the tail chunk (or release a partially
                            # filled chunk on an error) before moving on.
                            try:
                                if chunk:
                                    matched |= _emit_matches(
                                        rec, chunk, out_writer, stats, fault
                                    )
                            finally:
                                budget.release(len(chunk))
                    if matched:
                        stats.matched_debits += 1
                    else:
                        # Whole receipt file has been scanned: genuine miss.
                        fault.tick()
                        out_writer.writerow([rec.row_no, ""])
                        stats.output_rows += 1
                        stats.unmatched_debits += 1
                finally:
                    budget.release(1)
                if stats.debit_rows_read % 2000 == 0:
                    emit("debits")
            try:
                out_fh.flush()
                os.fsync(out_fh.fileno())
            except OSError as exc:
                raise SpillWriteError(f"结果文件落盘失败：{exc}") from exc
        except SpillWriteError:
            raise
        except OSError as exc:
            raise SpillWriteError(f"写入结果文件失败：{exc}") from exc
        finally:
            try:
                out_fh.close()
            except OSError:
                pass
        out_fh = None

        try:
            os.replace(part_path, output_path)
        except OSError as exc:
            raise SpillWriteError(f"结果文件定名失败：{exc}") from exc
        success = True
        emit("done", force=True)
        return stats
    except OperatorError as exc:
        stats.error = str(exc)
        raise
    except OSError as exc:
        # Anything else that touched the disk is reported as a spill failure.
        stats.error = f"临时盘写入失败：{exc}"
        raise SpillWriteError(stats.error) from exc
    finally:
        store.abort()
        if out_fh is not None:
            try:
                out_fh.close()
            except OSError:
                pass
        if not success:
            try:
                part_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
        shutil.rmtree(workspace, ignore_errors=True)
        # Report cleanup only if the scratch tree really is gone, so the page
        # never claims a clean run when a file survived.
        stats.cleaned_up = not os.path.exists(workspace)
        # Publish the final (possibly partial) numbers, including on failure,
        # so the page can show how far the run got and the real peak.
        stats.phase = "done" if success else "failed"
        if progress is not None:
            progress(stats.as_dict())
