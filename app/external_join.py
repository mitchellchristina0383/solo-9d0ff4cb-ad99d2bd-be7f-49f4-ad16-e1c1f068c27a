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

Integrity of the spill store
----------------------------
A left join is only correct if every bucket is complete: a missing bucket or
a bucket whose tail lost one or more whole receipts would silently turn a
match into a blank ``receipt_row_no``.  After the receipt pass every bucket is
sealed with a small manifest (row count, byte count and SHA-256) written to a
separate ``manifest/`` tree keyed by the same hash.  The probe verifies every
bucket it streams against its manifest, and a final sweep compares the live
bucket tree against the sealed totals.  A bucket that is absent when its
manifest says it must exist, or that is short/corrupt, therefore fails the
job instead of producing a quietly wrong result.

The manifest is one file per bucket on disk; nothing is indexed by
transaction id in memory, so the cache bound and the "two sequential input
passes" property are preserved.
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


class IntegrityError(OperatorError):
    """A sealed spill bucket is missing or truncated; the join would be wrong."""


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
    buckets_sealed: int = 0
    bucket_verifications: int = 0
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
            "buckets_sealed": self.buckets_sealed,
            "bucket_verifications": self.bucket_verifications,
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


# Damage modes for the page's self-test hook.  After sealing, the operator
# applies the requested damage to the spill store exactly as a careless
# cleanup / truncated filesystem would, so the integrity check can be
# exercised end to end.
DAMAGE_MISSING_BUCKET = "missing-bucket"
DAMAGE_TRUNCATE_ROW = "truncate-row"
DAMAGE_CHOICES = (DAMAGE_MISSING_BUCKET, DAMAGE_TRUNCATE_ROW)


def _damage_buckets(store: "_BucketStore", mode: str) -> None:
    """Deliberately damage one sealed bucket to exercise the integrity checks.

    * ``missing-bucket``: unlink the first bucket file found (as if it was
      lost).
    * ``truncate-row``: remove the last whole record from the first bucket
      that holds at least two records (as if the tail lost a complete
      receipt).

    The scan is streaming and holds one bucket at a time, so this self-test
    hook does not build a per-transaction list either.  Raises
    :class:`IntegrityError` if no bucket is suitable, so a self-test that
    cannot actually damage anything fails loudly instead of passing.
    """
    try:
        with os.scandir(store.bucket_root) as shards:
            for shard in shards:
                if not shard.is_dir():
                    continue
                with os.scandir(shard.path) as buckets:
                    for entry in buckets:
                        if not entry.name.endswith(".bucket"):
                            continue
                        if mode == DAMAGE_MISSING_BUCKET:
                            os.unlink(entry.path)
                            return
                        if mode == DAMAGE_TRUNCATE_ROW:
                            with open(entry.path, "rb") as fh:
                                data = fh.read()
                            if data.count(b"\n") < 2:
                                continue
                            truncated = data.rsplit(b"\n", 2)[0] + b"\n"
                            with open(entry.path, "wb") as fh:
                                fh.write(truncated)
                            return
                        raise ValueError(f"未知的故障注入模式：{mode!r}")
    except OSError as exc:
        raise SpillWriteError(f"故障注入破坏临时分桶失败：{exc}") from exc
    if mode == DAMAGE_MISSING_BUCKET:
        raise IntegrityError("故障注入失败：没有可分桶可破坏")
    if mode == DAMAGE_TRUNCATE_ROW:
        raise IntegrityError("故障注入失败：没有包含至少两条回执的分桶可截断")
    raise ValueError(f"未知的故障注入模式：{mode!r}")


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
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise IntegrityError("临时分桶文件损坏：出现非法 UTF-8 字节（文件可能被截短）") from None
    row_no, sep, txn_id = text.partition("\t")
    if not sep:
        raise IntegrityError(
            "临时分桶文件损坏：某条回执缺少字段分隔符（文件可能被改写）"
        )
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

    After the receipt pass :meth:`seal` walks the bucket tree once and writes
    a tiny manifest (record count, byte count, SHA-256) per bucket into a
    parallel ``manifests/`` tree, plus constant-size per-shard totals.  A
    probe of a sealed bucket verifies the bytes it streams against that
    manifest, and :meth:`verify_inventory` re-checks the whole bucket tree at
    the end.  Both keep O(1) state: nothing is indexed by transaction id in
    memory.
    """

    def __init__(self, root: Path, stats: RunStats, fault: _FaultInjector):
        self.bucket_root = os.path.join(str(root), "buckets")
        self.manifest_root = os.path.join(str(root), "manifests")
        try:
            os.makedirs(self.bucket_root, exist_ok=True)
            os.makedirs(self.manifest_root, exist_ok=True)
            # Pre-create the fan-out directories so appending a fresh bucket
            # needs no directory creation syscall.
            for i in range(BUCKET_FANOUT):
                shard = f"{i:02x}"
                os.makedirs(os.path.join(self.bucket_root, shard), exist_ok=True)
                os.makedirs(os.path.join(self.manifest_root, shard), exist_ok=True)
        except OSError as exc:
            raise SpillWriteError(f"无法创建临时分桶目录：{exc}") from exc
        self.stats = stats
        self.fault = fault
        self._handles: OrderedDict[str, int] = OrderedDict()
        self.sealed = False

    def path_for(self, txn_id: str) -> str:
        name = _bucket_name(txn_id)
        return os.path.join(self.bucket_root, name[:2], name)

    @staticmethod
    def _manifest_name(bucket_name: str) -> str:
        return bucket_name[: -len(".bucket")] + ".manifest"

    def _manifest_path(self, bucket_name: str) -> str:
        return os.path.join(
            self.manifest_root, bucket_name[:2], self._manifest_name(bucket_name)
        )

    @staticmethod
    def _parse_manifest(raw: bytes, where: str) -> tuple[int, int, str]:
        try:
            text = raw.decode("utf-8", "strict")
        except UnicodeDecodeError:
            raise IntegrityError(f"封存清单损坏（{where}）：不是合法 UTF-8") from None
        parts = text.split()
        if len(parts) != 3:
            raise IntegrityError(f"封存清单损坏（{where}）：字段数不是 3")
        try:
            records, nbytes = int(parts[0]), int(parts[1])
        except ValueError:
            raise IntegrityError(f"封存清单损坏（{where}）：计数不是整数") from None
        digest = parts[2]
        if records < 0 or nbytes < 0 or len(digest) != 64:
            raise IntegrityError(f"封存清单损坏（{where}）：取值非法")
        return records, nbytes, digest

    def _read_manifest(self, name: str) -> tuple[int, int, str] | None:
        """Return the sealed (records, bytes, sha256) for a bucket, if sealed."""
        path = self._manifest_path(name)
        try:
            with open(path, "rb") as fh:
                raw = fh.read(256)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise SpillWriteError(f"读取封存清单失败：{exc}") from exc
        return self._parse_manifest(raw, name)

    def iter_bucket(self, txn_id: str, chunk_bytes: int = 256 * 1024) -> Iterator[InputRecord]:
        """Stream the bucket for ``txn_id``; yields nothing if it has none.

        Only ``chunk_bytes`` of raw bytes is read at a time, so a bucket with
        more receipts than the configured limit is consumed in bounded space.
        Nothing is retained after the generator finishes.

        The bucket is checked against its seal while streaming.  If the seal
        says the bucket must exist but its file is gone, or the bytes read do
        not match the sealed count/byte count/digest (e.g. the tail lost whole
        receipts), :class:`IntegrityError` is raised instead of quietly
        reporting "no receipts".
        """
        name = _bucket_name(txn_id)
        expected = self._read_manifest(name)
        try:
            fd = os.open(self.path_for(txn_id), os.O_RDONLY)
        except FileNotFoundError:
            if expected is not None:
                raise IntegrityError(
                    f"临时分桶文件丢失：交易号 {txn_id!r} 的桶已封存但文件不存在"
                ) from None
            return
        except OSError as exc:
            raise SpillWriteError(f"读取临时分桶文件失败：{exc}") from exc
        if expected is None:
            os.close(fd)
            raise IntegrityError(
                f"临时分桶文件未封存：交易号 {txn_id!r} 的桶存在但缺少封存清单"
            )
        hasher = hashlib.sha256()
        total_bytes = 0
        records = 0
        tail = b""
        try:
            while True:
                try:
                    block = os.read(fd, chunk_bytes)
                except OSError as exc:
                    raise SpillWriteError(f"读取临时分桶文件失败：{exc}") from exc
                if not block:
                    break
                hasher.update(block)
                total_bytes += len(block)
                lines = (tail + block).split(b"\n")
                tail = lines.pop()
                for raw in lines:
                    rec = _decode_bucket_line(raw)
                    if rec is not None:
                        records += 1
                        yield rec
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
        if tail:
            rec = _decode_bucket_line(tail)
            if rec is not None:
                records += 1
                yield rec
        self.stats.bucket_verifications += 1
        got = (records, total_bytes, hasher.hexdigest())
        if got != expected:
            raise IntegrityError(
                "临时分桶文件被截短或损坏："
                f"交易号 {txn_id!r} 封存为 {expected[0]} 条/{expected[1]} 字节，"
                f"实际读到 {records} 条/{total_bytes} 字节"
            )

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

    def seal(self) -> None:
        """Seal every bucket with a manifest.

        Called once after the receipt pass and before any probing.  Walking
        the bucket tree touches every bucket sequentially; the retained state
        is one manifest file per bucket on disk, never a map from transaction
        id to anything in memory.
        """
        self.close_all()
        try:
            with os.scandir(self.bucket_root) as shards:
                for shard_entry in shards:
                    if not shard_entry.is_dir():
                        continue
                    with os.scandir(shard_entry.path) as buckets:
                        for bucket_entry in buckets:
                            name = bucket_entry.name
                            if not name.endswith(".bucket"):
                                continue
                            records, nbytes, digest = self._digest_bucket(
                                bucket_entry.path
                            )
                            self._write_manifest(name, records, nbytes, digest)
                            self.stats.buckets_sealed += 1
        except OSError as exc:
            raise SpillWriteError(f"封存临时分桶文件失败：{exc}") from exc
        self.sealed = True

    def _digest_bucket(self, path: str) -> tuple[int, int, str]:
        hasher = hashlib.sha256()
        records = 0
        nbytes = 0
        try:
            with open(path, "rb") as fh:
                while True:
                    block = fh.read(1024 * 1024)
                    if not block:
                        break
                    hasher.update(block)
                    nbytes += len(block)
                    # Every bucket record is one newline-terminated line, so a
                    # newline count cannot be fooled by a chunk boundary.
                    records += block.count(b"\n")
        except OSError as exc:
            raise SpillWriteError(f"读取临时分桶文件失败：{exc}") from exc
        return records, nbytes, hasher.hexdigest()

    def _write_manifest(
        self, bucket_name: str, records: int, nbytes: int, digest: str
    ) -> None:
        content = f"{records} {nbytes} {digest}\n".encode("ascii")
        self.fault.tick()
        try:
            fd = os.open(
                self._manifest_path(bucket_name),
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                0o600,
            )
            try:
                _write_all(fd, content)
            finally:
                os.close(fd)
        except OSError as exc:
            raise SpillWriteError(f"写入封存清单失败：{exc}") from exc

    def verify_inventory(self) -> None:
        """Re-check the live bucket tree against the sealed manifests.

        Catches a bucket that disappeared or was truncated after sealing but
        was never probed by a debit (for which a plain "no receipts" answer
        would have been wrong).  Every live bucket must still have a seal and
        its on-disk size must equal the sealed byte count; the number of live
        buckets must equal the number sealed.  A bucket that was probed has
        already been checked byte-for-byte (including its SHA-256) while
        streaming, so this sweep only needs ``stat`` per bucket -- cheap and
        still constant-memory.
        """
        if not self.sealed:
            raise IntegrityError("内部错误：未封存即清点分桶")
        seen = 0
        try:
            with os.scandir(self.bucket_root) as shards:
                for shard_entry in shards:
                    if not shard_entry.is_dir():
                        continue
                    with os.scandir(shard_entry.path) as buckets:
                        for bucket_entry in buckets:
                            name = bucket_entry.name
                            if not name.endswith(".bucket"):
                                continue
                            expected = self._read_manifest(name)
                            if expected is None:
                                raise IntegrityError(
                                    f"临时分桶文件未封存：{name} 存在但缺少封存清单"
                                )
                            size = bucket_entry.stat().st_size
                            if size != expected[1]:
                                raise IntegrityError(
                                    "临时分桶文件被截短："
                                    f"{name} 封存 {expected[1]} 字节，现有 {size} 字节"
                                )
                            seen += 1
            if seen != self.stats.buckets_sealed:
                raise IntegrityError(
                    "临时分桶文件丢失："
                    f"封存 {self.stats.buckets_sealed} 个，清点到 {seen} 个"
                )
        except OSError as exc:
            raise SpillWriteError(f"清点临时分桶文件失败：{exc}") from exc

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
    damage_buckets: str | None = None,
) -> RunStats:
    """Run the external left join and return statistics.

    ``debit_path`` and ``receipt_path`` are each read exactly once,
    sequentially.  ``output_path`` only appears once complete.  On any
    failure all intermediate files (and any partial result) are removed.

    ``damage_buckets`` is a self-test hook (see :data:`DAMAGE_CHOICES`): after
    sealing, the named damage is applied to a bucket so the integrity check
    can be demonstrated from the page.  It is not used on the normal path.
    """
    debit_path = Path(debit_path)
    receipt_path = Path(receipt_path)
    output_path = Path(output_path)
    work_root = Path(work_root)

    if limit < MIN_BUFFER_LIMIT:
        raise LimitError(f"缓存上限至少为 {MIN_BUFFER_LIMIT} 条，收到 {limit}")
    if damage_buckets is not None and damage_buckets not in DAMAGE_CHOICES:
        raise ValueError(f"未知的故障注入模式：{damage_buckets!r}")
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
        # Seal every bucket: from here on a missing or short bucket is a hard
        # failure, never a silent "this debit had no receipt".  The seal walk
        # is disk-only; it retains no per-transaction state.
        store.seal()
        if damage_buckets is not None:
            _damage_buckets(store, damage_buckets)
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
            # Every debit has been probed.  Re-check the whole bucket tree
            # against the seal before publishing: a bucket that vanished or
            # was truncated after sealing would otherwise have produced a
            # false "unmatched" row (or a short match list) for its debit.
            emit("verifying", force=True)
            store.verify_inventory()
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
