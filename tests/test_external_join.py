import csv
import os
from pathlib import Path

import pytest

from app import external_join as ej


def write_csv(path: Path, rows, header=("row_no", "txn_id")) -> Path:
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        if header:
            w.writerow(header)
        for r in rows:
            w.writerow(r)
    return path


def read_result(path: Path) -> list[tuple[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        rows = list(reader)
    assert rows[0] == ["debit_row_no", "receipt_row_no"]
    return [(r[0], r[1]) for r in rows[1:]]


@pytest.fixture()
def tmp(tmp_path):
    return tmp_path


def run(debit_rows, receipt_rows, limit=2, fault=None, tmp=None):
    debit = write_csv(tmp / "debit.csv", debit_rows)
    receipts = write_csv(tmp / "receipts.csv", receipt_rows)
    out = tmp / "out" / "result.csv"
    work = tmp / "work"
    stats = ej.reconcile(
        debit, receipts, out, limit=limit, work_root=work, fault_after_writes=fault
    )
    return out, stats, work


def test_each_debit_gets_all_matching_receipts(tmp_path):
    out, stats, work = run(
        [("1", "A"), ("2", "B"), ("3", "A")],
        [("10", "A"), ("11", "B"), ("12", "A"), ("13", "A")],
        tmp=tmp_path,
    )
    assert read_result(out) == [
        ("1", "10"),
        ("1", "12"),
        ("1", "13"),
        ("2", "11"),
        ("3", "10"),
        ("3", "12"),
        ("3", "13"),
    ]
    assert stats.matched_debits == 3 and stats.unmatched_debits == 0
    assert stats.output_rows == 7


def test_debit_without_receipt_keeps_receipt_row_blank(tmp_path):
    out, stats, work = run([("1", "A"), ("2", "NOPE")], [("10", "A")], tmp=tmp_path)
    assert read_result(out) == [("1", "10"), ("2", "")]
    assert stats.unmatched_debits == 1


def test_empty_txn_ids_never_match(tmp_path):
    out, _, _ = run(
        [("1", ""), ("2", "A")],
        [("10", ""), ("11", ""), ("12", "A")],
        tmp=tmp_path,
    )
    assert read_result(out) == [("1", ""), ("2", "12")]


def test_no_debits_still_produces_header_only_result(tmp_path):
    out, stats, _ = run([], [("1", "A")], tmp=tmp_path)
    assert read_result(out) == []
    assert stats.output_rows == 0


def test_hot_transaction_exceeding_limit_uses_disk(tmp_path):
    receipts = [(str(i), "HOT") for i in range(1, 2001)]
    receipts += [("3000", "COLD")]
    out, stats, work = run(
        [("1", "HOT"), ("2", "MISS")], receipts, limit=2, tmp=tmp_path
    )
    rows = read_result(out)
    assert len([r for r in rows if r[0] == "1"]) == 2000
    assert rows[-1] == ("2", "")
    assert stats.peak_cached_records <= 2
    assert stats.peak_cached_records == 2
    # workspace removed after a successful run
    assert list(work.iterdir()) == []


def test_peak_never_exceeds_limit_for_higher_limit(tmp_path):
    receipts = [(str(i), f"T{i % 7}") for i in range(1, 3001)]
    debits = [(str(i), f"T{i % 7}") for i in range(1, 501)]
    out, stats, _ = run(debits, receipts, limit=5, tmp=tmp_path)
    assert stats.peak_cached_records <= 5
    assert stats.output_rows == len(read_result(out))
    assert stats.matched_debits == 500


@pytest.mark.parametrize("limit", [0, 1, -3])
def test_limit_below_two_is_rejected(tmp_path, limit):
    with pytest.raises(ej.LimitError):
        run([("1", "A")], [("1", "A")], limit=limit, tmp=tmp_path)


def test_each_input_file_opened_exactly_once(tmp_path, monkeypatch):
    debit = write_csv(tmp_path / "debit.csv", [("1", "A"), ("2", "B")])
    receipts = write_csv(tmp_path / "receipts.csv", [("10", "A"), ("11", "B")])
    opens = {str(debit): 0, str(receipts): 0}
    real_open = Path.open

    def spy(self, *args, **kwargs):
        key = str(self)
        if key in opens:
            opens[key] += 1
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", spy)
    ej.reconcile(debit, receipts, tmp_path / "r.csv", limit=2, work_root=tmp_path / "w")
    assert opens == {str(debit): 1, str(receipts): 1}


def test_spill_failure_raises_and_cleans_everything(tmp_path):
    debit = write_csv(tmp_path / "debit.csv", [("1", "A")])
    receipts = write_csv(tmp_path / "receipts.csv", [(str(i), "A") for i in range(50)])
    out = tmp_path / "out" / "result.csv"
    work = tmp_path / "work"
    with pytest.raises(ej.SpillWriteError):
        ej.reconcile(
            debit, receipts, out, limit=2, work_root=work, fault_after_writes=3
        )
    assert not out.exists()
    assert not out.with_name(out.name + ".part").exists()
    assert list(work.iterdir()) == []


def test_spill_failure_during_output_write_cleans_up(tmp_path):
    # Writes before the first emitted pair: 1 receipt bucket write + 1 seal
    # manifest write, so failing at the 3rd write hits the result output.
    debit = write_csv(tmp_path / "debit.csv", [("1", "A")])
    receipts = write_csv(tmp_path / "receipts.csv", [("10", "A")])
    out = tmp_path / "out" / "result.csv"
    work = tmp_path / "work"
    with pytest.raises(ej.SpillWriteError):
        ej.reconcile(
            debit, receipts, out, limit=2, work_root=work, fault_after_writes=2
        )
    assert not out.exists()
    assert not out.with_name(out.name + ".part").exists()
    assert list(work.iterdir()) == []


def test_malformed_input_is_reported(tmp_path):
    bad = tmp_path / "debit.csv"
    bad.write_text("row_no,txn_id\n1,A\n2\n", encoding="utf-8")
    receipts = write_csv(tmp_path / "receipts.csv", [("10", "A")])
    with pytest.raises(ej.InputError, match="2 列"):
        ej.reconcile(bad, receipts, tmp_path / "r.csv", 2, tmp_path / "w")

    empty = tmp_path / "empty.csv"
    empty.write_text("", encoding="utf-8")
    with pytest.raises(ej.InputError):
        ej.reconcile(empty, receipts, tmp_path / "r.csv", 2, tmp_path / "w")


def test_real_oserror_on_bucket_write_is_wrapped_and_cleaned(tmp_path, monkeypatch):
    debit = write_csv(tmp_path / "debit.csv", [("1", "A")])
    receipts = write_csv(tmp_path / "receipts.csv", [("10", "A")])
    out = tmp_path / "out" / "result.csv"
    work = tmp_path / "work"
    real_os_open = os.open

    def failing_open(path, flags, *args, **kwargs):
        if str(path).endswith(".bucket") and flags & os.O_CREAT:
            raise OSError(28, "No space left on device")
        return real_os_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", failing_open)
    with pytest.raises(ej.SpillWriteError, match="临时分桶文件"):
        ej.reconcile(debit, receipts, out, limit=2, work_root=work)
    assert not out.exists()
    assert list(work.iterdir()) == []


def test_real_oserror_on_result_write_is_wrapped_and_cleaned(tmp_path, monkeypatch):
    debit = write_csv(tmp_path / "debit.csv", [("1", "A")])
    receipts = write_csv(tmp_path / "receipts.csv", [("10", "A")])
    out = tmp_path / "out" / "result.csv"
    work = tmp_path / "work"
    real_open = Path.open

    def failing_open(self, mode="r", *args, **kwargs):
        if "w" in mode and self.name.endswith(".part"):
            raise OSError(28, "No space left on device")
        return real_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", failing_open)
    with pytest.raises(ej.SpillWriteError, match="结果临时文件"):
        ej.reconcile(debit, receipts, out, limit=2, work_root=work)
    assert not out.exists()
    assert not out.with_name(out.name + ".part").exists()
    assert list(work.iterdir()) == []


def test_quoted_txn_ids_and_crlf_bom_are_preserved(tmp_path):
    debit = write_csv(tmp_path / "debit.csv", [("1", 'A,"x"'), ("2", "A")])
    receipts = write_csv(tmp_path / "receipts.csv", [("10", 'A,"x"'), ("11", "A")])
    # Re-save receipt file with BOM and CRLF line endings.
    text = receipts.read_text(encoding="utf-8").replace("\n", "\r\n")
    receipts.write_bytes(b"\xef\xbb\xbf" + text.encode("utf-8"))
    out = tmp_path / "result.csv"
    stats = ej.reconcile(debit, receipts, out, limit=2, work_root=tmp_path / "w")
    assert read_result(out) == [("1", "10"), ("2", "11")]
    assert stats.matched_debits == 2
    assert list((tmp_path / "w").iterdir()) == []


def test_duplicate_receipts_for_hot_txn_all_emitted_in_order(tmp_path):
    receipts = [("10", "H"), ("11", "H"), ("12", "H")]
    out, stats, _ = run([("1", "H"), ("2", "H")], receipts, limit=2, tmp=tmp_path)
    assert read_result(out) == [
        ("1", "10"), ("1", "11"), ("1", "12"),
        ("2", "10"), ("2", "11"), ("2", "12"),
    ]
    assert stats.output_rows == 6


@pytest.mark.parametrize("limit", [2, 3, 4, 7, 16, 64])
def test_all_limits_produce_identical_join(tmp_path, limit):
    """Regression: a partial final chunk in the merge phase must still be
    compared.  Any limits bug would make results depend on ``limit``."""
    receipts = [
        ("10", "A"), ("11", "A"), ("12", "B"), ("13", "A"),
        ("14", "C"), ("15", "B"), ("16", "A"),
    ]
    debits = [("1", "A"), ("2", "B"), ("3", "C"), ("4", "D"), ("5", "")]
    out, stats, _ = run(debits, receipts, limit=limit, tmp=tmp_path)
    assert read_result(out) == [
        ("1", "10"), ("1", "11"), ("1", "13"), ("1", "16"),
        ("2", "12"), ("2", "15"),
        ("3", "14"),
        ("4", ""),
        ("5", ""),
    ]
    assert stats.peak_cached_records <= limit


def test_tab_and_backslash_in_values_roundtrip_through_buckets(tmp_path):
    weird = 'a\tb\\c\nd'
    out, _, _ = run(
        [("1", weird), ("2", "plain")],
        [("10", weird), ("11", "plain"), ("12", weird)],
        tmp=tmp_path,
    )
    assert read_result(out) == [("1", "10"), ("1", "12"), ("2", "11")]


# ---------------------------------------------------------------------------
# Integrity of the sealed spill store
# ---------------------------------------------------------------------------

def test_missing_bucket_fails_job_instead_of_blanking_receipt(tmp_path):
    """Without the seal a lost bucket would look like "no receipts"."""
    debit = write_csv(tmp_path / "debit.csv", [("1", "A"), ("2", "B")])
    receipts = write_csv(tmp_path / "receipts.csv", [("10", "A"), ("11", "B")])
    out = tmp_path / "out" / "result.csv"
    work = tmp_path / "work"
    with pytest.raises(ej.IntegrityError, match="丢失"):
        ej.reconcile(
            debit, receipts, out, limit=2, work_root=work,
            damage_buckets=ej.DAMAGE_MISSING_BUCKET,
        )
    # No partial result may be published or left behind, and no scratch files.
    assert not out.exists()
    assert not out.with_name(out.name + ".part").exists()
    assert list(work.iterdir()) == []


def test_truncated_bucket_with_whole_missing_row_fails_job(tmp_path):
    """A tail that lost one or more complete receipts must not pass as a miss."""
    debit = write_csv(tmp_path / "debit.csv", [("1", "A"), ("2", "B")])
    receipts = write_csv(
        tmp_path / "receipts.csv", [("10", "A"), ("11", "A"), ("12", "B")]
    )
    out = tmp_path / "out" / "result.csv"
    work = tmp_path / "work"
    with pytest.raises(ej.IntegrityError, match="截短|条数"):
        ej.reconcile(
            debit, receipts, out, limit=2, work_root=work,
            damage_buckets=ej.DAMAGE_TRUNCATE_ROW,
        )
    assert not out.exists()
    assert not out.with_name(out.name + ".part").exists()
    assert list(work.iterdir()) == []


def test_bucket_damaged_after_seal_and_never_probed_is_caught(tmp_path, monkeypatch):
    """Final inventory check must catch a bucket lost before its debit is read.

    The debit file is empty here, so nothing ever streams the bucket; only the
    end-of-run inventory sweep can notice the loss.
    """
    debit = write_csv(tmp_path / "debit.csv", [])
    receipts = write_csv(tmp_path / "receipts.csv", [("10", "UNPROBED")])
    real_seal = ej._BucketStore.seal

    def seal_and_damage(self):
        real_seal(self)
        ej._damage_buckets(self, ej.DAMAGE_MISSING_BUCKET)

    monkeypatch.setattr(ej._BucketStore, "seal", seal_and_damage)
    with pytest.raises(ej.IntegrityError, match="丢失"):
        ej.reconcile(
            debit, receipts, tmp_path / "r.csv", limit=2, work_root=tmp_path / "w"
        )


def test_truncation_after_seal_with_no_probe_is_caught(tmp_path, monkeypatch):
    debit = write_csv(tmp_path / "debit.csv", [])
    receipts = write_csv(tmp_path / "receipts.csv", [("10", "X"), ("11", "X")])
    real_seal = ej._BucketStore.seal

    def seal_and_truncate(self):
        real_seal(self)
        ej._damage_buckets(self, ej.DAMAGE_TRUNCATE_ROW)

    monkeypatch.setattr(ej._BucketStore, "seal", seal_and_truncate)
    with pytest.raises(ej.IntegrityError):
        ej.reconcile(
            debit, receipts, tmp_path / "r.csv", limit=2, work_root=tmp_path / "w"
        )


def test_unknown_damage_mode_is_rejected(tmp_path):
    debit = write_csv(tmp_path / "debit.csv", [("1", "A")])
    receipts = write_csv(tmp_path / "receipts.csv", [("10", "A")])
    with pytest.raises(ValueError, match="未知"):
        ej.reconcile(
            debit, receipts, tmp_path / "r.csv", limit=2, work_root=tmp_path / "w",
            damage_buckets="nonsense",
        )


def test_seal_uses_one_manifest_per_bucket_and_no_memory_index(tmp_path):
    """Sealing must be disk-backed: one manifest file per bucket, no in-memory
    transaction-id index.  Probes then verify against the manifests."""
    receipts = [(str(i), f"T{i}") for i in range(1, 51)]
    debit = write_csv(tmp_path / "debit.csv", [("1", "T1"), ("2", "MISS")])
    rec = write_csv(tmp_path / "receipts.csv", receipts)
    out = tmp_path / "out" / "result.csv"
    stats = ej.reconcile(debit, rec, out, limit=2, work_root=tmp_path / "work")
    assert stats.buckets_sealed == 50
    assert stats.bucket_verifications == 1  # only T1 matched and was streamed
    assert read_result(out) == [("1", "1"), ("2", "")]


def test_normal_run_without_receipts_still_blanks_row_and_cleans(tmp_path):
    out, stats, work = run(
        [("1", "NOPE"), ("2", "A")], [("10", "A")], limit=2, tmp=tmp_path
    )
    assert read_result(out) == [("1", ""), ("2", "10")]
    assert stats.unmatched_debits == 1
    assert list(work.iterdir()) == []
