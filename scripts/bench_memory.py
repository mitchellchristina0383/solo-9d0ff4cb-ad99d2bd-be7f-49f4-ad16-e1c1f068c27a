"""Generate CSV data and measure the operator's peak process memory.

Run::

    python3 scripts/bench_memory.py <out_dir> [--debits N] [--receipts N] [--hot N] [--limit N]

The script is intentionally standalone so the numbers can be reproduced
without the web UI.  It prints peak RSS (``ru_maxrss``) of the process that
ran the operator, plus the operator's own cached-record peak.
"""

from __future__ import annotations

import argparse
import csv
import resource
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.external_join import reconcile  # noqa: E402


def generate(out: Path, debits: int, receipts: int, hot: int, distinct: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    with (out / "receipts.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["row_no", "txn_id"])
        for i in range(1, receipts + 1):
            if i <= hot:
                txn = "HOT"
            elif i % 1009 == 0:
                txn = ""
            else:
                txn = f"T{(i * 7919) % distinct}"
            w.writerow([f"R{i}", txn])
    with (out / "debits.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["row_no", "txn_id"])
        for i in range(1, debits + 1):
            if i <= 20:
                txn = "HOT"
            elif i % 997 == 0:
                txn = f"MISS{i}"
            elif i % 1013 == 0:
                txn = ""
            else:
                txn = f"T{i % distinct}"
            w.writerow([f"D{i}", txn])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("out_dir")
    parser.add_argument("--debits", type=int, default=600_000)
    parser.add_argument("--receipts", type=int, default=400_000)
    parser.add_argument("--hot", type=int, default=100_000, help="receipts for HOT")
    parser.add_argument("--distinct", type=int, default=100_000)
    parser.add_argument("--limit", type=int, default=2)
    args = parser.parse_args()

    out = Path(args.out_dir)
    generate(out, args.debits, args.receipts, args.hot, args.distinct)
    input_bytes = sum(p.stat().st_size for p in (out / "debits.csv", out / "receipts.csv"))
    print(f"input: {input_bytes / 1e6:.1f} MB "
          f"(debits={args.debits:,}, receipts={args.receipts:,}, hot_receipts={args.hot:,})")

    started = time.monotonic()
    stats = reconcile(
        debit_path=out / "debits.csv",
        receipt_path=out / "receipts.csv",
        output_path=out / "result.csv",
        limit=args.limit,
        work_root=out / "work",
    )
    elapsed = time.monotonic() - started
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    result_bytes = (out / "result.csv").stat().st_size
    print(f"limit={args.limit} peak_cached_records={stats.peak_cached_records} "
          f"matched={stats.matched_debits:,} unmatched={stats.unmatched_debits:,} "
          f"output_rows={stats.output_rows:,} output={result_bytes / 1e6:.1f} MB")
    print(f"elapsed={elapsed:.1f}s peak_rss={rss_kb / 1024:.1f} MiB")


if __name__ == "__main__":
    main()
