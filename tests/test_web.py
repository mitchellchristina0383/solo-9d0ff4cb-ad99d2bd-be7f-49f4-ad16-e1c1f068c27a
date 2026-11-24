import csv
import io
import time

import pytest

from app import create_app


@pytest.fixture()
def app(tmp_path):
    application = create_app(data_dir=tmp_path, testing=True)
    yield application


@pytest.fixture()
def client(app):
    return app.test_client()


def csv_bytes(rows, header=("row_no", "txn_id")):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if header:
        w.writerow(header)
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode("utf-8")


def submit(client, debit_rows, receipt_rows, limit=2, fault=None, damage=None):
    data = {
        "debit_csv": (io.BytesIO(csv_bytes(debit_rows)), "debit.csv"),
        "receipt_csv": (io.BytesIO(csv_bytes(receipt_rows)), "receipts.csv"),
        "buffer_limit": str(limit),
    }
    if fault is not None:
        data["fault_after_writes"] = str(fault)
    if damage is not None:
        data["damage_buckets"] = damage
    return client.post("/api/jobs", data=data, content_type="multipart/form-data")


def wait_done(client, job_id, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = client.get(f"/api/jobs/{job_id}").get_json()
        if snap["state"] in ("done", "failed"):
            return snap
        time.sleep(0.02)
    raise AssertionError("job did not finish")


def test_index_page_renders(client):
    res = client.get("/")
    assert res.status_code == 200
    body = res.get_data(as_text=True)
    assert "缓存的输入记录上限" in body
    assert "下载配对 CSV" in body


def test_end_to_end_upload_and_download(client):
    res = submit(
        client,
        [("1", "A"), ("2", "B"), ("3", "MISSING"), ("4", "")],
        [("10", "A"), ("11", "A"), ("12", "B"), ("13", ""), ("14", "Z")],
    )
    assert res.status_code == 202
    job_id = res.get_json()["job_id"]
    snap = wait_done(client, job_id)
    assert snap["state"] == "done"
    assert snap["result_available"] is True
    assert snap["stats"]["peak_cached_records"] <= 2
    assert snap["stats"]["peak_cached_records"] >= 1
    assert snap["temp_files_left"] == 0

    download = client.get(f"/api/jobs/{job_id}/result")
    assert download.status_code == 200
    assert "attachment" in download.headers["Content-Disposition"]
    rows = [tuple(r) for r in csv.reader(io.StringIO(download.get_data(as_text=True)))]
    assert rows[0] == ("debit_row_no", "receipt_row_no")
    assert ("1", "10") in rows and ("1", "11") in rows
    assert ("2", "12") in rows
    assert ("3", "") in rows
    assert ("4", "") in rows


def test_hot_transaction_through_web(client):
    receipts = [(str(i), "HOT") for i in range(1, 501)]
    res = submit(client, [("1", "HOT"), ("2", "COLD")], receipts, limit=2)
    job_id = res.get_json()["job_id"]
    snap = wait_done(client, job_id)
    assert snap["state"] == "done"
    assert snap["stats"]["peak_cached_records"] == 2
    rows = list(
        csv.reader(
            io.StringIO(client.get(f"/api/jobs/{job_id}/result").get_data(as_text=True))
        )
    )
    assert len([r for r in rows if r[0] == "1"]) == 500
    assert rows[-1] == ["2", ""]


def test_result_not_downloadable_before_completion(client, app):
    job = app.extensions["job_manager"].create_job(limit=2, fault_after_writes=None)
    # No result file yet and job not started.
    res = client.get(f"/api/jobs/{job.job_id}/result")
    assert res.status_code == 409


def test_write_failure_reports_error_and_cleans_files(client, app):
    res = submit(client, [("1", "A")], [(str(i), "A") for i in range(30)],
                 limit=2, fault=2)
    job_id = res.get_json()["job_id"]
    snap = wait_done(client, job_id)
    assert snap["state"] == "failed"
    assert "写入失败" in snap["error"] or "临时盘" in snap["error"]
    assert snap["result_available"] is False
    assert snap["temp_files_left"] == 0
    assert client.get(f"/api/jobs/{job_id}/result").status_code == 409

    job = app.extensions["job_manager"].get(job_id)
    assert list(job.work_root.iterdir()) == []
    assert not job.result_path.exists()
    assert not job.result_path.with_name(job.result_path.name + ".part").exists()


def test_limit_below_two_rejected(client):
    res = submit(client, [("1", "A")], [("1", "A")], limit=1)
    assert res.status_code == 400
    assert "至少" in res.get_json()["error"]


def test_missing_file_rejected(client):
    res = client.post(
        "/api/jobs",
        data={"buffer_limit": "2", "debit_csv": (io.BytesIO(b""), "d.csv")},
        content_type="multipart/form-data",
    )
    assert res.status_code == 400


def test_malformed_csv_becomes_failed_job(client):
    res = submit(client, [("1", "A"), ("2",)], [("1", "A")])
    job_id = res.get_json()["job_id"]
    snap = wait_done(client, job_id)
    assert snap["state"] == "failed"
    assert "2 列" in snap["error"]
    assert snap["temp_files_left"] == 0


def test_unknown_job_404(client):
    assert client.get("/api/jobs/nope").status_code == 404
    assert client.get("/api/jobs/nope/result").status_code == 404


def test_empty_transaction_ids_never_pair_through_web(client):
    res = submit(client, [("D1", "")], [("R1", ""), ("R2", "")])
    job_id = res.get_json()["job_id"]
    snap = wait_done(client, job_id)
    assert snap["state"] == "done"
    body = client.get(f"/api/jobs/{job_id}/result").get_data(as_text=True)
    assert body.splitlines() == ["debit_row_no,receipt_row_no", "D1,"]


def test_several_jobs_run_concurrently(client):
    ids = [submit(client, [("1", "A")], [("2", "A")]).get_json()["job_id"] for _ in range(4)]
    for job_id in ids:
        assert wait_done(client, job_id)["state"] == "done"
    for job_id in ids:
        body = client.get(f"/api/jobs/{job_id}/result").get_data(as_text=True)
        assert body.splitlines() == ["debit_row_no,receipt_row_no", "1,2"]


def test_missing_bucket_fails_job_and_blocks_download(client, app):
    res = submit(
        client,
        [("D1", "A"), ("D2", "B")],
        [("R1", "A"), ("R2", "B")],
        damage="missing-bucket",
    )
    assert res.status_code == 202
    job_id = res.get_json()["job_id"]
    snap = wait_done(client, job_id)
    assert snap["state"] == "failed"
    assert "丢失" in snap["error"]
    assert snap["result_available"] is False
    assert snap["temp_files_left"] == 0
    assert client.get(f"/api/jobs/{job_id}/result").status_code == 409

    job = app.extensions["job_manager"].get(job_id)
    assert not job.result_path.exists()
    assert not job.result_path.with_name(job.result_path.name + ".part").exists()
    assert list(job.work_root.iterdir()) == []


def test_truncated_bucket_fails_job_and_blocks_download(client, app):
    res = submit(
        client,
        [("D1", "A"), ("D2", "B")],
        [("R1", "A"), ("R2", "A"), ("R3", "B")],
        damage="truncate-row",
    )
    job_id = res.get_json()["job_id"]
    snap = wait_done(client, job_id)
    assert snap["state"] == "failed"
    assert "截短" in snap["error"]
    assert snap["result_available"] is False
    assert client.get(f"/api/jobs/{job_id}/result").status_code == 409
    job = app.extensions["job_manager"].get(job_id)
    assert list(job.work_root.iterdir()) == []
    assert not job.result_path.exists()


def test_unknown_damage_mode_rejected(client):
    res = submit(client, [("1", "A")], [("1", "A")], damage="bogus")
    assert res.status_code == 400


def test_normal_job_without_receipt_blank_row_downloads(client):
    res = submit(client, [("D1", "NOPE"), ("D2", "A")], [("R1", "A")])
    job_id = res.get_json()["job_id"]
    snap = wait_done(client, job_id)
    assert snap["state"] == "done"
    assert snap["stats"]["buckets_sealed"] == 1
    body = client.get(f"/api/jobs/{job_id}/result").get_data(as_text=True)
    assert body.splitlines() == ["debit_row_no,receipt_row_no", "D1,", "D2,R1"]
