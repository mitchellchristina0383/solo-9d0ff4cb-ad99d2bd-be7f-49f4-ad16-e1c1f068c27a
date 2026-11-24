"""Flask trial page for the external reconciliation operator."""

from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, abort, jsonify, render_template, request, send_file

from . import external_join, jobs

DATA_DIR = Path(os.environ.get("RECON_DATA_DIR", "/data"))


def create_app(data_dir: Path | None = None, testing: bool = False) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = jobs.MAX_UPLOAD_BYTES + 16 * 1024 * 1024
    app.config["TESTING"] = testing
    manager = jobs.JobManager(Path(data_dir) if data_dir else DATA_DIR)
    app.extensions["job_manager"] = manager

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            default_limit=external_join.DEFAULT_BUFFER_LIMIT,
            min_limit=external_join.MIN_BUFFER_LIMIT,
        )

    @app.post("/api/jobs")
    def create_job():
        debit = request.files.get("debit_csv")
        receipt = request.files.get("receipt_csv")
        if debit is None or receipt is None:
            return jsonify(error="需要同时上传扣款 CSV 和支付回执 CSV"), 400

        raw_limit = request.form.get("buffer_limit", str(external_join.DEFAULT_BUFFER_LIMIT))
        try:
            limit = int(raw_limit)
        except ValueError:
            return jsonify(error="缓存上限必须是整数"), 400
        if limit < external_join.MIN_BUFFER_LIMIT:
            return jsonify(
                error=f"缓存上限至少为 {external_join.MIN_BUFFER_LIMIT} 条"
            ), 400

        raw_fault = request.form.get("fault_after_writes", "").strip()
        fault_after: int | None = None
        if raw_fault:
            try:
                fault_after = int(raw_fault)
            except ValueError:
                return jsonify(error="故障注入次数必须是整数"), 400
            if fault_after < 0:
                return jsonify(error="故障注入次数不能为负"), 400

        raw_damage = request.form.get("damage_buckets", "").strip()
        if raw_damage and raw_damage not in external_join.DAMAGE_CHOICES:
            return jsonify(error="未知的分桶故障注入模式"), 400
        damage_buckets = raw_damage or None

        job = manager.create_job(
            limit=limit,
            fault_after_writes=fault_after,
            damage_buckets=damage_buckets,
        )
        try:
            jobs.save_upload(debit, job.debit_path)
            jobs.save_upload(receipt, job.receipt_path)
        except ValueError as exc:
            _discard(job)
            return jsonify(error=str(exc)), 400

        manager.start(job)
        return jsonify(job.snapshot()), 202

    @app.get("/healthz")
    def healthz():
        return jsonify(status="ok")

    @app.get("/api/jobs/<job_id>")
    def job_status(job_id: str):
        try:
            job = manager.get(job_id)
        except jobs.JobNotFound:
            abort(404)
        return jsonify(job.snapshot())

    @app.get("/api/jobs/<job_id>/result")
    def job_result(job_id: str):
        try:
            job = manager.get(job_id)
        except jobs.JobNotFound:
            abort(404)
        snapshot = job.snapshot()
        if not snapshot["result_available"]:
            return jsonify(error="结果尚未完整写出，暂不可下载"), 409
        return send_file(
            job.result_path,
            mimetype="text/csv",
            as_attachment=True,
            download_name="reconciliation-pairs.csv",
        )

    @app.errorhandler(413)
    def too_large(_exc):
        return jsonify(error="上传内容过大"), 413

    return app


def _discard(job) -> None:
    import shutil

    shutil.rmtree(job.root, ignore_errors=True)


def main() -> None:
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), threaded=True)


if __name__ == "__main__":
    main()
