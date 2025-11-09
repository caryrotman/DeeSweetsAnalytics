from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, abort, jsonify, render_template, request, send_file, url_for


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
QUERY_DIR = PROJECT_ROOT / "Queries"
CONFIG_PATH = BASE_DIR / "query_config.json"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg"}
DATA_EXTENSIONS = {".csv", ".txt", ".tsv", ".json", ".parquet"}
FILE_PATTERN = re.compile(r"(?P<path>[^\s\"']+\.(?:png|jpg|jpeg|svg|csv|txt|tsv|json|parquet))", re.IGNORECASE)
DATE_INPUT_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y")

try:
    from generate_query_module import create_module_code, infer_chart, infer_name, slugify
except ImportError:  # pragma: no cover
    create_module_code = infer_chart = infer_name = slugify = None


def normalize_date_input(value: str | None) -> str | None:
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    for pattern in DATE_INPUT_FORMATS:
        try:
            parsed = datetime.strptime(text, pattern)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


@dataclass
class QueryDefinition:
    identifier: str
    title: str
    file_path: Path
    summary: str | None = None


@dataclass
class JobResult:
    chart_path: Path | None
    data_files: list[Path]
    stdout: str
    stderr: str


class JobStatus:
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"


class JobManager:
    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def create_job(self, query: QueryDefinition) -> Dict[str, Any]:
        job_id = uuid.uuid4().hex
        with self._lock:
            self._jobs[job_id] = {
                "id": job_id,
                "query": query.identifier,
                "title": query.title,
                "status": JobStatus.QUEUED,
                "createdAt": datetime.utcnow().isoformat() + "Z",
                "updatedAt": datetime.utcnow().isoformat() + "Z",
                "stdout": "",
                "stderr": "",
                "chartPath": None,
                "dataFiles": [],
                "error": None,
                "parameters": {},
            }
        return self._jobs[job_id]

    def update_job(self, job_id: str, **changes: Any) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job.update(changes)
            job["updatedAt"] = datetime.utcnow().isoformat() + "Z"

    def get_job(self, job_id: str) -> Dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job else None

    def set_result(self, job_id: str, result: JobResult) -> None:
        payload = {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "chartPath": str(result.chart_path) if result.chart_path else None,
            "dataFiles": [str(path) for path in result.data_files],
        }
        self.update_job(job_id, **payload)


job_manager = JobManager()


def discover_queries() -> list[QueryDefinition]:
    if not QUERY_DIR.exists():
        return []

    definitions: list[QueryDefinition] = []
    for script_path in sorted(QUERY_DIR.glob("*.py")):
        metadata = extract_query_metadata(script_path)
        definitions.append(metadata)
    return definitions


def extract_query_metadata(script_path: Path) -> QueryDefinition:
    identifier = script_path.stem
    title = identifier.replace("_", " ").title()
    summary: str | None = None

    try:
        source = script_path.read_text(encoding="utf-8")
        module = ast.parse(source)
        docstring = ast.get_docstring(module)
        if docstring:
            first_line = docstring.strip().splitlines()[0]
            if first_line:
                summary = first_line

        for node in module.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "QUERY_NAME":
                        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                            candidate = node.value.value.strip()
                            if candidate:
                                title = candidate
            if isinstance(node, ast.Assign):
                continue
    except (OSError, SyntaxError):
        pass

    return QueryDefinition(identifier=identifier, title=title, file_path=script_path, summary=summary)


def sanitize_path(path: str) -> Path:
    candidate = Path(path).expanduser()
    resolved = (PROJECT_ROOT / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    if PROJECT_ROOT not in resolved.parents and resolved != PROJECT_ROOT:
        raise ValueError("Resolved path is outside the project directory.")
    if not resolved.exists():
        raise FileNotFoundError(f"File not found: {resolved}")
    return resolved


def parse_generated_files(stdout: str, stderr: str) -> JobResult:
    candidates = set(FILE_PATTERN.findall(stdout)) | set(FILE_PATTERN.findall(stderr))
    chart_path: Path | None = None
    data_files: list[Path] = []

    for candidate in candidates:
        try:
            resolved = sanitize_path(candidate)
        except (ValueError, FileNotFoundError):
            continue

        if resolved.suffix.lower() in IMAGE_EXTENSIONS and chart_path is None:
            chart_path = resolved
        elif resolved.suffix.lower() in DATA_EXTENSIONS:
            data_files.append(resolved)

    return JobResult(chart_path=chart_path, data_files=data_files, stdout=stdout, stderr=stderr)


def run_query_script(job_id: str, query: QueryDefinition, overrides: Dict[str, str] | None = None) -> None:
    job_manager.update_job(job_id, status=JobStatus.RUNNING)

    executable = os.environ.get("PYTHON_EXECUTABLE", sys.executable)
    extra_args = resolve_query_args(query, overrides)
    process = subprocess.Popen(
        [executable, str(query.file_path), *extra_args],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = process.communicate()

    result = parse_generated_files(stdout, stderr)
    extra_files: list[Path] = []

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_query = re.sub(r"[^A-Za-z0-9_-]+", "-", query.identifier).strip("-") or "query"
    if stdout.strip():
        stdout_file = OUTPUT_DIR / f"{safe_query}_{timestamp}_{job_id[:8]}_output.txt"
        stdout_file.write_text(stdout, encoding="utf-8")
        extra_files.append(stdout_file)
    if stderr.strip():
        stderr_file = OUTPUT_DIR / f"{safe_query}_{timestamp}_{job_id[:8]}_stderr.txt"
        stderr_file.write_text(stderr, encoding="utf-8")
        extra_files.append(stderr_file)

    result.data_files.extend(extra_files)
    job_manager.set_result(job_id, result)

    if process.returncode != 0:
        job_manager.update_job(
            job_id,
            status=JobStatus.ERROR,
            error=stderr.strip() or f"Query script exited with code {process.returncode}.",
        )
        return

    if result.chart_path is None:
        job_manager.update_job(
            job_id,
            status=JobStatus.ERROR,
            error="Query completed but no chart file was detected in the output.",
        )
        return

    job_manager.update_job(job_id, status=JobStatus.COMPLETED)


def serialize_query(definition: QueryDefinition) -> Dict[str, Any]:
    return {
        "id": definition.identifier,
        "title": definition.title,
        "summary": definition.summary,
        "filename": definition.file_path.name,
    }


def get_query_definition(query_id: str) -> QueryDefinition:
    for definition in discover_queries():
        if definition.identifier == query_id:
            return definition
    raise KeyError(f"Query '{query_id}' not found.")


def load_query_config() -> Dict[str, List[str]]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            normalized: Dict[str, List[str]] = {}
            for key, value in data.items():
                if isinstance(value, list):
                    normalized[key] = [str(item) for item in value]
            return normalized
    except (OSError, json.JSONDecodeError):
        return {}
    return {}


def resolve_query_args(query: QueryDefinition, overrides: Dict[str, str] | None = None) -> List[str]:
    config = load_query_config()
    baseline = config.get(query.identifier, [])
    if not overrides:
        return baseline

    result = baseline[:]
    for flag in ("--start-date", "--end-date"):
        if flag in overrides:
            try:
                idx = result.index(flag)
            except ValueError:
                idx = None
            if idx is not None and idx + 1 < len(result):
                result[idx + 1] = overrides[flag]
            else:
                result.extend([flag, overrides[flag]])
    return result


def create_app() -> Flask:
    app = Flask(__name__)

    @app.route("/")
    def index() -> str:
        queries = [serialize_query(q) for q in discover_queries()]
        initial_query = request.args.get("selected", "")
        autorun = request.args.get("autorun", "0") == "1"
        return render_template(
            "index.html",
            queries=queries,
            initial_query=initial_query,
            autorun=autorun,
        )

    @app.route("/queries/new", methods=["GET"])
    def new_query() -> str:
        return render_template("new_query.html")

    @app.route("/api/queries", methods=["POST"])
    def api_create_query():
        if create_module_code is None:
            return jsonify({"error": "Query generator is unavailable on this server."}), 500

        payload = request.get_json(force=True, silent=True) or {}
        sql = (payload.get("sql") or "").strip()
        name_override = (payload.get("name") or "").strip()
        if not sql:
            return jsonify({"error": "SQL is required."}), 400

        try:
            query_name = name_override or infer_name(sql)
            chart_suggestion = infer_chart(sql)
            base_slug = slugify(query_name) or f"query-{uuid.uuid4().hex[:8]}"
            unique_slug = base_slug
            suffix = 1
            while (QUERY_DIR / f"{unique_slug}.py").exists():
                unique_slug = f"{base_slug}-{suffix}"
                suffix += 1

            module_code = create_module_code(unique_slug, query_name, chart_suggestion, sql)
            QUERY_DIR.mkdir(parents=True, exist_ok=True)
            module_path = QUERY_DIR / f"{unique_slug}.py"
            module_path.write_text(module_code, encoding="utf-8")
        except Exception as exc:  # pragma: no cover
            return jsonify({"error": f"Failed to generate query: {exc}"}), 500

        return jsonify({"queryId": unique_slug, "queryName": query_name})

    @app.route("/api/queries", methods=["GET"])
    def api_queries():
        return jsonify([serialize_query(q) for q in discover_queries()])

    @app.route("/api/run-query", methods=["POST"])
    def api_run_query():
        payload = request.get_json(force=True, silent=True) or {}
        query_id = payload.get("queryId")
        start_date = payload.get("startDate")
        end_date = payload.get("endDate")
        if not query_id:
            return jsonify({"error": "Missing 'queryId' in request body."}), 400

        try:
            definition = get_query_definition(str(query_id))
        except KeyError as exc:
            return jsonify({"error": str(exc)}), 404

        job = job_manager.create_job(definition)
        overrides: Dict[str, str] = {}
        start_iso = normalize_date_input(start_date)
        if start_iso:
            overrides["--start-date"] = start_iso
        end_iso = normalize_date_input(end_date)
        if end_iso:
            overrides["--end-date"] = end_iso

        job_manager.update_job(
            job["id"],
            parameters={
                "startDate": start_iso,
                "endDate": end_iso,
            },
        )

        worker = threading.Thread(
            target=run_query_script,
            args=(job["id"], definition, overrides),
            daemon=True,
        )
        worker.start()

        return jsonify({"jobId": job["id"], "status": job["status"], "title": definition.title})

    @app.route("/api/queries/<query_id>", methods=["DELETE"])
    def api_delete_query(query_id: str):
        script_path = QUERY_DIR / f"{query_id}.py"
        if not script_path.exists():
            return jsonify({"error": f"Query '{query_id}' not found."}), 404

        backup_path = script_path.with_suffix(script_path.suffix + "_old")
        suffix = 1
        candidate = backup_path
        while candidate.exists():
            candidate = script_path.with_suffix(script_path.suffix + f"_old{suffix}")
            suffix += 1

        script_path.rename(candidate)
        return jsonify({"message": "Query archived.", "backup": candidate.name})

    @app.route("/api/jobs/<job_id>", methods=["GET"])
    def api_job_status(job_id: str):
        job = job_manager.get_job(job_id)
        if not job:
            return jsonify({"error": f"Job '{job_id}' not found."}), 404

        response = dict(job)
        # Hide internal paths from the client; expose only availability flags.
        response["hasChart"] = bool(job.get("chartPath"))
        response["queryId"] = job.get("query")
        response["dataFiles"] = [
            {
                "name": Path(path).name,
                "url": f"/api/jobs/{job_id}/files/{idx}",
            }
            for idx, path in enumerate(job.get("dataFiles", []))
        ]
        if response["hasChart"]:
            response["chartUrl"] = f"/api/jobs/{job_id}/chart"

        # Remove raw paths before returning
        response.pop("chartPath", None)
        return jsonify(response)

    @app.route("/api/jobs/<job_id>/chart", methods=["GET"])
    def api_job_chart(job_id: str):
        job = job_manager.get_job(job_id)
        if not job or not job.get("chartPath"):
            abort(404)

        try:
            chart_path = sanitize_path(job["chartPath"])
        except (ValueError, FileNotFoundError):
            abort(404)

        return send_file(chart_path, mimetype=f"image/{chart_path.suffix.lstrip('.')}")

    @app.route("/api/jobs/<job_id>/files/<int:file_index>", methods=["GET"])
    def api_job_file(job_id: str, file_index: int):
        job = job_manager.get_job(job_id)
        if not job:
            abort(404)

        files: List[str] = job.get("dataFiles", [])
        if file_index < 0 or file_index >= len(files):
            abort(404)

        try:
            file_path = sanitize_path(files[file_index])
        except (ValueError, FileNotFoundError):
            abort(404)

        return send_file(file_path, as_attachment=True, download_name=file_path.name)

    return app


if __name__ == "__main__":
    debug_mode = os.environ.get("FLASK_DEBUG", "0") == "1"
    host = os.environ.get("APP_HOST", "127.0.0.1")
    port_env = os.environ.get("APP_PORT", "5000")
    try:
        port = int(port_env)
    except ValueError:
        raise ValueError(f"Invalid APP_PORT value '{port_env}'. Expected an integer.")
    create_app().run(host=host, port=port, debug=debug_mode)

