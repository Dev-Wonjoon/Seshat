import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def http_json(method, url, value=None):
    body = None if value is None else json.dumps(value).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def canonical(doc):
    return json.dumps({
        "raw": doc["raw"],
        "parse_status": doc["parse_status"],
        "parsed": doc.get("parsed"),
        "parse_error_code": (doc.get("parse_error") or {}).get("code")
    }, sort_keys=True, ensure_ascii=True)


def expected_documents(lines):
    result = []
    for raw in lines:
        try:
            parsed = json.loads(raw)
            require(
                isinstance(parsed, dict),
                "Fixture root must be an object."
            )
            result.append({
                "raw": raw,
                "parse_status": "SUCCESS",
                "parsed": parsed
            })
        except json.JSONDecodeError:
            result.append({
                "raw": raw,
                "parse_status": "FAILED",
                "parse_error": {"code": "INVALID_JSON"}
            })
    return result


def replace_test_file(replacement, original):
    """Replace only the copied test input while the agent remains running."""
    replacement = Path(replacement).resolve(strict=True)
    original = Path(original).resolve(strict=True)

    require(
        replacement.parent == original.parent,
        "Replacement files must be in the same test directory."
    )

    if os.name != "nt":
        os.replace(replacement, original)
        return

    import ctypes
    from ctypes import wintypes

    backup = original.with_name(
        original.name + ".before-replacement"
    )
    require(
        not backup.exists(),
        "Replacement backup already exists."
    )

    kernel32 = ctypes.WinDLL(
        "kernel32",
        use_last_error=True
    )

    replace_file = kernel32.ReplaceFileW
    replace_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
        wintypes.DWORD,
        ctypes.c_void_p,
        ctypes.c_void_p
    ]
    replace_file.restype = wintypes.BOOL

    succeeded = replace_file(
        str(original),
        str(replacement),
        str(backup),
        0,
        None,
        None
    )

    if not succeeded:
        raise ctypes.WinError(ctypes.get_last_error())


class Suite:
    def __init__(self, project, java, timeout):
        self.project = project
        self.java = java
        self.timeout = timeout
        self.token = uuid.uuid4().hex[:12]

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.output = (
                project / "build" / "agent-e2e" / f"{stamp}-{self.token}"
        )
        self.output.mkdir(parents=True, exist_ok=False)

        self.database = self.output / "central.db"
        self.es = "http://127.0.0.1:9200"
        self.index = "seshat-logs-v1"

        self.children = []
        self.logs = {}
        self.server = None
        self.sequence = 0
        self.classpath = None

        self.fixture_hashes = {
            name: digest(project / name)
            for name in ("agent-input.ndjson", "agent-mixed.ndjson")
        }

        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]

        self.base = f"http://127.0.0.1:{self.port}"
        self.endpoint = self.base + "/api/logs/batches"

    def test_system_status(self):
        endpoint = self.base + "/api/system/status"

        def fetch():
            with urllib.request.urlopen(
                    endpoint,
                    timeout=5
            ) as response:
                require(
                    response.status == 200,
                    "System status did not return HTTP 200."
                )
                require(
                    response.headers.get_content_type()
                    == "application/json",
                    "System status did not return JSON."
                )
                require(
                    "no-store" in response.headers.get(
                        "Cache-Control", ""
                    ),
                    "System status allows cached responses."
                )
                return json.load(response)

        def wait_fresh():
            def ready():
                result = fetch()

                require(
                    result.get("metrics_status") in (
                        "STARTING", "FRESH", "STALE"
                    ),
                    "Unknown system metrics status."
                )

                return (
                    result
                    if result["metrics_status"] == "FRESH"
                    else None
                )

            return self.wait(
                ready,
                "fresh system metrics",
                process=self.server
            )

        def validate(result):
            required_fields = {
                "instance_id",
                "metrics_status",
                "server_time_ms",
                "sample_interval_ms",
                "stale_after_ms",
                "sample_sequence",
                "sampled_at_ms",
                "sample_age_ms",
                "uptime_ms",
                "process_cpu_load",
                "system_cpu_load",
                "heap_used_bytes",
                "heap_max_bytes"
            }

            require(
                required_fields <= result.keys(),
                "System status is missing required fields."
            )

            uuid.UUID(result["instance_id"])

            require(
                result["metrics_status"] == "FRESH",
                "System metrics are not fresh."
            )
            require(
                result["sample_interval_ms"] == 250,
                "Set the test server's system-sample-interval-ms to 250."
            )
            require(
                result["stale_after_ms"]
                == result["sample_interval_ms"] * 3,
                "Incorrect system metrics expiration threshold."
            )

            for field in (
                    "server_time_ms",
                    "sample_sequence",
                    "sampled_at_ms",
                    "sample_age_ms",
                    "uptime_ms"
            ):
                require(
                    type(result[field]) is int
                    and result[field] >= 0,
                    f"Invalid system status field: {field}"
                )

            require(
                result["server_time_ms"] > 0
                and result["sampled_at_ms"] > 0
                and result["sample_sequence"] > 0,
                "System metrics have no completed sample."
            )
            require(
                result["sample_age_ms"] < result["stale_after_ms"],
                "Fresh system metrics have expired."
            )

            for field in (
                    "process_cpu_load",
                    "system_cpu_load"
            ):
                value = result[field]
                require(
                    value is None
                    or (
                            type(value) in (int, float)
                            and 0 <= value <= 1
                    ),
                    f"Invalid CPU metric: {field}={value}"
                )

            used = result["heap_used_bytes"]
            maximum = result["heap_max_bytes"]

            require(
                used is None
                or (type(used) is int and used >= 0),
                "Invalid heap usage."
            )
            require(
                maximum is None
                or (type(maximum) is int and maximum > 0),
                "Invalid maximum heap size."
            )

            if used is not None and maximum is not None:
                require(
                    used <= maximum,
                    "Heap usage exceeds the maximum heap size."
                )

        first = wait_fresh()
        validate(first)

        # 표본 수집기가 독립적으로 동작하는 동안 상태 조회 요청을 보내지 않는다.
        time.sleep(2.0)

        second = fetch()
        validate(second)

        require(
            second["instance_id"] == first["instance_id"],
            "The central instance changed unexpectedly."
        )
        require(
            second["sample_sequence"]
            >= first["sample_sequence"] + 2,
            "Metrics did not advance independently of status requests."
        )
        require(
            second["uptime_ms"] >= first["uptime_ms"],
            "JVM uptime moved backwards."
        )

        self.stop_server()
        self.start_server()

        restarted = wait_fresh()
        validate(restarted)

        require(
            restarted["instance_id"] != first["instance_id"],
            "Central restart reused the previous metrics instance ID."
        )

        return {
            "sample_interval_ms": first["sample_interval_ms"],
            "independent_sample_advance": (
                    second["sample_sequence"]
                    - first["sample_sequence"]
            ),
            "verified": [
                "response_schema",
                "cache_control",
                "metric_units",
                "nullable_metric_contract",
                "sample_freshness",
                "independent_sampling",
                "central_restart"
            ]
        }

    def test_agent_monitoring(self):
        case = self.case(
            "monitoring",
            "agent-mixed.ndjson",
            "AGENT"
        )
        lines = case["source"].read_text(
            encoding="utf-8"
        ).splitlines()

        def list_agents(run_id):
            query = urllib.parse.urlencode({"run_id": run_id})
            return http_json(
                "GET",
                self.base + "/api/agents?" + query
            )

        def wait_status(expected_status, previous_instance=None):
            def ready():
                result = list_agents(case["run"])
                agents = result["agents"]

                if not agents:
                    return None

                require(
                    result["run_id"] == case["run"]
                    and len(agents) == 1,
                    "Incorrect agent registration or run filter."
                )

                item = agents[0]
                heartbeat = item["heartbeat"]

                require(
                    heartbeat["run_id"] == case["run"]
                    and heartbeat["source_id"] == "source-01"
                    and heartbeat["applied_mode"] == "AGENT",
                    "Incorrect heartbeat identity or mode."
                )

                if item["status"] != expected_status:
                    return None

                if previous_instance is not None:
                    if heartbeat["instance_id"] == previous_instance:
                        return None

                if expected_status == "ONLINE":
                    if (
                            heartbeat["pending_batch_count"] != 0
                            or heartbeat["pending_bytes"] != 0
                    ):
                        return None

                    require(
                        item["age_ms"] < result["stale_after_ms"],
                        "ONLINE status has an expired heartbeat."
                    )
                else:
                    require(
                        item["age_ms"] >= result["stale_after_ms"],
                        "STALE status has a fresh heartbeat."
                    )

                return item

            return self.wait(
                ready,
                "agent status " + expected_status,
                process=(
                    case["process"]
                    if expected_status == "ONLINE"
                    else None
                )
            )

        def post_heartbeat(payload):
            request = urllib.request.Request(
                self.base + "/api/agents/heartbeat",
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"}
            )

            with urllib.request.urlopen(request, timeout=5) as response:
                require(
                    response.status == 204
                    and response.read() == b"",
                    "Heartbeat did not return an empty HTTP 204 response."
                )

        try:
            self.start_agent(case)
            self.verify(case, lines)

            first = wait_status("ONLINE")
            heartbeat = first["heartbeat"]

            uuid.UUID(heartbeat["instance_id"])

            for field in ("process_cpu_load", "system_cpu_load"):
                value = heartbeat[field]
                require(
                    value is None
                    or (
                            isinstance(value, (int, float))
                            and 0 <= value <= 1
                    ),
                    f"Invalid CPU metric: {field}={value}"
                )

            for field in ("heap_used_bytes", "heap_max_bytes"):
                value = heartbeat[field]
                require(
                    value is None
                    or (
                            isinstance(value, int)
                            and value >= 0
                    ),
                    f"Invalid memory metric: {field}={value}"
                )

            require(
                heartbeat["max_pending_batches"] == case["capacity"]
                and heartbeat["max_pending_bytes"] > 0,
                "Incorrect queue capacity metrics."
            )

            empty = list_agents(
                f"auto-fq-{self.token}-missing-agent"
            )
            require(
                empty["agents"] == [],
                "Agent listing leaked another run's status."
            )

            self.stop(case["process"])
            wait_status("STALE")

            self.start_agent(case)
            restarted = wait_status(
                "ONLINE",
                previous_instance=heartbeat["instance_id"]
            )

            require(
                restarted["last_received_at_ms"]
                > first["last_received_at_ms"],
                "Restart did not update the central receive timestamp."
            )

            self.verify(case, lines)

            instance_id = restarted["heartbeat"]["instance_id"]

            self.stop_server()
            self.start_server()

            reconnected = wait_status("ONLINE")

            require(
                reconnected["heartbeat"]["instance_id"] == instance_id,
                "The agent restarted instead of reconnecting."
            )

            self.verify(case, lines)

            nullable = dict(reconnected["heartbeat"])
            nullable["run_id"] = f"auto-fq-{self.token}-nullable-metrics"
            nullable["instance_id"] = str(uuid.uuid4())

            for field in (
                    "process_cpu_load",
                    "system_cpu_load",
                    "heap_used_bytes",
                    "heap_max_bytes"
            ):
                nullable[field] = None

            post_heartbeat(nullable)

            nullable_result = list_agents(nullable["run_id"])
            require(
                len(nullable_result["agents"]) == 1
                and all(
                    nullable_result["agents"][0]["heartbeat"][field]
                    is None
                    for field in (
                        "process_cpu_load",
                        "system_cpu_load",
                        "heap_used_bytes",
                        "heap_max_bytes"
                    )
                ),
                "Unsupported metrics were not preserved as null."
            )

            for field, value in (
                    ("run_id", ""),
                    ("source_id", ""),
                    ("instance_id", "invalid"),
                    ("applied_mode", "INVALID"),
                    ("process_cpu_load", 1.1),
                    ("system_cpu_load", -0.1),
                    ("pending_batch_count", -1),
                    ("pending_bytes", -1),
                    ("max_pending_batches", 0)
            ):
                invalid = dict(nullable)
                invalid[field] = value

                try:
                    post_heartbeat(invalid)
                except urllib.error.HTTPError as error:
                    with error:
                        body = json.load(error)

                    require(
                        error.code == 400
                        and body.get("code") == "INVALID_REQUEST",
                        f"Incorrect heartbeat validation response: {body}"
                    )
                else:
                    raise AssertionError(
                        f"Invalid heartbeat field was accepted: {field}"
                    )

            return {
                "run_id": case["run"],
                "verified": [
                    "heartbeat_registration",
                    "identity_and_mode",
                    "resource_metrics",
                    "queue_metrics",
                    "run_isolation",
                    "stale_detection",
                    "agent_restart",
                    "central_reconnection",
                    "nullable_metrics",
                    "request_validation"
                ]
            }

        finally:
            self.stop(case["process"])

    def wait(self, predicate, description, process=None, timeout=None):
        duration = self.timeout if timeout is None else timeout
        deadline = time.monotonic() + duration
        last_error = ""

        while True:
            if process is not None and process.poll() is not None:
                raise RuntimeError(
                    f"Process exited during {description}.\n"
                    + self.tail(process)
                )

            try:
                result = predicate()
                if result:
                    return result
            except (
                    urllib.error.URLError,
                    TimeoutError,
                    ConnectionError
            ) as error:
                last_error = str(error)

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out: {description}. {last_error}"
                )

            time.sleep(0.2)

    def tail(self, process):
        path = self.logs.get(process.pid)
        if path is None:
            return ""

        return "\n".join(
            path.read_text(
                encoding="utf-8", errors="replace"
            ).splitlines()[-25:]
        )

    def test_log_query_api(self):
        lines = (
                self.project / "agent-mixed.ndjson"
        ).read_text(encoding="utf-8").splitlines()

        expected = expected_documents(lines)
        expected_count = len(expected)

        require(expected_count > 0, "The mixed fixture is empty.")

        def api_url(parameters, document_id=None):
            path = "/api/logs"

            if document_id is not None:
                path += "/" + document_id

            query = urllib.parse.urlencode(parameters)
            return self.base + path + "?" + query

        def search(run_id, **filters):
            parameters = {"run_id": run_id}
            parameters.update(filters)
            return http_json("GET", api_url(parameters))

        def expect_error(
                parameters,
                status,
                code,
                document_id=None
        ):
            try:
                http_json(
                    "GET",
                    api_url(parameters, document_id)
                )
            except urllib.error.HTTPError as error:
                with error:
                    body = json.load(error)

                require(
                    error.code == status,
                    f"Expected HTTP {status}, got {error.code}: {body}"
                )
                require(
                    body.get("code") == code,
                    f"Unexpected error body: {body}"
                )
                return

            raise AssertionError(
                f"Expected HTTP {status}, but the request succeeded."
            )

        for mode in ("CENTRAL", "AGENT"):
            run_id = f"auto-fq-{self.token}-mixed-{mode.lower()}"

            result = search(run_id)

            require(
                result["run_id"] == run_id
                and result["page"] == 0
                and result["size"] == 20
                and result["total_count"] == expected_count
                and len(result["items"]) == expected_count,
                "Incorrect default search response."
            )

            items = result["items"]

            require(
                all(
                    item["run_id"] == run_id
                    and item["source_id"] == "source-01"
                    and item["mode"] == mode
                    for item in items
                ),
                "Search returned incorrect metadata."
            )

            ordered_keys = [
                (item["source_id"], item["event_id"])
                for item in items
            ]

            require(
                ordered_keys == sorted(ordered_keys),
                "Search results are not sorted by source and event ID."
            )

            documents = []

            for item in items:
                detail = http_json(
                    "GET",
                    api_url(
                        {"run_id": run_id},
                        item["document_id"]
                    )
                )
                document = detail["document"]

                require(
                    detail["document_id"] == item["document_id"],
                    "Detail returned a different document ID."
                )

                for field in (
                        "run_id",
                        "source_id",
                        "event_id",
                        "mode",
                        "parse_status"
                ):
                    require(
                        document[field] == item[field],
                        f"List and detail differ for {field}."
                    )

                require(
                    len(item["raw_preview"]) <= 512,
                    "Raw preview exceeds its length limit."
                )

                if len(document["raw"]) <= 512:
                    require(
                        item["raw_preview"] == document["raw"],
                        "The short raw preview differs from the original."
                    )

                documents.append(document)

            require(
                Counter(map(canonical, documents))
                == Counter(map(canonical, expected)),
                "Detail changed raw data, parsed data, or parsing errors."
            )

            paged_ids = []

            for page in range((expected_count + 1) // 2):
                result_page = search(
                    run_id,
                    page=page,
                    size=2
                )

                require(
                    result_page["page"] == page
                    and result_page["size"] == 2
                    and result_page["total_count"] == expected_count,
                    "Incorrect pagination metadata."
                )

                paged_ids.extend(
                    item["document_id"]
                    for item in result_page["items"]
                )

            require(
                paged_ids
                == [item["document_id"] for item in items],
                "Pagination omitted, duplicated, or reordered documents."
            )

            for status in ("SUCCESS", "FAILED"):
                filtered = search(
                    run_id,
                    parse_status=status
                )
                count = sum(
                    document["parse_status"] == status
                    for document in expected
                )

                require(
                    filtered["total_count"] == count
                    and len(filtered["items"]) == count
                    and all(
                        item["parse_status"] == status
                        for item in filtered["items"]
                    ),
                    f"The {status} filter returned incorrect results."
                )

            matching = search(
                run_id,
                source_id="source-01",
                mode=mode
            )

            require(
                matching["total_count"] == expected_count,
                "Matching source and mode filters excluded documents."
            )

            other_mode = "AGENT" if mode == "CENTRAL" else "CENTRAL"

            for filters in (
                    {"source_id": "missing-source"},
                    {"mode": other_mode}
            ):
                filtered = search(run_id, **filters)

                require(
                    filtered["total_count"] == 0
                    and filtered["items"] == [],
                    "An unmatched filter returned documents."
                )

            text_result = search(run_id, q="first")

            require(
                text_result["total_count"] == 1
                and len(text_result["items"]) == 1
                and "first" in text_result["items"][0]["raw_preview"],
                "Raw text search returned incorrect results."
            )

            empty_page = search(
                run_id,
                page=expected_count + 1,
                size=1
            )

            require(
                empty_page["total_count"] == expected_count
                and empty_page["items"] == [],
                "An empty page lost its total count."
            )

            expect_error(
                {
                    "run_id":
                        f"auto-fq-{self.token}-mixed-{other_mode.lower()}"
                },
                404,
                "LOG_NOT_FOUND",
                items[0]["document_id"]
            )

        run_id = f"auto-fq-{self.token}-mixed-agent"

        empty = search(f"auto-fq-{self.token}-missing")

        require(
            empty["total_count"] == 0 and empty["items"] == [],
            "An unknown run returned documents."
        )

        invalid_requests = [
            {},
            {"run_id": ""},
            {"run_id": "   "},
            {"run_id": run_id, "source_id": ""},
            {"run_id": run_id, "mode": "INVALID"},
            {"run_id": run_id, "parse_status": "INVALID"},
            {"run_id": run_id, "page": -1},
            {"run_id": run_id, "page": "abc"},
            {"run_id": run_id, "size": 0},
            {"run_id": run_id, "size": 101},
            {"run_id": run_id, "page": 100, "size": 100},
            {"run_id": run_id, "q": "x" * 513}
        ]

        for parameters in invalid_requests:
            expect_error(
                parameters,
                400,
                "INVALID_REQUEST"
            )

        expect_error(
            {"run_id": run_id},
            404,
            "LOG_NOT_FOUND",
            "0" * 64
        )
        expect_error(
            {"run_id": run_id},
            400,
            "INVALID_REQUEST",
            "invalid-id"
        )
        expect_error(
            {},
            400,
            "INVALID_REQUEST",
            "0" * 64
        )

        return {
            "events_per_mode": expected_count,
            "verified": [
                "list",
                "detail",
                "raw_preservation",
                "filters",
                "text_search",
                "pagination",
                "run_isolation",
                "invalid_requests",
                "not_found"
            ]
        }

    def build(self):
        version = subprocess.run(
            [self.java, "-version"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True
        )
        self.java_version = (
                version.stdout + version.stderr
        ).strip()

        wrapper = (
                self.project / "gradle" / "wrapper" / "gradle-wrapper.jar"
        )
        require(wrapper.is_file(), "Gradle wrapper JAR was not found.")

        with (self.output / "build.log").open("wb") as log:
            result = subprocess.run([
                self.java,
                "-Dfile.encoding=UTF-8",
                "-classpath",
                str(wrapper),
                "org.gradle.wrapper.GradleWrapperMain",
                "--no-daemon",
                "bootJar"
            ], cwd=self.project, stdout=log, stderr=subprocess.STDOUT)

        require(
            result.returncode == 0,
            f"Build failed. See {self.output / 'build.log'}"
        )

        candidates = []
        marker = "BOOT-INF/classes/com/seshat/agent/AgentMain.class"

        for path in (self.project / "build" / "libs").glob("*.jar"):
            with zipfile.ZipFile(path) as archive:
                if marker in archive.namelist():
                    candidates.append(path)

        require(
            bool(candidates),
            "Executable application JAR was not found."
        )

        self.jar = max(
            candidates,
            key=lambda path: path.stat().st_mtime_ns
        )
        self.jar_hash = digest(self.jar)

        runtime = self.output / "runtime"
        runtime.mkdir()

        with zipfile.ZipFile(self.jar) as archive:
            for item in archive.infolist():
                if not item.filename.startswith((
                        "BOOT-INF/classes/",
                        "BOOT-INF/lib/"
                )):
                    continue

                target = (runtime / item.filename).resolve()
                require(
                    target.is_relative_to(runtime.resolve()),
                    "Unsafe archive path."
                )
                archive.extract(item, runtime)

        self.classpath = os.pathsep.join([
            str(runtime / "BOOT-INF" / "classes"),
            str(runtime / "BOOT-INF" / "lib" / "*")
        ])

    def prepare_es(self):
        self.wait(
            lambda: http_json("GET", self.es),
            "Elasticsearch availability"
        )

        uri = self.es + "/" + self.index

        try:
            mapping = http_json("GET", uri + "/_mapping")
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise

            spec = json.loads(
                (
                        self.project
                        / "src/main/resources/elasticsearch/seshat-logs-v1.json"
                ).read_text(encoding="utf-8")
            )
            http_json("PUT", uri, spec)
            mapping = http_json("GET", uri + "/_mapping")

        fields = mapping[self.index]["mappings"]["properties"]

        require(
            fields["run_id"]["type"] == "keyword"
            and fields["parse_status"]["type"] == "keyword",
            "Existing ES mapping differs; no index was deleted or reset."
        )

    def spawn(self, label, main_class, arguments):
        self.sequence += 1
        path = self.output / f"{self.sequence:02d}-{label}.log"

        with path.open("wb") as log:
            process = subprocess.Popen([
                self.java,
                "-Duser.language=en",
                "-Duser.country=US",
                "-Dfile.encoding=UTF-8",
                "-Dstdout.encoding=UTF-8",
                "-Dstderr.encoding=UTF-8",
                "-Dseshat.agent.heartbeat-interval-ms=250",
                "-cp",
                self.classpath,
                main_class,
                *map(str, arguments)
            ], cwd=self.project,
                stdout=log,
                stderr=subprocess.STDOUT,
                creationflags=getattr(
                    subprocess, "CREATE_NO_WINDOW", 0
                ))

        self.children.append(process)
        self.logs[process.pid] = path
        return process

    def stop(self, process):
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=10)

    def start_server(self):
        require(
            self.server is None or self.server.poll() is not None,
            "The suite server is already running."
        )

        database_url = (
                "jdbc:sqlite:" + self.database.as_posix()
                + "?journal_mode=WAL&synchronous=FULL&busy_timeout=5000"
        )

        self.server = self.spawn(
            "central",
            "com.seshat.SeshatApplication",
            [
                "--server.address=127.0.0.1",
                f"--server.port={self.port}",
                "--seshat.queue.worker-enabled=true",
                "--seshat.monitoring.stale-after-ms=2000",
                "--seshat.monitoring.system-sample-interval-ms=250",
                f"--spring.datasource.url={database_url}",
                f"--spring.elasticsearch.uris={self.es}"
            ]
        )

        self.wait(
            lambda: self.stats("readiness"),
            "central server startup",
            self.server
        )

    def stop_server(self):
        self.stop(self.server)
        self.server = None

    def case(
            self,
            name,
            fixture="agent-input.ndjson",
            mode="CENTRAL",
            capacity=100
    ):
        directory = self.output / name
        directory.mkdir()

        source = directory / "input.ndjson"
        shutil.copyfile(self.project / fixture, source)

        return {
            "run": f"auto-fq-{self.token}-{name}",
            "source": source,
            "queue": directory / "queue",
            "mode": mode,
            "capacity": capacity,
            "process": None,
            "endpoint": self.endpoint
        }

    def batches(self, case):
        return sorted(case["queue"].glob("*.batch"))

    def checkpoint_bytes(self, case):
        return (
                case["queue"] / "checkpoint.properties"
        ).read_bytes()

    def checkpoint_offset(self, case):
        text = self.checkpoint_bytes(case).decode("iso-8859-1")
        return int(next(
            line.split("=", 1)[1]
            for line in text.splitlines()
            if line.startswith("input.offset=")
        ))

    def start_agent(self, case):
        checkpoint = case["queue"] / "checkpoint.properties"
        before = (
            checkpoint.stat().st_mtime_ns
            if checkpoint.exists()
            else None
        )

        case["process"] = self.spawn(
            case["run"],
            "com.seshat.agent.AgentMain",
            [
                case["run"],
                "source-01",
                case["mode"],
                case["source"],
                case["endpoint"],
                case["capacity"],
                case["queue"]
            ]
        )

        self.wait(
            lambda: checkpoint.exists()
                    and (
                            before is None
                            or checkpoint.stat().st_mtime_ns != before
                    ),
            "agent checkpoint initialization",
            case["process"]
        )

    def stats(self, run_id):
        query = urllib.parse.urlencode({"run_id": run_id})
        return http_json(
            "GET",
            self.base + "/api/queue/stats?" + query
        )

    def database_rows(self, case):
        connection = sqlite3.connect(
            self.database.as_uri() + "?mode=ro",
            uri=True
        )
        connection.row_factory = sqlite3.Row

        try:
            return connection.execute(
                "SELECT event_id, mode, payload_json FROM log_queue "
                "WHERE run_id = ? ORDER BY id",
                (case["run"],)
            ).fetchall()
        finally:
            connection.close()

    def es_documents(self, run_id):
        result = http_json(
            "POST",
            self.es + "/" + self.index + "/_search",
            {
                "size": 100,
                "query": {"term": {"run_id": run_id}}
            }
        )
        return [
            hit["_source"] for hit in result["hits"]["hits"]
        ]

    def verify(self, case, lines):
        count = len(lines)

        def completed():
            stats = self.stats(case["run"])
            require(
                stats["total_count"] <= count,
                "Unexpected duplicate ingestion."
            )
            require(
                stats["storage_failed_count"] == 0,
                "ES storage failure."
            )
            return (
                    stats["total_count"] == count
                    and stats["completed_count"] == count
            )

        self.wait(
            completed,
            "central and ES completion",
            case["process"]
        )
        self.wait(
            lambda: not self.batches(case),
            "agent acknowledgement",
            case["process"]
        )

        def visible():
            documents = self.es_documents(case["run"])
            return documents if len(documents) == count else None

        docs = self.wait(
            visible,
            "ES search visibility",
            case["process"]
        )

        require(
            all(doc["mode"] == case["mode"] for doc in docs),
            "Wrong stored mode."
        )
        require(
            Counter(map(canonical, docs))
            == Counter(map(canonical, expected_documents(lines))),
            "Stored raw, parsed data, or parse error differs."
        )

        rows = self.database_rows(case)

        require(
            len(rows) == count
            and len({row["event_id"] for row in rows}) == count,
            "Incorrect unique event count."
        )
        require(
            all(row["mode"] == case["mode"] for row in rows),
            "Wrong queue mode."
        )
        require(
            [
                json.loads(row["payload_json"])["raw"]
                for row in rows
            ] == lines,
            "Queue raw data changed."
        )

        if case["mode"] == "AGENT":
            payloads = [
                json.loads(row["payload_json"]) for row in rows
            ]
            require(
                Counter(map(canonical, payloads))
                == Counter(map(canonical, expected_documents(lines))),
                "AGENT request metadata differs."
            )

        return docs

    def stable(self, case, predicate, message, seconds=1.5):
        deadline = time.monotonic() + seconds

        while time.monotonic() < deadline:
            require(
                case["process"].poll() is None,
                self.tail(case["process"])
            )
            require(predicate(), message)
            time.sleep(0.1)

    def test_modes(self):
        results = []

        for mode in ("CENTRAL", "AGENT"):
            case = self.case(
                "mixed-" + mode.lower(),
                "agent-mixed.ndjson",
                mode
            )
            lines = case["source"].read_text(
                encoding="utf-8"
            ).splitlines()

            self.start_agent(case)
            results.append(self.verify(case, lines))
            self.stop(case["process"])

        require(
            Counter(map(canonical, results[0]))
            == Counter(map(canonical, results[1])),
            "CENTRAL and AGENT results differ."
        )

        return {"events_per_mode": len(results[0])}

    def test_resume(self):
        case = self.case("resume")
        lines = case["source"].read_text(
            encoding="utf-8"
        ).splitlines()

        self.start_agent(case)
        self.verify(case, lines)

        first_ids = {
            row["event_id"] for row in self.database_rows(case)
        }

        self.stop(case["process"])
        self.start_agent(case)

        self.stable(
            case,
            lambda: self.stats(case["run"])["total_count"]
                    == len(lines),
            "Existing input was ingested again."
        )

        checkpoint = self.checkpoint_bytes(case)
        raw = '{"message":"appended-\uD55C\uAE00","level":"info"}'

        with case["source"].open("ab") as stream:
            stream.write(raw.encode("utf-8"))

        self.stable(
            case,
            lambda: self.stats(case["run"])["total_count"]
                    == len(lines)
                    and self.checkpoint_bytes(case) == checkpoint,
            "A partial line was committed before its newline."
        )

        with case["source"].open("ab") as stream:
            stream.write(b"\r\n")

        self.verify(case, lines + [raw])

        require(
            first_ids <= {
                row["event_id"] for row in self.database_rows(case)
            },
            "Existing event IDs changed."
        )

        self.stop(case["process"])

        return {
            "run_id": case["run"],
            "final_events": len(lines) + 1
        }

    def test_crash(self):
        self.stop_server()

        case = self.case("crash")
        lines = case["source"].read_text(
            encoding="utf-8"
        ).splitlines()

        self.start_agent(case)

        self.wait(
            lambda: len(self.batches(case)) == 1
                    and self.checkpoint_offset(case)
                    == case["source"].stat().st_size,
            "durable pending batch",
            case["process"]
        )

        path = self.batches(case)[0]
        before_hash = digest(path)
        packet = path.read_bytes()

        require(
            hashlib.sha256(packet[:-32]).digest()
            == packet[-32:],
            "Bad batch checksum."
        )

        hash_length = struct.unpack_from(">H", packet, 12)[0]
        position = 14 + hash_length
        length = struct.unpack_from(">i", packet, position)[0]

        require(
            position + 4 + length == len(packet) - 32,
            "Bad batch length."
        )

        body = json.loads(
            packet[position + 4:position + 4 + length]
        )
        expected_ids = {
            event["event_id"] for event in body["events"]
        }

        self.stop(case["process"])
        self.start_agent(case)

        require(
            len(self.batches(case)) == 1
            and digest(path) == before_hash,
            "Pending batch changed after forced restart."
        )

        self.start_server()
        self.verify(case, lines)

        require(
            expected_ids == {
                row["event_id"] for row in self.database_rows(case)
            },
            "Replayed event IDs changed."
        )

        self.stop(case["process"])

        return {
            "run_id": case["run"],
            "batch_sha256": before_hash
        }

    def test_backpressure(self):
        self.stop_server()

        case = self.case(
            "backpressure",
            "agent-mixed.ndjson",
            capacity=1
        )
        lines = case["source"].read_text(
            encoding="utf-8"
        ).splitlines()

        self.start_agent(case)

        self.wait(
            lambda: len(self.batches(case)) == 1
                    and self.checkpoint_offset(case) > 0,
            "full one-batch queue",
            case["process"]
        )

        checkpoint = self.checkpoint_bytes(case)
        raw = '{"message":"backpressure","level":"info"}'

        with case["source"].open("ab") as stream:
            stream.write((raw + "\n").encode("utf-8"))

        self.stable(
            case,
            lambda: len(self.batches(case)) == 1
                    and self.checkpoint_bytes(case) == checkpoint,
            "Queue limit or checkpoint backpressure failed."
        )

        self.start_server()
        self.verify(case, lines + [raw])
        self.stop(case["process"])

        return {
            "run_id": case["run"],
            "final_events": len(lines) + 1
        }

    def test_input_change(self, replacement=False):
        name = "replacement" if replacement else "truncation"
        case = self.case(name)
        lines = case["source"].read_text(
            encoding="utf-8"
        ).splitlines()

        self.start_agent(case)
        self.verify(case, lines)

        checkpoint = self.checkpoint_bytes(case)

        if replacement:
            length = case["source"].stat().st_size
            target = case["source"].with_suffix(".replacement")
            changed = {
                "message": "replacement-" + "x" * length,
                "level": "info"
            }
            changed_bytes = (
                    json.dumps(changed) + "\n"
            ).encode("utf-8")

            target.write_bytes(changed_bytes)

            require(
                target.stat().st_size >= length,
                "Replacement must not be shorter."
            )

            replace_test_file(target, case["source"])
        else:
            changed_bytes = b""
            case["source"].write_bytes(changed_bytes)

        process = case["process"]

        self.wait(
            lambda: process.poll() is not None,
            "input-change shutdown",
            timeout=10
        )

        require(
            process.returncode != 0,
            "Input change did not fail the agent."
        )

        require(
            case["source"].is_file()
            and case["source"].read_bytes() == changed_bytes,
            "The intended input change did not complete."
        )

        log = self.tail(process)

        detected_change = any(
            text in log for text in (
                "Input rotation or truncation detected.",
                "Input truncation detected while reading.",
                "Input checkpoint boundary changed.",
                "Checkpoint must immediately follow LF or CRLF."
            )
        )

        # Windows에서 파일 교체가 성공한 경우에만
        # 이 테스트 입력의 경로를 찾을 수 없다는 오류를 허용한다.
        missing_input_error = (
                "java.nio.file.NoSuchFileException: "
                + str(case["source"].resolve())
        )
        replacement_gap = (
                replacement
                and os.name == "nt"
                and missing_input_error in log
        )

        require(
            detected_change or replacement_gap,
            "Expected input-change error was not found.\n" + log
        )

        require(
            self.checkpoint_bytes(case) == checkpoint,
            "Checkpoint changed after input failure."
        )

        stats = self.stats(case["run"])

        require(
            stats["total_count"] == len(lines)
            and stats["completed_count"] == len(lines),
            "Stored count changed after input failure."
        )

        documents = self.es_documents(case["run"])

        require(
            Counter(map(canonical, documents))
            == Counter(map(canonical, expected_documents(lines))),
            "Stored documents changed after input failure."
        )

        return {
            "run_id": case["run"],
            "exit_code": process.returncode,
            "detection": (
                "missing_path_during_replacement"
                if replacement_gap
                else "input_change"
            )
        }

    def test_lost_ack(self):
        calls = []
        heartbeat_failures = []
        lock = threading.Lock()
        endpoint = self.endpoint

        class Proxy(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                if self.path == "/api/agents/heartbeat":
                    with lock:
                        heartbeat_failures.append(1)

                    self.send_error(
                        404,
                        "Heartbeat is unavailable in this test."
                    )
                    return

                if self.path != "/api/logs/batches":
                    self.send_error(404)
                    return
                body = self.rfile.read(
                    int(self.headers["Content-Length"])
                )

                request = urllib.request.Request(
                    endpoint,
                    data=body,
                    method="POST",
                    headers={"Content-Type": "application/json"}
                )

                with urllib.request.urlopen(
                        request, timeout=5
                ) as response:
                    status = response.status
                    reply = response.read()

                require(
                    status == 202,
                    "Central did not acknowledge the proxied batch."
                )

                with lock:
                    calls.append((body, json.loads(reply)))
                    drop = len(calls) == 1

                if drop:
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    self.connection.close()
                    return

                self.send_response(status)
                self.send_header(
                    "Content-Type", "application/json"
                )
                self.send_header(
                    "Content-Length", str(len(reply))
                )
                self.end_headers()
                self.wfile.write(reply)

        proxy = ThreadingHTTPServer(
            ("127.0.0.1", 0), Proxy
        )
        proxy.daemon_threads = True

        thread = threading.Thread(
            target=proxy.serve_forever,
            daemon=True
        )
        thread.start()

        case = self.case("lost-ack")
        case["endpoint"] = (
            f"http://127.0.0.1:{proxy.server_address[1]}"
            "/api/logs/batches"
        )

        try:
            lines = case["source"].read_text(
                encoding="utf-8"
            ).splitlines()

            self.start_agent(case)
            self.verify(case, lines)

            def heartbeat_was_rejected():
                with lock:
                    return bool(heartbeat_failures)

                self.wait(
                    heartbeat_was_rejected,
                    "heartbeat rejection without agent failure",
                    process=case["process"]
                )

                require(
                    case["process"].poll() is None,
                    "Heartbeat rejection stopped the agent."
                )

            with lock:
                recorded = list(calls)

            require(
                len(recorded) >= 2,
                "The lost response did not cause a retry."
            )
            require(
                all(
                    body == recorded[0][0]
                    for body, _ in recorded
                ),
                "HTTP request bytes changed during retry."
            )
            require(
                recorded[0][1]["inserted_count"] == 1
                and recorded[1][1]["duplicate_count"] == 1,
                "Central duplicate acknowledgement differs."
            )

            self.stop(case["process"])

            return {
                "run_id": case["run"],
                "received_requests": len(recorded)
            }

        finally:
            proxy.shutdown()
            proxy.server_close()
            thread.join(timeout=5)

    def cleanup(self):
        errors = []

        for process in reversed(self.children):
            try:
                self.stop(process)
            except Exception as error:
                errors.append(str(error))

        return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--java",
        default=shutil.which("java")
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=90
    )
    args = parser.parse_args()

    if not args.java:
        parser.error(
            "Java was not found. Specify --java with a Java 25 executable."
        )

    if args.timeout <= 0:
        parser.error("--timeout must be positive.")

    project = Path(__file__).resolve().parent
    suite = Suite(project, args.java, args.timeout)

    tests = [
        (
            "mixed_modes_and_es_equivalence",
            suite.test_modes
        ),
        (
            "log_query_api",
            suite.test_log_query_api
        ),
        (
            "system_status_api",
            suite.test_system_status
        ),
        (
            "agent_heartbeat_and_status",
            suite.test_agent_monitoring
        ),
        (
            "utf8_partial_line_and_checkpoint_resume",
            suite.test_resume
        ),
        (
            "pending_batch_forced_restart",
            suite.test_crash
        ),
        (
            "queue_backpressure_and_recovery",
            suite.test_backpressure
        ),
        (
            "input_truncation_detection",
            lambda: suite.test_input_change(False)
        ),
        (
            "changed_file_replacement_detection",
            lambda: suite.test_input_change(True)
        ),
        (
            "lost_acknowledgement_and_deduplication",
            suite.test_lost_ack
        ),
    ]

    report = {
        "output": str(suite.output),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "tests": [
            {"name": name, "status": "NOT_RUN"}
            for name, _ in tests
        ]
    }

    success = False
    print(f"Artifacts: {suite.output}", flush=True)

    try:
        print("Building application...", flush=True)
        suite.build()

        report["java_version"] = suite.java_version
        report["jar_sha256"] = suite.jar_hash
        report["fixture_sha256"] = suite.fixture_hashes

        print("Checking Elasticsearch...", flush=True)
        suite.prepare_es()
        suite.start_server()

        for record, (name, test) in zip(
                report["tests"], tests
        ):
            print(f"RUN  {name}", flush=True)
            started = time.monotonic()

            try:
                record["details"] = test()
                record["status"] = "PASS"

            except Exception as error:
                record["status"] = "FAIL"
                record["error"] = str(error)
                raise

            finally:
                record["seconds"] = round(
                    time.monotonic() - started, 3
                )

            print(f"PASS {name}", flush=True)

        require(
            all(
                digest(project / name) == value
                for name, value in suite.fixture_hashes.items()
            ),
            "Original fixture content changed."
        )

        success = True

    except Exception as error:
        report["error"] = str(error)
        print(
            f"FAIL {error}",
            file=sys.stderr,
            flush=True
        )

    finally:
        report["cleanup_errors"] = suite.cleanup()

        if report["cleanup_errors"]:
            success = False

        report["status"] = "PASS" if success else "FAIL"
        report["finished_at"] = (
            datetime.now(timezone.utc).isoformat()
        )
        report["process_logs"] = {
            str(pid): str(path)
            for pid, path in suite.logs.items()
        }

        (suite.output / "report.json").write_text(
            json.dumps(
                report,
                indent=2,
                ensure_ascii=True
            ),
            encoding="utf-8"
        )

        print(
            f"Result: {report['status']}; "
            f"report: {suite.output / 'report.json'}",
            flush=True
        )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
