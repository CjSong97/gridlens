# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# CELL 01 — Synthetic test configuration and validated config helpers
# This is a partial runtime notebook: no live entrypoint yet.
# TEST_CONFIG is only for mocks/planner fixtures, never live ingestion.
import json
import math
from datetime import datetime, timedelta, timezone

# EDIT TEST_CONFIGURATION HERE.
TEST_CONFIG = {
    "start_utc": "2026-01-01T00:00Z",
    "end_utc": "2026-01-08T00:00Z",
    "chunk_days": 14,
    "source_mode": "api",             # "api" or "snapshot"
    "snapshot_path": None,            # Required only for snapshot mode
    "catalog": "workspace",
    "schema": "bronze",               # Planned; not yet created/verified
    "table": "api_responses",
    "timeout_seconds": 20,
    "max_retries": 3,                 # Extra attempts after the first
    "retry_backoff_seconds": 2,
    "retry_backoff_cap_seconds": 30,
}

API_BASE_URL = "https://api.carbonintensity.org.uk"
RETRYABLE_HTTP_STATUSES = (408, 429, 500, 502, 503, 504)

def parse_utc(value):
    if not isinstance(value, str):
        raise ValueError("UTC inputs must be strings")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%MZ")
    except ValueError as exc:
        raise ValueError("Use UTC format YYYY-MM-DDTHH:MMZ") from exc
    if parsed.strftime("%Y-%m-%dT%H:%MZ") != value:
        raise ValueError("UTC input must use zero-padded components")
    return parsed.replace(tzinfo=timezone.utc)

def validate_config(config):
    start = parse_utc(config["start_utc"])
    end = parse_utc(config["end_utc"])

    if start >= end:
        raise ValueError("start_utc must be earlier than end_utc")
    if start.minute not in (0, 30) or end.minute not in (0, 30):
        raise ValueError("Use half-hour-aligned request bounds")

    days = config["chunk_days"]
    if type(days) is not int or not 1 <= days <= 14:
        raise ValueError("chunk_days must be an integer from 1 to 14")

    retries = config["max_retries"]
    if type(retries) is not int or not 0 <= retries <= 5:
        raise ValueError("max_retries must be an integer from 0 to 5")

    for key in (
        "timeout_seconds",
        "retry_backoff_seconds",
        "retry_backoff_cap_seconds",
    ):
        value = config[key]
        if (
            type(value) not in (int, float)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{key} must be a finite positive number")

    if config["timeout_seconds"] > 120:
        raise ValueError("timeout_seconds must not exceed 120")
    if config["retry_backoff_cap_seconds"] > 120:
        raise ValueError("retry_backoff_cap_seconds must not exceed 120")
    if (
        config["retry_backoff_seconds"]
        > config["retry_backoff_cap_seconds"]
    ):
        raise ValueError("Initial backoff must not exceed its cap")

    if config["source_mode"] not in ("api", "snapshot"):
        raise ValueError("source_mode must be api or snapshot")
    if config["source_mode"] == "snapshot":
        path = config["snapshot_path"]
        if not isinstance(path, str) or not path.strip():
            raise ValueError("Snapshot mode requires snapshot_path")

    # Conservative identifier policy for future SQL generation.
    for key in ("catalog", "schema", "table"):
        value = config[key]
        if (
            not isinstance(value, str)
            or not value
            or not value.isascii()
            or not (value[0].isalpha() or value[0] == "_")
            or not all(c.isalnum() or c == "_" for c in value)
        ):
            raise ValueError(f"{key} must be a simple SQL identifier")

    return start, end

start, end = validate_config(TEST_CONFIG)
print(json.dumps(TEST_CONFIG, indent=2))
print("Configured duration in days:", (end - start).total_seconds() / 86400)
print("Maximum attempts per request:", 1 + TEST_CONFIG["max_retries"])
print("Configuration validation: PASSED")

# COMMAND ----------

# CELL 02 — Request planner; prints URLs but makes no HTTP calls
def format_utc(value):
    return value.strftime("%Y-%m-%dT%H:%MZ")

def plan_windows(config):
    start, end = validate_config(config)
    step = timedelta(days=config["chunk_days"])
    windows = []
    cursor = start

    while cursor < end:
        next_cursor = min(cursor + step, end)
        windows.append((cursor, next_cursor))
        cursor = next_cursor

    return windows

def verify_windows(config, windows):
    start, end = validate_config(config)
    assert windows, "Expected at least one request window"
    assert windows[0][0] == start, "Wrong first boundary"
    assert windows[-1][1] == end, "Wrong final boundary"

    previous_end = start
    for left, right in windows:
        assert left == previous_end, "Gap or overlapping request windows"
        assert left < right, "Empty or reversed window"
        assert right - left <= timedelta(days=config["chunk_days"])
        assert right - left <= timedelta(days=14)
        previous_end = right

    assert sum(
        (right - left for left, right in windows), timedelta()
    ) == end - start, "Total planned duration does not match"

WINDOWS = plan_windows(TEST_CONFIG)
verify_windows(TEST_CONFIG, WINDOWS)

REQUEST_PLAN = [
    {
        "window_number": number,
        "from_utc": format_utc(left),
        "to_utc": format_utc(right),
        "url": (
            f"{API_BASE_URL}/intensity/"
            f"{format_utc(left)}/{format_utc(right)}"
        ),
    }
    for number, (left, right) in enumerate(WINDOWS, start=1)
]

print(json.dumps(REQUEST_PLAN, indent=2))
print("Window count:", len(WINDOWS))
print("Request coverage verification: PASSED")

# COMMAND ----------

# CELL 03 — Configuration/planner fixtures
def json_bytes(value):
    return json.dumps(value).encode("utf-8")

FIXTURES = {
    "success": {
        "status_code": 200,
        "body": json_bytes({
            "data": [{
                "from": "2026-10-10T00:00Z",
                "to": "2026-10-10T00:30Z",
                "intensity": {
                    "forecast": 100,
                    "actual": 95,
                    "index": "moderate",
                },
            }]
        }),
    },
    "malformed_json": {
        "status_code": 200,
        "body": b'{"data": [',
    },
    "empty_response": {
        "status_code": 200,
        "body": json_bytes({"data": []}),
    },
    "http_429": {
        "status_code": 429,
        "headers": {"Retry-After": "1"},
        "body": json_bytes({"error": "synthetic rate limit"}),
    },
    "http_503": {
        "status_code": 503,
        "body": json_bytes({"error": "synthetic unavailable"}),
    },
    "http_400": {
        "status_code": 400,
        "body": json_bytes({"error": "synthetic bad request"}),
    },
    "timeout": {
        "exception": TimeoutError("Synthetic timeout; no request made"),
    },
}

invalid_cases = [
    ("invalid date", {"start_utc": "not-a-date"}),
    ("missing UTC marker", {"start_utc": "2026-07-13T00:00"}),
    ("equal bounds", {"end_utc": TEST_CONFIG["start_utc"]}),
    ("reversed bounds", {"end_utc": "2025-12-31T00:00Z"}),
    ("unaligned bound", {"start_utc": "2026-07-13T00:15Z"}),
    ("oversized chunk", {"chunk_days": 15}),
    ("zero chunk", {"chunk_days": 0}),
    ("fractional chunk", {"chunk_days": 1.5}),
    ("negative retries", {"max_retries": -1}),
    ("fractional retries", {"max_retries": 1.5}),
    ("excessive retries", {"max_retries": 6}),
    ("zero timeout", {"timeout_seconds": 0}),
    ("infinite timeout", {"timeout_seconds": float("inf")}),
    ("invalid backoff", {"retry_backoff_seconds": 31}),
    ("invalid mode", {"source_mode": "unknown"}),
    ("missing snapshot", {"source_mode": "snapshot"}),
    ("invalid identifier", {"schema": "bronze; DROP TABLE x"}),
]

for label, override in invalid_cases:
    try:
        validate_config({**TEST_CONFIG, **override})
    except ValueError:
        pass
    else:
        raise AssertionError(f"Invalid config accepted: {label}")

window_cases = [
    ("half hour", "2026-01-01T00:00Z", "2026-01-01T00:30Z", 14, 1),
    ("exactly 14 days", "2026-01-01T00:00Z", "2026-01-15T00:00Z", 14, 1),
    ("14 days plus half hour", "2026-01-01T00:00Z", "2026-01-15T00:30Z", 14, 2),
    ("leap-day crossing", "2024-02-28T00:00Z", "2024-03-02T00:00Z", 1, 3),
    ("configured range", TEST_CONFIG["start_utc"], TEST_CONFIG["end_utc"], 14, 1),
]

for label, left, right, days, expected_count in window_cases:
    test_config = {
        **TEST_CONFIG,
        "start_utc": left,
        "end_utc": right,
        "chunk_days": days,
    }
    windows = plan_windows(test_config)
    verify_windows(test_config, windows)
    assert len(windows) == expected_count, label

# Validate fixture construction, not future ingestion behaviour.
assert len(json.loads(FIXTURES["success"]["body"])["data"]) == 1
assert json.loads(FIXTURES["empty_response"]["body"])["data"] == []
try:
    json.loads(FIXTURES["malformed_json"]["body"])
except json.JSONDecodeError:
    pass
else:
    raise AssertionError("Malformed fixture unexpectedly parsed")

assert FIXTURES["http_429"]["status_code"] in RETRYABLE_HTTP_STATUSES
assert FIXTURES["http_503"]["status_code"] in RETRYABLE_HTTP_STATUSES
assert FIXTURES["http_400"]["status_code"] not in RETRYABLE_HTTP_STATUSES
assert isinstance(FIXTURES["timeout"]["exception"], TimeoutError)

print(f"Invalid configurations rejected: {len(invalid_cases)}")
print(f"Window-planning cases passed: {len(window_cases)}")
print("Fixtures available:", ", ".join(FIXTURES))
print("Configuration and fixture checks: PASSED")

# COMMAND ----------

# CELL 04 — Fetcher definitions; default network transport disabled
import hashlib
import time
import uuid
import requests
from email.utils import parsedate_to_datetime

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def live_transport(url, timeout_seconds):
    raise RuntimeError("Live HTTP is disabled in this fixture-only checkpoint")

def check_response_envelope(raw_body):
    # Basic contract only: no Silver numeric/interval validation here.
    try:
        payload = json.loads(raw_body)
    except (ValueError, UnicodeDecodeError) as exc:
        return "malformed_json", str(exc)[:300], None

    if not isinstance(payload, dict):
        return "unexpected_envelope", "Expected a JSON object", None

    records = payload.get("data")
    if not isinstance(records, list):
        return "unexpected_envelope", "Expected a data list", None
    if not records:
        return "empty_response", "Expected at least one interval", 0

    for record in records:
        if not isinstance(record, dict):
            return "unexpected_envelope", "Interval is not an object", None
        if not all(isinstance(record.get(k), str) for k in ("from", "to")):
            return "unexpected_envelope", "Missing string interval bounds", None
        intensity = record.get("intensity")
        if not isinstance(intensity, dict):
            return "unexpected_envelope", "Missing intensity object", None
        if not {"forecast", "actual", "index"}.issubset(intensity):
            return "unexpected_envelope", "Missing intensity fields", None

    return None, None, len(records)

def retry_after_seconds(headers):
    value = next(
        (v for k, v in headers.items() if k.lower() == "retry-after"),
        None,
    )
    if value is None:
        return None

    text = str(value).strip()
    if text.isascii() and text.isdigit():
        return float(int(text))

    try:
        target = parsedate_to_datetime(text)
        if target.tzinfo is None:
            return None
        return max(
            0.0,
            (target - datetime.now(timezone.utc)).total_seconds(),
        )
    except (TypeError, ValueError, OverflowError):
        return None

def fetch_window(
    config,
    from_utc,
    to_utc,
    transport=live_transport,
    sleep_fn=time.sleep,
    run_id=None,
):
    start, end = validate_config({
        **config,
        "start_utc": from_utc,
        "end_utc": to_utc,
    })
    if config["source_mode"] != "api":
        raise ValueError("This fetcher supports API mode only")
    if end - start > timedelta(days=14):
        raise ValueError("A single API request must not exceed 14 days")

    url = f"{API_BASE_URL}/intensity/{from_utc}/{to_utc}"
    result = {
        "status": "failed",
        "run_id": run_id or uuid.uuid4().hex,
        "request_id": uuid.uuid4().hex,
        "attempts": [],
    }

    for number in range(1, config["max_retries"] + 2):
        attempt = {
            "run_id": result["run_id"],
            "request_id": result["request_id"],
            "attempt_number": number,
            "source": "national_carbon_intensity_api",
            "source_mode": "api",
            "request_url": url,
            "request_from_utc": from_utc,
            "request_to_utc": to_utc,
            "started_at_utc": utc_now(),
            "retrieved_at_utc": None,
            "finished_at_utc": None,
            "http_status": None,
            "response_headers": {},
            "raw_body": None,            # Exact bytes when a body is returned
            "response_sha256": None,
            "record_count": None,
            "status": "failed",
            "failure_kind": None,
            "error": None,
            "retry_delay_seconds": None,
        }
        retryable = False

        try:
            response = transport(url, config["timeout_seconds"])
            attempt["http_status"] = response["status_code"]
            attempt["response_headers"] = response.get("headers", {})
            raw_body = response["body"]
            if not isinstance(raw_body, bytes):
                raise TypeError("Transport must return body as bytes")

            attempt["raw_body"] = raw_body
            attempt["retrieved_at_utc"] = utc_now()
            attempt["response_sha256"] = hashlib.sha256(raw_body).hexdigest()

            if attempt["http_status"] == 200:
                kind, error, count = check_response_envelope(raw_body)
                attempt["failure_kind"] = kind
                attempt["error"] = error
                attempt["record_count"] = count
                if kind is None:
                    attempt["status"] = "succeeded"
            else:
                attempt["failure_kind"] = "http_error"
                attempt["error"] = f"HTTP {attempt['http_status']}"
                retryable = (
                    attempt["http_status"] in RETRYABLE_HTTP_STATUSES
                )

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            TimeoutError,
            ConnectionError,
        ) as exc:
            attempt["failure_kind"] = "transport_error"
            attempt["error"] = type(exc).__name__
            retryable = True
        except Exception as exc:
            attempt["failure_kind"] = "unexpected_error"
            attempt["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"

        attempt["finished_at_utc"] = utc_now()
        result["attempts"].append(attempt)

        if attempt["status"] == "succeeded":
            result["status"] = "succeeded"
            return result

        if not retryable or number == config["max_retries"] + 1:
            return result

        delay = min(
            config["retry_backoff_seconds"] * (2 ** (number - 1)),
            config["retry_backoff_cap_seconds"],
        )
        source_delay = retry_after_seconds(attempt["response_headers"])
        if source_delay is not None:
            if source_delay > config["retry_backoff_cap_seconds"]:
                attempt["error"] += (
                    "; Retry-After exceeds configured cap; stopping"
                )
                return result
            delay = max(delay, source_delay)

        attempt["retry_delay_seconds"] = delay
        sleep_fn(delay)

    return result

print("Fetcher definitions loaded; no request made.")

# COMMAND ----------

# CELL 05 — Mock transport retry/envelope tests
def mock_transport(sequence):
    state = {"calls": 0}

    def transport(url, timeout_seconds):
        index = state["calls"]
        state["calls"] += 1
        if index >= len(sequence):
            raise AssertionError("Unexpected extra transport call")

        item = sequence[index]
        if "exception" in item:
            raise item["exception"]
        return item

    return transport, state

def run_case(
    name,
    sequence,
    expected_status,
    expected_attempts,
    expected_kinds,
    expected_delays,
    override=None,
):
    config = {**TEST_CONFIG, **(override or {})}
    transport, state = mock_transport(sequence)
    delays = []

    result = fetch_window(
        config,
        "2026-10-10T00:00Z",
        "2026-10-10T01:00Z",
        transport=transport,
        sleep_fn=delays.append,
    )

    assert result["status"] == expected_status, name
    assert len(result["attempts"]) == expected_attempts, name
    assert state["calls"] == expected_attempts, name
    assert delays == expected_delays, name
    assert [
        a["failure_kind"] for a in result["attempts"]
    ] == expected_kinds, name

    for attempt in result["attempts"]:
        assert attempt["run_id"] == result["run_id"], name
        assert attempt["request_id"] == result["request_id"], name
        assert attempt["started_at_utc"] and attempt["finished_at_utc"], name
        if attempt["raw_body"] is not None:
            assert attempt["retrieved_at_utc"], name
            assert attempt["response_sha256"] == hashlib.sha256(
                attempt["raw_body"]
            ).hexdigest(), name

    # Confirm returned failure/success bodies were not discarded.
    for item, attempt in zip(sequence, result["attempts"]):
        if "body" in item:
            assert attempt["raw_body"] == item["body"], name

    return {"test": name, "status": "PASSED", "attempts": expected_attempts}

success = FIXTURES["success"]
unavailable = FIXTURES["http_503"]
timeout = FIXTURES["timeout"]

missing_fields = {
    "status_code": 200,
    "body": json_bytes({
        "data": [{
            "from": "2026-10-10T00:00Z",
            "to": "2026-10-10T00:30Z",
            "intensity": {},
        }]
    }),
}
long_retry_after = {
    **FIXTURES["http_429"],
    "headers": {"Retry-After": "120"},
}

TEST_REPORT = [
    run_case("success", [success], "succeeded", 1, [None], []),
    run_case(
        "503 then success", [unavailable, success], "succeeded", 2,
        ["http_error", None], [2],
    ),
    run_case(
        "timeout then success", [timeout, success], "succeeded", 2,
        ["transport_error", None], [2],
    ),
    run_case(
        "connection error then success",
        [{"exception": requests.exceptions.ConnectionError("synthetic")},
         success],
        "succeeded", 2, ["transport_error", None], [2],
    ),
    run_case(
        "429 then success", [FIXTURES["http_429"], success],
        "succeeded", 2, ["http_error", None], [2],
    ),
    run_case(
        "permanent 400", [FIXTURES["http_400"]],
        "failed", 1, ["http_error"], [],
    ),
    run_case(
        "malformed JSON", [FIXTURES["malformed_json"]],
        "failed", 1, ["malformed_json"], [],
    ),
    run_case(
        "empty response", [FIXTURES["empty_response"]],
        "failed", 1, ["empty_response"], [],
    ),
    run_case(
        "missing intensity fields", [missing_fields],
        "failed", 1, ["unexpected_envelope"], [],
    ),
    run_case(
        "503 retry exhaustion", [unavailable] * 4,
        "failed", 4, ["http_error"] * 4, [2, 4, 8],
    ),
    run_case(
        "timeout retry exhaustion", [timeout] * 4,
        "failed", 4, ["transport_error"] * 4, [2, 4, 8],
    ),
    run_case(
        "retries disabled", [unavailable],
        "failed", 1, ["http_error"], [], {"max_retries": 0},
    ),
    run_case(
        "backoff capped", [unavailable] * 4,
        "failed", 4, ["http_error"] * 4, [2, 3, 3],
        {"retry_backoff_cap_seconds": 3},
    ),
    run_case(
        "long Retry-After stops", [long_retry_after],
        "failed", 1, ["http_error"], [],
    ),
]

print(json.dumps(TEST_REPORT, indent=2))
print("Controlled fetcher tests passed:", len(TEST_REPORT))
print("FIXTURE-ONLY acceptance checks: PASSED")

# COMMAND ----------

# CELL 06 — Verify fail-closed transport and emit checkpoint evidence
try:
    live_transport("https://invalid.example", 1)
except RuntimeError as exc:
    if str(exc) != "Live HTTP is disabled in this fixture-only checkpoint":
        raise
else:
    raise RuntimeError("Default network transport was not blocked")

if len(TEST_REPORT) != 14 or not all(
    item["status"] == "PASSED" for item in TEST_REPORT
):
    raise RuntimeError("Mock transport checks incomplete")

print(json.dumps({
    "checkpoint": "runtime_ingestion_definitions_and_mock_tests_v1",
    "invalid_configurations_rejected": len(invalid_cases),
    "planner_tests_passed": len(window_cases),
    "mock_fetcher_tests_passed": len(TEST_REPORT),
    "default_network_transport": "BLOCKED",
    "persistent_writes": False,
    "live_refresh_executed": False,
    "runtime_notebook_complete": False,
    "next_stage": "parameter binding and persisted attempt evidence",
}, indent=2))

# COMMAND ----------

# CELL 07 — Read-only existing Bronze contract check
import json

spark.conf.set("spark.sql.session.timeZone", "UTC")

BRONZE_TABLE = "workspace.bronze.api_responses"

EXPECTED_BRONZE = {
    "event_id": ("string", False),
    "run_id": ("string", False),
    "request_id": ("string", False),
    "attempt_number": ("int", False),
    "source": ("string", False),
    "source_mode": ("string", False),
    "is_fixture": ("boolean", False),
    "event_kind": ("string", False),
    "request_url": ("string", False),
    "request_from_utc": ("timestamp", False),
    "request_to_utc": ("timestamp", False),
    "started_at_utc": ("timestamp", False),
    "finished_at_utc": ("timestamp", False),
    "retrieved_at_utc": ("timestamp", True),
    "stored_at_utc": ("timestamp", False),
    "http_status": ("int", True),
    "raw_body": ("binary", True),
    "raw_text": ("string", True),
    "response_sha256": ("string", True),
    "status": ("string", False),
    "failure_kind": ("string", True),
    "error": ("string", True),
    "record_count": ("int", True),
    "retry_delay_seconds": ("double", True),
    "response_headers_json": ("string", False),
}

actual_schema = spark.table(BRONZE_TABLE).schema
actual = {
    field.name: (field.dataType.simpleString(), field.nullable)
    for field in actual_schema.fields
}

if actual != EXPECTED_BRONZE:
    differences = {
        name: {
            "expected": EXPECTED_BRONZE.get(name),
            "actual": actual.get(name),
        }
        for name in sorted(set(EXPECTED_BRONZE) | set(actual))
        if EXPECTED_BRONZE.get(name) != actual.get(name)
    }
    raise RuntimeError(
        "Bronze schema mismatch; no changes made: "
        + json.dumps(differences, sort_keys=True)
    )

detail = spark.sql(
    "DESCRIBE DETAIL `workspace`.`bronze`.`api_responses`"
).first()

if detail["format"] != "delta":
    raise RuntimeError("Bronze is not Delta; no changes made")

print(json.dumps({
    "bronze_contract": "PASS",
    "table": BRONZE_TABLE,
    "column_count": len(actual),
    "types_and_nullability_match": True,
    "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
    "persistent_writes": False,
}, indent=2))

# COMMAND ----------

# CELL 08 — Fail-closed parameter contract; no live execution
import json
from datetime import datetime, timedelta, timezone

PARAMETER_DEFAULTS = {
    "execution_mode": "fixture",
    "allow_live_writes": "false",
    "run_anchor_utc": "2026-01-08T00:17:00Z",
    "overlap_days": "7",
    "publication_start_utc": "",
    "publication_end_utc": "",
    "orchestration_id": "",
    "task_run_id": "",
    "task_execution_count": "",
}

# Preserves existing values, including supplied job parameters.
for name, default in PARAMETER_DEFAULTS.items():
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


def bind_runtime_parameters(raw):
    values = {}
    for name in PARAMETER_DEFAULTS:
        value = raw.get(name, "")
        if not isinstance(value, str):
            raise ValueError(f"Parameter must be a string: {name}")
        value = value.strip()
        if "{{" in value or "}}" in value:
            raise ValueError(f"Unresolved parameter: {name}")
        values[name] = value

    mode = values["execution_mode"]
    if mode not in ("fixture", "live"):
        raise ValueError("execution_mode must be fixture or live")
    flag = values["allow_live_writes"]
    if flag not in ("true", "false"):
        raise ValueError("allow_live_writes must be true or false")
    if mode == "fixture" and flag != "false":
        raise ValueError("Fixture mode forbids live write authorization")
    if mode == "live" and flag != "true":
        raise ValueError("Live mode requires explicit write authorization")

    anchor = datetime.fromisoformat(
        values["run_anchor_utc"].replace("Z", "+00:00")
    )
    if anchor.tzinfo is None:
        raise ValueError("run_anchor_utc must include a timezone")
    anchor = anchor.astimezone(timezone.utc)
    latest_end = anchor.replace(
        minute=(anchor.minute // 30) * 30, second=0, microsecond=0
    )

    overlap_text = values["overlap_days"]
    if not overlap_text.isascii() or not overlap_text.isdigit():
        raise ValueError("overlap_days must be an integer string")
    overlap = int(overlap_text)
    if not 1 <= overlap <= 14:
        raise ValueError("This initial runtime supports 1–14 overlap days")

    left_text = values["publication_start_utc"]
    right_text = values["publication_end_utc"]
    if bool(left_text) != bool(right_text):
        raise ValueError("Supply both publication bounds, or neither")
    if left_text:
        # Reuse the verified strict YYYY-MM-DDTHH:MMZ parser.
        left, right = parse_utc(left_text), parse_utc(right_text)
    else:
        right = latest_end
        left = right - timedelta(days=overlap)
    if left >= right:
        raise ValueError("Publication bounds must increase")
    if left.minute not in (0, 30) or right.minute not in (0, 30):
        raise ValueError("Publication bounds must be half-hour aligned")
    if right > latest_end:
        raise ValueError("Publication end exceeds the fixed run anchor")
    if right - left > timedelta(days=14):
        raise ValueError("Initial runtime range cap is 14 days")

    if mode == "live":
        for name in ("orchestration_id", "task_run_id"):
            if not values[name].isascii() or not values[name].isdigit():
                raise ValueError(f"Missing/invalid job identity: {name}")
        count_text = values["task_execution_count"]
        if not count_text.isascii() or not count_text.isdigit():
            raise ValueError("Missing/invalid task_execution_count")
        execution_count = int(count_text)
        if execution_count < 1:
            raise ValueError("task_execution_count must be positive")
        identity = {
            "orchestration_id": values["orchestration_id"],
            "task_run_id": values["task_run_id"],
            "task_execution_count": execution_count,
            "identity_kind": "job",
        }
    else:
        # Explicitly synthetic; not a job identity or a success flag.
        identity = {
            "orchestration_id": "fixture-orchestration",
            "task_run_id": "fixture-task",
            "task_execution_count": 1,
            "identity_kind": "synthetic",
        }

    config = {
        **TEST_CONFIG,
        "start_utc": format_utc(left),
        "end_utc": format_utc(right),
        "source_mode": "api",
        "snapshot_path": None,
        "chunk_days": 14,
    }
    windows = plan_windows(config)
    verify_windows(config, windows)
    manifest = {
        **identity,
        "execution_mode": mode,
        "allow_live_writes": flag == "true",
        "run_anchor_utc": anchor.isoformat(),
        "publication_start_utc": format_utc(left),
        "publication_end_utc": format_utc(right),
        "overlap_days": overlap,
        "policy_version": "gridlens_ops_v1",
        "expected_interval_count": int((right-left).total_seconds() // 1800),
        "planned_request_windows": len(windows),
    }
    return config, windows, manifest


raw_parameters = {
    name: dbutils.widgets.get(name) for name in PARAMETER_DEFAULTS
}
RUNTIME_CONFIG, RUNTIME_WINDOWS, RUNTIME_MANIFEST = bind_runtime_parameters(
    raw_parameters
)
# This checkpoint cannot execute live even if binding succeeds.
if RUNTIME_MANIFEST["execution_mode"] != "fixture":
    raise RuntimeError("This checkpoint permits fixture execution only")
print(json.dumps({"parameter_manifest": RUNTIME_MANIFEST}, indent=2))

# COMMAND ----------

# CELL 09 — Bronze attempt conversion definitions; no persistence
import hashlib
import uuid


def timestamp_value(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include its UTC offset")
    return parsed.astimezone(timezone.utc)


def fixture_rows(results):
    rows = []
    for result in results:
        for attempt in result["attempts"]:
            raw = attempt["raw_body"]
            if raw is not None:
                if not isinstance(raw, bytes):
                    raise TypeError("Expected raw response bytes")
                expected_hash = hashlib.sha256(raw).hexdigest()
                if attempt["response_sha256"] != expected_hash:
                    raise ValueError("Input response hash mismatch")
            headers = {
                key.lower(): value
                for key, value in attempt["response_headers"].items()
                if key.lower() in ("content-type", "retry-after")
            }
            row = {
                "event_id": uuid.uuid4().hex,
                "run_id": result["run_id"],
                "request_id": result["request_id"],
                "attempt_number": attempt["attempt_number"],
                "source": attempt["source"],
                "source_mode": "fixture",
                "is_fixture": True,
                "event_kind": "response" if raw is not None else "transport_failure",
                "request_url": attempt["request_url"],
                "request_from_utc": parse_utc(attempt["request_from_utc"]),
                "request_to_utc": parse_utc(attempt["request_to_utc"]),
                "started_at_utc": timestamp_value(attempt["started_at_utc"]),
                "finished_at_utc": timestamp_value(attempt["finished_at_utc"]),
                "retrieved_at_utc": timestamp_value(attempt["retrieved_at_utc"]),
                "stored_at_utc": datetime.now(timezone.utc),
                "http_status": attempt["http_status"],
                "raw_body": raw,
                "raw_text": raw.decode("utf-8", errors="replace") if raw is not None else None,
                "response_sha256": attempt["response_sha256"],
                "status": attempt["status"],
                "failure_kind": attempt["failure_kind"],
                "error": attempt["error"],
                "record_count": attempt["record_count"],
                "retry_delay_seconds": float(attempt["retry_delay_seconds"])
                    if attempt["retry_delay_seconds"] is not None else None,
                "response_headers_json": json.dumps(headers),
            }
            rows.append(row)
    return rows


def prepare_fixture_attempt_rows(results):
    """Stricter wrapper around the verified row converter, still fixture-only."""
    if not results:
        raise ValueError("No results supplied")
    seen_requests = set()
    for result in results:
        if not result["run_id"] or not result["request_id"]:
            raise ValueError("Missing result identity")
        identity = (result["run_id"], result["request_id"])
        if identity in seen_requests:
            raise ValueError("Repeated request result")
        seen_requests.add(identity)
        attempts = result["attempts"]
        if not attempts:
            raise ValueError("No attempt evidence")
        if [a["attempt_number"] for a in attempts] != list(range(1, len(attempts)+1)):
            raise ValueError("Attempt sequence is not contiguous")
        for i, attempt in enumerate(attempts):
            if attempt["run_id"] != result["run_id"] or attempt["request_id"] != result["request_id"]:
                raise ValueError("Attempt/result identity mismatch")
            if attempt["status"] not in ("succeeded", "failed"):
                raise ValueError("Unknown attempt outcome")
            if attempt["status"] == "succeeded" and i != len(attempts)-1:
                raise ValueError("Attempts after successful terminal outcome")
            if attempt["raw_body"] is None and (
                attempt["response_sha256"] is not None
                or attempt["retrieved_at_utc"] is not None
            ):
                raise ValueError("Bodyless attempt has response provenance")
        if result["status"] != attempts[-1]["status"]:
            raise ValueError("Terminal result/attempt outcome mismatch")
    rows = fixture_rows(results)
    required = [name for name, (_, nullable) in EXPECTED_BRONZE.items() if not nullable]
    for row in rows:
        if set(row) != set(EXPECTED_BRONZE):
            raise ValueError("Converted row columns differ from Bronze contract")
        if any(row[name] is None for name in required):
            raise ValueError("Converted row violates required-field contract")
        if row["is_fixture"] is not True or row["source_mode"] != "fixture":
            raise ValueError("Fixture labelling lost")
    return rows

print("Attempt conversion definitions loaded; no writes performed.")

# COMMAND ----------

# CELL 10 — Contract/conversion fixtures plus in-memory Spark roundtrip
from copy import deepcopy
from pyspark.sql import types as T

PARAMETER_TEST_RESULTS = []
base = dict(PARAMETER_DEFAULTS)
_, _, first = bind_runtime_parameters(base)
if first["publication_start_utc"] != "2026-01-01T00:00Z" or first["publication_end_utc"] != "2026-01-08T00:00Z":
    raise AssertionError("Default fixed-anchor bounds wrong")
if first["expected_interval_count"] != 336:
    raise AssertionError("Seven-day interval count wrong")
PARAMETER_TEST_RESULTS.append("fixed-anchor seven-day bounds")
if bind_runtime_parameters(base)[2] != first:
    raise AssertionError("Bound manifest changed on repeated binding")
PARAMETER_TEST_RESULTS.append("stable repeated binding")
explicit = {**base, "publication_start_utc": "2026-01-06T00:00Z", "publication_end_utc": "2026-01-07T00:00Z"}
if bind_runtime_parameters(explicit)[2]["expected_interval_count"] != 48:
    raise AssertionError("Explicit bounds wrong")
PARAMETER_TEST_RESULTS.append("explicit bounds")

invalid_parameters = [
    ("missing mode", {"execution_mode": ""}),
    ("unknown mode", {"execution_mode": "other"}),
    ("invalid boolean", {"allow_live_writes": "yes"}),
    ("fixture write authorization", {"allow_live_writes": "true"}),
    ("unresolved anchor", {"run_anchor_utc": "{{job.start_time.iso_datetime}}"}),
    ("naive anchor", {"run_anchor_utc": "2026-01-08T00:17:00"}),
    ("zero overlap", {"overlap_days": "0"}),
    ("fractional overlap", {"overlap_days": "1.5"}),
    ("oversized overlap", {"overlap_days": "15"}),
    ("one bound only", {"publication_start_utc": "2026-01-06T00:00Z"}),
    ("unaligned bounds", {"publication_start_utc": "2026-01-06T00:15Z", "publication_end_utc": "2026-01-07T00:00Z"}),
    ("future bounds", {"publication_start_utc": "2026-01-08T00:00Z", "publication_end_utc": "2026-01-09T00:00Z"}),
    ("oversized range", {"publication_start_utc": "2025-12-01T00:00Z", "publication_end_utc": "2026-01-07T00:00Z"}),
    ("live without authorization", {"execution_mode": "live"}),
    ("live missing identities", {"execution_mode": "live", "allow_live_writes": "true"}),
]
for label, override in invalid_parameters:
    try:
        bind_runtime_parameters({**base, **override})
    except ValueError:
        PARAMETER_TEST_RESULTS.append(label)
    else:
        raise AssertionError(f"Invalid parameters accepted: {label}")

# Same invocation bounds and Bronze run ID for all synthetic request results.
probe_run_id = uuid.uuid4().hex
ATTEMPT_TEST_RESULTS = []
sequences = [
    [FIXTURES["http_503"], FIXTURES["success"]],
    [FIXTURES["timeout"], FIXTURES["success"]],
    [FIXTURES["http_400"]],
    [FIXTURES["http_503"]] * 4,
    [FIXTURES["malformed_json"]],
    [{**FIXTURES["success"], "headers": {"Content-Type": "application/json", "X-Private-Test": "synthetic"}}],
]
results = []
for sequence in sequences:
    transport, state = mock_transport(sequence)
    result = fetch_window(
        TEST_CONFIG, "2026-10-10T00:00Z", "2026-10-10T01:00Z",
        transport=transport, sleep_fn=lambda delay: None, run_id=probe_run_id,
    )
    results.append(result)

ATTEMPT_ROWS = prepare_fixture_attempt_rows(results)
if len(ATTEMPT_ROWS) != 11:
    raise AssertionError("Failed/successful attempts were not all retained")
ATTEMPT_TEST_RESULTS.append("all 11 attempts retained")
if sum(row["status"] == "succeeded" for row in ATTEMPT_ROWS) != 3:
    raise AssertionError("Attempt outcome counts wrong")
ATTEMPT_TEST_RESULTS.append("three success and eight failure rows")
if sum(row["event_kind"] == "transport_failure" for row in ATTEMPT_ROWS) != 1:
    raise AssertionError("Bodyless transport failure not distinguished")
ATTEMPT_TEST_RESULTS.append("transport failure distinguished")
if len({row["event_id"] for row in ATTEMPT_ROWS}) != 11:
    raise AssertionError("Event IDs not unique")
ATTEMPT_TEST_RESULTS.append("unique event IDs")
for row in ATTEMPT_ROWS:
    if row["raw_body"] is not None:
        if hashlib.sha256(row["raw_body"]).hexdigest() != row["response_sha256"]:
            raise AssertionError("Hash mismatch")
        if row["raw_text"] != row["raw_body"].decode("utf-8", errors="replace"):
            raise AssertionError("Text mismatch")
ATTEMPT_TEST_RESULTS.append("raw body hash/text fidelity")
if any(set(json.loads(row["response_headers_json"])) - {"content-type", "retry-after"} for row in ATTEMPT_ROWS):
    raise AssertionError("Unapproved response headers retained")
ATTEMPT_TEST_RESULTS.append("header allowlist")

# Re-conversion represents distinct retrieval events, not an append retry policy.
second_rows = prepare_fixture_attempt_rows(results)
if {r["event_id"] for r in ATTEMPT_ROWS} & {r["event_id"] for r in second_rows}:
    raise AssertionError("Re-conversion event IDs not independent")
ATTEMPT_TEST_RESULTS.append("distinct conversion event IDs")

for label in ("hash corruption", "identity mismatch", "sequence mismatch", "outcome mismatch", "response-less provenance", "request repeated"):
    altered = deepcopy(results)
    if label == "hash corruption":
        altered[0]["attempts"][0]["response_sha256"] = "wrong"
    elif label == "identity mismatch":
        altered[0]["attempts"][0]["run_id"] = "wrong"
    elif label == "sequence mismatch":
        altered[0]["attempts"][0]["attempt_number"] = 2
    elif label == "outcome mismatch":
        altered[0]["status"] = "failed"
    elif label == "response-less provenance":
        altered[1]["attempts"][0]["retrieved_at_utc"] = "2026-01-01T00:00:00Z"
    else:
        altered.append(deepcopy(altered[0]))
    try:
        prepare_fixture_attempt_rows(altered)
    except ValueError:
        ATTEMPT_TEST_RESULTS.append(label)
    else:
        raise AssertionError(f"Corruption accepted: {label}")

spark.conf.set("spark.sql.session.timeZone", "UTC")
types_by_name = {
    "string": T.StringType(), "int": T.IntegerType(),
    "boolean": T.BooleanType(), "timestamp": T.TimestampType(),
    "binary": T.BinaryType(), "double": T.DoubleType(),
}
ATTEMPT_SCHEMA = T.StructType([
    T.StructField(name, types_by_name[kind], nullable)
    for name, (kind, nullable) in EXPECTED_BRONZE.items()
])
ATTEMPT_DF = spark.createDataFrame(ATTEMPT_ROWS, ATTEMPT_SCHEMA)
actual_signature = {
    field.name: (field.dataType.simpleString(), field.nullable)
    for field in ATTEMPT_DF.schema.fields
}
if actual_signature != EXPECTED_BRONZE:
    raise AssertionError("In-memory DataFrame schema differs")
collected = [row.asDict(recursive=True) for row in ATTEMPT_DF.collect()]

def normalise_fixture_row(row):
    return {
        name: (value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc))
            if isinstance(value, datetime)
            else bytes(value) if isinstance(value, (bytes, bytearray))
            else value
        for name, value in row.items()
    }

before = {row["event_id"]: normalise_fixture_row(row) for row in ATTEMPT_ROWS}
after = {row["event_id"]: normalise_fixture_row(row) for row in collected}
if len(collected) != 11 or before != after:
    raise AssertionError("In-memory Spark roundtrip altered rows")
ATTEMPT_TEST_RESULTS.append("explicit-schema Spark roundtrip")

print(json.dumps({
    "checkpoint": "parameter_binding_and_attempt_conversion_v1",
    "parameter_tests_passed": len(PARAMETER_TEST_RESULTS),
    "attempt_tests_passed": len(ATTEMPT_TEST_RESULTS),
    "fixture_attempt_rows": len(ATTEMPT_ROWS),
    "fixture_success_rows": 3,
    "fixture_failure_rows": 8,
    "in_memory_spark_roundtrip": "PASS",
    "persistent_writes": False,
    "api_requests": False,
    "live_refresh_executed": False,
    "runtime_notebook_complete": False,
}, indent=2))

# COMMAND ----------

# CELL 11 — Approved disposable Bronze fixture table
import json
import re
import uuid
from pyspark.sql import functions as F

if RUNTIME_MANIFEST["execution_mode"] != "fixture":
    raise RuntimeError("This checkpoint requires fixture mode")
if RUNTIME_MANIFEST["allow_live_writes"]:
    raise RuntimeError("Live write authorization must remain false")

if globals().get("RUNTIME_FIXTURE_ACTIVE", False):
    raise RuntimeError(
        "A fixture table may still exist. Clean it up before rerunning Cell 11."
    )

RUNTIME_FIXTURE_TABLE = (
    "workspace.bronze._gridlens_runtime_fixture_" + uuid.uuid4().hex
)
RUNTIME_FIXTURE_SQL = ".".join(
    f"`{part}`" for part in RUNTIME_FIXTURE_TABLE.split(".")
)

# Set before CREATE so an uncertain outcome remains traceable.
RUNTIME_FIXTURE_ACTIVE = True

print(json.dumps({
    "fixture_table": RUNTIME_FIXTURE_TABLE,
    "cleanup_sql": f"DROP TABLE IF EXISTS {RUNTIME_FIXTURE_SQL}",
    "live_tables_touched": False,
}, indent=2))

column_ddl = ",\n".join(
    f"`{name}` {kind.upper()}"
    + ("" if nullable else " NOT NULL")
    for name, (kind, nullable) in EXPECTED_BRONZE.items()
)

spark.sql(f"""
    CREATE TABLE {RUNTIME_FIXTURE_SQL} (
        {column_ddl}
    ) USING DELTA
""")


def require_runtime_fixture():
    if not RUNTIME_FIXTURE_ACTIVE:
        raise RuntimeError("Fixture table is not active")
    if not re.fullmatch(
        r"workspace\.bronze\._gridlens_runtime_fixture_[0-9a-f]{32}",
        RUNTIME_FIXTURE_TABLE,
    ):
        raise RuntimeError("Unapproved fixture destination")

    actual = {
        field.name: (field.dataType.simpleString(), field.nullable)
        for field in spark.table(RUNTIME_FIXTURE_TABLE).schema.fields
    }
    if actual != EXPECTED_BRONZE:
        raise RuntimeError("Fixture schema mismatch; no schema evolution")

    detail = spark.sql(
        f"DESCRIBE DETAIL {RUNTIME_FIXTURE_SQL}"
    ).first()
    if detail["format"] != "delta":
        raise RuntimeError("Fixture destination is not Delta")


require_runtime_fixture()
print("PASS: isolated Bronze fixture table created and checked.")

# COMMAND ----------

# CELL 12 — Bounded prepared-event persistence and reconciliation
from copy import deepcopy
from datetime import datetime, timezone


class FixturePersistenceError(RuntimeError):
    def __init__(self, message, receipt):
        super().__init__(message)
        self.receipt = deepcopy(receipt)


def normalise_attempt(row):
    return {
        key: (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
        if isinstance(value, datetime)
        else bytes(value)
        if isinstance(value, (bytes, bytearray))
        else value
        for key, value in row.items()
    }


def persist_prepared_fixture_rows(
    prepared, read_fn, append_fn, max_append_calls=2
):
    if not prepared:
        raise ValueError("Empty prepared batch")
    if type(max_append_calls) is not int or not 0 <= max_append_calls <= 2:
        raise ValueError("Append budget must be 0–2")

    expected = {}
    for row in prepared:
        if set(row) != set(EXPECTED_BRONZE):
            raise ValueError("Prepared row schema mismatch")
        if row["is_fixture"] is not True or row["source_mode"] != "fixture":
            raise ValueError("Only fixture rows are permitted")

        event_id = row["event_id"]
        if (
            not isinstance(event_id, str)
            or not event_id
            or event_id in expected
        ):
            raise ValueError("Missing or duplicate prepared event ID")

        expected[event_id] = normalise_attempt(row)

    receipt = {
        "expected_events": len(expected),
        "append_calls": 0,
        "append_errors": [],
        "confirmed_events": 0,
        "status": "UNCONFIRMED",
    }

    while True:
        try:
            observed_rows = read_fn(list(expected))
            observed = {}

            for row in observed_rows:
                event_id = row["event_id"]
                if event_id in observed or event_id not in expected:
                    raise ValueError(
                        "Duplicate or unexpected persisted event ID"
                    )

                observed[event_id] = normalise_attempt(row)
                if observed[event_id] != expected[event_id]:
                    raise ValueError(
                        "Persisted event identity/content collision"
                    )

        except Exception as exc:
            receipt["status"] = "READ_OR_CONTENT_FAILURE"
            raise FixturePersistenceError(
                str(exc)[:500], receipt
            ) from exc

        receipt["confirmed_events"] = len(observed)
        missing = [
            row for row in prepared
            if row["event_id"] not in observed
        ]

        if not missing:
            receipt["status"] = (
                "CONFIRMED_AFTER_ERROR"
                if receipt["append_errors"]
                else "CONFIRMED"
            )
            return receipt

        if receipt["append_calls"] >= max_append_calls:
            receipt["status"] = "INCOMPLETE"
            raise FixturePersistenceError(
                "Prepared batch remains incomplete", receipt
            )

        receipt["append_calls"] += 1
        try:
            append_fn(deepcopy(missing))
        except Exception as exc:
            receipt["append_errors"].append({
                "append_call": receipt["append_calls"],
                "error_type": type(exc).__name__,
                "message": str(exc)[:300],
            })
            # Reconcile committed rows before considering another append.


def read_fixture_events(event_ids):
    require_runtime_fixture()
    return [
        row.asDict(recursive=True)
        for row in (
            spark.table(RUNTIME_FIXTURE_TABLE)
            .where(F.col("event_id").isin(event_ids))
            .limit(len(event_ids) + 1)
            .collect()
        )
    ]


def append_fixture_events(rows):
    require_runtime_fixture()
    if any(
        row["is_fixture"] is not True
        or row["source_mode"] != "fixture"
        for row in rows
    ):
        raise ValueError("Non-fixture write blocked")

    spark.createDataFrame(rows, ATTEMPT_SCHEMA).write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(RUNTIME_FIXTURE_TABLE)


print("Persistence helpers loaded; no rows written by this cell.")

# COMMAND ----------

# CELL 13 — Fixture persistence/recovery tests and cleanup
TESTS_13 = []
RECEIPTS_13 = {}
EVIDENCE_13 = {
    "checkpoint": "fixture_persistence_reconciliation_v1",
    "tests_passed": 0,
    "fixture_rows_before_cleanup": None,
    "cleanup": "NOT_STARTED",
    "persistent_fixture_writes": True,
    "live_table_writes": False,
    "api_requests": False,
    "live_refresh_executed": False,
    "runtime_notebook_complete": False,
}


def check_13(name, condition):
    if not condition:
        raise AssertionError(name)
    TESTS_13.append(name)


def new_fixture_batch():
    rows = deepcopy(ATTEMPT_ROWS)
    for row in rows:
        row["event_id"] = uuid.uuid4().hex
    return rows


def fault_once(kind):
    state = {"calls": 0}

    def writer(rows):
        state["calls"] += 1
        first = state["calls"] == 1

        if first and kind == "before_commit":
            raise RuntimeError("Synthetic failure before commit")

        if first and kind == "partial_batch":
            append_fixture_events(rows[:max(1, len(rows) // 2)])
            raise RuntimeError("Synthetic interruption after partial batch")

        append_fixture_events(rows)

        if first and kind == "after_commit":
            raise RuntimeError("Synthetic failure after commit")

    return writer


try:
    require_runtime_fixture()

    # 1. Initial write: retain successful AND failed source attempts.
    baseline = deepcopy(ATTEMPT_ROWS)
    receipt = persist_prepared_fixture_rows(
        baseline, read_fixture_events, append_fixture_events
    )
    RECEIPTS_13["initial"] = receipt
    check_13(
        "initial 11-event write",
        receipt["confirmed_events"] == 11
        and receipt["append_calls"] == 1,
    )

    # 2. Replay the exact prepared rows; do not regenerate event IDs.
    receipt = persist_prepared_fixture_rows(
        baseline, read_fixture_events, append_fixture_events
    )
    RECEIPTS_13["identical_replay"] = receipt
    check_13(
        "identical replay performs no append",
        receipt["confirmed_events"] == 11
        and receipt["append_calls"] == 0,
    )

    # 3–5. Each scenario gets new, clearly synthetic event IDs.
    for kind, expected_calls in (
        ("before_commit", 2),
        ("after_commit", 1),
        ("partial_batch", 2),
    ):
        batch = new_fixture_batch()
        receipt = persist_prepared_fixture_rows(
            batch, read_fixture_events, fault_once(kind)
        )
        RECEIPTS_13[kind] = receipt
        check_13(
            kind,
            receipt["status"] == "CONFIRMED_AFTER_ERROR"
            and receipt["confirmed_events"] == 11
            and receipt["append_calls"] == expected_calls
            and len(receipt["append_errors"]) == 1,
        )

    # 6. Exhausted write failures must raise, not return success.
    never_written = new_fixture_batch()

    def always_fail(rows):
        raise RuntimeError("Synthetic repeated pre-commit failure")

    try:
        persist_prepared_fixture_rows(
            never_written, read_fixture_events, always_fail
        )
    except FixturePersistenceError as exc:
        RECEIPTS_13["exhausted"] = exc.receipt
        check_13(
            "exhausted write raises and retains no rows",
            exc.receipt["status"] == "INCOMPLETE"
            and exc.receipt["append_calls"] == 2
            and len(read_fixture_events(
                [row["event_id"] for row in never_written]
            )) == 0,
        )
    else:
        raise AssertionError("Exhausted failure was reported as success")

    # 7. Same event ID with different content must stop before appending.
    collision = deepcopy(baseline)
    collision[0]["source"] += "_changed"

    try:
        persist_prepared_fixture_rows(
            collision, read_fixture_events, append_fixture_events
        )
    except FixturePersistenceError as exc:
        check_13(
            "identity/content collision blocked",
            exc.receipt["status"] == "READ_OR_CONTENT_FAILURE"
            and exc.receipt["append_calls"] == 0,
        )
    else:
        raise AssertionError("Content collision was accepted")

    # 8. Duplicate IDs in a prepared batch must be rejected.
    try:
        persist_prepared_fixture_rows(
            baseline + [deepcopy(baseline[0])],
            read_fixture_events,
            append_fixture_events,
        )
    except ValueError:
        TESTS_13.append("duplicate prepared IDs blocked")
    else:
        raise AssertionError("Duplicate prepared IDs accepted")

    # 9. Non-fixture labels must be rejected.
    wrong_label = deepcopy(baseline)
    wrong_label[0]["source_mode"] = "api"

    try:
        persist_prepared_fixture_rows(
            wrong_label, read_fixture_events, append_fixture_events
        )
    except ValueError:
        TESTS_13.append("non-fixture label blocked")
    else:
        raise AssertionError("Non-fixture label accepted")

    # 10. Four committed batches: 44 unique events, no replay duplicates.
    stored = spark.table(RUNTIME_FIXTURE_TABLE)
    stored_count = stored.count()
    duplicate_count = (
        stored.groupBy("event_id").count()
        .where(F.col("count") != 1)
        .count()
    )
    success_count = stored.where(F.col("status") == "succeeded").count()
    failure_count = stored.where(F.col("status") == "failed").count()
    non_fixture_count = stored.where(
        (F.col("is_fixture") != True)
        | (F.col("source_mode") != "fixture")
    ).count()

    check_13(
        "final counts and labels",
        stored_count == 44
        and duplicate_count == 0
        and success_count == 12
        and failure_count == 32
        and non_fixture_count == 0,
    )
    EVIDENCE_13["fixture_rows_before_cleanup"] = stored_count

except Exception as exc:
    EVIDENCE_13["failure_type"] = type(exc).__name__
    EVIDENCE_13["failure_message"] = str(exc)[:500]
    if isinstance(exc, FixturePersistenceError):
        EVIDENCE_13["failure_receipt"] = exc.receipt
    raise

finally:
    EVIDENCE_13["tests_passed"] = len(TESTS_13)
    try:
        # Never drop a destination that fails the fixture-name guard.
        if not re.fullmatch(
            r"workspace\.bronze\._gridlens_runtime_fixture_[0-9a-f]{32}",
            RUNTIME_FIXTURE_TABLE,
        ):
            raise RuntimeError("Cleanup destination guard failed")

        spark.sql(f"DROP TABLE IF EXISTS {RUNTIME_FIXTURE_SQL}")
        if spark.catalog.tableExists(RUNTIME_FIXTURE_TABLE):
            raise RuntimeError("Fixture table remains after DROP")

        RUNTIME_FIXTURE_ACTIVE = False
        EVIDENCE_13["cleanup"] = "PASS"

    except Exception as cleanup_exc:
        EVIDENCE_13["cleanup"] = "UNRESOLVED"
        EVIDENCE_13["cleanup_error"] = str(cleanup_exc)[:500]

    print("WRITE RECEIPTS")
    print(json.dumps(RECEIPTS_13, indent=2))
    print("CHECKPOINT EVIDENCE")
    print(json.dumps(EVIDENCE_13, indent=2))

if EVIDENCE_13["cleanup"] != "PASS":
    raise RuntimeError("Fixture cleanup unresolved; stop and review")

print("PASS: fixture persistence/reconciliation tests and cleanup.")

# COMMAND ----------

# CELL 14 — Fixture-only ingestion runner
from copy import deepcopy
from datetime import timedelta
import json
import uuid


class FixtureIngestionError(RuntimeError):
    def __init__(self, message, evidence):
        super().__init__(message)
        self.evidence = deepcopy(evidence)


def run_fixture_ingestion(config, manifest, loader, persist_fn):
    if (
        manifest["execution_mode"] != "fixture"
        or manifest["allow_live_writes"]
    ):
        raise ValueError("Only non-live fixture execution is supported")
    if manifest["identity_kind"] != "synthetic":
        raise ValueError("Fixture identity required")

    start, end = validate_config(config)
    if end - start > timedelta(days=14):
        raise ValueError("Initial runtime range cap is 14 days")
    if config["source_mode"] != "api":
        raise ValueError("This fixture runner tests the API fetch path only")

    if (
        format_utc(start) != manifest["publication_start_utc"]
        or format_utc(end) != manifest["publication_end_utc"]
    ):
        raise ValueError("Config/manifest publication bounds differ")

    windows = plan_windows(config)
    verify_windows(config, windows)

    if len(windows) != manifest["planned_request_windows"]:
        raise ValueError("Config/manifest request-window counts differ")
    if (
        int((end - start).total_seconds() // 1800)
        != manifest["expected_interval_count"]
    ):
        raise ValueError("Config/manifest interval counts differ")

    run_id = uuid.uuid4().hex
    pack = {
        "summary": {
            "run_id": run_id,
            "status": "RUNNING",
            "planned_windows": len(windows),
            "attempted_windows": 0,
            "successful_windows": 0,
            "confirmed_attempt_events": 0,
            "unattempted_windows": [],
        },
        "outcomes": [],
        "prepared_batches": [],
        "handoff": None,
    }

    try:
        for number, (left, right) in enumerate(windows, start=1):
            outcome = {
                "window": number,
                "from_utc": format_utc(left),
                "to_utc": format_utc(right),
                "fetch_status": "NOT_COMPLETED",
                "persistence_status": "NOT_COMPLETED",
            }
            pack["outcomes"].append(outcome)
            pack["summary"]["attempted_windows"] += 1

            result = loader(
                outcome["from_utc"], outcome["to_utc"], run_id
            )

            if result["run_id"] != run_id:
                raise ValueError("Loader returned wrong source run ID")

            for attempt in result["attempts"]:
                if (
                    attempt["request_from_utc"] != outcome["from_utc"]
                    or attempt["request_to_utc"] != outcome["to_utc"]
                ):
                    raise ValueError(
                        "Attempt bounds differ from planned request"
                    )

            outcome["fetch_status"] = result["status"]
            prepared = prepare_fixture_attempt_rows([result])
            pack["prepared_batches"].append(prepared)

            receipt = persist_fn(prepared)
            outcome["persistence_receipt"] = receipt

            if (
                receipt["status"]
                not in ("CONFIRMED", "CONFIRMED_AFTER_ERROR")
                or receipt["confirmed_events"] != len(prepared)
            ):
                raise RuntimeError(
                    "Attempt evidence not fully confirmed"
                )

            outcome["persistence_status"] = receipt["status"]
            pack["summary"]["confirmed_attempt_events"] += len(prepared)

            if result["status"] != "succeeded":
                raise RuntimeError(
                    "Source request did not complete successfully"
                )

            pack["summary"]["successful_windows"] += 1

        pack["summary"]["status"] = "SUCCEEDED"
        pack["handoff"] = {
            "source_run_ids_json": json.dumps([run_id]),
            "publication_start_utc": manifest["publication_start_utc"],
            "publication_end_utc": manifest["publication_end_utc"],
            "source_mode": "fixture",
            "orchestration_id": manifest["orchestration_id"],
            "chunk_days": str(config["chunk_days"]),
            "run_anchor_utc": manifest["run_anchor_utc"],
            "policy_version": manifest["policy_version"],
        }
        return pack

    except Exception as exc:
        pack["summary"]["status"] = "FAILED"
        pack["summary"]["error_type"] = type(exc).__name__
        pack["summary"]["error_message"] = str(exc)[:500]
        pack["summary"]["unattempted_windows"] = [
            {
                "from_utc": format_utc(left),
                "to_utc": format_utc(right),
            }
            for left, right in windows[len(pack["outcomes"]):]
        ]

        receipt = getattr(exc, "receipt", None)
        if receipt is not None:
            pack["outcomes"][-1]["persistence_receipt"] = deepcopy(receipt)

        raise FixtureIngestionError(
            "Fixture ingestion failed", pack
        ) from exc


print("Fixture runner loaded; no fetches or writes performed.")

# COMMAND ----------

# CELL 15 — Integrated fixture runner tests and cleanup
from datetime import timedelta
import re

if globals().get("RUNTIME_FIXTURE_ACTIVE", False):
    raise RuntimeError(
        "A previous fixture table may remain. Resolve cleanup first."
    )

RUNTIME_FIXTURE_TABLE = (
    "workspace.bronze._gridlens_runtime_fixture_" + uuid.uuid4().hex
)
RUNTIME_FIXTURE_SQL = ".".join(
    f"`{part}`" for part in RUNTIME_FIXTURE_TABLE.split(".")
)

if spark.catalog.tableExists(RUNTIME_FIXTURE_TABLE):
    raise RuntimeError("Unexpected fixture-name collision; no changes made")

RUNTIME_FIXTURE_ACTIVE = True
TESTS_15 = []
SUMMARIES_15 = {}
EVIDENCE_15 = {
    "checkpoint": "fixture_ingestion_runner_v1",
    "tests_passed": 0,
    "fixture_rows_before_cleanup": None,
    "cleanup": "NOT_STARTED",
    "persistent_fixture_writes": True,
    "live_table_writes": False,
    "api_requests": False,
    "task_values_emitted": False,
    "live_refresh_executed": False,
    "runtime_notebook_complete": False,
}

print("FIXTURE TABLE:", RUNTIME_FIXTURE_TABLE)


def check_15(name, condition):
    if not condition:
        raise AssertionError(name)
    TESTS_15.append(name)


def test_contract(days):
    raw = {
        **PARAMETER_DEFAULTS,
        "publication_start_utc": "2026-01-01T00:00Z",
        "publication_end_utc": format_utc(
            parse_utc("2026-01-01T00:00Z") + timedelta(days=days)
        ),
    }
    config, _, manifest = bind_runtime_parameters(raw)

    # Small chunks deliberately exercise multiple request windows.
    config["chunk_days"] = 1
    manifest["planned_request_windows"] = len(plan_windows(config))
    return config, manifest


def make_fixture_loader(config, scenarios):
    state = {"calls": 0}

    def load(left, right, run_id):
        index = state["calls"]
        state["calls"] += 1
        if index >= len(scenarios):
            raise AssertionError("Unexpected extra request window")

        scenario = scenarios[index]
        if scenario == "raise":
            raise RuntimeError("Synthetic loader exception")

        interval_end = format_utc(
            parse_utc(left) + timedelta(minutes=30)
        )
        success = {
            "status_code": 200,
            "body": json_bytes({
                "data": [{
                    "from": left,
                    "to": interval_end,
                    "intensity": {
                        "forecast": 100,
                        "actual": 95,
                        "index": "moderate",
                    },
                }]
            }),
        }

        if scenario == "retry_success":
            sequence = [FIXTURES["http_503"], success]
        elif scenario == "success":
            sequence = [success]
        elif scenario == "failure":
            sequence = [FIXTURES["http_400"]]
        else:
            raise ValueError("Unknown fixture scenario")

        transport, _ = mock_transport(sequence)
        return fetch_window(
            config, left, right,
            transport=transport,
            sleep_fn=lambda delay: None,
            run_id=run_id,
        )

    return load, state


def confirmed_fixture_persist(rows):
    return persist_prepared_fixture_rows(
        rows, read_fixture_events, append_fixture_events
    )


try:
    column_ddl = ",\n".join(
        f"`{name}` {kind.upper()}"
        + ("" if nullable else " NOT NULL")
        for name, (kind, nullable) in EXPECTED_BRONZE.items()
    )
    spark.sql(f"""
        CREATE TABLE {RUNTIME_FIXTURE_SQL} (
            {column_ddl}
        ) USING DELTA
    """)
    require_runtime_fixture()

    # 1. Two windows succeed, each after one transient HTTP failure.
    config2, manifest2 = test_contract(2)
    loader, state = make_fixture_loader(
        config2, ["retry_success", "retry_success"]
    )
    success_pack = run_fixture_ingestion(
        config2, manifest2, loader, confirmed_fixture_persist
    )
    SUMMARIES_15["success"] = success_pack["summary"]
    check_15(
        "complete successful run",
        success_pack["summary"]["status"] == "SUCCEEDED"
        and success_pack["summary"]["successful_windows"] == 2
        and success_pack["summary"]["confirmed_attempt_events"] == 4
        and state["calls"] == 2,
    )

    # 2. Persist the failed source request, fail the run, skip window 3.
    config3, manifest3 = test_contract(3)
    loader, state = make_fixture_loader(
        config3, ["success", "failure", "success"]
    )
    try:
        run_fixture_ingestion(
            config3, manifest3, loader, confirmed_fixture_persist
        )
    except FixtureIngestionError as exc:
        source_failure = exc.evidence
        SUMMARIES_15["source_failure"] = source_failure["summary"]
        check_15(
            "source failure gates handoff and later windows",
            source_failure["summary"]["status"] == "FAILED"
            and source_failure["summary"]["confirmed_attempt_events"] == 2
            and len(source_failure["summary"]["unattempted_windows"]) == 1
            and source_failure["handoff"] is None
            and state["calls"] == 2,
        )
    else:
        raise AssertionError("Source failure did not raise")

    # 3. Exhausted persistence fails before later windows or handoff.
    def always_fail_append(rows):
        raise RuntimeError("Synthetic persistence outage")

    def failing_persist(rows):
        return persist_prepared_fixture_rows(
            rows, read_fixture_events, always_fail_append
        )

    loader, state = make_fixture_loader(
        config3, ["success", "success", "success"]
    )
    try:
        run_fixture_ingestion(
            config3, manifest3, loader, failing_persist
        )
    except FixtureIngestionError as exc:
        pack = exc.evidence
        SUMMARIES_15["persistence_failure"] = pack["summary"]
        receipt = pack["outcomes"][0]["persistence_receipt"]
        check_15(
            "persistence failure raises with retained prepared evidence",
            pack["handoff"] is None
            and len(pack["prepared_batches"]) == 1
            and len(pack["summary"]["unattempted_windows"]) == 2
            and receipt["status"] == "INCOMPLETE"
            and receipt["append_calls"] == 2
            and state["calls"] == 1,
        )
    else:
        raise AssertionError("Persistence failure did not raise")

    # 4. Unexpected loader errors must not invent durable attempt evidence.
    loader, state = make_fixture_loader(config3, ["raise"])
    try:
        run_fixture_ingestion(
            config3, manifest3, loader, confirmed_fixture_persist
        )
    except FixtureIngestionError as exc:
        pack = exc.evidence
        SUMMARIES_15["loader_failure"] = pack["summary"]
        check_15(
            "unexpected loader exception fails visibly",
            pack["handoff"] is None
            and pack["prepared_batches"] == []
            and pack["summary"]["confirmed_attempt_events"] == 0
            and len(pack["summary"]["unattempted_windows"]) == 2,
        )
    else:
        raise AssertionError("Loader exception did not raise")

    # 5. Config/manifest disagreement must fail before any fetch.
    loader, state = make_fixture_loader(config2, ["success", "success"])
    bad_manifest = {
        **manifest2,
        "publication_end_utc": "2026-01-04T00:00Z",
    }
    try:
        run_fixture_ingestion(
            config2, bad_manifest, loader, confirmed_fixture_persist
        )
    except ValueError:
        check_15(
            "manifest mismatch blocked before fetching",
            state["calls"] == 0,
        )
    else:
        raise AssertionError("Manifest mismatch accepted")

    # 6. Failed-run source evidence is retained, separately selectable.
    failed_run_id = source_failure["summary"]["run_id"]
    failed_rows = (
        spark.table(RUNTIME_FIXTURE_TABLE)
        .where(F.col("run_id") == failed_run_id)
        .collect()
    )
    check_15(
        "failed-run attempts retained",
        len(failed_rows) == 2
        and sum(r["status"] == "succeeded" for r in failed_rows) == 1
        and sum(r["status"] == "failed" for r in failed_rows) == 1,
    )

    # 7. Successful handoff names only its own complete source run.
    selected_ids = json.loads(
        success_pack["handoff"]["source_run_ids_json"]
    )
    check_15(
        "handoff selects only the complete current run",
        selected_ids == [success_pack["summary"]["run_id"]]
        and failed_run_id not in selected_ids
        and success_pack["handoff"]["source_mode"] == "fixture",
    )

    # 8. Only the successful and source-failure scenarios committed rows.
    stored = spark.table(RUNTIME_FIXTURE_TABLE)
    stored_count = stored.count()
    duplicate_count = (
        stored.groupBy("event_id").count()
        .where(F.col("count") != 1).count()
    )
    check_15(
        "final isolated event counts",
        stored_count == 6
        and duplicate_count == 0
        and stored.where(F.col("status") == "succeeded").count() == 3
        and stored.where(F.col("status") == "failed").count() == 3
        and stored.where(
            (F.col("is_fixture") != True)
            | (F.col("source_mode") != "fixture")
        ).count() == 0,
    )
    EVIDENCE_15["fixture_rows_before_cleanup"] = stored_count

except Exception as exc:
    EVIDENCE_15["failure_type"] = type(exc).__name__
    EVIDENCE_15["failure_message"] = str(exc)[:500]
    raise

finally:
    EVIDENCE_15["tests_passed"] = len(TESTS_15)
    try:
        if not re.fullmatch(
            r"workspace\.bronze\._gridlens_runtime_fixture_[0-9a-f]{32}",
            RUNTIME_FIXTURE_TABLE,
        ):
            raise RuntimeError("Cleanup destination guard failed")

        spark.sql(f"DROP TABLE IF EXISTS {RUNTIME_FIXTURE_SQL}")
        if spark.catalog.tableExists(RUNTIME_FIXTURE_TABLE):
            raise RuntimeError("Fixture table remains after DROP")

        RUNTIME_FIXTURE_ACTIVE = False
        EVIDENCE_15["cleanup"] = "PASS"
    except Exception as cleanup_exc:
        EVIDENCE_15["cleanup"] = "UNRESOLVED"
        EVIDENCE_15["cleanup_error"] = str(cleanup_exc)[:500]

    print("RUN SUMMARIES")
    print(json.dumps(SUMMARIES_15, indent=2))
    print("CHECKPOINT EVIDENCE")
    print(json.dumps(EVIDENCE_15, indent=2))

if EVIDENCE_15["cleanup"] != "PASS":
    raise RuntimeError("Fixture cleanup unresolved; stop and review")

print("PASS: integrated fixture ingestion runner and cleanup.")