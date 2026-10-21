# Databricks notebook source
import json
import math
from datetime import datetime, timedelta, timezone

# EDIT CONFIGURATION HERE.
CONFIG = {
    "start_utc": "2026-07-13T00:00Z",
    "end_utc": "2026-10-11T00:00Z",
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

start, end = validate_config(CONFIG)
print(json.dumps(CONFIG, indent=2))
print("Configured duration in days:", (end - start).total_seconds() / 86400)
print("Maximum attempts per request:", 1 + CONFIG["max_retries"])
print("Configuration validation: PASSED")

# COMMAND ----------

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

WINDOWS = plan_windows(CONFIG)
verify_windows(CONFIG, WINDOWS)

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
    ("equal bounds", {"end_utc": CONFIG["start_utc"]}),
    ("reversed bounds", {"end_utc": "2026-07-12T00:00Z"}),
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
        validate_config({**CONFIG, **override})
    except ValueError:
        pass
    else:
        raise AssertionError(f"Invalid config accepted: {label}")

window_cases = [
    ("half hour", "2026-01-01T00:00Z", "2026-01-01T00:30Z", 14, 1),
    ("exactly 14 days", "2026-01-01T00:00Z", "2026-01-15T00:00Z", 14, 1),
    ("14 days plus half hour", "2026-01-01T00:00Z", "2026-01-15T00:30Z", 14, 2),
    ("leap-day crossing", "2024-02-28T00:00Z", "2024-03-02T00:00Z", 1, 3),
    ("configured range", CONFIG["start_utc"], CONFIG["end_utc"], 14, 7),
]

for label, left, right, days, expected_count in window_cases:
    test_config = {
        **CONFIG,
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

import hashlib
import time
import uuid
import requests
from email.utils import parsedate_to_datetime

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def live_transport(url, timeout_seconds):
    # No automatic redirects or hidden retry layer.
    with requests.get(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "GridLens-source-ingestion/0.1",
        },
        timeout=timeout_seconds,
        allow_redirects=False,
    ) as response:
        return {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "body": response.content,
        }

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
    config = {**CONFIG, **(override or {})}
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

LIVE_RESULT = fetch_window(
    CONFIG,
    "2026-10-10T00:00Z",
    "2026-10-10T01:00Z",
)

# Keep full raw bodies in LIVE_RESULT; print only concise diagnostics.
LIVE_SUMMARY = {
    "status": LIVE_RESULT["status"],
    "run_id": LIVE_RESULT["run_id"],
    "request_id": LIVE_RESULT["request_id"],
    "attempt_count": len(LIVE_RESULT["attempts"]),
    "attempts": [
        {
            key: attempt[key]
            for key in (
                "attempt_number",
                "request_from_utc",
                "request_to_utc",
                "http_status",
                "retrieved_at_utc",
                "response_sha256",
                "record_count",
                "status",
                "failure_kind",
                "error",
                "retry_delay_seconds",
            )
        }
        for attempt in LIVE_RESULT["attempts"]
    ],
}

print(json.dumps(LIVE_SUMMARY, indent=2))

if LIVE_RESULT["status"] != "succeeded":
    raise RuntimeError(
        "Live fetch failed. Attempt evidence remains in LIVE_RESULT; "
        "do not proceed to persistence or backfill."
    )

print("Small live fetch: PASSED")

# COMMAND ----------

from pyspark.sql import types as T

validate_config(CONFIG)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Exact SQL types, Python types and nullability.
BRONZE_COLUMNS = [
    ("event_id", "STRING", T.StringType(), False),
    ("run_id", "STRING", T.StringType(), False),
    ("request_id", "STRING", T.StringType(), False),
    ("attempt_number", "INT", T.IntegerType(), False),
    ("source", "STRING", T.StringType(), False),
    ("source_mode", "STRING", T.StringType(), False),
    ("is_fixture", "BOOLEAN", T.BooleanType(), False),
    ("event_kind", "STRING", T.StringType(), False),
    ("request_url", "STRING", T.StringType(), False),
    ("request_from_utc", "TIMESTAMP", T.TimestampType(), False),
    ("request_to_utc", "TIMESTAMP", T.TimestampType(), False),
    ("started_at_utc", "TIMESTAMP", T.TimestampType(), False),
    ("finished_at_utc", "TIMESTAMP", T.TimestampType(), False),
    ("retrieved_at_utc", "TIMESTAMP", T.TimestampType(), True),
    ("stored_at_utc", "TIMESTAMP", T.TimestampType(), False),
    ("http_status", "INT", T.IntegerType(), True),
    ("raw_body", "BINARY", T.BinaryType(), True),
    ("raw_text", "STRING", T.StringType(), True),
    ("response_sha256", "STRING", T.StringType(), True),
    ("status", "STRING", T.StringType(), False),
    ("failure_kind", "STRING", T.StringType(), True),
    ("error", "STRING", T.StringType(), True),
    ("record_count", "INT", T.IntegerType(), True),
    ("retry_delay_seconds", "DOUBLE", T.DoubleType(), True),
    ("response_headers_json", "STRING", T.StringType(), False),
]

BRONZE_SCHEMA = T.StructType([
    T.StructField(name, python_type, nullable)
    for name, sql_type, python_type, nullable in BRONZE_COLUMNS
])

# Prevent losing track of an existing fixture table on a cell rerun.
if globals().get("FIXTURE_TABLE_CREATED", False):
    raise RuntimeError(
        "Clean up the current fixture table with cell 11 before rerunning cell 07."
    )

catalog = CONFIG["catalog"]
schema = CONFIG["schema"]
fixture_name = f"_gridlens_task04_fixture_{uuid.uuid4().hex}"

FIXTURE_TABLE = f"{catalog}.{schema}.{fixture_name}"
FIXTURE_TABLE_SQL = f"`{catalog}`.`{schema}`.`{fixture_name}`"
FIXTURE_TABLE_CREATED = False

print("FIXTURE_TABLE:", FIXTURE_TABLE)
print("MANUAL CLEANUP SQL:", f"DROP TABLE {FIXTURE_TABLE_SQL};")

# Schema only: create if needed, never replace.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

if spark.catalog.tableExists(FIXTURE_TABLE):
    raise RuntimeError("Unexpected table-name collision; no existing table changed.")

column_ddl = ",\n".join(
    f"`{name}` {sql_type}" + ("" if nullable else " NOT NULL")
    for name, sql_type, python_type, nullable in BRONZE_COLUMNS
)

# Strict CREATE for this new, disposable table; no REPLACE/overwrite.
spark.sql(f"""
    CREATE TABLE {FIXTURE_TABLE_SQL} (
        {column_ddl}
    ) USING DELTA
""")
FIXTURE_TABLE_CREATED = True

detail = spark.sql(
    f"DESCRIBE DETAIL {FIXTURE_TABLE_SQL}"
).first().asDict()
assert detail["format"] == "delta", "Expected Delta storage"

print("Bronze schema access and fixture table creation: PASSED")

# COMMAND ----------

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

            # Only retain selected public response headers.
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
                "event_kind": (
                    "response" if raw is not None else "transport_failure"
                ),
                "request_url": attempt["request_url"],
                "request_from_utc": parse_utc(attempt["request_from_utc"]),
                "request_to_utc": parse_utc(attempt["request_to_utc"]),
                "started_at_utc": timestamp_value(attempt["started_at_utc"]),
                "finished_at_utc": timestamp_value(attempt["finished_at_utc"]),
                "retrieved_at_utc": timestamp_value(
                    attempt["retrieved_at_utc"]
                ),
                "stored_at_utc": datetime.now(timezone.utc),
                "http_status": attempt["http_status"],
                "raw_body": raw,
                # Binary is authoritative if decoding replaces bad characters.
                "raw_text": (
                    raw.decode("utf-8", errors="replace")
                    if raw is not None else None
                ),
                "response_sha256": attempt["response_sha256"],
                "status": attempt["status"],
                "failure_kind": attempt["failure_kind"],
                "error": attempt["error"],
                "record_count": attempt["record_count"],
                "retry_delay_seconds": (
                    float(attempt["retry_delay_seconds"])
                    if attempt["retry_delay_seconds"] is not None else None
                ),
                "response_headers_json": json.dumps(headers),
            }
            rows.append(row)

    return rows

def append_fixture_results(results):
    if not FIXTURE_TABLE_CREATED:
        raise RuntimeError("Run cell 07 first")
    if not spark.catalog.tableExists(FIXTURE_TABLE):
        raise RuntimeError("Fixture table is missing; refusing implicit creation")

    actual_schema = spark.table(FIXTURE_TABLE).schema
    expected_types = [
        (field.name, field.dataType) for field in BRONZE_SCHEMA
    ]
    actual_types = [
        (field.name, field.dataType) for field in actual_schema
    ]
    if actual_types != expected_types:
        raise ValueError("Existing schema differs; no automatic schema changes")

    rows = fixture_rows(results)
    if not rows:
        raise ValueError("No attempt evidence to persist")

    frame = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    frame.write.format("delta").mode("append").saveAsTable(FIXTURE_TABLE)
    return rows

def make_fixture_batch():
    # Fresh request IDs per batch; all transport responses are synthetic.
    batch_run_id = uuid.uuid4().hex
    sequences = [
        [FIXTURES["success"]],
        [FIXTURES["malformed_json"]],
        [FIXTURES["http_503"], FIXTURES["success"]],
        [FIXTURES["timeout"]],
    ]

    results = []
    for sequence in sequences:
        transport, state = mock_transport(sequence)
        result = fetch_window(
            {**CONFIG, "max_retries": 1 if len(sequence) == 2 else 0},
            "2026-10-10T00:00Z",
            "2026-10-10T01:00Z",
            transport=transport,
            sleep_fn=lambda seconds: None,
            run_id=batch_run_id,
        )
        assert state["calls"] == len(sequence)
        results.append(result)

    return results

print("Fixture append writer loaded; no rows written yet.")

# COMMAND ----------

# Run this cell once per fresh fixture table.
initial_count = spark.table(FIXTURE_TABLE).count()
assert initial_count == 0, (
    "Fixture table is not empty. Do not rerun this cell; "
    "review existing results or clean up and start again."
)

FIRST_ROWS = append_fixture_results(make_fixture_batch())
after_first = spark.table(FIXTURE_TABLE).count()
assert len(FIRST_ROWS) == 5
assert after_first == 5, f"Expected 5 rows, found {after_first}"

SECOND_ROWS = append_fixture_results(make_fixture_batch())
after_second = spark.table(FIXTURE_TABLE).count()
assert len(SECOND_ROWS) == 5
assert after_second == 10, f"Expected 10 rows, found {after_second}"

EXPECTED_ROWS = FIRST_ROWS + SECOND_ROWS
expected_by_id = {row["event_id"]: row for row in EXPECTED_ROWS}
actual_rows = spark.table(FIXTURE_TABLE).collect()

assert len(actual_rows) == len(expected_by_id) == 10
assert {row["event_id"] for row in actual_rows} == set(expected_by_id)

for actual in actual_rows:
    expected = expected_by_id[actual["event_id"]]
    actual_body = (
        bytes(actual["raw_body"])
        if actual["raw_body"] is not None else None
    )
    assert actual_body == expected["raw_body"], "Raw bytes changed"
    assert actual["raw_text"] == expected["raw_text"], "Raw text changed"

    for key in (
        "run_id", "request_id", "attempt_number",
        "source_mode", "is_fixture", "event_kind",
        "status", "failure_kind", "http_status", "response_sha256",
    ):
        assert actual[key] == expected[key], f"Read-back mismatch: {key}"

    if actual_body is not None:
        assert actual["response_sha256"] == hashlib.sha256(
            actual_body
        ).hexdigest()

print(json.dumps({
    "rows_after_first_append": after_first,
    "rows_after_second_append": after_second,
    "exact_raw_body_readback": "PASSED",
    "attempt_metadata_readback": "PASSED",
    "append_without_overwrite": "PASSED",
}, indent=2))

# COMMAND ----------

required_columns = [
    name for name, sql_type, python_type, nullable in BRONZE_COLUMNS
    if not nullable
]
missing_required = " OR ".join(
    f"`{name}` IS NULL" for name in required_columns
)

CHECKS = spark.sql(f"""
    SELECT
        COUNT(*) AS total_events,
        COUNT(DISTINCT event_id) AS distinct_event_ids,
        SUM(CASE WHEN {missing_required}
            THEN 1 ELSE 0 END) AS missing_required_fields,
        SUM(CASE WHEN is_fixture <> TRUE OR source_mode <> 'fixture'
            THEN 1 ELSE 0 END) AS incorrectly_labelled_events,
        SUM(CASE WHEN request_to_utc <= request_from_utc
            THEN 1 ELSE 0 END) AS invalid_request_bounds,
        SUM(CASE
            WHEN event_kind = 'response' AND (
                raw_body IS NULL OR raw_text IS NULL
                OR retrieved_at_utc IS NULL
                OR http_status IS NULL OR response_sha256 IS NULL
            ) THEN 1
            WHEN event_kind = 'transport_failure' AND (
                raw_body IS NOT NULL OR raw_text IS NOT NULL
                OR retrieved_at_utc IS NOT NULL
                OR http_status IS NOT NULL OR response_sha256 IS NOT NULL
            ) THEN 1
            WHEN event_kind NOT IN ('response', 'transport_failure') THEN 1
            ELSE 0
        END) AS invalid_body_provenance,
        SUM(CASE
            WHEN raw_body IS NOT NULL
                AND response_sha256 <> sha2(raw_body, 256)
            THEN 1 ELSE 0
        END) AS hash_mismatches,
        SUM(CASE
            WHEN status = 'succeeded'
                AND (failure_kind IS NOT NULL OR error IS NOT NULL) THEN 1
            WHEN status = 'failed'
                AND (failure_kind IS NULL OR error IS NULL) THEN 1
            WHEN status NOT IN ('succeeded', 'failed') THEN 1
            ELSE 0
        END) AS invalid_status_metadata
    FROM {FIXTURE_TABLE_SQL}
""").first().asDict()

assert CHECKS["total_events"] == 10
assert CHECKS["distinct_event_ids"] == 10
for name, value in CHECKS.items():
    if name not in ("total_events", "distinct_event_ids"):
        assert value == 0, f"{name}: {value}"

print("PROVENANCE_CHECKS")
print(json.dumps(CHECKS, indent=2))

# Expected groups: 4 success, 2 HTTP error, 2 malformed, 2 transport failure.
GROUPS = spark.sql(f"""
    SELECT event_kind, status, failure_kind, COUNT(*) AS events
    FROM {FIXTURE_TABLE_SQL}
    GROUP BY event_kind, status, failure_kind
    ORDER BY event_kind, status, failure_kind
""").collect()

actual_groups = {
    (row["event_kind"], row["status"], row["failure_kind"]): row["events"]
    for row in GROUPS
}
assert actual_groups == {
    ("response", "succeeded", None): 4,
    ("response", "failed", "http_error"): 2,
    ("response", "failed", "malformed_json"): 2,
    ("transport_failure", "failed", "transport_error"): 2,
}

print("EVENT_GROUPS")
print(json.dumps([row.asDict() for row in GROUPS], indent=2))

# Demonstrate queryable raw text, including malformed/error bodies.
spark.sql(f"""
    SELECT
        event_kind, status, failure_kind, http_status,
        substring(raw_text, 1, 160) AS raw_text_preview
    FROM {FIXTURE_TABLE_SQL}
    ORDER BY event_kind, status, failure_kind
""").show(10, truncate=False)

print("Bronze provenance and failure distinction: PASSED")

# COMMAND ----------

if not globals().get("FIXTURE_TABLE_CREATED", False):
    print("No tracked fixture table needs cleanup.")
else:
    assert fixture_name.startswith("_gridlens_task04_fixture_")
    assert FIXTURE_TABLE == f"{catalog}.{schema}.{fixture_name}"

    spark.sql(f"DROP TABLE IF EXISTS {FIXTURE_TABLE_SQL}")
    assert not spark.catalog.tableExists(FIXTURE_TABLE), (
        "Fixture table is still registered"
    )

    FIXTURE_TABLE_CREATED = False
    print("Fixture table cleanup: PASSED")
    print("Bronze schema retained; live api_responses table untouched.")

# COMMAND ----------

import re

# Change to True only to approve persistent table creation and appends.
ALLOW_LIVE_BRONZE_WRITES = True

def quoted_table(name):
    parts = name.split(".")
    if len(parts) != 3 or not all(
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part)
        for part in parts
    ):
        raise ValueError("Expected a simple catalog.schema.table name")
    return ".".join(f"`{part}`" for part in parts)

def ensure_bronze_table(name):
    sql_name = quoted_table(name)
    existed = spark.catalog.tableExists(name)

    ddl = ",\n".join(
        f"`{column}` {sql_type}" + ("" if nullable else " NOT NULL")
        for column, sql_type, python_type, nullable in BRONZE_COLUMNS
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {sql_name} (
            {ddl}
        ) USING DELTA
    """)

    detail = spark.sql(
        f"DESCRIBE DETAIL {sql_name}"
    ).first().asDict()
    if detail["format"] != "delta":
        raise ValueError("Existing destination is not Delta")

    expected = {
        field.name: field.dataType for field in BRONZE_SCHEMA
    }
    actual = {
        field.name: field.dataType for field in spark.table(name).schema
    }
    if actual != expected:
        raise ValueError(
            "Existing table schema differs. No ALTER or overwrite performed."
        )
    return not existed

LIVE_TABLE = (
    f"{CONFIG['catalog']}.{CONFIG['schema']}.{CONFIG['table']}"
)
LIVE_TABLE_SQL = quoted_table(LIVE_TABLE)

if not ALLOW_LIVE_BRONZE_WRITES:
    raise RuntimeError(
        "No writes approved. Set ALLOW_LIVE_BRONZE_WRITES=True "
        "only if you approve live table creation and append-only writes."
    )

LIVE_TABLE_CREATED_THIS_CELL = ensure_bronze_table(LIVE_TABLE)
print(json.dumps({
    "destination": LIVE_TABLE,
    "created_this_cell": LIVE_TABLE_CREATED_THIS_CELL,
    "schema_and_delta_checks": "PASSED",
    "existing_rows": spark.table(LIVE_TABLE).count(),
}, indent=2))

# COMMAND ----------

from pathlib import Path
import tempfile

MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024

def append_results(results, table, is_fixture=False):
    if is_fixture:
        if not table.split(".")[-1].startswith("_gridlens_task05_fixture_"):
            raise ValueError("Fixtures require a separate task-05 fixture table")
    elif not ALLOW_LIVE_BRONZE_WRITES or table != LIVE_TABLE:
        raise ValueError("Live writes are not approved for this destination")

    if not spark.catalog.tableExists(table):
        raise ValueError("Destination missing; refusing implicit creation")

    expected = {f.name: f.dataType for f in BRONZE_SCHEMA}
    actual = {f.name: f.dataType for f in spark.table(table).schema}
    if actual != expected:
        raise ValueError("Destination schema mismatch; no schema evolution")

    rows = []
    for result in results:
        mode = result.get("source_mode", "api")
        if mode not in ("api", "snapshot"):
            raise ValueError("Unknown result source mode")

        # Reuse the previously tested row conversion, then label honestly.
        converted = fixture_rows([result])
        for row in converted:
            row["source_mode"] = "fixture" if is_fixture else mode
            row["is_fixture"] = is_fixture
        rows.extend(converted)

    if not rows:
        raise ValueError("No attempt evidence to append")

    spark.createDataFrame(rows, BRONZE_SCHEMA).write \
        .format("delta").mode("append").saveAsTable(table)
    return len(rows)

def run_ingestion(config, table, loader, is_fixture=False):
    # Planning is independent of the loader's API/snapshot source mode.
    planning_config = {**config, "source_mode": "api"}
    windows = plan_windows(planning_config)
    verify_windows(planning_config, windows)

    run_id = uuid.uuid4().hex
    results, outcomes = [], []
    print("RUN_ID:", run_id)

    for number, (left, right) in enumerate(windows, start=1):
        bounds = (format_utc(left), format_utc(right))
        outcome = {
            "window": number,
            "from_utc": bounds[0],
            "to_utc": bounds[1],
            "fetch_status": "not_completed",
            "persistence_status": "not_completed",
            "stored_attempts": 0,
        }
        outcomes.append(outcome)

        try:
            result = loader(bounds[0], bounds[1], run_id)
            results.append(result)  # Retain evidence even if writing fails.
            outcome["fetch_status"] = result["status"]
            outcome["source_mode"] = result.get("source_mode", "api")
            outcome["stored_attempts"] = append_results(
                [result], table, is_fixture=is_fixture
            )
            outcome["persistence_status"] = "passed"
        except Exception as exc:
            outcome["error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
            print(json.dumps(outcome))
            break  # Do not widen further after an unexpected/write failure.

        print(json.dumps(outcome))

    successful = [
        item for item in outcomes
        if item["fetch_status"] == "succeeded"
        and item["persistence_status"] == "passed"
    ]
    failed = [item for item in outcomes if item not in successful]

    summary = {
        "run_id": run_id,
        "destination": table,
        "is_fixture": is_fixture,
        "planned_windows": len(windows),
        "attempted_windows": len(outcomes),
        "successful_windows": len(successful),
        "failed_windows": failed,
        "unattempted_windows": [
            {"from_utc": format_utc(left), "to_utc": format_utc(right)}
            for left, right in windows[len(outcomes):]
        ],
        "stored_attempts": sum(
            item["stored_attempts"]
            for item in outcomes
            if item["persistence_status"] == "passed"
        ),
        "status": (
            "succeeded"
            if len(successful) == len(windows) else "failed"
        ),
    }
    print("RUN_SUMMARY")
    print(json.dumps(summary, indent=2))
    return {"summary": summary, "results": results}

def api_loader(config, transport=live_transport, sleep_fn=time.sleep):
    def load(left, right, run_id):
        result = fetch_window(
            config, left, right,
            transport=transport, sleep_fn=sleep_fn, run_id=run_id,
        )
        result["source_mode"] = "api"
        return result
    return load

def checkpoint(pack):
    summary = pack["summary"]
    table_sql = quoted_table(summary["destination"])
    run_id = summary["run_id"]  # Generated UUID, not free-form SQL input.

    required = " OR ".join(
        f"`{name}` IS NULL"
        for name, sql_type, python_type, nullable in BRONZE_COLUMNS
        if not nullable
    )
    checks = spark.sql(f"""
        SELECT
            COUNT(*) AS stored_events,
            COUNT(DISTINCT event_id) AS distinct_event_ids,
            COUNT(DISTINCT request_id) AS stored_requests,
            SUM(CASE WHEN {required}
                THEN 1 ELSE 0 END) AS missing_required_fields,
            SUM(CASE
                WHEN raw_body IS NOT NULL AND (
                    raw_text IS NULL OR response_sha256 IS NULL
                    OR response_sha256 <> sha2(raw_body, 256)
                ) THEN 1
                WHEN raw_body IS NULL AND (
                    raw_text IS NOT NULL OR response_sha256 IS NOT NULL
                ) THEN 1
                ELSE 0
            END) AS invalid_raw_provenance,
            SUM(CASE
                WHEN source_mode = 'api' AND raw_body IS NOT NULL
                    AND (retrieved_at_utc IS NULL OR http_status IS NULL)
                THEN 1 ELSE 0
            END) AS missing_live_response_metadata,
            SUM(CASE
                WHEN request_to_utc <= request_from_utc THEN 1
                WHEN status = 'succeeded'
                    AND (failure_kind IS NOT NULL OR error IS NOT NULL) THEN 1
                WHEN status = 'failed'
                    AND (failure_kind IS NULL OR error IS NULL) THEN 1
                ELSE 0
            END) AS invalid_bounds_or_status
        FROM {table_sql}
        WHERE run_id = '{run_id}'
    """).first().asDict()

    assert checks["stored_events"] == summary["stored_attempts"]
    assert checks["stored_events"] > 0, "No persisted evidence"
    assert checks["distinct_event_ids"] == checks["stored_events"]
    assert checks["stored_requests"] == sum(
        len(result["attempts"]) > 0 for result in pack["results"]
    )
    for key in (
        "missing_required_fields", "invalid_raw_provenance",
        "missing_live_response_metadata", "invalid_bounds_or_status",
    ):
        assert checks[key] == 0, f"{key}: {checks[key]}"

    # Check persisted request IDs and bounds against in-memory evidence.
    stored = spark.sql(f"""
        SELECT DISTINCT request_id,
            date_format(request_from_utc, "yyyy-MM-dd'T'HH:mm'Z'") AS from_utc,
            date_format(request_to_utc, "yyyy-MM-dd'T'HH:mm'Z'") AS to_utc
        FROM {table_sql}
        WHERE run_id = '{run_id}'
    """).collect()
    actual_bounds = {
        (row["request_id"], row["from_utc"], row["to_utc"]) for row in stored
    }
    expected_bounds = {
        (
            result["request_id"],
            attempt["request_from_utc"],
            attempt["request_to_utc"],
        )
        for result in pack["results"]
        for attempt in result["attempts"]
    }
    assert actual_bounds == expected_bounds, "Persisted request-bound mismatch"

    print("CHECKPOINT")
    print(json.dumps(checks, indent=2))
    print("Stored attempt provenance: PASSED")
    return checks

def read_snapshot_file(path, fixture_local=False):
    if fixture_local:
        file = Path(path)
        if file.stat().st_size > MAX_SNAPSHOT_BYTES:
            raise ValueError("Fixture snapshot exceeds size limit")
        return file.read_bytes(), file.resolve().as_uri()

    # Use an exact file in an existing accessible Unity Catalog Volume.
    if not path.startswith("/Volumes/") or any(c in path for c in "*?[]"):
        raise ValueError("Supply an exact /Volumes/... file path, not a glob")

    frame = spark.read.format("binaryFile").load(path)
    metadata = frame.select("path", "length").limit(2).collect()
    if len(metadata) != 1:
        raise ValueError("Expected exactly one snapshot file")
    if metadata[0]["length"] > MAX_SNAPSHOT_BYTES:
        raise ValueError("Snapshot exceeds the 16 MiB import limit")

    raw = bytes(frame.select("content").first()["content"])
    if len(raw) != metadata[0]["length"] or len(raw) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Snapshot size changed or exceeds limit")
    return raw, metadata[0]["path"]

def snapshot_result(
    raw, source_file, left, right, run_id,
    original_retrieved_at=None, original_http_status=None, read_error=None,
):
    start, end = validate_config({
        **CONFIG, "source_mode": "api",
        "start_utc": left, "end_utc": right,
    })
    if end - start > timedelta(days=14):
        raise ValueError("Snapshot request bounds exceed 14 days")
    if original_http_status not in (None, 200):
        raise ValueError("Only known HTTP 200 source snapshots are supported")
    if original_retrieved_at is not None:
        original_retrieved_at = timestamp_value(
            original_retrieved_at
        ).isoformat()

    started = utc_now()
    if raw is None:
        kind, error, count = "snapshot_read_error", read_error, None
    else:
        if not isinstance(raw, bytes):
            raise TypeError("Snapshot body must be bytes")
        kind, error, count = check_response_envelope(raw)

    request_id = uuid.uuid4().hex
    attempt = {
        "run_id": run_id,
        "request_id": request_id,
        "attempt_number": 1,
        "source": source_file,
        "source_mode": "snapshot",
        "request_url": f"{API_BASE_URL}/intensity/{left}/{right}",
        "request_from_utc": left,
        "request_to_utc": right,
        "started_at_utc": started,
        "finished_at_utc": utc_now(),
        "retrieved_at_utc": (
            original_retrieved_at if raw is not None else None
        ),
        "http_status": original_http_status if raw is not None else None,
        "response_headers": {},
        "raw_body": raw,
        "response_sha256": (
            hashlib.sha256(raw).hexdigest() if raw is not None else None
        ),
        "record_count": count,
        "status": "succeeded" if kind is None else "failed",
        "failure_kind": kind,
        "error": error,
        "retry_delay_seconds": None,
    }
    return {
        "status": attempt["status"],
        "source_mode": "snapshot",
        "run_id": run_id,
        "request_id": request_id,
        "attempts": [attempt],
    }

def snapshot_loader(manifest, fixture_local=False):
    entries = {}
    for entry in manifest:
        key = (entry["from_utc"], entry["to_utc"])
        if key in entries:
            raise ValueError("Duplicate snapshot bounds in manifest")
        entries[key] = entry

    def load(left, right, run_id):
        entry = entries[(left, right)]
        try:
            raw, source_file = read_snapshot_file(
                entry["path"], fixture_local=fixture_local
            )
        except Exception as exc:
            return snapshot_result(
                None, entry["path"], left, right, run_id,
                read_error=f"{type(exc).__name__}: {str(exc)[:300]}",
            )

        return snapshot_result(
            raw, source_file, left, right, run_id,
            original_retrieved_at=entry.get("original_retrieved_at"),
            original_http_status=entry.get("original_http_status"),
        )
    return load

print("Runner, append writer, checkpoint and snapshot adapter loaded.")

# COMMAND ----------

TASK05_FIXTURE_TABLE = (
    f"{CONFIG['catalog']}.{CONFIG['schema']}."
    f"_gridlens_task05_fixture_{uuid.uuid4().hex}"
)
TASK05_FIXTURE_SQL = quoted_table(TASK05_FIXTURE_TABLE)
FALLBACK_FIXTURE_CHECKS_PASSED = False

print("FIXTURE_TABLE:", TASK05_FIXTURE_TABLE)
print("MANUAL CLEANUP:", f"DROP TABLE {TASK05_FIXTURE_SQL};")

created = False
try:
    assert not spark.catalog.tableExists(TASK05_FIXTURE_TABLE)
    ensure_bronze_table(TASK05_FIXTURE_TABLE)
    created = True

    # Two request windows: transient recovery, then permanent failure.
    test_config = {
        **CONFIG,
        "start_utc": "2026-10-10T00:00Z",
        "end_utc": "2026-10-12T00:00Z",
        "chunk_days": 1,
    }
    transport, state = mock_transport([
        FIXTURES["http_503"],
        FIXTURES["success"],
        FIXTURES["http_400"],
    ])
    failed_run = run_ingestion(
        test_config,
        TASK05_FIXTURE_TABLE,
        api_loader(test_config, transport, lambda seconds: None),
        is_fixture=True,
    )
    assert state["calls"] == 3
    assert failed_run["summary"]["status"] == "failed"
    assert failed_run["summary"]["successful_windows"] == 1
    assert len(failed_run["summary"]["failed_windows"]) == 1
    assert failed_run["summary"]["stored_attempts"] == 3
    checkpoint(failed_run)

    with tempfile.TemporaryDirectory(prefix="gridlens_snapshot_") as folder:
        valid = Path(folder) / "valid.json"
        malformed = Path(folder) / "malformed.json"
        missing = Path(folder) / "missing.json"
        valid.write_bytes(FIXTURES["success"]["body"])
        malformed.write_bytes(FIXTURES["malformed_json"]["body"])

        one_window = {
            **CONFIG,
            "start_utc": "2026-10-10T00:00Z",
            "end_utc": "2026-10-10T01:00Z",
        }
        for label, file, expected_status in [
            ("valid file", valid, "succeeded"),
            ("malformed file", malformed, "failed"),
            ("missing file", missing, "failed"),
        ]:
            manifest = [{
                "from_utc": one_window["start_utc"],
                "to_utc": one_window["end_utc"],
                "path": str(file),
                "original_retrieved_at": None,
                "original_http_status": None,
            }]
            imported = run_ingestion(
                one_window,
                TASK05_FIXTURE_TABLE,
                snapshot_loader(manifest, fixture_local=True),
                is_fixture=True,
            )
            assert imported["summary"]["status"] == expected_status
            checkpoint(imported)

            attempt = imported["results"][0]["attempts"][0]
            assert attempt["retrieved_at_utc"] is None
            assert attempt["http_status"] is None
            if label == "valid file":
                assert attempt["raw_body"] == FIXTURES["success"]["body"]
            print(f"Snapshot test: {label}: PASSED")

    assert spark.table(TASK05_FIXTURE_TABLE).count() == 6
    FALLBACK_FIXTURE_CHECKS_PASSED = True
    print("Runner failure visibility and snapshot-file fixture tests: PASSED")

finally:
    if created:
        spark.sql(f"DROP TABLE IF EXISTS {TASK05_FIXTURE_SQL}")
        assert not spark.catalog.tableExists(TASK05_FIXTURE_TABLE)
        print("Task-05 fixture cleanup: PASSED")

# COMMAND ----------

SMALL_CHECKPOINT_PASSED = False

SMALL_CONFIG = {
    **CONFIG,
    "source_mode": "api",
    "start_utc": "2026-10-10T00:00Z",
    "end_utc": "2026-10-10T01:00Z",
}

SMALL_RUN = run_ingestion(
    SMALL_CONFIG, LIVE_TABLE, api_loader(SMALL_CONFIG)
)
SMALL_CHECKS = checkpoint(SMALL_RUN)

if SMALL_RUN["summary"]["status"] != "succeeded":
    raise RuntimeError(
        "Small live ingestion failed. Evidence was retained; "
        "do not run the full backfill."
    )

spark.sql(f"""
    SELECT
        request_from_utc, request_to_utc,
        source_mode, http_status, status,
        retrieved_at_utc, response_sha256,
        substring(raw_text, 1, 160) AS raw_text_preview
    FROM {LIVE_TABLE_SQL}
    WHERE run_id = '{SMALL_RUN["summary"]["run_id"]}'
    ORDER BY attempt_number
""").show(truncate=False)

SMALL_CHECKPOINT_PASSED = True
print("Small live fetch, persistence and checkpoint: PASSED")

# COMMAND ----------

REFETCH_CHECKPOINT_PASSED = False
assert SMALL_CHECKPOINT_PASSED, "Complete the small checkpoint first"

first_run_id = SMALL_RUN["summary"]["run_id"]
first_event_ids = {
    row["event_id"] for row in spark.sql(f"""
        SELECT event_id FROM {LIVE_TABLE_SQL}
        WHERE run_id = '{first_run_id}'
    """).collect()
}
before_count = spark.table(LIVE_TABLE).count()

REFETCH_RUN = run_ingestion(
    SMALL_CONFIG, LIVE_TABLE, api_loader(SMALL_CONFIG)
)
checkpoint(REFETCH_RUN)

retained_event_ids = {
    row["event_id"] for row in spark.sql(f"""
        SELECT event_id FROM {LIVE_TABLE_SQL}
        WHERE run_id = '{first_run_id}'
    """).collect()
}
assert retained_event_ids == first_event_ids, "Earlier evidence changed"
assert spark.table(LIVE_TABLE).count() == (
    before_count + REFETCH_RUN["summary"]["stored_attempts"]
)
assert REFETCH_RUN["summary"]["run_id"] != first_run_id

if REFETCH_RUN["summary"]["status"] != "succeeded":
    raise RuntimeError("Re-fetch failed; failure evidence was retained")

REFETCH_CHECKPOINT_PASSED = True
print("Repeated retrieval appended; earlier run evidence retained: PASSED")

# COMMAND ----------

RUN_FULL_BACKFILL = True

if not RUN_FULL_BACKFILL:
    print("Full backfill skipped. Review cells 15–16 before enabling.")
else:
    assert FALLBACK_FIXTURE_CHECKS_PASSED
    assert SMALL_CHECKPOINT_PASSED
    assert REFETCH_CHECKPOINT_PASSED

    FULL_CONFIG = {**CONFIG, "source_mode": "api"}
    FULL_RUN = run_ingestion(
        FULL_CONFIG, LIVE_TABLE, api_loader(FULL_CONFIG)
    )
    FULL_CHECKS = checkpoint(FULL_RUN)

    if FULL_RUN["summary"]["status"] != "succeeded":
        raise RuntimeError(
            "Backfill is NOT complete. Review failed/unattempted windows; "
            "persisted successful and failed evidence was retained."
        )

    print("Configured API backfill and stored-request checkpoint: PASSED")