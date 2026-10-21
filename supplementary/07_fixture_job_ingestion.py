# Databricks notebook source
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

# Definition-only shared Bronze schema.
# No table reads, table creation, writes or task values.

import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Do not erase an active fixture lifecycle by rerunning setup.
if (
    globals().get("RUNTIME_FIXTURE_ACTIVE", False)
    or globals().get("SILVER_FIXTURE_ACTIVE", False)
):
    raise RuntimeError(
        "An active fixture lifecycle exists. Stop and resolve it "
        "before rerunning definition setup."
    )

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

bronze_types = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "boolean": T.BooleanType(),
    "timestamp": T.TimestampType(),
    "binary": T.BinaryType(),
    "double": T.DoubleType(),
}

ATTEMPT_SCHEMA = T.StructType([
    T.StructField(name, bronze_types[kind], nullable)
    for name, (kind, nullable) in EXPECTED_BRONZE.items()
])

SOURCE_BRONZE_EXPECTED = dict(EXPECTED_BRONZE)
SOURCE_BRONZE_SCHEMA = T.StructType([
    T.StructField(name, bronze_types[kind], nullable)
    for name, (kind, nullable) in SOURCE_BRONZE_EXPECTED.items()
])

# Inactive placeholders, not fixture authorization.
RUNTIME_FIXTURE_ACTIVE = False
RUNTIME_FIXTURE_TABLE = None
RUNTIME_FIXTURE_SQL = None

SILVER_FIXTURE_ACTIVE = False
SILVER_FIXTURE_NATIONAL = None
SILVER_FIXTURE_QUARANTINE = None

spark.conf.set("spark.sql.session.timeZone", "UTC")

print("Bronze schema definitions loaded; fixture lifecycles inactive.")

# COMMAND ----------

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

# Bronze fixture guard only.
# No fixture table is created or activated here.

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


print("Bronze fixture guard loaded; no destination activated.")

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

# Definition-loading checkpoint, not runtime acceptance.

import inspect

required_functions = [
    "parse_utc",
    "validate_config",
    "format_utc",
    "plan_windows",
    "verify_windows",
    "utc_now",
    "live_transport",
    "check_response_envelope",
    "retry_after_seconds",
    "fetch_window",
    "bind_runtime_parameters",
    "timestamp_value",
    "fixture_rows",
    "prepare_fixture_attempt_rows",
    "require_runtime_fixture",
    "normalise_attempt",
    "persist_prepared_fixture_rows",
    "read_fixture_events",
    "append_fixture_events",
    "run_fixture_ingestion",
]

missing = [
    name for name in required_functions
    if not callable(globals().get(name))
]
if missing:
    raise RuntimeError(f"Missing ingestion definitions: {missing}")

for name in ("FixturePersistenceError", "FixtureIngestionError"):
    value = globals().get(name)
    if not isinstance(value, type) or not issubclass(value, RuntimeError):
        raise RuntimeError(f"Missing/invalid exception definition: {name}")

if len(ATTEMPT_SCHEMA.fields) != 25:
    raise RuntimeError("Expected the 25-column Bronze schema")

if RUNTIME_FIXTURE_ACTIVE or RUNTIME_FIXTURE_TABLE is not None:
    raise RuntimeError("Definition loading unexpectedly activated a fixture")

# Confirm the fetcher still captures the fail-closed transport.
captured_transport = inspect.signature(
    fetch_window
).parameters["transport"].default

if captured_transport is not live_transport:
    raise RuntimeError("Fetcher default transport differs from the source")

# This must stop before any Spark table access.
try:
    require_runtime_fixture()
except RuntimeError as exc:
    if str(exc) != "Fixture table is not active":
        raise
else:
    raise RuntimeError("Inactive Bronze fixture guard did not reject access")

print(json.dumps({
    "checkpoint": "fixture_job_ingestion_definitions_v1",
    "definitions_loaded": required_functions,
    "bronze_schema_columns": len(ATTEMPT_SCHEMA.fields),
    "inactive_destination_guard": "REJECTED_AS_EXPECTED",
    "default_transport": "ORIGINAL_FAIL_CLOSED_FUNCTION",
    "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
    "execution_scope": "DEFINITION_INVENTORY_ONLY",
    "ingestion_executed": False,
    "task_values_emitted": False,
}, indent=2))

# COMMAND ----------

# CELL 11 — Synthetic producer definitions only.
# No HTTP, Delta writes, task values or job entrypoint execution.

def producer_fixture_contract():
    raw = {
        **PARAMETER_DEFAULTS,
        "execution_mode": "fixture",
        "allow_live_writes": "false",
        "run_anchor_utc": "2026-01-03T00:00:00Z",
        "overlap_days": "2",
        "publication_start_utc": "2026-01-01T00:00Z",
        "publication_end_utc": "2026-01-03T00:00Z",
    }
    config, _, manifest = bind_runtime_parameters(raw)

    # Two explicitly planned requests rather than one two-day request.
    config = {
        **config,
        "chunk_days": 1,
        "max_retries": 1,
    }
    windows = plan_windows(config)
    verify_windows(config, windows)

    manifest = {
        **manifest,
        "planned_request_windows": len(windows),
    }
    return config, manifest


def make_producer_fixture_loader(config, fail_window=None):
    planned = [
        (format_utc(left), format_utc(right))
        for left, right in plan_windows(config)
    ]

    if fail_window is not None and (
        type(fail_window) is not int
        or not 1 <= fail_window <= len(planned)
    ):
        raise ValueError("Invalid failing fixture window")

    state = {
        "windows_called": [],
        "transport_calls": 0,
        "mock_delays": [],
    }

    def loader(from_utc, to_utc, run_id):
        ordinal = len(state["windows_called"]) + 1

        if (
            ordinal > len(planned)
            or (from_utc, to_utc) != planned[ordinal - 1]
        ):
            raise ValueError(
                "Fixture loader called outside the planned order"
            )

        state["windows_called"].append([from_utc, to_utc])

        records = []
        cursor = parse_utc(from_utc)
        right = parse_utc(to_utc)

        while cursor < right:
            following = cursor + timedelta(minutes=30)
            records.append({
                "from": format_utc(cursor),
                "to": format_utc(following),
                "intensity": {
                    "forecast": 100,
                    "actual": 90,
                    "index": "moderate",
                },
            })
            cursor = following

        body = json.dumps(
            {"data": records},
            separators=(",", ":"),
        ).encode("utf-8")

        expected_url = (
            f"{API_BASE_URL}/intensity/{from_utc}/{to_utc}"
        )
        request_calls = 0

        def synthetic_transport(url, timeout_seconds):
            nonlocal request_calls

            if (
                url != expected_url
                or timeout_seconds != config["timeout_seconds"]
            ):
                raise ValueError(
                    "Unexpected synthetic request arguments"
                )

            request_calls += 1
            state["transport_calls"] += 1

            if request_calls > 2:
                raise ValueError(
                    "Unexpected extra synthetic transport call"
                )

            # First attempt always fails.
            # A designated failing window also fails its retry.
            if request_calls == 1 or ordinal == fail_window:
                return {
                    "status_code": 503,
                    "headers": {},
                    "body": b'{"error":"synthetic"}',
                }

            return {
                "status_code": 200,
                "headers": {
                    "content-type": "application/json",
                },
                "body": body,
            }

        def mock_sleep(delay):
            state["mock_delays"].append(delay)

        return fetch_window(
            config,
            from_utc,
            to_utc,
            transport=synthetic_transport,
            sleep_fn=mock_sleep,
            run_id=run_id,
        )

    return loader, state


def make_in_memory_fixture_persistence():
    # Mock storage only: not Delta and not durable.
    stored = {}
    state = {"append_calls": 0}

    def read_rows(event_ids):
        return [
            deepcopy(stored[event_id])
            for event_id in event_ids
            if event_id in stored
        ]

    def append_rows(rows):
        state["append_calls"] += 1
        for row in rows:
            if row["event_id"] in stored:
                raise ValueError(
                    "Mock append received an existing event ID"
                )
            stored[row["event_id"]] = deepcopy(row)

    def persist(rows):
        return persist_prepared_fixture_rows(
            rows,
            read_rows,
            append_rows,
        )

    return persist, stored, state


print(
    "Synthetic producer definitions loaded; "
    "no execution or storage writes."
)

# COMMAND ----------

# CELL 13 — Disposable Bronze lifecycle definitions.
# Loading this cell creates no tables.

PRODUCER_FIXTURE_OWNER_KEY = "gridlens.fixture_owner"


def create_producer_delta_fixture():
    global RUNTIME_FIXTURE_ACTIVE
    global RUNTIME_FIXTURE_TABLE
    global RUNTIME_FIXTURE_SQL
    global PRODUCER_FIXTURE_INTENT

    if RUNTIME_FIXTURE_ACTIVE:
        raise RuntimeError("A Bronze fixture is already active")

    owner = uuid.uuid4().hex
    table = "workspace.bronze._gridlens_runtime_fixture_" + owner
    sql_table = ".".join(
        f"`{part}`" for part in table.split(".")
    )

    if spark.catalog.tableExists(table):
        raise RuntimeError("Fixture name collision; no changes made")

    PRODUCER_FIXTURE_INTENT = {
        "table": table,
        "sql_table": sql_table,
        "owner": owner,
        "delta_table_id": None,
    }

    # Record intent before CREATE, including uncertain CREATE outcomes.
    RUNTIME_FIXTURE_TABLE = table
    RUNTIME_FIXTURE_SQL = sql_table
    RUNTIME_FIXTURE_ACTIVE = True

    print(json.dumps({
        "fixture_creation_intent": PRODUCER_FIXTURE_INTENT,
        "manual_cleanup_sql": f"DROP TABLE IF EXISTS {sql_table}",
    }, indent=2))

    ddl = ",\n".join(
        f"`{name}` {kind.upper()}"
        + ("" if nullable else " NOT NULL")
        for name, (kind, nullable) in EXPECTED_BRONZE.items()
    )

    spark.sql(f"""
        CREATE TABLE {sql_table} (
            {ddl}
        ) USING DELTA
        TBLPROPERTIES (
            '{PRODUCER_FIXTURE_OWNER_KEY}' = '{owner}'
        )
    """)

    require_runtime_fixture()

    detail = spark.sql(
        f"DESCRIBE DETAIL {sql_table}"
    ).first().asDict(recursive=True)

    if (
        detail["properties"].get(PRODUCER_FIXTURE_OWNER_KEY)
        != owner
    ):
        raise RuntimeError("Fixture ownership property mismatch")

    PRODUCER_FIXTURE_INTENT["delta_table_id"] = str(detail["id"])
    return deepcopy(PRODUCER_FIXTURE_INTENT)


def drop_producer_delta_fixture():
    global RUNTIME_FIXTURE_ACTIVE
    global RUNTIME_FIXTURE_TABLE
    global RUNTIME_FIXTURE_SQL

    intent = globals().get("PRODUCER_FIXTURE_INTENT")
    if not isinstance(intent, dict):
        raise RuntimeError("Missing explicit fixture cleanup intent")

    owner = intent["owner"]
    if (
        not isinstance(owner, str)
        or not re.fullmatch(r"[0-9a-f]{32}", owner)
    ):
        raise RuntimeError("Invalid cleanup owner")

    expected_table = (
        "workspace.bronze._gridlens_runtime_fixture_" + owner
    )
    expected_sql = ".".join(
        f"`{part}`" for part in expected_table.split(".")
    )

    if (
        intent["table"] != expected_table
        or intent["sql_table"] != expected_sql
        or RUNTIME_FIXTURE_TABLE != expected_table
        or RUNTIME_FIXTURE_SQL != expected_sql
    ):
        raise RuntimeError(
            "Cleanup target differs from allocated fixture"
        )

    if spark.catalog.tableExists(expected_table):
        detail = spark.sql(
            f"DESCRIBE DETAIL {expected_sql}"
        ).first().asDict(recursive=True)

        if (
            detail["format"] != "delta"
            or detail["properties"].get(PRODUCER_FIXTURE_OWNER_KEY)
            != owner
            or (
                intent["delta_table_id"] is not None
                and str(detail["id"]) != intent["delta_table_id"]
            )
        ):
            raise RuntimeError(
                "Cleanup ownership/identity check failed; no DROP"
            )

        spark.sql(f"DROP TABLE {expected_sql}")

    if spark.catalog.tableExists(expected_table):
        raise RuntimeError("Fixture still exists after cleanup")

    RUNTIME_FIXTURE_ACTIVE = False
    RUNTIME_FIXTURE_TABLE = None
    RUNTIME_FIXTURE_SQL = None

    return {
        "table": expected_table,
        "result": "ABSENT_CONFIRMED",
    }


print(
    "Disposable Bronze lifecycle definitions loaded; "
    "no tables created."
)

# COMMAND ----------

# Explicit fixture-job parameter definitions.
# No entrypoint execution or table access.

def required_fixture_widget(name):
    # Preserve retrieval errors; never create a blank fallback widget.
    value = dbutils.widgets.get(name)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Missing fixture job parameter: {name}"
        )

    value = value.strip()

    # Reject only a whole unresolved dynamic reference.
    # Nested JSON closing braces are valid.
    if re.fullmatch(r"\{\{[^}]*\}\}", value):
        raise ValueError(
            f"Unresolved fixture job parameter: {name}"
        )

    return value


def validate_fixture_job_context(raw):
    names = {
        "job_run_id",
        "task_run_id",
        "task_execution_count",
    }

    if not isinstance(raw, dict) or set(raw) != names:
        raise ValueError(
            "Unexpected fixture job context keys/type"
        )

    for name in names:
        if (
            not isinstance(raw[name], str)
            or not re.fullmatch(
                r"[1-9][0-9]{0,31}",
                raw[name],
            )
        ):
            raise ValueError(
                f"Invalid explicit job context: {name}"
            )

    return dict(raw)


def read_fixture_job_context():
    if required_fixture_widget("fixture_job_approved") != "true":
        raise ValueError(
            "This entrypoint requires explicit fixture approval"
        )

    return validate_fixture_job_context({
        name: required_fixture_widget(name)
        for name in (
            "job_run_id",
            "task_run_id",
            "task_execution_count",
        )
    })


print(
    "Fixture-job parameter definitions loaded; "
    "no entrypoint executed."
)

# COMMAND ----------

# CELL 14 — Fixture-job producer entrypoint.
# Synthetic transport only; retains Bronze on success.

job_context = read_fixture_job_context()

if RUNTIME_FIXTURE_ACTIVE:
    raise RuntimeError("An active fixture already exists")

try:
    fixture = create_producer_delta_fixture()
    config, manifest = producer_fixture_contract()
    loader, loader_state = make_producer_fixture_loader(config)

    def job_delta_persist(rows):
        return persist_prepared_fixture_rows(
            rows,
            read_fixture_events,
            append_fixture_events,
        )

    pack = run_fixture_ingestion(
        config,
        manifest,
        loader,
        job_delta_persist,
    )

    prepared = [
        row
        for batch in pack["prepared_batches"]
        for row in batch
    ]

    persisted = [
        row.asDict(recursive=True)
        for row in spark.table(RUNTIME_FIXTURE_TABLE).collect()
    ]

    if (
        pack["summary"]["status"] != "SUCCEEDED"
        or pack["summary"]["successful_windows"] != 2
        or pack["summary"]["confirmed_attempt_events"] != 4
        or len(persisted) != 4
        or len({row["event_id"] for row in persisted}) != 4
        or {
            row["run_id"] for row in persisted
        } != {pack["summary"]["run_id"]}
        or {
            row["event_id"]: normalise_attempt(row)
            for row in persisted
        } != {
            row["event_id"]: normalise_attempt(row)
            for row in prepared
        }
    ):
        raise RuntimeError(
            "Producer durable completeness/readback failed"
        )

    version = int(spark.sql(
        f"DESCRIBE HISTORY {RUNTIME_FIXTURE_SQL} LIMIT 1"
    ).first()["version"])

    envelope = {
        "envelope_version": "gridlens_fixture_artifact_v1",
        "handoff": deepcopy(pack["handoff"]),
        "artifact": {
            "source_table": fixture["table"],
            "delta_version": version,
            "delta_table_id": fixture["delta_table_id"],
            "fixture_owner": fixture["owner"],
        },
    }

    envelope_json = json.dumps(envelope)
    if len(envelope_json.encode("utf-8")) > 16 * 1024:
        raise RuntimeError(
            "Fixture envelope exceeds consumer size limit"
        )

    dbutils.jobs.taskValues.set(
        key="fixture_envelope_json",
        value=envelope_json,
    )
    dbutils.jobs.taskValues.set(
        key="fixture_producer_context_json",
        value=json.dumps(job_context),
    )

    print(json.dumps({
        "checkpoint": "fixture_job_producer_entrypoint_v1",
        "job_context": job_context,
        "envelope": envelope,
        "planned_windows": 2,
        "persisted_attempt_events": 4,
        "artifact_retained_for_consumer": True,
        "task_value_set_calls_completed": True,
        "source_identity_kind": manifest["identity_kind"],
        "cleanup_pending": True,
    }, indent=2))

except Exception:
    try:
        if RUNTIME_FIXTURE_ACTIVE:
            print(json.dumps({
                "producer_failure_cleanup":
                    drop_producer_delta_fixture(),
            }, indent=2))

    except Exception as cleanup_exc:
        print(json.dumps({
            "producer_failure_cleanup": "UNRESOLVED",
            "message": str(cleanup_exc)[:500],
        }, indent=2))
        raise

    raise