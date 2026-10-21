# Databricks notebook source
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
