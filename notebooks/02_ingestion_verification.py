# Databricks notebook source
import json
import re
import hashlib
from datetime import datetime, timedelta, timezone
from collections import defaultdict

TABLE = "workspace.bronze.api_responses"

BACKFILL_RUN_ID = "ddb2766af88740bc82eabb38bc557d69"
SMALL_RUN_ID = "cda8fe6de1504113b34554aefd438ed5"
REFETCH_RUN_ID = "1806ccddd894492cb384a211c276b471"

START_UTC = "2026-07-13T00:00Z"
END_UTC = "2026-10-11T00:00Z"
CHUNK_DAYS = 14

EXPECTED_EVENT_COUNTS = {
    BACKFILL_RUN_ID: 7,
    SMALL_RUN_ID: 1,
    REFETCH_RUN_ID: 1,
}

ORIGINAL_SMALL_RESPONSE_HASH = (
    "e8ce4c7322c94de1a3e9dd71d14cd17"
    "fbef638ac653418844bd3dd8ed692dcbf"
)

def parse_utc(text):
    return datetime.strptime(
        text, "%Y-%m-%dT%H:%MZ"
    ).replace(tzinfo=timezone.utc)

def format_utc(value):
    return value.strftime("%Y-%m-%dT%H:%MZ")

parts = TABLE.split(".")
assert len(parts) == 3
assert all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts)
TABLE_SQL = ".".join(f"`{p}`" for p in parts)

for run_id in EXPECTED_EVENT_COUNTS:
    assert re.fullmatch(r"[0-9a-f]{32}", run_id), "Invalid run ID"

spark.conf.set("spark.sql.session.timeZone", "UTC")
assert spark.catalog.tableExists(TABLE), "Bronze table not found"

detail = spark.sql(f"DESCRIBE DETAIL {TABLE_SQL}").first().asDict()
assert detail["format"] == "delta", "Expected Delta storage"

expected_types = {
    name: "string" for name in [
        "event_id", "run_id", "request_id", "source", "source_mode",
        "event_kind", "request_url", "raw_text", "response_sha256",
        "status", "failure_kind", "error", "response_headers_json",
    ]
}
expected_types.update({
    "attempt_number": "int",
    "http_status": "int",
    "record_count": "int",
    "is_fixture": "boolean",
    "raw_body": "binary",
    "retry_delay_seconds": "double",
})
expected_types.update({
    name: "timestamp" for name in [
        "request_from_utc", "request_to_utc",
        "started_at_utc", "finished_at_utc",
        "retrieved_at_utc", "stored_at_utc",
    ]
})

actual_types = {
    field.name: field.dataType.simpleString()
    for field in spark.table(TABLE).schema
}
assert actual_types == expected_types, "Bronze schema differs from the contract"

print("Destination:", TABLE)
print("Delta and explicit schema contract: PASSED")
print("Verification is read-only; no source request or table write.")

# COMMAND ----------

run_filter = ", ".join(
    f"'{run_id}'" for run_id in EXPECTED_EVENT_COUNTS
)

SELECTED_ROWS = spark.sql(f"""
    SELECT *,
        date_format(
            request_from_utc, "yyyy-MM-dd'T'HH:mm'Z'"
        ) AS from_text,
        date_format(
            request_to_utc, "yyyy-MM-dd'T'HH:mm'Z'"
        ) AS to_text
    FROM {TABLE_SQL}
    WHERE run_id IN ({run_filter})
    ORDER BY run_id, request_from_utc, attempt_number
""").limit(1001).collect()

assert len(SELECTED_ROWS) <= 1000, "Unexpected evidence volume; review scope"
assert len(SELECTED_ROWS) == sum(EXPECTED_EVENT_COUNTS.values()), (
    "Selected run event counts differ from returned execution evidence"
)

ROWS_BY_RUN = defaultdict(list)
event_ids = set()

required_values = [
    "event_id", "run_id", "request_id", "attempt_number",
    "source", "source_mode", "is_fixture", "event_kind",
    "request_url", "request_from_utc", "request_to_utc",
    "started_at_utc", "finished_at_utc", "stored_at_utc",
    "status", "response_headers_json",
]

for row in SELECTED_ROWS:
    for field in required_values:
        assert row[field] is not None, f"Missing {field}"

    assert row["event_id"] not in event_ids, "Duplicate event ID"
    event_ids.add(row["event_id"])
    ROWS_BY_RUN[row["run_id"]].append(row)

    assert row["source_mode"] == "api"
    assert row["is_fixture"] is False
    assert row["source"] == "national_carbon_intensity_api"
    assert row["event_kind"] == "response"
    assert row["status"] == "succeeded"
    assert row["http_status"] == 200
    assert row["failure_kind"] is None and row["error"] is None
    assert row["retrieved_at_utc"] is not None
    assert row["attempt_number"] >= 1
    assert row["request_to_utc"] > row["request_from_utc"]

    expected_url = (
        "https://api.carbonintensity.org.uk/intensity/"
        f"{row['from_text']}/{row['to_text']}"
    )
    assert row["request_url"] == expected_url

    raw = bytes(row["raw_body"])
    assert hashlib.sha256(raw).hexdigest() == row["response_sha256"]
    assert raw.decode("utf-8", errors="replace") == row["raw_text"]
    assert isinstance(json.loads(row["response_headers_json"]), dict)

    payload = json.loads(raw)
    assert isinstance(payload, dict)
    records = payload.get("data")
    assert isinstance(records, list) and records
    assert row["record_count"] == len(records)

    for record in records:
        assert isinstance(record, dict)
        assert isinstance(record.get("from"), str)
        assert isinstance(record.get("to"), str)
        intensity = record.get("intensity")
        assert isinstance(intensity, dict)
        assert {"forecast", "actual", "index"}.issubset(intensity)

for run_id, expected_count in EXPECTED_EVENT_COUNTS.items():
    assert len(ROWS_BY_RUN[run_id]) == expected_count, run_id

print("Selected persisted events:", len(SELECTED_ROWS))
print("Stored bytes, text, hashes and response envelopes: PASSED")
print("Live source labels and required provenance: PASSED")

# COMMAND ----------

start, end = parse_utc(START_UTC), parse_utc(END_UTC)
assert start < end
assert type(CHUNK_DAYS) is int and 1 <= CHUNK_DAYS <= 14

EXPECTED_WINDOWS = []
cursor = start
while cursor < end:
    next_cursor = min(cursor + timedelta(days=CHUNK_DAYS), end)
    EXPECTED_WINDOWS.append((format_utc(cursor), format_utc(next_cursor)))
    cursor = next_cursor

backfill_rows = ROWS_BY_RUN[BACKFILL_RUN_ID]
actual_windows = sorted(
    (row["from_text"], row["to_text"]) for row in backfill_rows
)

assert actual_windows == EXPECTED_WINDOWS, (
    "Missing, duplicate, extra or incorrectly bounded backfill requests"
)
assert len({row["request_id"] for row in backfill_rows}) == len(
    EXPECTED_WINDOWS
)

previous_end = start
for left, right in actual_windows:
    left_dt, right_dt = parse_utc(left), parse_utc(right)
    assert left_dt == previous_end, "Request gap or overlap"
    assert timedelta(0) < right_dt - left_dt <= timedelta(days=14)
    previous_end = right_dt
assert previous_end == end

small = ROWS_BY_RUN[SMALL_RUN_ID][0]
refetch = ROWS_BY_RUN[REFETCH_RUN_ID][0]

expected_small_bounds = (
    "2026-10-10T00:00Z", "2026-10-10T01:00Z"
)
assert (small["from_text"], small["to_text"]) == expected_small_bounds
assert (refetch["from_text"], refetch["to_text"]) == expected_small_bounds

assert small["run_id"] != refetch["run_id"]
assert small["request_id"] != refetch["request_id"]
assert small["event_id"] != refetch["event_id"]

# Check the original retained response against the hash already reported.
assert small["response_sha256"] == ORIGINAL_SMALL_RESPONSE_HASH

WINDOW_EVIDENCE = [
    {
        "from_utc": row["from_text"],
        "to_utc": row["to_text"],
        "outcome": row["status"],
    }
    for row in sorted(backfill_rows, key=lambda row: row["from_text"])
]

print(json.dumps(WINDOW_EVIDENCE, indent=2))
print("Exact persisted request-window coverage: PASSED")
print("Original sample and separate re-fetch evidence retained: PASSED")

# COMMAND ----------

VERIFICATION_SUMMARY = {
    "verification_time_utc": datetime.now(timezone.utc).isoformat(),
    "destination": TABLE,
    "actual_ingestion_mode": "api",
    "configured_from_utc": START_UTC,
    "configured_to_utc": END_UTC,
    "backfill_run_id": BACKFILL_RUN_ID,
    "backfill_windows_verified": len(EXPECTED_WINDOWS),
    "selected_persisted_events_verified": len(SELECTED_ROWS),
    "read_only_checks": {
        "delta_and_schema": "passed",
        "raw_body_text_hash_and_envelope": "passed",
        "required_live_provenance": "passed",
        "exact_request_window_coverage": "passed",
        "original_sample_and_refetch_retained": "passed",
    },
    "previously_reviewed_execution_evidence_not_rerun_here": {
        "bounded_fetcher_controlled_tests": "14 passed",
        "runner_partial_failure_reporting": "passed",
        "local_snapshot_valid_malformed_missing_files": "passed",
        "fixture_cleanup": "passed",
        "append_growth_and_original_event_id_retention": "passed",
    },
    "intentionally_not_executed": [
        "cell 18: seven-day recent-window re-fetch",
        "cell 19: real uploaded-Volume snapshot import",
    ],
    "remaining_boundaries": [
        "HTTP/request coverage is not interval-level completeness",
        "Boundary intervals remain in raw responses",
        "No Silver validation, deduplication or revision MERGE yet",
        "Requests timeout is not a strict total-download deadline",
        "No scheduled job, repository publication or production claim",
    ],
    "code_assistance": (
        "Notion AI supplied code; the user executed it in Databricks "
        "and returned outputs for review."
    ),
    "source_documentation": (
        "https://carbon-intensity.github.io/api-definitions/"
    ),
    "verification_status": "passed",
}

print(json.dumps(VERIFICATION_SUMMARY, indent=2))
print("Read-only ingestion verification: PASSED")