# Databricks notebook source
# CELL 01 — Python
import json
import hashlib
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

BRONZE = "workspace.bronze.api_responses"
NATIONAL = "workspace.silver.national_intensity"
QUARANTINE = "workspace.silver.quarantined_records"

RUN_ID = "ddb2766af88740bc82eabb38bc557d69"
RANGE_START = datetime(2026, 7, 13, tzinfo=timezone.utc)
RANGE_END = datetime(2026, 10, 11, tzinfo=timezone.utc)

PARSER_VERSION = "gridlens_parse_v1"
VALIDATION_VERSION = "gridlens_validation_v1+gridlens_parse_v1"
CANONICAL_VERSION = "gridlens_canonical_v1"

MAX_ROWS = 100_000
MAX_BODY_BYTES = 32 * 1024 * 1024

def utc(value):
    if value is None:
        return None
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )

def normalise(row):
    return {
        name: utc(value) if isinstance(value, datetime) else value
        for name, value in row.asDict().items()
    }

def compact_json(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False
    )

# Pin each table to an explicit Delta version for repeatable reads.
# These are separate table snapshots, not one multi-table transaction.
table_versions = {}
tables = {}
for name in (BRONZE, NATIONAL, QUARANTINE):
    version = int(
        spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first()["version"]
    )
    table_versions[name] = version
    tables[name] = spark.sql(
        f"SELECT * FROM {name} VERSION AS OF {version}"
    )

expected_national_types = {
    "interval_start_utc": "timestamp",
    "interval_end_utc": "timestamp",
    "forecast_intensity": "int",
    "actual_intensity": "int",
    "source_index": "string",
    "source": "string",
    "source_mode": "string",
    "source_retrieved_at_utc": "timestamp",
    "bronze_event_id": "string",
    "bronze_run_id": "string",
    "bronze_request_id": "string",
    "source_record_ordinal": "int",
    "response_sha256": "string",
    "record_value_sha256": "string",
    "parser_version": "string",
    "validation_contract_version": "string",
    "canonicalisation_version": "string",
}

expected_quarantine_types = {
    "rejection_id": "string",
    "rejection_scope": "string",
    "bronze_event_id": "string",
    "bronze_run_id": "string",
    "bronze_request_id": "string",
    "source": "string",
    "source_mode": "string",
    "source_retrieved_at_utc": "timestamp",
    "response_sha256": "string",
    "source_record_ordinal": "int",
    "raw_record_json": "string",
    "rejection_reasons": "array<string>",
    "parser_version": "string",
    "validation_contract_version": "string",
    "processing_run_id": "string",
    "processed_at_utc": "timestamp",
}

for name, expected in (
    (NATIONAL, expected_national_types),
    (QUARANTINE, expected_quarantine_types),
):
    actual = {
        field.name: field.dataType.simpleString()
        for field in tables[name].schema.fields
    }
    assert actual == expected, f"Schema mismatch: {name}"
    print(name)
    tables[name].printSchema()

national_fields = {
    field.name: field for field in tables[NATIONAL].schema.fields
}
for name in expected_national_types:
    if name != "actual_intensity":
        assert not national_fields[name].nullable, \
            f"Missing NOT NULL contract: {name}"

required_quarantine_fields = [
    "rejection_id", "rejection_scope", "bronze_event_id",
    "rejection_reasons", "parser_version",
    "validation_contract_version", "processing_run_id", "processed_at_utc",
]
quarantine_fields = {
    field.name: field for field in tables[QUARANTINE].schema.fields
}
assert all(
    not quarantine_fields[name].nullable
    for name in required_quarantine_fields
)

assert tables[NATIONAL].count() <= MAX_ROWS
assert tables[QUARANTINE].count() <= MAX_ROWS

silver_rows = [normalise(row) for row in tables[NATIONAL].collect()]
quarantine_rows = [
    normalise(row) for row in tables[QUARANTINE].collect()
]

bronze_selected = tables[BRONZE].where(F.col("run_id") == RUN_ID)
assert bronze_selected.count() == 7
assert bronze_selected.select("event_id").distinct().count() == 7

body_bytes = bronze_selected.agg(
    F.sum(F.length("raw_body")).alias("body_bytes")
).first()["body_bytes"] or 0
assert body_bytes <= MAX_BODY_BYTES

bronze_rows = [normalise(row) for row in bronze_selected.collect()]

print("PINNED TABLE VERSIONS")
print(json.dumps(table_versions, indent=2))
print("PASS: schemas and bounded persisted reads.")

# COMMAND ----------

# CELL 02 — Python
utc_pattern = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}"
    r"(?::\d{2}(?:\.\d{1,6})?)?(?:Z|\+00:00)$"
)

def source_time(value):
    if not isinstance(value, str) or not utc_pattern.fullmatch(value):
        raise ValueError("invalid_utc_timestamp")
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)

def unique_object(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("duplicate_json_key")
        output[key] = value
    return output

def reject_constant(value):
    raise ValueError(f"nonstandard_json_constant:{value}")

def signature(row):
    return compact_json([
        row["interval_start_utc"].isoformat(),
        row["interval_end_utc"].isoformat(),
        row["forecast_intensity"],
        row["actual_intensity"],
        row["source_index"],
    ])

def valid_values(record):
    if not isinstance(record, dict):
        raise ValueError("record_not_object")
    intensity = record["intensity"]
    if not isinstance(intensity, dict):
        raise ValueError("intensity_not_object")

    start = source_time(record["from"])
    end = source_time(record["to"])

    if (end - start).total_seconds() != 1800:
        raise ValueError("invalid_duration")
    for value in (start, end):
        if value.minute % 30 or value.second or value.microsecond:
            raise ValueError("invalid_grid_alignment")

    forecast = intensity["forecast"]
    actual = intensity["actual"]  # Missing is not equivalent to explicit null.
    index = intensity["index"]

    for name, value, nullable in (
        ("forecast", forecast, False),
        ("actual", actual, True),
    ):
        if nullable and value is None:
            continue
        if type(value) is not int or not 0 <= value <= 2**31 - 1:
            raise ValueError(f"invalid_{name}")
    if not isinstance(index, str) or not index.strip():
        raise ValueError("invalid_index")

    return start, end, forecast, actual, index

# Independently construct the intended request windows.
expected_windows = []
cursor = RANGE_START
while cursor < RANGE_END:
    next_cursor = min(cursor + timedelta(days=14), RANGE_END)
    expected_windows.append((cursor, next_cursor))
    cursor = next_cursor

observed_windows = sorted(
    (event["request_from_utc"], event["request_to_utc"])
    for event in bronze_rows
)
assert observed_windows == expected_windows
assert len({event["request_id"] for event in bronze_rows}) == 7

groups = defaultdict(list)
invalid_records = []
envelope_defects = []
out_of_range_count = 0
raw_record_count = 0

for event in bronze_rows:
    assert event["status"] == "succeeded"
    assert event["http_status"] == 200
    assert event["source_mode"] == "api"
    assert event["is_fixture"] is False
    assert event["event_kind"] == "response"
    assert isinstance(event["retrieved_at_utc"], datetime)
    assert all(event[name] for name in (
        "event_id", "run_id", "request_id", "source", "response_sha256"
    ))

    raw = bytes(event["raw_body"])
    assert hashlib.sha256(raw).hexdigest() == event["response_sha256"]
    assert raw.decode("utf-8", errors="replace") == event["raw_text"]

    try:
        envelope = json.loads(
            event["raw_text"],
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
        if not isinstance(envelope, dict):
            raise ValueError("envelope_not_object")
        data = envelope["data"]
        if not isinstance(data, list) or not data:
            raise ValueError("invalid_data_array")
        if event["record_count"] != len(data):
            raise ValueError("record_count_mismatch")
    except (ValueError, KeyError, TypeError, RecursionError) as exc:
        envelope_defects.append({
            "event_id": event["event_id"],
            "reason": str(exc),
        })
        continue

    for ordinal, record in enumerate(data):
        raw_record_count += 1
        try:
            start, end, forecast, actual, index = valid_values(record)
        except (ValueError, KeyError, TypeError, OverflowError) as exc:
            invalid_records.append({
                "event_id": event["event_id"],
                "ordinal": ordinal,
                "reason": str(exc),
            })
            continue

        if not (start >= RANGE_START and end <= RANGE_END):
            out_of_range_count += 1
            continue

        candidate = {
            "interval_start_utc": start,
            "interval_end_utc": end,
            "forecast_intensity": forecast,
            "actual_intensity": actual,
            "source_index": index,
            "source": event["source"],
            "source_mode": event["source_mode"],
            "source_retrieved_at_utc": event["retrieved_at_utc"],
            "bronze_event_id": event["event_id"],
            "bronze_run_id": event["run_id"],
            "bronze_request_id": event["request_id"],
            "source_record_ordinal": ordinal,
            "response_sha256": event["response_sha256"],
            "parser_version": PARSER_VERSION,
            "validation_contract_version": VALIDATION_VERSION,
            "canonicalisation_version": CANONICAL_VERSION,
        }
        candidate["record_value_sha256"] = hashlib.sha256(
            signature(candidate).encode("utf-8")
        ).hexdigest()
        groups[(start, end)].append(candidate)

expected_winners = {}
conflicting_keys = []
conflict_blocked_record_count = 0
duplicate_loser_count = 0

for key, candidates in groups.items():
    latest = max(
        row["source_retrieved_at_utc"] for row in candidates
    )
    newest = [
        row for row in candidates
        if row["source_retrieved_at_utc"] == latest
    ]
    if len({signature(row) for row in newest}) > 1:
        conflicting_keys.append([key[0].isoformat(), key[1].isoformat()])
        conflict_blocked_record_count += len(candidates)
        continue

    expected_winners[key] = min(
        newest,
        key=lambda row: (
            row["bronze_event_id"], row["source_record_ordinal"]
        ),
    )
    duplicate_loser_count += len(candidates) - 1

reconstructed_counts = {
    "raw_records": raw_record_count,
    "invalid_records": len(invalid_records),
    "out_of_range_records": out_of_range_count,
    "conflict_blocked_records": conflict_blocked_record_count,
    "duplicate_losers": duplicate_loser_count,
    "expected_winners": len(expected_winners),
    "envelope_defects": len(envelope_defects),
    "conflicting_keys": len(conflicting_keys),
}

assert raw_record_count == (
    len(invalid_records)
    + out_of_range_count
    + conflict_blocked_record_count
    + duplicate_loser_count
    + len(expected_winners)
)

print("INDEPENDENT BRONZE RECONSTRUCTION")
print(json.dumps(reconstructed_counts, indent=2))
print("INVALID RECORDS:", json.dumps(invalid_records[:10], indent=2))
print("ENVELOPE DEFECTS:", json.dumps(envelope_defects[:10], indent=2))
print("CONFLICTING KEYS:", json.dumps(conflicting_keys[:10], indent=2))

# These assertions apply to this reviewed initial backfill.
assert not invalid_records and not envelope_defects and not conflicting_keys
assert reconstructed_counts["raw_records"] == 4327
assert out_of_range_count == 1
assert duplicate_loser_count == 6
assert len(expected_winners) == 4320

# COMMAND ----------

# CELL 03 — Python
expected_grid = set()
cursor = RANGE_START
while cursor < RANGE_END:
    end = cursor + timedelta(minutes=30)
    assert end <= RANGE_END
    expected_grid.add((cursor, end))
    cursor = end

actual_by_key = {}
duplicate_published_keys = []
silver_validation_errors = []

for row in silver_rows:
    key = (row["interval_start_utc"], row["interval_end_utc"])
    if key in actual_by_key:
        duplicate_published_keys.append(key)
    actual_by_key[key] = row

    try:
        # Independently validate persisted business fields.
        valid_values({
            "from": row["interval_start_utc"].isoformat(),
            "to": row["interval_end_utc"].isoformat(),
            "intensity": {
                "forecast": row["forecast_intensity"],
                "actual": row["actual_intensity"],
                "index": row["source_index"],
            },
        })
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        silver_validation_errors.append({
            "event_id": row["bronze_event_id"],
            "ordinal": row["source_record_ordinal"],
            "reason": str(exc),
        })

actual_keys = set(actual_by_key)
missing_keys = sorted(expected_grid - actual_keys)
unexpected_keys = sorted(actual_keys - expected_grid)

missing_reconstructed_keys = sorted(
    set(expected_winners) - actual_keys
)
unexpected_reconstructed_keys = sorted(
    actual_keys - set(expected_winners)
)

mismatched_rows = []
for key in actual_keys & set(expected_winners):
    actual = actual_by_key[key]
    expected = expected_winners[key]
    differences = [
        name for name in expected_national_types
        if actual[name] != expected[name]
    ]
    if differences:
        mismatched_rows.append({
            "interval_start_utc": key[0].isoformat(),
            "different_fields": differences,
        })

present_expected = actual_keys & expected_grid
with_actual = sum(
    actual_by_key[key]["actual_intensity"] is not None
    for key in present_expected
)
without_actual = len(present_expected) - with_actual

coverage_summary = {
    "expected_interval_count": len(expected_grid),
    "present_expected_interval_count": len(present_expected),
    "missing_interval_count": len(missing_keys),
    "unexpected_interval_count": len(unexpected_keys),
    "duplicate_published_key_count": len(duplicate_published_keys),
    "present_intervals_with_actual": with_actual,
    "present_intervals_without_actual": without_actual,
    "interval_presence_percent": (
        100 * len(present_expected) / len(expected_grid)
    ),
    "actual_value_coverage_percent": (
        100 * with_actual / len(expected_grid)
    ),
    "persisted_value_validation_error_count": len(silver_validation_errors),
    "raw_bronze_value_or_provenance_mismatch_count": len(mismatched_rows),
}

print("INDEPENDENT GRID AND PROVENANCE CHECKS")
print(json.dumps(coverage_summary, indent=2))
print("MISSING GRID KEYS:", [
    (start.isoformat(), end.isoformat())
    for start, end in missing_keys[:10]
])
print("UNEXPECTED GRID KEYS:", [
    (start.isoformat(), end.isoformat())
    for start, end in unexpected_keys[:10]
])
print("SILVER VALIDATION ERRORS:", silver_validation_errors[:10])
print("VALUE/PROVENANCE MISMATCHES:", mismatched_rows[:10])

assert not duplicate_published_keys
assert not missing_keys and not unexpected_keys
assert not silver_validation_errors
assert not missing_reconstructed_keys and not unexpected_reconstructed_keys
assert not mismatched_rows

# Actual-value coverage is reported, not required to be 100%.
assert with_actual + without_actual == len(present_expected)

print("PASS: exact grid, valid persisted values and all-field Bronze reconciliation.")

# COMMAND ----------

# CELL 04 — Python
quarantine_ids = [row["rejection_id"] for row in quarantine_rows]
assert len(quarantine_ids) == len(set(quarantine_ids))

for row in quarantine_rows:
    identity = [
        row["validation_contract_version"],
        row["bronze_event_id"],
        row["rejection_scope"],
        row["source_record_ordinal"],
    ]
    expected_id = hashlib.sha256(
        compact_json(identity).encode("utf-8")
    ).hexdigest()
    assert row["rejection_id"] == expected_id
    assert row["rejection_scope"] in ("record", "envelope")
    assert row["rejection_reasons"]
    assert row["rejection_reasons"] == sorted(set(row["rejection_reasons"]))
    if row["rejection_scope"] == "envelope":
        assert row["source_record_ordinal"] is None
    else:
        assert (
            type(row["source_record_ordinal"]) is int
            and row["source_record_ordinal"] >= 0
        )

# This initial publication had no independently observed source rejects.
# Non-empty persistence and identity behaviour were tested on fixtures.
assert not invalid_records and not envelope_defects
assert len(quarantine_rows) == 0, \
    "Unexpected live quarantine rows; review their source/contract."

verification_evidence = {
    "verified_at_utc": datetime.now(timezone.utc).isoformat(),
    "notebook": "04_delta_model_verification",
    "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
    "table_versions": table_versions,
    "source_run_id": RUN_ID,
    "publication_start": RANGE_START.isoformat(),
    "publication_end": RANGE_END.isoformat(),
    "bronze_reconstruction": reconstructed_counts,
    "coverage": coverage_summary,
    "published_national_rows": len(silver_rows),
    "published_quarantine_rows": len(quarantine_rows),
    "schema_checks_passed": True,
    "independent_raw_body_hash_checks_passed": True,
    "independent_request_window_checks_passed": True,
    "independent_grid_checks_passed": True,
    "all_field_values_and_provenance_reconciled": True,
    "quarantine_checks_passed": True,
    "persistent_writes": False,
    "evidence_boundary": {
        "this_notebook": "Fresh read-only persisted-data checks",
        "previously_reviewed": (
            "18 parser, 17 validation, 12 canonicalisation and 11 "
            "disposable Delta tests; live insert and 4320-row identical rerun"
        ),
        "not_claimed": (
            "Live forced revision/conflict/rejection events, scheduling, "
            "Gold, dashboard, repository publication or roadmap reconciliation"
        ),
    },
}

print("FINAL VERIFICATION EVIDENCE")
print(json.dumps(verification_evidence, indent=2))
print("PASS: independent persisted Delta verification.")