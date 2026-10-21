# Databricks notebook source
# CELL 01 — Python
import json
import hashlib
import platform
from collections import Counter, defaultdict
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

BRONZE_TABLE = "workspace.bronze.api_responses"
SILVER_SCHEMA = "workspace.silver"

BACKFILL_RUN_ID = "ddb2766af88740bc82eabb38bc557d69"
SAMPLE_RUN_ID = "cda8fe6de1504113b34554aefd438ed5"
REFETCH_RUN_ID = "1806ccddd894492cb384a211c276b471"

INCLUDE_SAMPLE_REFETCH = False

SELECTED_RUN_IDS = [BACKFILL_RUN_ID]
EXPECTED_EVENT_COUNTS = {BACKFILL_RUN_ID: 7}

if INCLUDE_SAMPLE_REFETCH:
    SELECTED_RUN_IDS += [SAMPLE_RUN_ID, REFETCH_RUN_ID]
    EXPECTED_EVENT_COUNTS.update({
        SAMPLE_RUN_ID: 1,
        REFETCH_RUN_ID: 1,
    })

PUBLICATION_START = datetime(2026, 7, 13, tzinfo=timezone.utc)
PUBLICATION_END = datetime(2026, 10, 11, tzinfo=timezone.utc)

# These are inspection safety limits, not production ingestion limits.
MAX_SELECTED_EVENTS = 20
MAX_TOTAL_BODY_BYTES = 32 * 1024 * 1024

assert PUBLICATION_START < PUBLICATION_END
assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

print(json.dumps({
    "python_version": platform.python_version(),
    "spark_version": spark.version,
    "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
    "bronze_table": BRONZE_TABLE,
    "selected_run_ids": SELECTED_RUN_IDS,
    "include_sample_refetch": INCLUDE_SAMPLE_REFETCH,
    "publication_start": PUBLICATION_START.isoformat(),
    "publication_end": PUBLICATION_END.isoformat(),
    "range_policy_proposed": "fully contained intervals within [start, end)",
}, indent=2))

# COMMAND ----------

# CELL 02 — Python
bronze = spark.table(BRONZE_TABLE)

print("ACTUAL BRONZE SCHEMA")
bronze.printSchema()

provider_rows = (
    spark.sql(f"DESCRIBE TABLE EXTENDED {BRONZE_TABLE}")
    .where(F.col("col_name") == "Provider")
    .select("data_type")
    .collect()
)
assert len(provider_rows) == 1, "Could not establish table provider."
assert provider_rows[0]["data_type"].lower() == "delta", \
    "Stop: Bronze is not a Delta table."

required_columns = {
    "event_id", "run_id", "request_id", "attempt_number",
    "source", "source_mode", "is_fixture", "event_kind",
    "request_from_utc", "request_to_utc", "retrieved_at_utc",
    "http_status", "raw_body", "raw_text", "response_sha256",
    "status", "failure_kind", "record_count",
}

missing = sorted(required_columns - set(bronze.columns))
assert not missing, f"Stop: required Bronze columns missing: {missing}"

actual_types = {
    field.name: field.dataType.simpleString()
    for field in bronze.schema.fields
}
assert actual_types["raw_body"] == "binary", \
    "Stop: raw_body is not binary."
assert actual_types["raw_text"] == "string", \
    "Stop: raw_text is not string."

for name in ("request_from_utc", "request_to_utc", "retrieved_at_utc"):
    assert actual_types[name] == "timestamp", \
        f"Stop: {name} is {actual_types[name]}; review timestamp semantics."

selected_events = bronze.where(F.col("run_id").isin(SELECTED_RUN_IDS))

observed_counts = {
    row["run_id"]: row["count"]
    for row in selected_events.groupBy("run_id").count().collect()
}
print("EXPECTED EVENT COUNTS:", EXPECTED_EVENT_COUNTS)
print("OBSERVED EVENT COUNTS:", observed_counts)
assert observed_counts == EXPECTED_EVENT_COUNTS, \
    "Stop: selected run evidence differs from the reviewed runbook."

event_count = selected_events.count()
assert event_count <= MAX_SELECTED_EVENTS, "Inspection event limit exceeded."

assert (
    selected_events.groupBy("event_id").count()
    .where(F.col("count") != 1).count() == 0
), "Stop: duplicate event_id values."

assert selected_events.where(
    F.col("event_id").isNull()
    | F.col("request_id").isNull()
    | F.col("run_id").isNull()
).count() == 0, "Stop: missing identity fields."

print("OUTCOME LABELS — copy the exact successful status into Cell 03")
display(
    selected_events.groupBy(
        "status", "http_status", "source_mode", "is_fixture", "event_kind"
    ).count()
)

print("SELECTED REQUEST EVENTS")
display(
    selected_events.select(
        "run_id", "event_id", "request_id", "attempt_number",
        "source", "source_mode", "is_fixture", "event_kind",
        "status", "http_status", "failure_kind",
        "request_from_utc", "request_to_utc", "retrieved_at_utc",
        "record_count", "response_sha256",
    ).orderBy("run_id", "request_from_utc", "event_id")
)

print("Cell 02 passed. No persistent data was changed.")

# COMMAND ----------

# CELL 03 — Python
SUCCESS_STATUS = "succeeded"  # Replace with the exact string shown in Cell 02.

assert isinstance(SUCCESS_STATUS, str) and SUCCESS_STATUS, \
    "Set SUCCESS_STATUS to the observed successful status from Cell 02."

eligible = selected_events.where(
    (F.col("status") == SUCCESS_STATUS)
    & (F.col("http_status") == 200)
    & (F.col("source_mode") == "api")
    & (F.col("is_fixture") == F.lit(False))
)

excluded = selected_events.join(
    eligible.select("event_id"), on="event_id", how="left_anti"
)
print("EXCLUDED ATTEMPTS")
display(
    excluded.select(
        "event_id", "status", "http_status", "source_mode",
        "is_fixture", "failure_kind"
    )
)

assert eligible.count() == event_count, \
    "Stop: reviewed selected runs contain unexpected excluded events."

assert eligible.where(
    F.col("raw_body").isNull()
    | F.col("raw_text").isNull()
    | F.col("retrieved_at_utc").isNull()
).count() == 0, "Stop: successful API evidence lacks body/text/retrieval time."

body_bytes = eligible.agg(
    F.sum(F.length("raw_body")).alias("total_bytes")
).first()["total_bytes"]

assert body_bytes is not None and body_bytes <= MAX_TOTAL_BODY_BYTES, \
    "Stop: body collection limit exceeded; use distributed inspection."

# Bounded collection of the already-reviewed small set of response events.
events = (
    eligible.select(
        "event_id", "run_id", "request_id",
        "request_from_utc", "request_to_utc", "retrieved_at_utc",
        "raw_body", "raw_text", "response_sha256", "record_count"
    )
    .orderBy("request_from_utc", "event_id")
    .collect()
)

def utc_value(value):
    # Spark timestamp collection may return naive Python datetimes.
    # The session has explicitly been set to UTC.
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)

def parse_source_time(value):
    if not isinstance(value, str):
        raise ValueError("timestamp_not_string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp_missing_offset")
    if parsed.utcoffset().total_seconds() != 0:
        raise ValueError("timestamp_not_utc")
    return parsed.astimezone(timezone.utc)

field_types = defaultdict(Counter)
interval_keys = Counter()
duration_seconds = Counter()
issues = Counter()
range_counts = Counter()
boundary_reports = []
samples = []
record_total = 0

for event in events:
    raw_bytes = bytes(event["raw_body"])
    assert hashlib.sha256(raw_bytes).hexdigest() == event["response_sha256"], \
        f"Body hash mismatch: {event['event_id']}"
    assert raw_bytes.decode("utf-8", errors="replace") == event["raw_text"], \
        f"Body/text mismatch: {event['event_id']}"

    envelope = json.loads(event["raw_text"])
    assert isinstance(envelope, dict), "Unexpected top-level envelope."
    data = envelope.get("data")
    assert isinstance(data, list), "Envelope data is not an array."
    assert event["record_count"] == len(data), \
        f"Bronze record_count mismatch: {event['event_id']}"

    request_start = utc_value(event["request_from_utc"])
    request_end = utc_value(event["request_to_utc"])
    assert request_start is not None and request_end is not None
    assert request_start < request_end

    boundaries = Counter()
    parsed_starts = []
    parsed_ends = []

    for ordinal, record in enumerate(data):
        record_total += 1
        if not isinstance(record, dict):
            issues["record_not_object"] += 1
            continue

        for field in ("from", "to"):
            field_types[field][
                "missing" if field not in record
                else type(record[field]).__name__
            ] += 1

        intensity = record.get("intensity")
        if not isinstance(intensity, dict):
            issues["intensity_not_object"] += 1
            continue

        for field in ("forecast", "actual", "index"):
            field_types[f"intensity.{field}"][
                "missing" if field not in intensity
                else type(intensity[field]).__name__
            ] += 1

        if len(samples) < 3:
            samples.append({
                "event_id": event["event_id"],
                "record_ordinal": ordinal,
                "record": record,
            })

        try:
            start = parse_source_time(record.get("from"))
            end = parse_source_time(record.get("to"))
        except (ValueError, TypeError, OverflowError) as exc:
            issues[f"timestamp_error:{exc}"] += 1
            continue

        if end <= start:
            issues["nonpositive_interval"] += 1
            continue

        parsed_starts.append(start)
        parsed_ends.append(end)
        interval_keys[(start.isoformat(), end.isoformat())] += 1
        duration_seconds[int((end - start).total_seconds())] += 1

        if start >= PUBLICATION_START and end <= PUBLICATION_END:
            range_counts["fully_inside_publication_range"] += 1
        elif end <= PUBLICATION_START or start >= PUBLICATION_END:
            range_counts["outside_publication_range"] += 1
        else:
            range_counts["crosses_publication_boundary"] += 1

        if start < request_start:
            boundaries["starts_before_request"] += 1
        if end == request_start:
            boundaries["ends_at_request_start"] += 1
        if start == request_start:
            boundaries["starts_at_request_start"] += 1
        if end == request_end:
            boundaries["ends_at_request_end"] += 1
        if start == request_end:
            boundaries["starts_at_request_end"] += 1
        if end > request_end:
            boundaries["ends_after_request"] += 1

    boundary_reports.append({
        "event_id": event["event_id"],
        "request_from_utc": request_start.isoformat(),
        "request_to_utc": request_end.isoformat(),
        "envelope_records": len(data),
        "minimum_parsed_start": (
            min(parsed_starts).isoformat() if parsed_starts else None
        ),
        "maximum_parsed_end": (
            max(parsed_ends).isoformat() if parsed_ends else None
        ),
        "boundary_observations": dict(boundaries),
    })

inspection_summary = {
    "selected_events": len(events),
    "total_envelope_records": record_total,
    "observed_field_types": {
        name: dict(counts) for name, counts in field_types.items()
    },
    "observed_duration_seconds": dict(duration_seconds),
    "diagnostic_issues": dict(issues),
    "range_diagnostics_for_positive_parseable_intervals": dict(range_counts),
    "distinct_parseable_positive_interval_keys": len(interval_keys),
    "keys_with_multiple_occurrences": sum(
        count > 1 for count in interval_keys.values()
    ),
    "extra_key_occurrences": sum(
        count - 1 for count in interval_keys.values()
    ),
}

print("INSPECTION SUMMARY")
print(json.dumps(inspection_summary, indent=2))
print("REQUEST BOUNDARIES")
print(json.dumps(boundary_reports, indent=2))
print("SMALL RAW RECORD SAMPLE")
print(json.dumps(samples, indent=2))

print(
    "Diagnostic counts are not validated candidates, "
    "deduplication winners or a completeness result."
)

# COMMAND ----------

# CELL 04 — Python
schema_rows = spark.sql("SHOW SCHEMAS IN workspace").collect()

# SHOW SCHEMAS returns the schema name as its first column.
visible_schemas = {str(row[0]) for row in schema_rows}
silver_visible = "silver" in visible_schemas

print("silver_schema_visible:", silver_visible)

if silver_visible:
    tables = spark.sql("SHOW TABLES IN workspace.silver")
    display(tables)

    visible_table_names = {
        row["tableName"] for row in tables.collect()
        if not row["isTemporary"]
    }

    for table_name in ("national_intensity", "quarantined_records"):
        if table_name in visible_table_names:
            full_name = f"workspace.silver.{table_name}"
            print(f"EXISTING DESTINATION: {full_name}")
            spark.table(full_name).printSchema()
            display(spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}"))
        else:
            print(f"No visible existing destination: {table_name}")
else:
    print(
        "Silver is not visible. This does not establish whether it is "
        "absent or inaccessible. Review access before enabling creation."
    )

print("Cell 04 made no persistent changes.")

# COMMAND ----------

# CELL 04B — Python
# Read-only schema diagnosis. No schema or table creation.
try:
    display(spark.sql("DESCRIBE SCHEMA EXTENDED workspace.silver"))
    print("Silver schema exists and its metadata is readable.")
except Exception as exc:
    if hasattr(exc, "getCondition"):
        condition = exc.getCondition()
    elif hasattr(exc, "getErrorClass"):
        condition = exc.getErrorClass()
    else:
        condition = None

    print("Schema diagnosis failed.")
    print("error_condition:", condition)
    print("error_message:", str(exc))
    print(
        "Do not enable schema creation until this error has been reviewed. "
        "A visibility failure alone does not prove the schema is absent."
    )

# COMMAND ----------

# CELL 05 — Python
ALLOW_DISPOSABLE_SILVER_PROBE = True
ALLOW_CREATE_SILVER_SCHEMA = True

if not ALLOW_DISPOSABLE_SILVER_PROBE:
    print("SKIPPED: disposable Silver write probe is not approved.")
else:
    if not silver_visible:
        if not ALLOW_CREATE_SILVER_SCHEMA:
            raise RuntimeError(
                "Silver is not visible. Review absence/access first. "
                "Schema creation has not been approved."
            )
        spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
        print("Silver schema creation requested; schema will be retained.")

    probe_name = f"_gridlens_delta_contract_probe_{uuid4().hex}"
    probe_table = f"workspace.silver.{probe_name}"
    probe_created = False

    print("Disposable probe table:", probe_table)

    try:
        # No IF NOT EXISTS: never adopt or overwrite an existing table.
        spark.sql(f"""
            CREATE TABLE {probe_table} (
                probe_id STRING,
                probe_value INT,
                source_retrieved_at_utc TIMESTAMP
            ) USING DELTA
        """)
        probe_created = True

        spark.sql(f"""
            INSERT INTO {probe_table}
            VALUES ('probe', 1, TIMESTAMP '2026-01-01 00:00:00')
        """)

        spark.sql(f"""
            MERGE INTO {probe_table} AS target
            USING (
                SELECT
                    'probe' AS probe_id,
                    2 AS probe_value,
                    TIMESTAMP '2026-01-02 00:00:00'
                        AS source_retrieved_at_utc
            ) AS source
            ON target.probe_id = source.probe_id
            WHEN MATCHED AND
                source.source_retrieved_at_utc >
                target.source_retrieved_at_utc
            THEN UPDATE SET
                target.probe_value = source.probe_value,
                target.source_retrieved_at_utc =
                    source.source_retrieved_at_utc
        """)

        rows = spark.table(probe_table).collect()
        assert len(rows) == 1 and rows[0]["probe_value"] == 2, \
            "Probe MERGE/read-back failed."

        provider = (
            spark.sql(f"DESCRIBE TABLE EXTENDED {probe_table}")
            .where(F.col("col_name") == "Provider")
            .select("data_type").first()["data_type"]
        )
        assert provider.lower() == "delta"

        print("PASS: managed Delta create/insert/MERGE/read.")
        display(spark.table(probe_table))

    finally:
        if probe_created:
            try:
                spark.sql(f"DROP TABLE {probe_table}")
                remaining = (
                    spark.sql("SHOW TABLES IN workspace.silver")
                    .where(F.col("tableName") == probe_name)
                    .count()
                )
                assert remaining == 0, "Probe table still visible."
                print("PASS: disposable table dropped; absence verified.")
            except Exception:
                print(
                    "CLEANUP FAILED. Review only this disposable table:",
                    probe_table
                )
                raise

# COMMAND ----------

# CELL 06 — Python
import re
from copy import deepcopy
from pyspark.sql import types as T

PARSER_VERSION = "gridlens_parse_v1"

PROVENANCE_FIELDS = [
    T.StructField("bronze_event_id", T.StringType(), False),
    T.StructField("bronze_run_id", T.StringType(), False),
    T.StructField("bronze_request_id", T.StringType(), False),
    T.StructField("source", T.StringType(), False),
    T.StructField("source_mode", T.StringType(), False),
    T.StructField("source_retrieved_at_utc", T.TimestampType(), False),
    T.StructField("response_sha256", T.StringType(), False),
]

CANDIDATE_SCHEMA = T.StructType(PROVENANCE_FIELDS + [
    T.StructField("source_record_ordinal", T.IntegerType(), False),
    T.StructField("interval_start_utc", T.TimestampType(), True),
    T.StructField("interval_end_utc", T.TimestampType(), True),
    T.StructField("forecast_intensity", T.IntegerType(), True),
    T.StructField("actual_intensity", T.IntegerType(), True),
    T.StructField("source_index", T.StringType(), True),
    T.StructField("raw_record_json", T.StringType(), False),
    T.StructField(
        "parse_errors", T.ArrayType(T.StringType(), False), False
    ),
    T.StructField("parser_version", T.StringType(), False),
])

ENVELOPE_DIAGNOSTIC_SCHEMA = T.StructType([
    T.StructField("bronze_event_id", T.StringType(), True),
    T.StructField("bronze_run_id", T.StringType(), True),
    T.StructField("bronze_request_id", T.StringType(), True),
    T.StructField("failure_scope", T.StringType(), False),
    T.StructField(
        "parse_errors", T.ArrayType(T.StringType(), False), False
    ),
    T.StructField("parser_version", T.StringType(), False),
])

print(CANDIDATE_SCHEMA.simpleString())
print(ENVELOPE_DIAGNOSTIC_SCHEMA.simpleString())

# COMMAND ----------

# CELL 07 — Python
UTC_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}"
    r"(?::\d{2}(?:\.\d{1,6})?)?(?:Z|\+00:00)$"
)

def canonical_json(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False
    )

def reject_constant(value):
    raise ValueError(f"nonstandard_json_constant:{value}")

def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate_json_key:{key}")
        result[key] = value
    return result

def strict_json_loads(text):
    return json.loads(
        text,
        parse_constant=reject_constant,
        object_pairs_hook=unique_object,
    )

def typed_utc(value):
    if not isinstance(value, str) or not UTC_PATTERN.fullmatch(value):
        raise ValueError("expected_explicit_utc_timestamp")
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)

def parse_record(record, provenance, ordinal):
    errors = []

    output = {
        **provenance,
        "source_record_ordinal": ordinal,
        "interval_start_utc": None,
        "interval_end_utc": None,
        "forecast_intensity": None,
        "actual_intensity": None,
        "source_index": None,
        "raw_record_json": canonical_json(record),
        "parse_errors": errors,
        "parser_version": PARSER_VERSION,
    }

    if not isinstance(record, dict):
        errors.append("record:not_object")
        return output

    for raw_name, output_name in (
        ("from", "interval_start_utc"),
        ("to", "interval_end_utc"),
    ):
        if raw_name not in record:
            errors.append(f"{raw_name}:missing")
        else:
            try:
                output[output_name] = typed_utc(record[raw_name])
            except (ValueError, TypeError, OverflowError):
                errors.append(f"{raw_name}:invalid_utc_timestamp")

    intensity = record.get("intensity")
    if not isinstance(intensity, dict):
        errors.append(
            "intensity:missing" if "intensity" not in record
            else "intensity:not_object"
        )
    else:
        for name, output_name, nullable in (
            ("forecast", "forecast_intensity", False),
            ("actual", "actual_intensity", True),
        ):
            if name not in intensity:
                errors.append(f"intensity.{name}:missing")
                continue

            value = intensity[name]
            if value is None and nullable:
                # Explicit JSON null actual is legitimate.
                continue

            # bool is a subclass of int in Python: reject it explicitly.
            if type(value) is not int:
                errors.append(f"intensity.{name}:not_integer")
            elif not -(2**31) <= value <= 2**31 - 1:
                errors.append(f"intensity.{name}:outside_int32")
            else:
                output[output_name] = value

        if "index" not in intensity:
            errors.append("intensity.index:missing")
        elif not isinstance(intensity["index"], str):
            errors.append("intensity.index:not_string")
        else:
            output["source_index"] = intensity["index"]

    output["parse_errors"] = sorted(set(errors))
    return output

def parse_event(event):
    """
    Returns (candidate_rows, envelope_diagnostics, exclusion).
    Failed/excluded events produce no invented interval.
    """
    event = dict(event)
    exclusions = []

    if event.get("status") != "succeeded":
        exclusions.append("status_not_succeeded")
    if event.get("http_status") != 200:
        exclusions.append("http_status_not_200")
    if event.get("source_mode") != "api":
        exclusions.append("source_mode_not_api")
    if event.get("is_fixture") is not False:
        exclusions.append("not_explicitly_nonfixture")
    if event.get("event_kind") != "response":
        exclusions.append("event_kind_not_response")

    if exclusions:
        return [], [], {
            "event_id": event.get("event_id"),
            "reasons": sorted(exclusions),
        }

    def envelope_failure(reason):
        return [], [{
            "bronze_event_id": event.get("event_id"),
            "bronze_run_id": event.get("run_id"),
            "bronze_request_id": event.get("request_id"),
            "failure_scope": "envelope",
            "parse_errors": [reason],
            "parser_version": PARSER_VERSION,
        }], None

    for name in (
        "event_id", "run_id", "request_id", "source", "response_sha256"
    ):
        if not isinstance(event.get(name), str) or not event[name]:
            return envelope_failure(f"provenance:{name}:missing_or_invalid")

    retrieved = event.get("retrieved_at_utc")
    if not isinstance(retrieved, datetime):
        return envelope_failure("provenance:retrieval_timestamp_invalid")
    retrieved = utc_value(retrieved)

    raw_body = event.get("raw_body")
    raw_text = event.get("raw_text")

    if not isinstance(raw_body, (bytes, bytearray, memoryview)):
        return envelope_failure("body:missing_or_invalid_binary")
    if not isinstance(raw_text, str):
        return envelope_failure("body:missing_or_invalid_text")

    raw_bytes = bytes(raw_body)
    if hashlib.sha256(raw_bytes).hexdigest() != event["response_sha256"]:
        return envelope_failure("body:sha256_mismatch")
    if raw_bytes.decode("utf-8", errors="replace") != raw_text:
        return envelope_failure("body:text_mismatch")

    try:
        envelope = strict_json_loads(raw_text)
    except (ValueError, TypeError, OverflowError, RecursionError):
        return envelope_failure("envelope:invalid_or_ambiguous_json")

    if not isinstance(envelope, dict):
        return envelope_failure("envelope:not_object")
    if "data" not in envelope:
        return envelope_failure("envelope:data_missing")
    if not isinstance(envelope["data"], list):
        return envelope_failure("envelope:data_not_array")

    records = envelope["data"]
    if not records:
        return envelope_failure("envelope:data_empty")

    if (
        type(event.get("record_count")) is not int
        or event["record_count"] != len(records)
    ):
        return envelope_failure("envelope:record_count_mismatch")

    provenance = {
        "bronze_event_id": event["event_id"],
        "bronze_run_id": event["run_id"],
        "bronze_request_id": event["request_id"],
        "source": event["source"],
        "source_mode": event["source_mode"],
        "source_retrieved_at_utc": retrieved,
        "response_sha256": event["response_sha256"],
    }

    candidates = [
        parse_record(record, provenance, ordinal)
        for ordinal, record in enumerate(records)
    ]
    return candidates, [], None

print("Parser definitions loaded. No live data has been written.")

# COMMAND ----------

# CELL 08 — Python
def fixture_event(payload=None, text=None):
    if text is None:
        text = canonical_json(payload)
    body = text.encode("utf-8")
    return {
        "event_id": "fixture_event",
        "run_id": "fixture_run",
        "request_id": "fixture_request",
        "source": "synthetic_in_memory",
        "source_mode": "api",
        "is_fixture": False,
        "event_kind": "response",
        "status": "succeeded",
        "http_status": 200,
        "retrieved_at_utc": datetime(
            2026, 10, 12, 20, 0, tzinfo=timezone.utc
        ),
        "raw_body": body,
        "raw_text": text,
        "response_sha256": hashlib.sha256(body).hexdigest(),
        "record_count": (
            len(payload["data"])
            if isinstance(payload, dict)
            and isinstance(payload.get("data"), list)
            else 0
        ),
    }

good_record = {
    "from": "2026-07-13T00:00Z",
    "to": "2026-07-13T00:30Z",
    "intensity": {
        "forecast": 111,
        "actual": 114,
        "index": "moderate",
    },
}

passed_tests = []

rows, diagnostics, excluded = parse_event(
    fixture_event({"data": [good_record]})
)
assert len(rows) == 1 and not diagnostics and excluded is None
assert rows[0]["parse_errors"] == []
assert rows[0]["forecast_intensity"] == 111
assert rows[0]["actual_intensity"] == 114
assert rows[0]["interval_start_utc"].tzinfo is not None
assert rows[0]["bronze_request_id"] == "fixture_request"
assert rows[0]["source_record_ordinal"] == 0
passed_tests.append("valid_values_utc_and_provenance")

record = deepcopy(good_record)
record["intensity"]["actual"] = None
rows, _, _ = parse_event(fixture_event({"data": [record]}))
assert rows[0]["actual_intensity"] is None
assert rows[0]["parse_errors"] == []
passed_tests.append("explicit_null_actual")

for name, value, expected_error in [
    ("actual", "114", "intensity.actual:not_integer"),
    ("forecast", True, "intensity.forecast:not_integer"),
    ("forecast", 111.0, "intensity.forecast:not_integer"),
    ("forecast", 2**31, "intensity.forecast:outside_int32"),
]:
    record = deepcopy(good_record)
    record["intensity"][name] = value
    rows, _, _ = parse_event(fixture_event({"data": [record]}))
    assert expected_error in rows[0]["parse_errors"]
    assert json.loads(rows[0]["raw_record_json"])["intensity"][name] == value
    passed_tests.append(expected_error)

record = deepcopy(good_record)
del record["intensity"]["actual"]
rows, _, _ = parse_event(fixture_event({"data": [record]}))
assert "intensity.actual:missing" in rows[0]["parse_errors"]
passed_tests.append("missing_actual_not_valid_null")

record = deepcopy(good_record)
record["from"] = "2026-07-13T00:00"
rows, _, _ = parse_event(fixture_event({"data": [record]}))
assert "from:invalid_utc_timestamp" in rows[0]["parse_errors"]
passed_tests.append("naive_source_timestamp_rejected")

rows, _, _ = parse_event(fixture_event({"data": [None, good_record]}))
assert len(rows) == 2
assert rows[0]["parse_errors"] == ["record:not_object"]
assert rows[1]["source_record_ordinal"] == 1
passed_tests.append("malformed_record_position_retained")

for label, event, expected_error in [
    (
        "malformed_json",
        fixture_event(text='{"data":['),
        "envelope:invalid_or_ambiguous_json",
    ),
    (
        "duplicate_json_key",
        fixture_event(text='{"data":[],"data":[]}'),
        "envelope:invalid_or_ambiguous_json",
    ),
    (
        "missing_data",
        fixture_event({}),
        "envelope:data_missing",
    ),
    (
        "wrong_data_type",
        fixture_event({"data": {}}),
        "envelope:data_not_array",
    ),
]:
    rows, diagnostics, excluded = parse_event(event)
    assert not rows and excluded is None
    assert diagnostics[0]["parse_errors"] == [expected_error]
    passed_tests.append(label)

event = fixture_event({"data": [good_record]})
event.update(status="failed", http_status=None, raw_body=None, raw_text=None)
rows, diagnostics, excluded = parse_event(event)
assert not rows and not diagnostics and excluded is not None
passed_tests.append("failed_no_body_attempt_excluded")

event = fixture_event({"data": [good_record]})
event.update(source_mode="snapshot", retrieved_at_utc=None)
rows, diagnostics, excluded = parse_event(event)
assert not rows and excluded is not None
passed_tests.append("snapshot_unknown_acquisition_excluded")

event = fixture_event({"data": [good_record]})
event["is_fixture"] = True
rows, diagnostics, excluded = parse_event(event)
assert not rows and excluded is not None
passed_tests.append("fixture_event_excluded")

event = fixture_event({"data": [good_record]})
event["raw_body"] = b"changed"
rows, diagnostics, _ = parse_event(event)
assert not rows
assert diagnostics[0]["parse_errors"] == ["body:sha256_mismatch"]
passed_tests.append("body_integrity_failure")

# Check actual Spark types, including nullable actual.
null_record = deepcopy(good_record)
null_record["intensity"]["actual"] = None
rows, _, _ = parse_event(
    fixture_event({"data": [good_record, null_record]})
)
fixture_candidates = spark.createDataFrame(rows, CANDIDATE_SCHEMA)
assert fixture_candidates.schema == CANDIDATE_SCHEMA
assert fixture_candidates.count() == 2
assert fixture_candidates.where("actual_intensity IS NULL").count() == 1
passed_tests.append("explicit_spark_schema_and_null_actual")

assert len(passed_tests) == 18
print(f"PASS: {len(passed_tests)} controlled parser tests.")
print(json.dumps(passed_tests, indent=2))
fixture_candidates.printSchema()
display(fixture_candidates)

# COMMAND ----------

# CELL 09 — Python
assert SUCCESS_STATUS == "succeeded"
assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
assert len(passed_tests) == 18

live_events_df = spark.table(BRONZE_TABLE).where(
    F.col("run_id").isin(SELECTED_RUN_IDS)
)

counts = {
    row["run_id"]: row["count"]
    for row in live_events_df.groupBy("run_id").count().collect()
}
assert counts == EXPECTED_EVENT_COUNTS, \
    "Selected evidence changed; stop for review."

assert live_events_df.count() <= MAX_SELECTED_EVENTS
assert (
    live_events_df.groupBy("event_id").count()
    .where(F.col("count") != 1).count() == 0
), "Duplicate event identities."

total_bytes = live_events_df.agg(
    F.sum(F.length("raw_body")).alias("body_bytes")
).first()["body_bytes"] or 0
assert total_bytes <= MAX_TOTAL_BODY_BYTES, \
    "Inspection collection limit exceeded."

event_columns = [
    "event_id", "run_id", "request_id", "source", "source_mode",
    "is_fixture", "event_kind", "status", "http_status",
    "retrieved_at_utc", "raw_body", "raw_text",
    "response_sha256", "record_count",
]

live_candidate_rows = []
live_envelope_diagnostics = []
live_exclusions = []
event_reconciliation = []

for row in live_events_df.select(*event_columns).collect():
    event = row.asDict()
    candidates, diagnostics, exclusion = parse_event(event)

    live_candidate_rows.extend(candidates)
    live_envelope_diagnostics.extend(diagnostics)
    if exclusion is not None:
        live_exclusions.append(exclusion)

    if candidates:
        assert len(candidates) == event["record_count"]

    event_reconciliation.append({
        "event_id": event["event_id"],
        "declared_record_count": event["record_count"],
        "emitted_candidate_count": len(candidates),
        "envelope_diagnostic_count": len(diagnostics),
        "excluded": exclusion is not None,
    })

parsed_candidates = spark.createDataFrame(
    live_candidate_rows, CANDIDATE_SCHEMA
)
envelope_diagnostics = spark.createDataFrame(
    live_envelope_diagnostics, ENVELOPE_DIAGNOSTIC_SCHEMA
)

# Deterministic intermediate parsing under the same contract.
repeat_candidates = []
repeat_diagnostics = []
repeat_exclusions = []
for row in live_events_df.select(*event_columns).collect():
    candidates, diagnostics, exclusion = parse_event(row.asDict())
    repeat_candidates.extend(candidates)
    repeat_diagnostics.extend(diagnostics)
    if exclusion is not None:
        repeat_exclusions.append(exclusion)

def stable_rows(rows):
    return sorted(
        rows,
        key=lambda row: (
            row["bronze_event_id"],
            row["source_record_ordinal"],
        ),
    )

assert stable_rows(repeat_candidates) == stable_rows(live_candidate_rows)
assert sorted(
    repeat_diagnostics, key=lambda row: row["bronze_event_id"]
) == sorted(
    live_envelope_diagnostics, key=lambda row: row["bronze_event_id"]
)
assert sorted(
    repeat_exclusions, key=lambda row: row["event_id"]
) == sorted(
    live_exclusions, key=lambda row: row["event_id"]
)

parse_error_count = parsed_candidates.where(
    F.size("parse_errors") > 0
).count()

summary = {
    "parser_version": PARSER_VERSION,
    "selected_event_count": len(event_reconciliation),
    "excluded_event_count": len(live_exclusions),
    "envelope_diagnostic_count": len(live_envelope_diagnostics),
    "typed_candidate_count": parsed_candidates.count(),
    "candidates_with_parse_errors": parse_error_count,
    "parse_clean_candidates": (
        parsed_candidates.count() - parse_error_count
    ),
    "parse_clean_null_actual_candidates": parsed_candidates.where(
        (F.size("parse_errors") == 0)
        & F.col("actual_intensity").isNull()
    ).count(),
    "deterministic_reparse_passed": True,
    "persistent_writes": False,
}

print("LIVE PARSING SUMMARY")
print(json.dumps(summary, indent=2))
print("PER-EVENT RECONCILIATION")
print(json.dumps(
    sorted(event_reconciliation, key=lambda row: row["event_id"]),
    indent=2
))
print("EXCLUDED EVENTS")
print(json.dumps(live_exclusions, indent=2))

print("ACTUAL CANDIDATE SCHEMA")
parsed_candidates.printSchema()

print("ENVELOPE DIAGNOSTICS")
display(envelope_diagnostics)

print("RECORD PARSING ERRORS")
display(
    parsed_candidates.where(F.size("parse_errors") > 0)
    .select(
        "bronze_event_id", "source_record_ordinal",
        "raw_record_json", "parse_errors"
    ).limit(20)
)

print("SMALL TYPED SAMPLE")
display(
    parsed_candidates.orderBy(
        "interval_start_utc", "bronze_event_id", "source_record_ordinal"
    ).limit(5)
)

print(
    "No validation, range exclusion, deduplication, quarantine persistence "
    "or Silver publication has occurred."
)

# COMMAND ----------

# CELL 10 — Python
VALIDATION_CONTRACT_VERSION = "gridlens_validation_v1+gridlens_parse_v1"
VALIDATION_RUN_ID = uuid4().hex
VALIDATION_STARTED_AT = datetime.now(timezone.utc)

CLASSIFIED_SCHEMA = T.StructType(list(CANDIDATE_SCHEMA.fields) + [
    T.StructField("classification", T.StringType(), False),
    T.StructField(
        "validation_errors", T.ArrayType(T.StringType(), False), False
    ),
    T.StructField("rejection_id", T.StringType(), True),
    T.StructField("validation_contract_version", T.StringType(), False),
])

QUARANTINE_SCHEMA = T.StructType([
    T.StructField("rejection_id", T.StringType(), False),
    T.StructField("rejection_scope", T.StringType(), False),
    T.StructField("bronze_event_id", T.StringType(), False),
    T.StructField("bronze_run_id", T.StringType(), True),
    T.StructField("bronze_request_id", T.StringType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("source_mode", T.StringType(), True),
    T.StructField("source_retrieved_at_utc", T.TimestampType(), True),
    T.StructField("response_sha256", T.StringType(), True),
    T.StructField("source_record_ordinal", T.IntegerType(), True),
    T.StructField("raw_record_json", T.StringType(), True),
    T.StructField(
        "rejection_reasons", T.ArrayType(T.StringType(), False), False
    ),
    T.StructField("parser_version", T.StringType(), False),
    T.StructField("validation_contract_version", T.StringType(), False),
    T.StructField("processing_run_id", T.StringType(), False),
    T.StructField("processed_at_utc", T.TimestampType(), False),
])

print("Validation contract:", VALIDATION_CONTRACT_VERSION)
print("Processing run:", VALIDATION_RUN_ID)

# COMMAND ----------

# CELL 11 — Python
def rejection_identity(event_id, scope, ordinal):
    if not isinstance(event_id, str) or not event_id:
        raise ValueError(
            "Missing Bronze event identity; cannot safely identify rejection."
        )
    identity = [
        VALIDATION_CONTRACT_VERSION, event_id, scope, ordinal
    ]
    return hashlib.sha256(
        canonical_json(identity).encode("utf-8")
    ).hexdigest()

def validate_candidate(
    candidate,
    publication_start=PUBLICATION_START,
    publication_end=PUBLICATION_END,
):
    candidate = dict(candidate)
    errors = list(candidate["parse_errors"])

    start = candidate["interval_start_utc"]
    end = candidate["interval_end_utc"]

    for name, value in (
        ("interval_start_utc", start),
        ("interval_end_utc", end),
    ):
        if not isinstance(value, datetime):
            errors.append(f"{name}:missing_or_invalid")
        elif (
            value.tzinfo is None
            or value.utcoffset().total_seconds() != 0
        ):
            errors.append(f"{name}:not_explicit_utc")
        elif (
            value.minute % 30 != 0
            or value.second != 0
            or value.microsecond != 0
        ):
            errors.append(f"{name}:not_half_hour_aligned")

    times_usable = all(
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset().total_seconds() == 0
        for value in (start, end)
    )

    if times_usable:
        duration = (end - start).total_seconds()
        if duration <= 0:
            errors.append("interval:nonpositive_duration")
        if duration != 1800:
            errors.append("interval:duration_not_1800_seconds")

    for name, nullable in (
        ("forecast_intensity", False),
        ("actual_intensity", True),
    ):
        value = candidate[name]
        if value is None and nullable:
            # Parse errors still distinguish missing/bad actual from valid null.
            continue
        if type(value) is not int:
            errors.append(f"{name}:missing_or_invalid_integer")
        elif not -(2**31) <= value <= 2**31 - 1:
            errors.append(f"{name}:outside_int32")
        elif value < 0:
            errors.append(f"{name}:negative")

    index = candidate["source_index"]
    if not isinstance(index, str) or not index.strip():
        errors.append("source_index:missing_or_empty")

    errors = sorted(set(errors))

    # Invalid takes precedence over range classification.
    if errors:
        classification = "invalid"
    elif start >= publication_start and end <= publication_end:
        classification = "valid_in_range"
    else:
        classification = "out_of_range"

    rejection_id = (
        rejection_identity(
            candidate["bronze_event_id"],
            "record",
            candidate["source_record_ordinal"],
        )
        if classification == "invalid"
        else None
    )

    return {
        **candidate,
        "classification": classification,
        "validation_errors": errors,
        "rejection_id": rejection_id,
        "validation_contract_version": VALIDATION_CONTRACT_VERSION,
    }

def quarantine_record(classified):
    assert classified["classification"] == "invalid"
    return {
        **{
            name: classified[name]
            for name in (
                "bronze_event_id", "bronze_run_id", "bronze_request_id",
                "source", "source_mode", "source_retrieved_at_utc",
                "response_sha256", "source_record_ordinal",
                "raw_record_json", "parser_version",
            )
        },
        "rejection_id": classified["rejection_id"],
        "rejection_scope": "record",
        "rejection_reasons": classified["validation_errors"],
        "validation_contract_version": VALIDATION_CONTRACT_VERSION,
        "processing_run_id": VALIDATION_RUN_ID,
        "processed_at_utc": VALIDATION_STARTED_AT,
    }

def quarantine_envelope(diagnostic, original_event):
    event = dict(original_event)
    event_id = diagnostic["bronze_event_id"]
    assert event.get("event_id") == event_id

    retrieved = event.get("retrieved_at_utc")
    retrieved = (
        utc_value(retrieved) if isinstance(retrieved, datetime) else None
    )

    return {
        "rejection_id": rejection_identity(event_id, "envelope", None),
        "rejection_scope": "envelope",
        "bronze_event_id": event_id,
        "bronze_run_id": event.get("run_id"),
        "bronze_request_id": event.get("request_id"),
        "source": event.get("source"),
        "source_mode": event.get("source_mode"),
        "source_retrieved_at_utc": retrieved,
        "response_sha256": event.get("response_sha256"),
        "source_record_ordinal": None,
        # For envelope defects this may deliberately be malformed JSON.
        "raw_record_json": event.get("raw_text"),
        "rejection_reasons": sorted(set(diagnostic["parse_errors"])),
        "parser_version": diagnostic["parser_version"],
        "validation_contract_version": VALIDATION_CONTRACT_VERSION,
        "processing_run_id": VALIDATION_RUN_ID,
        "processed_at_utc": VALIDATION_STARTED_AT,
    }

print("Validation definitions loaded. No persistent writes.")

# COMMAND ----------

# CELL 12 — Python
validation_tests = []

def classify_fixture(record, **range_options):
    event = fixture_event({"data": [record]})
    candidates, diagnostics, exclusion = parse_event(event)
    assert not diagnostics and exclusion is None
    assert len(candidates) == 1
    return validate_candidate(candidates[0], **range_options)

def changed_record(section, name, value):
    record = deepcopy(good_record)
    target = record if section == "record" else record["intensity"]
    target[name] = value
    return record

def missing_record(section, name):
    record = deepcopy(good_record)
    target = record if section == "record" else record["intensity"]
    del target[name]
    return record

cases = [
    ("valid", deepcopy(good_record), "valid_in_range", None),
    (
        "null_actual",
        changed_record("intensity", "actual", None),
        "valid_in_range", None,
    ),
    (
        "negative_forecast",
        changed_record("intensity", "forecast", -1),
        "invalid", "forecast_intensity:negative",
    ),
    (
        "negative_actual",
        changed_record("intensity", "actual", -1),
        "invalid", "actual_intensity:negative",
    ),
    (
        "numeric_string",
        changed_record("intensity", "actual", "114"),
        "invalid", "intensity.actual:not_integer",
    ),
    (
        "missing_actual",
        missing_record("intensity", "actual"),
        "invalid", "intensity.actual:missing",
    ),
    (
        "invalid_timestamp",
        changed_record("record", "from", "not-a-time"),
        "invalid", "from:invalid_utc_timestamp",
    ),
    (
        "reversed_bounds",
        changed_record("record", "to", "2026-07-12T23:30Z"),
        "invalid", "interval:nonpositive_duration",
    ),
    (
        "wrong_duration",
        changed_record("record", "to", "2026-07-13T01:00Z"),
        "invalid", "interval:duration_not_1800_seconds",
    ),
    (
        "misaligned_grid",
        {
            **deepcopy(good_record),
            "from": "2026-07-13T00:15Z",
            "to": "2026-07-13T00:45Z",
        },
        "invalid", "interval_start_utc:not_half_hour_aligned",
    ),
    (
        "missing_index",
        missing_record("intensity", "index"),
        "invalid", "intensity.index:missing",
    ),
    (
        "empty_index",
        changed_record("intensity", "index", ""),
        "invalid", "source_index:missing_or_empty",
    ),
    (
        "outside_range",
        {
            **deepcopy(good_record),
            "from": "2026-07-12T23:30Z",
            "to": "2026-07-13T00:00Z",
        },
        "out_of_range", None,
    ),
]

fixture_classified_rows = []
fixture_quarantine_rows = []

for label, record, expected_class, expected_error in cases:
    classified = classify_fixture(record)
    assert classified["classification"] == expected_class, label
    if expected_error:
        assert expected_error in classified["validation_errors"], label
    else:
        assert classified["validation_errors"] == [], label

    if label == "null_actual":
        assert classified["actual_intensity"] is None

    # These are independent events, not conflicting reuse of one fixture ID.
    classified["bronze_event_id"] = f"fixture_{label}"
    if expected_class == "invalid":
        classified["rejection_id"] = rejection_identity(
            classified["bronze_event_id"], "record",
            classified["source_record_ordinal"]
        )
        fixture_quarantine_rows.append(quarantine_record(classified))
    else:
        assert classified["rejection_id"] is None

    fixture_classified_rows.append(classified)
    validation_tests.append(label)

# An aligned valid interval crossing a deliberately narrower test range.
crossing = classify_fixture(
    good_record,
    publication_start=datetime(
        2026, 7, 13, 0, 15, tzinfo=timezone.utc
    ),
)
assert crossing["classification"] == "out_of_range"
assert crossing["validation_errors"] == []
validation_tests.append("crosses_range_not_clipped")

malformed_event = fixture_event(text='{"data":[')
rows, diagnostics, exclusion = parse_event(malformed_event)
assert not rows and len(diagnostics) == 1 and exclusion is None
envelope_reject = quarantine_envelope(diagnostics[0], malformed_event)
assert envelope_reject["rejection_scope"] == "envelope"
assert envelope_reject["source_record_ordinal"] is None
assert envelope_reject["raw_record_json"] == '{"data":['
fixture_quarantine_rows.append(envelope_reject)
validation_tests.append("envelope_rejection_separate")

# Repeat processing: same rejection identity and reasons.
bad_record = changed_record("intensity", "forecast", -1)
first = classify_fixture(bad_record)
second = classify_fixture(bad_record)
assert first["rejection_id"] == second["rejection_id"]
assert first["validation_errors"] == second["validation_errors"]
assert first["rejection_id"] != envelope_reject["rejection_id"]
validation_tests.append("stable_rejection_identity")

assert (
    rejection_identity("fixture_event", "record", 0)
    != rejection_identity("fixture_event", "record", 1)
)
validation_tests.append("distinct_record_positions")

fixture_classified = spark.createDataFrame(
    fixture_classified_rows, CLASSIFIED_SCHEMA
)
fixture_quarantine = spark.createDataFrame(
    fixture_quarantine_rows, QUARANTINE_SCHEMA
)

assert fixture_classified.schema == CLASSIFIED_SCHEMA
assert fixture_quarantine.schema == QUARANTINE_SCHEMA
assert fixture_quarantine.count() == 11
assert (
    fixture_quarantine.select("rejection_id").distinct().count()
    == fixture_quarantine.count()
)

assert len(validation_tests) == 17
print(f"PASS: {len(validation_tests)} validation/quarantine tests.")
print(json.dumps(validation_tests, indent=2))

display(
    fixture_classified.select(
        "bronze_event_id", "classification",
        "actual_intensity", "validation_errors", "rejection_id"
    )
)
display(
    fixture_quarantine.select(
        "rejection_id", "rejection_scope", "bronze_event_id",
        "source_record_ordinal", "rejection_reasons"
    )
)

# COMMAND ----------

# CELL 13 — Python
assert len(validation_tests) == 17

classified_rows = [
    validate_candidate(row) for row in live_candidate_rows
]

record_quarantine_rows = [
    quarantine_record(row)
    for row in classified_rows
    if row["classification"] == "invalid"
]

# Read selected event metadata/raw text for envelope provenance.
original_events = {}
for row in live_events_df.select(*event_columns).collect():
    event = row.asDict()
    assert event["event_id"] not in original_events
    original_events[event["event_id"]] = event

envelope_quarantine_rows = [
    quarantine_envelope(
        diagnostic,
        original_events[diagnostic["bronze_event_id"]],
    )
    for diagnostic in live_envelope_diagnostics
]

quarantine_rows = record_quarantine_rows + envelope_quarantine_rows

classified_candidates = spark.createDataFrame(
    classified_rows, CLASSIFIED_SCHEMA
)
quarantine_candidates = spark.createDataFrame(
    quarantine_rows, QUARANTINE_SCHEMA
)

valid_in_range = classified_candidates.where(
    F.col("classification") == "valid_in_range"
)
out_of_range = classified_candidates.where(
    F.col("classification") == "out_of_range"
)
invalid_records = classified_candidates.where(
    F.col("classification") == "invalid"
)

classification_counts = {
    row["classification"]: row["count"]
    for row in classified_candidates.groupBy("classification").count().collect()
}

assert sum(classification_counts.values()) == len(live_candidate_rows)
assert classified_candidates.count() == len(live_candidate_rows)

assert (
    classified_candidates.groupBy(
        "bronze_event_id", "source_record_ordinal"
    ).count().where(F.col("count") != 1).count() == 0
), "Duplicate source record identities."

assert quarantine_candidates.count() == (
    invalid_records.count() + len(live_envelope_diagnostics)
)
assert (
    quarantine_candidates.select("rejection_id").distinct().count()
    == quarantine_candidates.count()
), "Duplicate rejection identities."

# Independently repeat validation of the same parsed inputs.
repeat_classified_rows = [
    validate_candidate(row) for row in live_candidate_rows
]
assert stable_rows(classified_rows) == stable_rows(repeat_classified_rows)

repeat_quarantine_ids = sorted(
    [
        row["rejection_id"]
        for row in repeat_classified_rows
        if row["classification"] == "invalid"
    ] + [
        rejection_identity(
            diagnostic["bronze_event_id"], "envelope", None
        )
        for diagnostic in live_envelope_diagnostics
    ]
)
assert repeat_quarantine_ids == sorted(
    row["rejection_id"] for row in quarantine_rows
)

validation_summary = {
    "validation_contract_version": VALIDATION_CONTRACT_VERSION,
    "typed_candidate_count": len(live_candidate_rows),
    "invalid_record_count": classification_counts.get("invalid", 0),
    "valid_out_of_range_count": classification_counts.get("out_of_range", 0),
    "valid_in_range_count": classification_counts.get("valid_in_range", 0),
    "envelope_rejection_count": len(envelope_quarantine_rows),
    "quarantine_candidate_count": len(quarantine_rows),
    "valid_in_range_null_actual_count": valid_in_range.where(
        F.col("actual_intensity").isNull()
    ).count(),
    "classification_reconciliation_passed": True,
    "deterministic_validation_passed": True,
    "stable_rejection_identities_passed": True,
    "persistent_writes": False,
}

print("LIVE VALIDATION SUMMARY")
print(json.dumps(validation_summary, indent=2))

print("INVALID RECORDS")
display(
    invalid_records.select(
        "bronze_event_id", "source_record_ordinal",
        "raw_record_json", "validation_errors", "rejection_id"
    ).limit(20)
)

print("VALID OUT-OF-RANGE RECORDS")
display(
    out_of_range.select(
        "bronze_event_id", "source_record_ordinal",
        "interval_start_utc", "interval_end_utc"
    ).limit(20)
)

print("QUARANTINE CANDIDATES")
quarantine_candidates.printSchema()
display(quarantine_candidates.limit(20))

print(
    "No persistent writes. Valid in-range rows still include overlaps; "
    "winner selection and independent coverage checks remain pending."
)

# COMMAND ----------

# CELL 14 — Python
CANONICALISATION_VERSION = "gridlens_canonical_v1"

AUDIT_SCHEMA = T.StructType(list(CLASSIFIED_SCHEMA.fields) + [
    T.StructField("record_value_sha256", T.StringType(), True),
    T.StructField("canonical_outcome", T.StringType(), False),
    T.StructField("canonical_reason", T.StringType(), False),
    T.StructField("values_differ_from_winner", T.BooleanType(), True),
    T.StructField("canonicalisation_version", T.StringType(), False),
])

def business_key(row):
    return (
        utc_value(row["interval_start_utc"]),
        utc_value(row["interval_end_utc"]),
    )

def business_signature(row):
    start, end = business_key(row)
    return canonical_json([
        start.isoformat(),
        end.isoformat(),
        row["forecast_intensity"],
        row["actual_intensity"],
        row["source_index"],
    ])

def canonicalise(input_rows):
    """
    Returns:
      audit_rows: one outcome for every input occurrence
      conflicts: one diagnostic per blocked key

    Pure, bounded in-memory selection; no table writes.
    """
    audit_rows = []
    eligible_groups = defaultdict(list)
    seen_identities = {}

    for original in input_rows:
        row = dict(original)
        assert row["validation_contract_version"] == \
            VALIDATION_CONTRACT_VERSION

        identity = (
            row["bronze_event_id"], row["source_record_ordinal"]
        )
        if identity in seen_identities:
            assert seen_identities[identity] == row, \
                "Same source identity has inconsistent content; stop."
        else:
            seen_identities[identity] = row

        audit = {
            **row,
            "record_value_sha256": None,
            "canonical_outcome": "",
            "canonical_reason": "",
            "values_differ_from_winner": None,
            "canonicalisation_version": CANONICALISATION_VERSION,
        }
        audit_rows.append(audit)

        classification = row["classification"]
        if classification == "invalid":
            audit.update(
                canonical_outcome="invalid",
                canonical_reason="rejected_before_ranking",
            )
            continue
        if classification == "out_of_range":
            audit.update(
                canonical_outcome="out_of_range",
                canonical_reason="outside_publication_range",
            )
            continue

        assert classification == "valid_in_range"
        assert row["validation_errors"] == []

        if row["source_mode"] != "api":
            audit.update(
                canonical_outcome="excluded_source",
                canonical_reason="snapshot_or_non_api_excluded",
            )
            continue

        retrieved = row["source_retrieved_at_utc"]
        if not isinstance(retrieved, datetime):
            audit.update(
                canonical_outcome="excluded_source",
                canonical_reason="unknown_source_retrieval_time",
            )
            continue

        signature = business_signature(row)
        audit["record_value_sha256"] = hashlib.sha256(
            signature.encode("utf-8")
        ).hexdigest()

        eligible_groups[business_key(row)].append(
            (audit, utc_value(retrieved), signature)
        )

    conflicts = []

    for key, group in eligible_groups.items():
        latest_time = max(item[1] for item in group)
        newest = [item for item in group if item[1] == latest_time]

        # Compare actual canonical values, not only their hashes.
        newest_signatures = {item[2] for item in newest}

        if len(newest_signatures) > 1:
            for audit, _, _ in group:
                audit.update(
                    canonical_outcome="conflict_blocked",
                    canonical_reason="newest_equal_time_values_conflict",
                )

            conflicts.append({
                "interval_start_utc": key[0].isoformat(),
                "interval_end_utc": key[1].isoformat(),
                "latest_source_retrieved_at_utc": latest_time.isoformat(),
                "newest_distinct_value_count": len(newest_signatures),
                "blocked_input_count": len(group),
                "newest_candidates": sorted([
                    {
                        "event_id": item[0]["bronze_event_id"],
                        "record_ordinal": item[0]["source_record_ordinal"],
                        "value_sha256": item[0]["record_value_sha256"],
                    }
                    for item in newest
                ], key=lambda item: (
                    item["event_id"], item["record_ordinal"]
                )),
            })
            continue

        ordered_newest = sorted(
            newest,
            key=lambda item: (
                item[0]["bronze_event_id"],
                item[0]["source_record_ordinal"],
            ),
        )
        winner_audit, _, winner_signature = ordered_newest[0]

        winner_audit.update(
            canonical_outcome="winner",
            canonical_reason="latest_valid_retrieval_stable_tie",
        )

        for audit, retrieved, signature in group:
            if audit is winner_audit:
                continue
            audit.update(
                canonical_outcome="duplicate_loser",
                canonical_reason=(
                    "older_valid_retrieval"
                    if retrieved < latest_time
                    else "equivalent_equal_time_identity_tie"
                ),
                values_differ_from_winner=(
                    signature != winner_signature
                ),
            )

    assert all(row["canonical_outcome"] for row in audit_rows)

    winner_keys = [
        business_key(row)
        for row in audit_rows
        if row["canonical_outcome"] == "winner"
    ]
    assert len(winner_keys) == len(set(winner_keys))

    return audit_rows, sorted(
        conflicts,
        key=lambda item: (
            item["interval_start_utc"], item["interval_end_utc"]
        ),
    )

print("Canonicalisation definitions loaded. No persistent writes.")

# COMMAND ----------

# CELL 15 — Python
canonical_tests = []

BASE_RETRIEVAL = datetime(2026, 10, 12, 20, 0, tzinfo=timezone.utc)
NEW_RETRIEVAL = datetime(2026, 10, 12, 21, 0, tzinfo=timezone.utc)

def canonical_fixture(event_id, actual=114, retrieved=BASE_RETRIEVAL):
    record = deepcopy(good_record)
    record["intensity"]["actual"] = actual
    event = fixture_event({"data": [record]})
    event["event_id"] = event_id
    event["retrieved_at_utc"] = retrieved

    rows, diagnostics, exclusion = parse_event(event)
    assert len(rows) == 1 and not diagnostics and exclusion is None
    return validate_candidate(rows[0])

def fixture_winners(rows):
    audit, conflicts = canonicalise(rows)
    winners = [
        row for row in audit if row["canonical_outcome"] == "winner"
    ]
    return audit, winners, conflicts

# 1. Equivalent same-key rows collapse.
a = canonical_fixture("event_a")
b = canonical_fixture("event_b")
audit, winners, conflicts = fixture_winners([b, a])
assert len(winners) == 1 and not conflicts
assert winners[0]["bronze_event_id"] == "event_a"
canonical_tests.append("equivalent_duplicates_collapse")

# 2. Newer valid revised actual wins.
new = canonical_fixture("event_new", actual=120, retrieved=NEW_RETRIEVAL)
audit, winners, conflicts = fixture_winners([a, new])
assert winners[0]["actual_intensity"] == 120 and not conflicts
assert winners[0]["bronze_event_id"] == "event_new"
canonical_tests.append("newer_valid_revision_wins")

# 3. Invalid newer record cannot replace valid older data.
bad_new = canonical_fixture(
    "event_bad_new", actual=-1, retrieved=NEW_RETRIEVAL
)
audit, winners, conflicts = fixture_winners([a, bad_new])
assert winners[0]["bronze_event_id"] == "event_a"
assert any(row["canonical_outcome"] == "invalid" for row in audit)
canonical_tests.append("invalid_newer_cannot_win")

# 4. Stale input loses; changed values are visible.
audit, winners, conflicts = fixture_winners([new, a])
loser = next(
    row for row in audit
    if row["canonical_outcome"] == "duplicate_loser"
)
assert loser["canonical_reason"] == "older_valid_retrieval"
assert loser["values_differ_from_winner"] is True
canonical_tests.append("stale_input_loses")

# 5. Equivalent equal-time tie retains stable identity.
audit, winners, conflicts = fixture_winners([b, a])
assert winners[0]["bronze_event_id"] == "event_a"
assert next(
    row for row in audit if row["canonical_outcome"] == "duplicate_loser"
)["canonical_reason"] == "equivalent_equal_time_identity_tie"
canonical_tests.append("equal_time_equivalent_stable_identity")

# 6. Newest equal-time conflict blocks the entire key, no older fallback.
conflict_new = canonical_fixture(
    "event_conflict_new", actual=121, retrieved=NEW_RETRIEVAL
)
audit, winners, conflicts = fixture_winners([a, new, conflict_new])
assert not winners and len(conflicts) == 1
assert all(row["canonical_outcome"] == "conflict_blocked" for row in audit)
assert conflicts[0]["blocked_input_count"] == 3
canonical_tests.append("newest_conflict_blocks_without_fallback")

# 7. Older equal-time disagreement does not defeat newer valid retrieval.
old_conflict = canonical_fixture("event_old_conflict", actual=115)
audit, winners, conflicts = fixture_winners([a, old_conflict, new])
assert len(winners) == 1 and not conflicts
assert winners[0]["bronze_event_id"] == "event_new"
canonical_tests.append("older_conflict_superseded_by_newer_valid")

# 8. Snapshot with unknown acquisition time is explicitly excluded.
snapshot = deepcopy(a)
snapshot.update(
    bronze_event_id="event_snapshot",
    source_mode="snapshot",
    source_retrieved_at_utc=None,
)
audit, winners, conflicts = fixture_winners([a, snapshot])
assert winners[0]["bronze_event_id"] == "event_a"
assert any(
    row["canonical_reason"] == "snapshot_or_non_api_excluded"
    for row in audit
)
canonical_tests.append("unknown_acquisition_snapshot_excluded")

# 9. Unknown API retrieval time is not ranked as fresh.
unknown = deepcopy(a)
unknown.update(
    bronze_event_id="event_unknown",
    source_retrieved_at_utc=None,
)
audit, winners, conflicts = fixture_winners([a, unknown])
assert winners[0]["bronze_event_id"] == "event_a"
assert any(
    row["canonical_reason"] == "unknown_source_retrieval_time"
    for row in audit
)
canonical_tests.append("unknown_api_retrieval_excluded")

# 10. Null and non-null actuals are different business values.
null_actual = canonical_fixture("event_null", actual=None)
audit, winners, conflicts = fixture_winners([a, null_actual])
assert not winners and len(conflicts) == 1
canonical_tests.append("null_actual_equal_time_conflict")

# 11. Input ordering cannot change selected winners.
_, forward, forward_conflicts = fixture_winners([a, b, new])
_, reverse, reverse_conflicts = fixture_winners([new, b, a])
assert stable_rows(forward) == stable_rows(reverse)
assert forward_conflicts == reverse_conflicts
canonical_tests.append("input_order_independent")

# 12. Repeating identical input occurrences preserves the winner.
_, once, _ = fixture_winners([a, b, new])
repeated_audit, twice, _ = fixture_winners([a, b, new, a, b, new])
assert stable_rows(once) == stable_rows(twice)
assert sum(
    row["canonical_outcome"] == "winner" for row in repeated_audit
) == 1
canonical_tests.append("repeated_inputs_preserve_winner")

assert len(canonical_tests) == 12
print(f"PASS: {len(canonical_tests)} canonicalisation tests.")
print(json.dumps(canonical_tests, indent=2))

# COMMAND ----------

# CELL 16 — Python
assert len(canonical_tests) == 12
assert len(classified_rows) <= 100_000, \
    "Bounded canonicalisation limit exceeded; use distributed processing."

canonical_audit_rows, live_conflicts = canonicalise(classified_rows)

canonical_audit = spark.createDataFrame(
    canonical_audit_rows, AUDIT_SCHEMA
)
canonical_winners = canonical_audit.where(
    F.col("canonical_outcome") == "winner"
)
duplicate_losers = canonical_audit.where(
    F.col("canonical_outcome") == "duplicate_loser"
)

outcome_counts = {
    row["canonical_outcome"]: row["count"]
    for row in canonical_audit.groupBy("canonical_outcome").count().collect()
}

allowed_outcomes = {
    "invalid", "out_of_range", "excluded_source",
    "conflict_blocked", "winner", "duplicate_loser",
}
assert set(outcome_counts).issubset(allowed_outcomes)
assert sum(outcome_counts.values()) == len(classified_rows)

assert outcome_counts.get("invalid", 0) == \
    classification_counts.get("invalid", 0)
assert outcome_counts.get("out_of_range", 0) == \
    classification_counts.get("out_of_range", 0)

valid_input_count = classification_counts.get("valid_in_range", 0)
assert valid_input_count == sum(
    outcome_counts.get(name, 0)
    for name in (
        "excluded_source", "conflict_blocked", "winner", "duplicate_loser"
    )
)

assert (
    canonical_winners.groupBy(
        "interval_start_utc", "interval_end_utc"
    ).count().where(F.col("count") != 1).count() == 0
), "Winner business keys are not unique."

# Independently confirm each winner has its group's latest eligible retrieval.
eligible_audit = [
    row for row in canonical_audit_rows
    if row["canonical_outcome"] in (
        "winner", "duplicate_loser", "conflict_blocked"
    )
]
latest_by_key = {}
for row in eligible_audit:
    key = business_key(row)
    retrieved = utc_value(row["source_retrieved_at_utc"])
    latest_by_key[key] = max(
        latest_by_key.get(key, retrieved), retrieved
    )

winner_rows = [
    row for row in canonical_audit_rows
    if row["canonical_outcome"] == "winner"
]
for row in winner_rows:
    assert utc_value(row["source_retrieved_at_utc"]) == \
        latest_by_key[business_key(row)]

# Repeat using reversed input order.
reverse_audit, reverse_conflicts = canonicalise(
    list(reversed(classified_rows))
)
reverse_winners = [
    row for row in reverse_audit if row["canonical_outcome"] == "winner"
]
assert stable_rows(winner_rows) == stable_rows(reverse_winners)
assert live_conflicts == reverse_conflicts

canonical_summary = {
    "canonicalisation_version": CANONICALISATION_VERSION,
    "classified_input_count": len(classified_rows),
    "outcome_counts": outcome_counts,
    "canonical_winner_count": len(winner_rows),
    "conflicting_key_count": len(live_conflicts),
    "duplicate_losers_with_changed_business_values": (
        duplicate_losers.where(
            F.col("values_differ_from_winner") == True
        ).count()
    ),
    "winner_keys_unique": True,
    "latest_valid_retrieval_check_passed": True,
    "input_order_independent": True,
    "input_reconciliation_passed": True,
    "persistent_writes": False,
}

print("LIVE CANONICALISATION SUMMARY")
print(json.dumps(canonical_summary, indent=2))
print("CONFLICT DIAGNOSTICS")
print(json.dumps(live_conflicts, indent=2))

print("OVERLAP LOSERS AND THEIR WINNERS")
losers_view = duplicate_losers.select(
    "interval_start_utc", "interval_end_utc",
    F.col("bronze_event_id").alias("loser_event_id"),
    F.col("source_retrieved_at_utc").alias("loser_retrieved_at_utc"),
    F.col("forecast_intensity").alias("loser_forecast"),
    F.col("actual_intensity").alias("loser_actual"),
    "canonical_reason", "values_differ_from_winner",
)
winners_view = canonical_winners.select(
    "interval_start_utc", "interval_end_utc",
    F.col("bronze_event_id").alias("winner_event_id"),
    F.col("source_retrieved_at_utc").alias("winner_retrieved_at_utc"),
    F.col("forecast_intensity").alias("winner_forecast"),
    F.col("actual_intensity").alias("winner_actual"),
)
display(
    losers_view.join(
        winners_view,
        ["interval_start_utc", "interval_end_utc"],
        "inner",
    ).orderBy("interval_start_utc").limit(20)
)

print(
    "No live writes. Unique candidate keys do not independently prove "
    "complete interval coverage or Delta MERGE behaviour."
)

if live_conflicts:
    raise RuntimeError(
        "Publication blocked: review conflicting keys before write logic."
    )

# COMMAND ----------

# CELL 17 — Python
from datetime import timedelta

ALLOW_DISPOSABLE_MERGE_TESTS = True

NATIONAL_COLUMNS = [
    "interval_start_utc", "interval_end_utc",
    "forecast_intensity", "actual_intensity", "source_index",
    "source", "source_mode", "source_retrieved_at_utc",
    "bronze_event_id", "bronze_run_id", "bronze_request_id",
    "source_record_ordinal", "response_sha256", "record_value_sha256",
    "parser_version", "validation_contract_version",
    "canonicalisation_version",
]

audit_fields = {field.name: field for field in AUDIT_SCHEMA.fields}
NATIONAL_SCHEMA = T.StructType([
    T.StructField(
        name,
        audit_fields[name].dataType,
        nullable=(name == "actual_intensity"),
    )
    for name in NATIONAL_COLUMNS
])

FIXTURE_TABLE_PATTERN = re.compile(
    r"^workspace\.silver\._gridlens_merge_fixture_"
    r"[0-9a-f]{32}_(national|quarantine)$"
)

class MergeGuardError(ValueError):
    pass

def require_fixture_table(table):
    if not FIXTURE_TABLE_PATTERN.fullmatch(table):
        raise MergeGuardError("This stage accepts disposable fixture tables only.")

def schema_signature(schema):
    return [
        (field.name, field.dataType.simpleString())
        for field in schema.fields
    ]

def check_table_schema(table, expected_schema):
    require_fixture_table(table)
    actual = spark.table(table).schema
    if schema_signature(actual) != schema_signature(expected_schema):
        raise MergeGuardError("Incompatible table schema; no evolution allowed.")

    provider = (
        spark.sql(f"DESCRIBE TABLE EXTENDED {table}")
        .where(F.col("col_name") == "Provider")
        .select("data_type").first()["data_type"]
    )
    if provider.lower() != "delta":
        raise MergeGuardError("Destination is not Delta.")

def create_fixture_table(table, schema):
    require_fixture_table(table)
    ddl = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        + ("" if field.nullable else " NOT NULL")
        for field in schema.fields
    )
    # No IF NOT EXISTS: never adopt an existing table.
    spark.sql(f"CREATE TABLE {table} ({ddl}) USING DELTA")

def require_unique(df, keys):
    if df.groupBy(*keys).count().where(F.col("count") > 1).count():
        raise MergeGuardError(f"Duplicate source/target keys: {keys}")
    null_key = F.lit(False)
    for key in keys:
        null_key = null_key | F.col(key).isNull()
    if df.where(null_key).count():
        raise MergeGuardError(f"Null source/target keys: {keys}")

def check_national_values(df):
    null_required = F.lit(False)
    for field in NATIONAL_SCHEMA.fields:
        if not field.nullable:
            null_required = null_required | F.col(field.name).isNull()

    invalid = (
        null_required
        | (F.col("source_mode") != "api")
        | (F.col("forecast_intensity") < 0)
        | (F.col("actual_intensity") < 0)
        | (F.length(F.trim(F.col("source_index"))) == 0)
        | (
            F.col("interval_end_utc").cast("long")
            - F.col("interval_start_utc").cast("long") != 1800
        )
    )
    for name in ("interval_start_utc", "interval_end_utc"):
        invalid = invalid | (
            F.date_trunc("minute", F.col(name)) != F.col(name)
        ) | (F.minute(F.col(name)) % 30 != 0)

    if df.where(invalid).count():
        raise MergeGuardError("Invalid national source/target values.")

def execute_fixture_merge(table, source_df, keys, update_condition=None):
    require_fixture_table(table)
    view = f"_gridlens_merge_source_{uuid4().hex}"
    source_df.createOrReplaceTempView(view)
    columns = source_df.columns

    match = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    names = ", ".join(f"`{name}`" for name in columns)
    values = ", ".join(f"s.`{name}`" for name in columns)

    matched_clause = ""
    if update_condition:
        assignments = ", ".join(
            f"t.`{name}` = s.`{name}`" for name in columns
        )
        matched_clause = (
            f"WHEN MATCHED AND {update_condition} "
            f"THEN UPDATE SET {assignments}"
        )

    try:
        spark.sql(f"""
            MERGE INTO {table} AS t
            USING {view} AS s
            ON {match}
            {matched_clause}
            WHEN NOT MATCHED THEN
                INSERT ({names}) VALUES ({values})
        """)
    finally:
        spark.catalog.dropTempView(view)

def merge_national_fixture(table, source_df):
    check_table_schema(table, NATIONAL_SCHEMA)
    if schema_signature(source_df.schema) != schema_signature(NATIONAL_SCHEMA):
        raise MergeGuardError("Unexpected national source schema.")

    target_df = spark.table(table)
    keys = ["interval_start_utc", "interval_end_utc"]

    require_unique(source_df, keys)
    require_unique(target_df, keys)
    check_national_values(source_df)
    check_national_values(target_df)

    pairs = source_df.alias("s").join(
        target_df.alias("t"),
        (F.col("s.interval_start_utc") == F.col("t.interval_start_utc"))
        & (F.col("s.interval_end_utc") == F.col("t.interval_end_utc")),
        "inner",
    )
    equivalent = F.lit(True)
    for name in ("forecast_intensity", "actual_intensity", "source_index"):
        equivalent = equivalent & F.col(f"s.{name}").eqNullSafe(
            F.col(f"t.{name}")
        )

    conflicts = pairs.where(
        (
            F.col("s.source_retrieved_at_utc")
            == F.col("t.source_retrieved_at_utc")
        ) & ~equivalent
    )
    if conflicts.limit(1).count():
        raise MergeGuardError("Equal-time source/target business-value conflict.")

    execute_fixture_merge(
        table, source_df, keys,
        update_condition=(
            "s.source_retrieved_at_utc > t.source_retrieved_at_utc"
        ),
    )

def merge_quarantine_fixture(table, source_df):
    check_table_schema(table, QUARANTINE_SCHEMA)
    if schema_signature(source_df.schema) != schema_signature(QUARANTINE_SCHEMA):
        raise MergeGuardError("Unexpected quarantine source schema.")

    target_df = spark.table(table)
    require_unique(source_df, ["rejection_id"])
    require_unique(target_df, ["rejection_id"])

    pairs = source_df.alias("s").join(
        target_df.alias("t"),
        F.col("s.rejection_id") == F.col("t.rejection_id"),
        "inner",
    )
    immutable_columns = [
        name for name in source_df.columns
        if name not in ("processing_run_id", "processed_at_utc")
    ]
    same_content = F.lit(True)
    for name in immutable_columns:
        same_content = same_content & F.col(f"s.{name}").eqNullSafe(
            F.col(f"t.{name}")
        )
    if pairs.where(~same_content).limit(1).count():
        raise MergeGuardError("Rejection identity has inconsistent content.")

    # Insert only: reruns preserve the first stored rejection/provenance.
    execute_fixture_merge(table, source_df, ["rejection_id"])

print("Fixture-only write helpers loaded.")
print("Disposable write approval:", ALLOW_DISPOSABLE_MERGE_TESTS)

# COMMAND ----------

# CELL 18 — Python
def national_fixture_source(
    event_id,
    actual=114,
    retrieved=BASE_RETRIEVAL,
    offset_minutes=0,
):
    record = deepcopy(good_record)
    start = PUBLICATION_START + timedelta(minutes=offset_minutes)
    end = start + timedelta(minutes=30)

    record["from"] = start.isoformat().replace("+00:00", "Z")
    record["to"] = end.isoformat().replace("+00:00", "Z")
    record["intensity"]["actual"] = actual

    event = fixture_event({"data": [record]})
    event["event_id"] = event_id
    event["retrieved_at_utc"] = retrieved

    rows, diagnostics, exclusion = parse_event(event)
    assert not diagnostics and exclusion is None
    classified = [validate_candidate(row) for row in rows]
    audit, conflicts = canonicalise(classified)
    assert not conflicts

    winners = [
        row for row in audit if row["canonical_outcome"] == "winner"
    ]
    assert len(winners) == 1

    publication_rows = [
        {name: row[name] for name in NATIONAL_COLUMNS}
        for row in winners
    ]
    return spark.createDataFrame(publication_rows, NATIONAL_SCHEMA)

def table_snapshot(table, order_columns):
    return [
        row.asDict()
        for row in spark.table(table).orderBy(*order_columns).collect()
    ]

def expect_guard_failure(action):
    try:
        action()
    except MergeGuardError as exc:
        print("EXPECTED GUARD:", str(exc))
    else:
        raise AssertionError("Expected guard failure did not occur.")

print("Synthetic input builders loaded. No table writes.")

# COMMAND ----------

# CELL 19 — Python
if not ALLOW_DISPOSABLE_MERGE_TESTS:
    raise RuntimeError(
        "Disposable writes are not approved. "
        "Enable the Cell 17 flag only if you approve these fixture operations."
    )

suffix = uuid4().hex
national_fixture_table = (
    f"workspace.silver._gridlens_merge_fixture_{suffix}_national"
)
quarantine_fixture_table = (
    f"workspace.silver._gridlens_merge_fixture_{suffix}_quarantine"
)

created_fixture_tables = []
merge_tests = []
FIXTURE_MERGE_TESTS_PASSED = False
FIXTURE_CLEANUP_PASSED = False

print("National fixture:", national_fixture_table)
print("Quarantine fixture:", quarantine_fixture_table)

try:
    create_fixture_table(national_fixture_table, NATIONAL_SCHEMA)
    created_fixture_tables.append(national_fixture_table)
    create_fixture_table(quarantine_fixture_table, QUARANTINE_SCHEMA)
    created_fixture_tables.append(quarantine_fixture_table)

    original = national_fixture_source("merge_original")
    revised = national_fixture_source(
        "merge_revised", actual=120, retrieved=NEW_RETRIEVAL
    )

    # 1. First insert.
    merge_national_fixture(national_fixture_table, original)
    first = table_snapshot(national_fixture_table, [
        "interval_start_utc", "interval_end_utc"
    ])
    assert len(first) == 1 and first[0]["actual_intensity"] == 114
    merge_tests.append("first_insert")

    # 2. Identical rerun preserves every stored field.
    merge_national_fixture(national_fixture_table, original)
    assert table_snapshot(national_fixture_table, [
        "interval_start_utc", "interval_end_utc"
    ]) == first
    merge_tests.append("identical_rerun_preserves_full_row")

    # 3. Newer valid revision updates values and provenance.
    merge_national_fixture(national_fixture_table, revised)
    after_revision = table_snapshot(national_fixture_table, [
        "interval_start_utc", "interval_end_utc"
    ])
    assert after_revision[0]["actual_intensity"] == 120
    assert after_revision[0]["bronze_event_id"] == "merge_revised"
    merge_tests.append("newer_valid_revision_updates")

    # 4. Stale input cannot regress the revised row.
    merge_national_fixture(national_fixture_table, original)
    assert table_snapshot(national_fixture_table, [
        "interval_start_utc", "interval_end_utc"
    ]) == after_revision
    merge_tests.append("stale_input_noop")

    # 5. Equal-time equivalent values preserve existing provenance.
    equivalent = national_fixture_source(
        "merge_equivalent_other_identity",
        actual=120, retrieved=NEW_RETRIEVAL
    )
    merge_national_fixture(national_fixture_table, equivalent)
    assert table_snapshot(national_fixture_table, [
        "interval_start_utc", "interval_end_utc"
    ]) == after_revision
    merge_tests.append("equivalent_equal_time_preserves_provenance")

    # 6. Equal-time conflict stops before changing the table.
    conflicting = national_fixture_source(
        "merge_conflicting", actual=121, retrieved=NEW_RETRIEVAL
    )
    expect_guard_failure(
        lambda: merge_national_fixture(national_fixture_table, conflicting)
    )
    assert table_snapshot(national_fixture_table, [
        "interval_start_utc", "interval_end_utc"
    ]) == after_revision
    merge_tests.append("equal_time_conflict_no_write")

    # 7. A newer valid explicit null actual remains null.
    newest_time = NEW_RETRIEVAL + timedelta(hours=1)
    nullable_revision = national_fixture_source(
        "merge_null_actual", actual=None, retrieved=newest_time
    )
    merge_national_fixture(national_fixture_table, nullable_revision)
    after_null = table_snapshot(national_fixture_table, [
        "interval_start_utc", "interval_end_utc"
    ])
    assert after_null[0]["actual_intensity"] is None
    assert after_null[0]["forecast_intensity"] == 111
    merge_tests.append("newer_null_actual_not_replaced_by_forecast")

    # 8. A second business key inserts independently.
    second_key = national_fixture_source(
        "merge_second_key", offset_minutes=30
    )
    merge_national_fixture(national_fixture_table, second_key)
    two_rows = table_snapshot(national_fixture_table, [
        "interval_start_utc", "interval_end_utc"
    ])
    assert len(two_rows) == 2
    merge_tests.append("second_key_inserts")

    # 9. Duplicate source keys are blocked before MERGE.
    expect_guard_failure(
        lambda: merge_national_fixture(
            national_fixture_table, second_key.unionByName(second_key)
        )
    )
    assert table_snapshot(national_fixture_table, [
        "interval_start_utc", "interval_end_utc"
    ]) == two_rows
    merge_tests.append("duplicate_source_keys_blocked")

    # 10. Rejection rerun keeps identities and first stored provenance.
    merge_quarantine_fixture(
        quarantine_fixture_table, fixture_quarantine
    )
    first_rejections = table_snapshot(
        quarantine_fixture_table, ["rejection_id"]
    )
    assert len(first_rejections) == 11

    rerun_rejections = (
        fixture_quarantine
        .withColumn("processing_run_id", F.lit(uuid4().hex))
        .withColumn("processed_at_utc", F.current_timestamp())
    )
    merge_quarantine_fixture(
        quarantine_fixture_table, rerun_rejections
    )
    assert table_snapshot(
        quarantine_fixture_table, ["rejection_id"]
    ) == first_rejections
    merge_tests.append("quarantine_rerun_preserves_stored_rows")

    # 11. Same rejection identity with different content is blocked.
    corrupt_rejections = fixture_quarantine.withColumn(
        "raw_record_json", F.lit("changed content under same identity")
    )
    expect_guard_failure(
        lambda: merge_quarantine_fixture(
            quarantine_fixture_table, corrupt_rejections
        )
    )
    assert table_snapshot(
        quarantine_fixture_table, ["rejection_id"]
    ) == first_rejections
    merge_tests.append("quarantine_identity_collision_blocked")

    assert len(merge_tests) == 11
    FIXTURE_MERGE_TESTS_PASSED = True
    print("PASS: 11 disposable Delta MERGE/quarantine tests.")
    print(json.dumps(merge_tests, indent=2))

    print("FINAL NATIONAL FIXTURE ROWS")
    display(spark.table(national_fixture_table))
    print("FINAL QUARANTINE FIXTURE COUNT:", len(first_rejections))

finally:
    cleanup_errors = []

    # Attempt cleanup of every table this execution successfully created.
    for table in reversed(created_fixture_tables):
        try:
            require_fixture_table(table)
            spark.sql(f"DROP TABLE {table}")
            table_name = table.split(".")[-1]
            remaining = (
                spark.sql("SHOW TABLES IN workspace.silver")
                .where(F.col("tableName") == table_name)
                .count()
            )
            assert remaining == 0, f"Fixture still visible: {table}"
            print("CLEANED:", table)
        except Exception as exc:
            cleanup_errors.append((table, str(exc)))

    if cleanup_errors:
        print("CLEANUP ERRORS:", cleanup_errors)
        raise RuntimeError("Fixture cleanup incomplete; review listed tables.")

    FIXTURE_CLEANUP_PASSED = True
    print("PASS: disposable fixture cleanup and absence verification.")

# COMMAND ----------

# CELL 20 — Python
LIVE_NATIONAL_TABLE = "workspace.silver.national_intensity"
LIVE_QUARANTINE_TABLE = "workspace.silver.quarantined_records"
LIVE_DESTINATIONS = {
    LIVE_NATIONAL_TABLE: NATIONAL_SCHEMA,
    LIVE_QUARANTINE_TABLE: QUARANTINE_SCHEMA,
}
MAX_LIVE_REVIEW_ROWS = 100_000

assert FIXTURE_MERGE_TESTS_PASSED and FIXTURE_CLEANUP_PASSED
assert not live_conflicts
assert SELECTED_RUN_IDS == [BACKFILL_RUN_ID]
assert INCLUDE_SAMPLE_REFETCH is False
assert len(winner_rows) == 4320
assert len(quarantine_rows) == 0
assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

live_national_rows = [
    {name: row[name] for name in NATIONAL_COLUMNS}
    for row in winner_rows
]
live_national_source = spark.createDataFrame(
    live_national_rows, NATIONAL_SCHEMA
)
live_quarantine_source = spark.createDataFrame(
    quarantine_rows, QUARANTINE_SCHEMA
)

def normalise_row(row):
    data = row.asDict() if hasattr(row, "asDict") else dict(row)
    return {
        key: utc_value(value) if isinstance(value, datetime) else value
        for key, value in data.items()
    }

def national_key(row):
    return (
        utc_value(row["interval_start_utc"]),
        utc_value(row["interval_end_utc"]),
    )

def verify_selected_bronze_metadata():
    columns = [
        "event_id", "run_id", "request_id", "source", "source_mode",
        "is_fixture", "event_kind", "status", "http_status",
        "retrieved_at_utc", "response_sha256", "record_count",
    ]
    current = (
        spark.table(BRONZE_TABLE)
        .where(F.col("run_id").isin(SELECTED_RUN_IDS))
        .select(*columns)
    )
    require_unique(current, ["event_id"])
    current_rows = current.collect()
    assert len(current_rows) == 7, "Selected Bronze evidence changed."

    for row in current_rows:
        now = normalise_row(row)
        prior = normalise_row(original_events[now["event_id"]])
        assert all(now[name] == prior[name] for name in columns), \
            "Selected Bronze metadata changed; repeat read-only review."

def read_live_destination(table):
    if table not in LIVE_DESTINATIONS:
        raise MergeGuardError("Unapproved live destination.")

    # Permission errors are not treated as absence.
    if not spark.catalog.tableExists(table):
        return None, []

    expected = LIVE_DESTINATIONS[table]
    df = spark.table(table)

    if schema_signature(df.schema) != schema_signature(expected):
        raise MergeGuardError(f"Incompatible schema: {table}")

    actual_fields = {field.name: field for field in df.schema.fields}
    for field in expected.fields:
        if not field.nullable and actual_fields[field.name].nullable:
            raise MergeGuardError(
                f"Missing required NOT NULL contract: {table}.{field.name}"
            )

    provider = (
        spark.sql(f"DESCRIBE TABLE EXTENDED {table}")
        .where(F.col("col_name") == "Provider")
        .select("data_type").first()["data_type"]
    )
    if provider.lower() != "delta":
        raise MergeGuardError(f"Not a Delta table: {table}")

    assert df.count() <= MAX_LIVE_REVIEW_ROWS, \
        "Destination exceeds bounded review limit; stop for scalable review."

    keys = (
        ["interval_start_utc", "interval_end_utc"]
        if table == LIVE_NATIONAL_TABLE else ["rejection_id"]
    )
    require_unique(df, keys)

    if table == LIVE_NATIONAL_TABLE:
        check_national_values(df)

    return df, [normalise_row(row) for row in df.collect()]

def live_preflight():
    verify_selected_bronze_metadata()

    require_unique(
        live_national_source,
        ["interval_start_utc", "interval_end_utc"],
    )
    require_unique(live_quarantine_source, ["rejection_id"])
    check_national_values(live_national_source)

    for row in live_national_rows:
        assert (
            row["interval_start_utc"] >= PUBLICATION_START
            and row["interval_end_utc"] <= PUBLICATION_END
        )
        expected_hash = hashlib.sha256(
            business_signature(row).encode("utf-8")
        ).hexdigest()
        assert expected_hash == row["record_value_sha256"]

    national_df, national_before = read_live_destination(
        LIVE_NATIONAL_TABLE
    )
    quarantine_df, quarantine_before = read_live_destination(
        LIVE_QUARANTINE_TABLE
    )

    national_by_key = {
        national_key(row): row for row in national_before
    }
    changes = Counter()

    for incoming in live_national_rows:
        row = normalise_row(incoming)
        existing = national_by_key.get(national_key(row))

        if existing is None:
            changes["insert"] += 1
        elif row["source_retrieved_at_utc"] > \
                existing["source_retrieved_at_utc"]:
            changes["update_newer_retrieval"] += 1
        elif row["source_retrieved_at_utc"] < \
                existing["source_retrieved_at_utc"]:
            changes["stale_noop"] += 1
        elif business_signature(row) != business_signature(existing):
            raise MergeGuardError(
                "Equal-time conflict against existing live national row."
            )
        else:
            changes["equal_time_equivalent_noop"] += 1

    quarantine_by_id = {
        row["rejection_id"]: row for row in quarantine_before
    }
    immutable_names = [
        field.name for field in QUARANTINE_SCHEMA.fields
        if field.name not in ("processing_run_id", "processed_at_utc")
    ]
    for incoming in quarantine_rows:
        row = normalise_row(incoming)
        existing = quarantine_by_id.get(row["rejection_id"])
        if existing is not None:
            if any(row[name] != existing[name] for name in immutable_names):
                raise MergeGuardError(
                    "Existing quarantine identity has inconsistent content."
                )

    return {
        "national_before": national_before,
        "quarantine_before": quarantine_before,
        "national_exists": national_df is not None,
        "quarantine_exists": quarantine_df is not None,
        "national_actions": dict(changes),
    }

def create_live_destination(table):
    if table not in LIVE_DESTINATIONS:
        raise MergeGuardError("Unapproved live destination.")
    schema = LIVE_DESTINATIONS[table]
    ddl = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        + ("" if field.nullable else " NOT NULL")
        for field in schema.fields
    )
    # No overwrite, replacement or adoption through IF NOT EXISTS.
    spark.sql(f"CREATE TABLE {table} ({ddl}) USING DELTA")

def execute_live_merge(table, source_df, keys, update_condition=None):
    if table not in LIVE_DESTINATIONS:
        raise MergeGuardError("Unapproved live destination.")
    expected_schema = LIVE_DESTINATIONS[table]
    if schema_signature(source_df.schema) != schema_signature(expected_schema):
        raise MergeGuardError("Unexpected live source schema.")

    view = f"_gridlens_live_source_{uuid4().hex}"
    source_df.createOrReplaceTempView(view)
    columns = source_df.columns
    match = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    names = ", ".join(f"`{name}`" for name in columns)
    values = ", ".join(f"s.`{name}`" for name in columns)

    matched_clause = ""
    if update_condition:
        assignments = ", ".join(
            f"t.`{name}` = s.`{name}`" for name in columns
        )
        matched_clause = (
            f"WHEN MATCHED AND {update_condition} "
            f"THEN UPDATE SET {assignments}"
        )

    try:
        spark.sql(f"""
            MERGE INTO {table} AS t
            USING {view} AS s
            ON {match}
            {matched_clause}
            WHEN NOT MATCHED THEN
                INSERT ({names}) VALUES ({values})
        """)
    finally:
        spark.catalog.dropTempView(view)

print("Live inputs prepared; no persistent writes.")

# COMMAND ----------

# CELL 21 — Python
preview = live_preflight()

publication_preview = {
    "source_run_ids": SELECTED_RUN_IDS,
    "publication_start": PUBLICATION_START.isoformat(),
    "publication_end": PUBLICATION_END.isoformat(),
    "national_destination": LIVE_NATIONAL_TABLE,
    "quarantine_destination": LIVE_QUARANTINE_TABLE,
    "national_source_rows": len(live_national_rows),
    "quarantine_source_rows": len(quarantine_rows),
    "national_table_exists": preview["national_exists"],
    "quarantine_table_exists": preview["quarantine_exists"],
    "existing_national_rows": len(preview["national_before"]),
    "existing_quarantine_rows": len(preview["quarantine_before"]),
    "national_actions": preview["national_actions"],
    "persistent_writes": False,
}

print("LIVE PUBLICATION PREVIEW")
print(json.dumps(publication_preview, indent=2))

# COMMAND ----------

# CELL 22 — Python
ALLOW_LIVE_SILVER_WRITES = True
CONFIRM_SINGLE_WRITER = True

if not ALLOW_LIVE_SILVER_WRITES or not CONFIRM_SINGLE_WRITER:
    raise RuntimeError(
        "Live publication is not approved, or single-writer execution "
        "has not been confirmed."
    )

assert FIXTURE_MERGE_TESTS_PASSED and FIXTURE_CLEANUP_PASSED
assert not live_conflicts

publication_state = {
    "national_created": False,
    "quarantine_created": False,
    "quarantine_merge": "not_started",
    "national_merge": "not_started",
    "readback_verified": False,
}

try:
    # Refresh the preflight immediately before mutations.
    before = live_preflight()

    # Compute expected final rows independently of the MERGE SQL.
    expected_national = {
        national_key(row): row for row in before["national_before"]
    }
    for incoming in live_national_rows:
        row = normalise_row(incoming)
        key = national_key(row)
        existing = expected_national.get(key)
        if (
            existing is None
            or row["source_retrieved_at_utc"] >
                existing["source_retrieved_at_utc"]
        ):
            expected_national[key] = row

    expected_quarantine = {
        row["rejection_id"]: row for row in before["quarantine_before"]
    }
    for incoming in quarantine_rows:
        row = normalise_row(incoming)
        expected_quarantine.setdefault(row["rejection_id"], row)

    if not before["national_exists"]:
        create_live_destination(LIVE_NATIONAL_TABLE)
        publication_state["national_created"] = True
    if not before["quarantine_exists"]:
        create_live_destination(LIVE_QUARANTINE_TABLE)
        publication_state["quarantine_created"] = True

    # Check actual created/existing destination contracts again.
    live_preflight()

    if quarantine_rows:
        publication_state["quarantine_merge"] = "attempting"
        execute_live_merge(
            LIVE_QUARANTINE_TABLE,
            live_quarantine_source,
            ["rejection_id"],
        )
        publication_state["quarantine_merge"] = "returned_successfully"
    else:
        publication_state["quarantine_merge"] = "skipped_empty_source"

    publication_state["national_merge"] = "attempting"
    execute_live_merge(
        LIVE_NATIONAL_TABLE,
        live_national_source,
        ["interval_start_utc", "interval_end_utc"],
        update_condition=(
            "s.source_retrieved_at_utc > t.source_retrieved_at_utc"
        ),
    )
    publication_state["national_merge"] = "returned_successfully"

    _, national_after = read_live_destination(LIVE_NATIONAL_TABLE)
    _, quarantine_after = read_live_destination(LIVE_QUARANTINE_TABLE)

    actual_national = {
        national_key(row): row for row in national_after
    }
    actual_quarantine = {
        row["rejection_id"]: row for row in quarantine_after
    }

    assert actual_national == expected_national, \
        "National full-row read-back differs from expected results."
    assert actual_quarantine == expected_quarantine, \
        "Quarantine full-row read-back differs from expected results."

    publication_state["readback_verified"] = True

    live_publication_summary = {
        "source_run_ids": SELECTED_RUN_IDS,
        "national_source_rows": len(live_national_rows),
        "quarantine_source_rows": len(quarantine_rows),
        "national_actions_before_merge": before["national_actions"],
        "national_rows_after": len(national_after),
        "quarantine_rows_after": len(quarantine_after),
        "national_unique_keys": True,
        "quarantine_unique_ids": True,
        "full_row_values_and_provenance_verified": True,
        "existing_unmatched_rows_preserved": True,
        "publication_state": publication_state,
    }

    print("LIVE PUBLICATION SUMMARY")
    print(json.dumps(live_publication_summary, indent=2))
    print("SMALL PUBLISHED SAMPLE")
    display(
        spark.table(LIVE_NATIONAL_TABLE)
        .orderBy("interval_start_utc").limit(5)
    )

except Exception:
    print("PUBLICATION STOPPED — some operations may have committed.")
    print(json.dumps(publication_state, indent=2))
    print(
        "Do not drop tables or delete data. Review the error and current "
        "tables before an idempotent rerun. Multi-table writes are not atomic."
    )
    raise