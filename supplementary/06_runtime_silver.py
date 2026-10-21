# Databricks notebook source
# CELL 01 — Standalone Silver setup; read-only schema checks
import json
import hashlib
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pyspark.sql import types as T

spark.conf.set("spark.sql.session.timeZone", "UTC")

PARSER_VERSION = "gridlens_parse_v1"
ADMISSION_VERSION = "gridlens_runtime_admission_v1"

NATIONAL_TABLE = "workspace.silver.national_intensity"
QUARANTINE_TABLE = "workspace.silver.quarantined_records"

EXPECTED_NATIONAL = {
    "interval_start_utc": ("timestamp", False),
    "interval_end_utc": ("timestamp", False),
    "forecast_intensity": ("int", False),
    "actual_intensity": ("int", True),
    "source_index": ("string", False),
    "source": ("string", False),
    "source_mode": ("string", False),
    "source_retrieved_at_utc": ("timestamp", False),
    "bronze_event_id": ("string", False),
    "bronze_run_id": ("string", False),
    "bronze_request_id": ("string", False),
    "source_record_ordinal": ("int", False),
    "response_sha256": ("string", False),
    "record_value_sha256": ("string", False),
    "parser_version": ("string", False),
    "validation_contract_version": ("string", False),
    "canonicalisation_version": ("string", False),
}

EXPECTED_QUARANTINE = {
    "rejection_id": ("string", False),
    "rejection_scope": ("string", False),
    "bronze_event_id": ("string", False),
    "bronze_run_id": ("string", True),
    "bronze_request_id": ("string", True),
    "source": ("string", True),
    "source_mode": ("string", True),
    "source_retrieved_at_utc": ("timestamp", True),
    "response_sha256": ("string", True),
    "source_record_ordinal": ("int", True),
    "raw_record_json": ("string", True),
    "rejection_reasons": ("array<string>", False),
    "parser_version": ("string", False),
    "validation_contract_version": ("string", False),
    "processing_run_id": ("string", False),
    "processed_at_utc": ("timestamp", False),
}

SILVER_SCHEMA_EVIDENCE = {}

for table, expected in (
    (NATIONAL_TABLE, EXPECTED_NATIONAL),
    (QUARANTINE_TABLE, EXPECTED_QUARANTINE),
):
    schema = spark.table(table).schema
    actual = {
        field.name: (field.dataType.simpleString(), field.nullable)
        for field in schema.fields
    }

    if actual != expected:
        differences = {
            name: {
                "expected": expected.get(name),
                "actual": actual.get(name),
            }
            for name in sorted(set(expected) | set(actual))
            if expected.get(name) != actual.get(name)
        }
        raise RuntimeError(
            f"Schema mismatch for {table}; no changes made: "
            + json.dumps(differences, sort_keys=True)
        )

    sql_table = ".".join(f"`{part}`" for part in table.split("."))
    detail = spark.sql(f"DESCRIBE DETAIL {sql_table}").first()
    if detail["format"] != "delta":
        raise RuntimeError(f"Expected Delta table: {table}")

    if table == QUARANTINE_TABLE:
        reasons_type = schema["rejection_reasons"].dataType
        if reasons_type != T.ArrayType(T.StringType(), True):
            raise RuntimeError("Quarantine array-element schema changed")

    SILVER_SCHEMA_EVIDENCE[table] = {
        "status": "PASS",
        "column_count": len(actual),
        "format": "delta",
    }

print(json.dumps({
    "silver_schema_checks": SILVER_SCHEMA_EVIDENCE,
    "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
    "persistent_writes": False,
}, indent=2))

# COMMAND ----------

# CELL 02 — Strict parsing definitions; no reads or writes
UTC_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}"
    r"(?::\d{2}(?:\.\d{1,6})?)?(?:Z|\+00:00)$"
)


def utc_value(value):
    if value is None:
        return None
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def canonical_json(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
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
                continue

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


def parse_runtime_event(event, execution_mode):
    if execution_mode not in ("fixture", "live"):
        raise ValueError("Unknown execution mode")

    expected_mode = "fixture" if execution_mode == "fixture" else "api"
    expected_fixture = execution_mode == "fixture"
    event = dict(event)
    exclusions = []

    if event.get("status") != "succeeded":
        exclusions.append("status_not_succeeded")
    if event.get("http_status") != 200:
        exclusions.append("http_status_not_200")
    if event.get("source_mode") != expected_mode:
        exclusions.append("source_mode_not_expected")
    if event.get("is_fixture") is not expected_fixture:
        exclusions.append("fixture_label_mismatch")
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
        "event_id", "run_id", "request_id", "source", "response_sha256",
    ):
        if not isinstance(event.get(name), str) or not event[name]:
            return envelope_failure(
                f"provenance:{name}:missing_or_invalid"
            )

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


print("Strict Silver parser loaded; no source data selected or written.")

# COMMAND ----------

# CELL 03 — Synthetic parser/admission tests; no persistent writes
PARSER_TESTS = []

BASE_RECORD = {
    "from": "2026-01-01T00:00Z",
    "to": "2026-01-01T00:30Z",
    "intensity": {
        "forecast": 100,
        "actual": 95,
        "index": "moderate",
    },
}


def synthetic_event(raw_body=None):
    raw = (
        canonical_json({"data": [BASE_RECORD]}).encode("utf-8")
        if raw_body is None else raw_body
    )
    return {
        "event_id": uuid.uuid4().hex,
        "run_id": uuid.uuid4().hex,
        "request_id": uuid.uuid4().hex,
        "source": "synthetic_parser_fixture",
        "source_mode": "fixture",
        "is_fixture": True,
        "event_kind": "response",
        "status": "succeeded",
        "http_status": 200,
        "retrieved_at_utc": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "raw_body": raw,
        "raw_text": raw.decode("utf-8", errors="replace"),
        "response_sha256": hashlib.sha256(raw).hexdigest(),
        "record_count": 1,
    }


def check_parser(name, condition):
    if not condition:
        raise AssertionError(name)
    PARSER_TESTS.append(name)


def parse_fixture_record(record):
    raw = canonical_json({"data": [record]}).encode("utf-8")
    candidates, diagnostics, exclusion = parse_runtime_event(
        synthetic_event(raw), "fixture"
    )
    if diagnostics or exclusion or len(candidates) != 1:
        raise AssertionError("Unexpected fixture envelope outcome")
    return candidates[0]


valid_candidate = parse_fixture_record(BASE_RECORD)
check_parser(
    "valid typed record",
    valid_candidate["actual_intensity"] == 95
    and valid_candidate["parse_errors"] == []
    and valid_candidate["source_mode"] == "fixture",
)

record = deepcopy(BASE_RECORD)
record["intensity"]["actual"] = None
candidate = parse_fixture_record(record)
check_parser(
    "explicit null actual accepted",
    candidate["actual_intensity"] is None
    and candidate["parse_errors"] == [],
)

record = deepcopy(BASE_RECORD)
del record["intensity"]["actual"]
check_parser(
    "missing actual is not silently null",
    "intensity.actual:missing"
    in parse_fixture_record(record)["parse_errors"],
)

for label, value, expected_error in (
    ("boolean rejected", True, "intensity.actual:not_integer"),
    ("numeric string rejected", "95", "intensity.actual:not_integer"),
    ("int32 overflow rejected", 2**31, "intensity.actual:outside_int32"),
):
    record = deepcopy(BASE_RECORD)
    record["intensity"]["actual"] = value
    check_parser(
        label,
        expected_error in parse_fixture_record(record)["parse_errors"],
    )

record = deepcopy(BASE_RECORD)
record["from"] = "2026-01-01T00:00"
check_parser(
    "timezone-free source timestamp rejected",
    "from:invalid_utc_timestamp"
    in parse_fixture_record(record)["parse_errors"],
)

for label, raw in (
    ("malformed JSON rejected", b'{"data":['),
    ("duplicate JSON key rejected", b'{"data":[],"data":[]}'),
    ("nonstandard JSON constant rejected", b'{"data":NaN}'),
):
    candidates, diagnostics, exclusion = parse_runtime_event(
        synthetic_event(raw), "fixture"
    )
    check_parser(
        label,
        candidates == [] and bool(diagnostics) and exclusion is None,
    )

for label, field, value in (
    ("hash mismatch rejected", "response_sha256", "wrong"),
    ("text mismatch rejected", "raw_text", "wrong"),
    ("record-count mismatch rejected", "record_count", 2),
):
    event = synthetic_event()
    event[field] = value
    candidates, diagnostics, exclusion = parse_runtime_event(
        event, "fixture"
    )
    check_parser(
        label,
        candidates == [] and bool(diagnostics) and exclusion is None,
    )

event = synthetic_event()
event["status"] = "failed"
candidates, diagnostics, exclusion = parse_runtime_event(event, "fixture")
check_parser(
    "failed attempt excluded",
    candidates == [] and diagnostics == [] and exclusion is not None,
)

candidates, diagnostics, exclusion = parse_runtime_event(
    synthetic_event(), "live"
)
check_parser(
    "live admission excludes fixture",
    candidates == [] and diagnostics == [] and exclusion is not None,
)

event = synthetic_event()
event["source_mode"] = "api"
event["is_fixture"] = False
candidates, diagnostics, exclusion = parse_runtime_event(event, "fixture")
check_parser(
    "fixture admission excludes API-labelled event",
    candidates == [] and diagnostics == [] and exclusion is not None,
)

try:
    parse_runtime_event(synthetic_event(), "unknown")
except ValueError:
    PARSER_TESTS.append("unknown admission mode rejected")
else:
    raise AssertionError("Unknown admission mode accepted")

# Exact candidate schema, separate from the final national schema.
CANDIDATE_SCHEMA = T.StructType([
    T.StructField("bronze_event_id", T.StringType(), False),
    T.StructField("bronze_run_id", T.StringType(), False),
    T.StructField("bronze_request_id", T.StringType(), False),
    T.StructField("source", T.StringType(), False),
    T.StructField("source_mode", T.StringType(), False),
    T.StructField("source_retrieved_at_utc", T.TimestampType(), False),
    T.StructField("response_sha256", T.StringType(), False),
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

candidate_df = spark.createDataFrame(
    [valid_candidate], CANDIDATE_SCHEMA
)
returned = candidate_df.collect()

if len(returned) != 1:
    raise AssertionError("Candidate roundtrip changed row count")

normalised = {
    name: utc_value(value) if isinstance(value, datetime) else value
    for name, value in returned[0].asDict(recursive=True).items()
}
if normalised != valid_candidate:
    raise AssertionError("Candidate roundtrip changed values/provenance")

if len(PARSER_TESTS) != 17:
    raise AssertionError("Parser test count incomplete")

print(json.dumps({
    "checkpoint": "runtime_silver_parser_v1",
    "silver_schema_checks_passed": len(SILVER_SCHEMA_EVIDENCE),
    "parser_tests_passed": len(PARSER_TESTS),
    "candidate_column_count": len(CANDIDATE_SCHEMA.fields),
    "candidate_spark_roundtrip": "PASS",
    "admission_version": ADMISSION_VERSION,
    "fixture_labels_preserved": True,
    "persistent_writes": False,
    "api_requests": False,
    "live_source_selection": False,
    "runtime_notebook_complete": False,
}, indent=2))

# COMMAND ----------

# CELL 04 — Explicit-range validation and quarantine construction
VALIDATION_CONTRACT_VERSION = "gridlens_validation_v1+" + PARSER_VERSION
CANONICALISATION_VERSION = "gridlens_canonical_v1"


def require_utc_datetime(value, name):
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset().total_seconds() != 0
    ):
        raise ValueError(f"{name} must be an explicit UTC datetime")


def rejection_identity(event_id, scope, ordinal):
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("Missing Bronze event identity")
    if scope not in ("record", "envelope"):
        raise ValueError("Unknown rejection scope")
    if scope == "record" and (
        type(ordinal) is not int or ordinal < 0
    ):
        raise ValueError("Record rejection needs a nonnegative ordinal")
    if scope == "envelope" and ordinal is not None:
        raise ValueError("Envelope rejection ordinal must be null")

    identity = [
        VALIDATION_CONTRACT_VERSION, event_id, scope, ordinal
    ]
    return hashlib.sha256(
        canonical_json(identity).encode("utf-8")
    ).hexdigest()


def validate_candidate(candidate, publication_start, publication_end):
    require_utc_datetime(publication_start, "publication_start")
    require_utc_datetime(publication_end, "publication_end")
    if publication_start >= publication_end:
        raise ValueError("Publication bounds must increase")
    for bound in (publication_start, publication_end):
        if bound.minute % 30 or bound.second or bound.microsecond:
            raise ValueError("Publication bounds must be half-hour aligned")

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

    # Invalid records must not escape validation through range filtering.
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


def require_processing_metadata(processing_run_id, processed_at_utc):
    if not isinstance(processing_run_id, str) or not processing_run_id:
        raise ValueError("Missing processing run ID")
    require_utc_datetime(processed_at_utc, "processed_at_utc")


def quarantine_record(classified, processing_run_id, processed_at_utc):
    require_processing_metadata(processing_run_id, processed_at_utc)
    if classified["classification"] != "invalid":
        raise ValueError("Only invalid records belong in record quarantine")

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
        "processing_run_id": processing_run_id,
        "processed_at_utc": processed_at_utc,
    }


def quarantine_envelope(
    diagnostic, original_event, processing_run_id, processed_at_utc
):
    require_processing_metadata(processing_run_id, processed_at_utc)
    event = dict(original_event)
    event_id = diagnostic["bronze_event_id"]
    if event.get("event_id") != event_id:
        raise ValueError("Diagnostic/event identity mismatch")

    retrieved = event.get("retrieved_at_utc")
    retrieved = utc_value(retrieved) if isinstance(retrieved, datetime) else None

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
        "raw_record_json": event.get("raw_text"),
        "rejection_reasons": sorted(set(diagnostic["parse_errors"])),
        "parser_version": diagnostic["parser_version"],
        "validation_contract_version": VALIDATION_CONTRACT_VERSION,
        "processing_run_id": processing_run_id,
        "processed_at_utc": processed_at_utc,
    }


print("Validation/quarantine definitions loaded; no writes performed.")

# COMMAND ----------

# CELL 05 — Bounded canonicalisation; no writes
from collections import defaultdict


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


def canonicalise(input_rows, execution_mode):
    if execution_mode not in ("fixture", "live"):
        raise ValueError("Unknown execution mode")
    if len(input_rows) > 100_000:
        raise ValueError("Bounded canonicalisation limit exceeded")

    expected_source_mode = (
        "fixture" if execution_mode == "fixture" else "api"
    )
    audit_rows = []
    eligible_groups = defaultdict(list)
    seen_identities = {}

    for original in input_rows:
        row = dict(original)
        if row["validation_contract_version"] != VALIDATION_CONTRACT_VERSION:
            raise ValueError("Validation contract version mismatch")

        identity = (
            row["bronze_event_id"], row["source_record_ordinal"]
        )
        if identity in seen_identities:
            if seen_identities[identity] != row:
                raise ValueError(
                    "Same source identity has inconsistent content"
                )
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

        if row["classification"] == "invalid":
            audit.update(
                canonical_outcome="invalid",
                canonical_reason="rejected_before_ranking",
            )
            continue
        if row["classification"] == "out_of_range":
            audit.update(
                canonical_outcome="out_of_range",
                canonical_reason="outside_publication_range",
            )
            continue
        if (
            row["classification"] != "valid_in_range"
            or row["validation_errors"] != []
        ):
            raise ValueError("Inconsistent valid classification")

        if row["source_mode"] != expected_source_mode:
            audit.update(
                canonical_outcome="excluded_source",
                canonical_reason="source_mode_not_admitted",
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
        newest_signatures = {item[2] for item in newest}

        if len(newest_signatures) > 1:
            # Block the whole key; do not silently fall back to older data.
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

        winner_audit, _, winner_signature = sorted(
            newest,
            key=lambda item: (
                item[0]["bronze_event_id"],
                item[0]["source_record_ordinal"],
            ),
        )[0]

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
                values_differ_from_winner=signature != winner_signature,
            )

    if not all(row["canonical_outcome"] for row in audit_rows):
        raise ValueError("Unclassified canonicalisation outcome")

    winner_keys = [
        business_key(row)
        for row in audit_rows
        if row["canonical_outcome"] == "winner"
    ]
    if len(winner_keys) != len(set(winner_keys)):
        raise ValueError("Duplicate winner keys")

    return audit_rows, sorted(
        conflicts,
        key=lambda item: (
            item["interval_start_utc"], item["interval_end_utc"]
        ),
    )


print("Canonicalisation definitions loaded; no writes performed.")

# COMMAND ----------

# CELL 06 — Synthetic validation/selection tests and quarantine roundtrip
from datetime import timedelta

FIXTURE_START = typed_utc("2026-01-01T00:00Z")
FIXTURE_END = typed_utc("2026-01-01T01:00Z")

VALIDATION_TESTS = []
CANONICAL_TESTS = []


def check_case(results, name, condition):
    if not condition:
        raise AssertionError(name)
    results.append(name)


def classified_fixture(record=None, event_id=None, retrieval_hours=0):
    record = deepcopy(BASE_RECORD if record is None else record)
    raw = canonical_json({"data": [record]}).encode("utf-8")
    event = synthetic_event(raw)
    if event_id is not None:
        event["event_id"] = event_id
    event["retrieved_at_utc"] += timedelta(hours=retrieval_hours)

    candidates, diagnostics, exclusion = parse_runtime_event(
        event, "fixture"
    )
    if diagnostics or exclusion or len(candidates) != 1:
        raise AssertionError("Unexpected classified-fixture envelope")

    return validate_candidate(
        candidates[0], FIXTURE_START, FIXTURE_END
    )


def select_fixture(rows):
    audit, conflicts = canonicalise(rows, "fixture")
    winners = [
        row for row in audit
        if row["canonical_outcome"] == "winner"
    ]
    return winners, audit, conflicts


# Validation: required values, boundaries and stable rejection identity.
check_case(
    VALIDATION_TESTS, "valid interval",
    classified_fixture()["classification"] == "valid_in_range",
)

for label, value, expected in (
    ("explicit null actual", None, "valid_in_range"),
    ("negative actual", -1, "invalid"),
):
    record = deepcopy(BASE_RECORD)
    record["intensity"]["actual"] = value
    check_case(
        VALIDATION_TESTS, label,
        classified_fixture(record)["classification"] == expected,
    )

record = deepcopy(BASE_RECORD)
del record["intensity"]["actual"]
check_case(
    VALIDATION_TESTS, "missing actual",
    classified_fixture(record)["classification"] == "invalid",
)

record = deepcopy(BASE_RECORD)
record["intensity"]["forecast"] = -1
check_case(
    VALIDATION_TESTS, "negative forecast",
    classified_fixture(record)["classification"] == "invalid",
)

for label, left, right in (
    ("unaligned grid", "2026-01-01T00:15Z", "2026-01-01T00:45Z"),
    ("wrong duration", "2026-01-01T00:00Z", "2026-01-01T01:00Z"),
    ("zero duration", "2026-01-01T00:00Z", "2026-01-01T00:00Z"),
):
    record = {**deepcopy(BASE_RECORD), "from": left, "to": right}
    check_case(
        VALIDATION_TESTS, label,
        classified_fixture(record)["classification"] == "invalid",
    )

record = deepcopy(BASE_RECORD)
record["intensity"]["index"] = " "
check_case(
    VALIDATION_TESTS, "empty source index",
    classified_fixture(record)["classification"] == "invalid",
)

for label, left, right, expected in (
    (
        "interval before lower bound",
        "2025-12-31T23:30Z", "2026-01-01T00:00Z", "out_of_range",
    ),
    (
        "interval ending at upper bound",
        "2026-01-01T00:30Z", "2026-01-01T01:00Z", "valid_in_range",
    ),
    (
        "interval starting at upper bound",
        "2026-01-01T01:00Z", "2026-01-01T01:30Z", "out_of_range",
    ),
):
    record = {**deepcopy(BASE_RECORD), "from": left, "to": right}
    check_case(
        VALIDATION_TESTS, label,
        classified_fixture(record)["classification"] == expected,
    )

record = {
    **deepcopy(BASE_RECORD),
    "from": "2025-12-31T23:30Z",
    "to": "2026-01-01T00:00Z",
}
record["intensity"]["actual"] = -1
invalid_row = classified_fixture(record)

check_case(
    VALIDATION_TESTS, "invalid takes precedence over range",
    invalid_row["classification"] == "invalid",
)

q_record = quarantine_record(
    invalid_row, "fixture-processing-one", FIXTURE_START
)
q_repeat = quarantine_record(
    invalid_row, "fixture-processing-two", FIXTURE_END
)

check_case(
    VALIDATION_TESTS, "rejection identity independent of processing run",
    q_record["rejection_id"] == q_repeat["rejection_id"],
)
check_case(
    VALIDATION_TESTS, "record ordinal participates in identity",
    rejection_identity(invalid_row["bronze_event_id"], "record", 0)
    != rejection_identity(invalid_row["bronze_event_id"], "record", 1),
)
check_case(
    VALIDATION_TESTS, "rejection scope participates in identity",
    rejection_identity(invalid_row["bronze_event_id"], "record", 0)
    != rejection_identity(invalid_row["bronze_event_id"], "envelope", None),
)

malformed_event = synthetic_event(b'{"data":[')
_, diagnostics, exclusion = parse_runtime_event(
    malformed_event, "fixture"
)
if exclusion or len(diagnostics) != 1:
    raise AssertionError("Expected one malformed-envelope diagnostic")

q_envelope = quarantine_envelope(
    diagnostics[0], malformed_event,
    "fixture-envelope-processing", FIXTURE_START,
)
check_case(
    VALIDATION_TESTS, "envelope quarantine identity and content",
    q_envelope["rejection_scope"] == "envelope"
    and q_envelope["source_record_ordinal"] is None
    and q_envelope["raw_record_json"] == malformed_event["raw_text"]
    and q_envelope["rejection_id"] == rejection_identity(
        malformed_event["event_id"], "envelope", None
    ),
)

# Canonicalisation: validate before ranking, preserve revisions and conflicts.
a = classified_fixture(event_id="a")
check_case(
    CANONICAL_TESTS, "single winner",
    len(select_fixture([a])[0]) == 1,
)

record = deepcopy(BASE_RECORD)
record["intensity"]["actual"] = 80
b = classified_fixture(record, "b", 1)
check_case(
    CANONICAL_TESTS, "newer valid revision wins",
    select_fixture([a, b])[0][0]["actual_intensity"] == 80,
)

record["intensity"]["actual"] = -1
newer_invalid = classified_fixture(record, "invalid", 2)
check_case(
    CANONICAL_TESTS, "newer invalid record cannot displace valid record",
    select_fixture([a, newer_invalid])[0][0]["bronze_event_id"] == "a",
)

record["intensity"]["actual"] = None
newer_null = classified_fixture(record, "null", 2)
check_case(
    CANONICAL_TESTS, "newer explicit null wins without substitution",
    select_fixture([a, newer_null])[0][0]["actual_intensity"] is None,
)

equivalent = classified_fixture(event_id="b")
check_case(
    CANONICAL_TESTS, "equivalent equal-time tie is deterministic",
    select_fixture([equivalent, a])[0][0]["bronze_event_id"] == "a",
)

record["intensity"]["actual"] = 70
c = classified_fixture(record, "c", 1)
winners, audit, conflicts = select_fixture([a, b, c])
check_case(
    CANONICAL_TESTS, "newest equal-time conflict blocks entire key",
    winners == []
    and len(conflicts) == 1
    and all(row["canonical_outcome"] == "conflict_blocked" for row in audit),
)

record = {
    **deepcopy(BASE_RECORD),
    "from": "2026-01-01T00:30Z",
    "to": "2026-01-01T01:00Z",
}
other_key = classified_fixture(record, "other")
check_case(
    CANONICAL_TESTS, "distinct interval keys remain distinct",
    len(select_fixture([a, other_key])[0]) == 2,
)

record.update({
    "from": "2026-01-01T01:00Z",
    "to": "2026-01-01T01:30Z",
})
outside = classified_fixture(record, "outside")
check_case(
    CANONICAL_TESTS, "out-of-range record cannot win",
    select_fixture([outside])[0] == [],
)

snapshot = {**a, "source_mode": "snapshot"}
check_case(
    CANONICAL_TESTS, "snapshot excluded",
    select_fixture([snapshot])[1][0]["canonical_outcome"] == "excluded_source",
)

unknown_time = {**a, "source_retrieved_at_utc": None}
check_case(
    CANONICAL_TESTS, "unknown source acquisition time excluded",
    select_fixture([unknown_time])[1][0]["canonical_outcome"] == "excluded_source",
)

identity_order = lambda row: (
    row["bronze_event_id"], row["source_record_ordinal"]
)
check_case(
    CANONICAL_TESTS, "input-order invariance",
    sorted(select_fixture([a, b])[1], key=identity_order)
    == sorted(select_fixture([b, a])[1], key=identity_order),
)

try:
    select_fixture([a, {**a, "actual_intensity": 70}])
except ValueError:
    CANONICAL_TESTS.append("inconsistent source identity blocked")
else:
    raise AssertionError("Inconsistent source identity accepted")

# In-memory roundtrip using the persisted quarantine schema contract.
types_by_name = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "timestamp": T.TimestampType(),
    "array<string>": T.ArrayType(T.StringType(), True),
}
QUARANTINE_SCHEMA = T.StructType([
    T.StructField(name, types_by_name[kind], nullable)
    for name, (kind, nullable) in EXPECTED_QUARANTINE.items()
])

quarantine_rows = [q_record, q_envelope]
for row in quarantine_rows:
    reasons = row["rejection_reasons"]
    if (
        not reasons
        or any(not isinstance(reason, str) or not reason for reason in reasons)
        or reasons != sorted(set(reasons))
    ):
        raise AssertionError("Invalid generated quarantine reasons")

quarantine_df = spark.createDataFrame(
    quarantine_rows, QUARANTINE_SCHEMA
)
returned = [
    row.asDict(recursive=True) for row in quarantine_df.collect()
]


def normalise_quarantine(row):
    return {
        name: utc_value(value) if isinstance(value, datetime) else value
        for name, value in row.items()
    }


before = {
    row["rejection_id"]: normalise_quarantine(row)
    for row in quarantine_rows
}
after = {
    row["rejection_id"]: normalise_quarantine(row)
    for row in returned
}
if len(returned) != 2 or before != after:
    raise AssertionError("Quarantine roundtrip changed rows")

if len(VALIDATION_TESTS) != 17 or len(CANONICAL_TESTS) != 12:
    raise AssertionError("Fixture test counts incomplete")

print(json.dumps({
    "checkpoint": "runtime_silver_validation_selection_v1",
    "validation_identity_tests_passed": len(VALIDATION_TESTS),
    "canonicalisation_tests_passed": len(CANONICAL_TESTS),
    "quarantine_fixture_rows": len(quarantine_rows),
    "quarantine_spark_roundtrip": "PASS",
    "fixture_labels_preserved": all(
        row["source_mode"] == "fixture" for row in quarantine_rows
    ),
    "persistent_writes": False,
    "api_requests": False,
    "live_source_selection": False,
    "runtime_notebook_complete": False,
}, indent=2))

# COMMAND ----------

# CELL 07 — Fixture schemas, destination guards and bounded reads
from pyspark.sql import functions as F

if globals().get("SILVER_FIXTURE_ACTIVE", False):
    raise RuntimeError(
        "Previous Silver fixtures may remain. Resolve cleanup before rerunning."
    )

fixture_types = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "timestamp": T.TimestampType(),
    "array<string>": T.ArrayType(T.StringType(), True),
}

NATIONAL_SCHEMA = T.StructType([
    T.StructField(name, fixture_types[kind], nullable)
    for name, (kind, nullable) in EXPECTED_NATIONAL.items()
])

QUARANTINE_SCHEMA = T.StructType([
    T.StructField(name, fixture_types[kind], nullable)
    for name, (kind, nullable) in EXPECTED_QUARANTINE.items()
])


class MergeGuardError(ValueError):
    pass


def schema_signature(schema):
    return [
        (field.name, field.dataType.json(), field.nullable)
        for field in schema.fields
    ]


def quoted_fixture(table):
    if not re.fullmatch(
        r"workspace\.silver\._gridlens_runtime_silver_"
        r"[0-9a-f]{32}_(national|quarantine)",
        table,
    ):
        raise MergeGuardError("Unapproved fixture table name")
    return ".".join(f"`{part}`" for part in table.split("."))


def require_fixture_destination(table, role):
    if not globals().get("SILVER_FIXTURE_ACTIVE", False):
        raise MergeGuardError("Fixture lifecycle is not active")

    expected_name = {
        "national": globals().get("SILVER_FIXTURE_NATIONAL"),
        "quarantine": globals().get("SILVER_FIXTURE_QUARANTINE"),
    }.get(role)

    if table != expected_name or expected_name is None:
        raise MergeGuardError("Destination is not this test's approved fixture")

    sql_table = quoted_fixture(table)
    expected_schema = (
        NATIONAL_SCHEMA if role == "national" else QUARANTINE_SCHEMA
    )
    actual_schema = spark.table(table).schema

    if schema_signature(actual_schema) != schema_signature(expected_schema):
        raise MergeGuardError("Destination schema mismatch; no evolution")

    detail = spark.sql(f"DESCRIBE DETAIL {sql_table}").first()
    if detail["format"] != "delta":
        raise MergeGuardError("Fixture destination is not Delta")


def normalise_stored(row):
    data = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    return {
        name: utc_value(value) if isinstance(value, datetime) else value
        for name, value in data.items()
    }


def bounded_rows(df):
    rows = df.limit(100_001).collect()
    if len(rows) > 100_000:
        raise MergeGuardError("Bounded fixture inspection limit exceeded")
    return [normalise_stored(row) for row in rows]


def fixture_snapshot(table, role):
    require_fixture_destination(table, role)
    rows = bounded_rows(spark.table(table))
    if role == "national":
        return sorted(rows, key=business_key)
    return sorted(rows, key=lambda row: row["rejection_id"])


def require_unique(df, keys):
    if df.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
        raise MergeGuardError(f"Duplicate source/target keys: {keys}")

    null_key = F.lit(False)
    for key in keys:
        null_key = null_key | F.col(key).isNull()
    if df.where(null_key).limit(1).count():
        raise MergeGuardError(f"Null source/target keys: {keys}")


print("Fixture schemas/guards loaded; no tables created or written.")

# COMMAND ----------

# CELL 08 — Guarded fixture-only MERGEs
def check_national_values(df):
    for row in bounded_rows(df):
        for name, (_, nullable) in EXPECTED_NATIONAL.items():
            if not nullable and row[name] is None:
                raise MergeGuardError(f"Missing required national value: {name}")

        if row["source_mode"] != "fixture":
            raise MergeGuardError("Fixture national writes require fixture labels")

        for name in ("forecast_intensity", "actual_intensity"):
            value = row[name]
            if value is None and name == "actual_intensity":
                continue
            if type(value) is not int or not 0 <= value <= 2**31 - 1:
                raise MergeGuardError(f"Invalid intensity: {name}")

        if (
            not isinstance(row["source_index"], str)
            or not row["source_index"].strip()
        ):
            raise MergeGuardError("Missing/empty source index")

        start = row["interval_start_utc"]
        end = row["interval_end_utc"]
        for name, value in (
            ("interval_start_utc", start),
            ("interval_end_utc", end),
            ("source_retrieved_at_utc", row["source_retrieved_at_utc"]),
        ):
            if (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset().total_seconds() != 0
            ):
                raise MergeGuardError(f"Invalid UTC timestamp: {name}")

        if (end - start).total_seconds() != 1800:
            raise MergeGuardError("Invalid interval duration")
        for value in (start, end):
            if value.minute % 30 or value.second or value.microsecond:
                raise MergeGuardError("Interval is not on the half-hour grid")

        for name in (
            "source", "bronze_event_id", "bronze_run_id", "bronze_request_id",
            "response_sha256", "record_value_sha256",
        ):
            if not isinstance(row[name], str) or not row[name]:
                raise MergeGuardError(f"Missing national provenance: {name}")

        if (
            type(row["source_record_ordinal"]) is not int
            or row["source_record_ordinal"] < 0
        ):
            raise MergeGuardError("Invalid source record ordinal")

        if (
            row["parser_version"] != PARSER_VERSION
            or row["validation_contract_version"] != VALIDATION_CONTRACT_VERSION
            or row["canonicalisation_version"] != CANONICALISATION_VERSION
        ):
            raise MergeGuardError("National contract version mismatch")

        expected_hash = hashlib.sha256(
            business_signature(row).encode("utf-8")
        ).hexdigest()
        if row["record_value_sha256"] != expected_hash:
            raise MergeGuardError("National business-value hash mismatch")


def check_quarantine_values(df):
    for row in bounded_rows(df):
        for name, (_, nullable) in EXPECTED_QUARANTINE.items():
            if not nullable and row[name] is None:
                raise MergeGuardError(f"Missing required quarantine value: {name}")

        if row["source_mode"] != "fixture":
            raise MergeGuardError("Fixture quarantine requires fixture labels")

        reasons = row["rejection_reasons"]
        if (
            not isinstance(reasons, list)
            or not reasons
            or any(not isinstance(reason, str) or not reason for reason in reasons)
            or reasons != sorted(set(reasons))
        ):
            raise MergeGuardError("Invalid quarantine reasons")

        if (
            row["parser_version"] != PARSER_VERSION
            or row["validation_contract_version"] != VALIDATION_CONTRACT_VERSION
        ):
            raise MergeGuardError("Quarantine contract version mismatch")

        try:
            expected_id = rejection_identity(
                row["bronze_event_id"],
                row["rejection_scope"],
                row["source_record_ordinal"],
            )
        except ValueError as exc:
            raise MergeGuardError(str(exc)) from exc

        if row["rejection_id"] != expected_id:
            raise MergeGuardError("Quarantine identity hash mismatch")

        if not isinstance(row["processing_run_id"], str) or not row["processing_run_id"]:
            raise MergeGuardError("Missing quarantine processing run ID")


def execute_fixture_merge(table, role, source_df, keys, update_condition=None):
    require_fixture_destination(table, role)
    sql_table = quoted_fixture(table)
    view = "_gridlens_runtime_merge_source_" + uuid.uuid4().hex
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
            MERGE INTO {sql_table} AS t
            USING `{view}` AS s
            ON {match}
            {matched_clause}
            WHEN NOT MATCHED THEN
                INSERT ({names}) VALUES ({values})
        """)
    finally:
        spark.catalog.dropTempView(view)


def merge_national_fixture(table, source_df):
    require_fixture_destination(table, "national")
    if schema_signature(source_df.schema) != schema_signature(NATIONAL_SCHEMA):
        raise MergeGuardError("Unexpected national source schema")

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

    if pairs.where(
        (
            F.col("s.source_retrieved_at_utc")
            == F.col("t.source_retrieved_at_utc")
        ) & ~equivalent
    ).limit(1).count():
        raise MergeGuardError("Equal-time source/target business-value conflict")

    execute_fixture_merge(
        table, "national", source_df, keys,
        update_condition=(
            "s.source_retrieved_at_utc > t.source_retrieved_at_utc"
        ),
    )


def merge_quarantine_fixture(table, source_df):
    require_fixture_destination(table, "quarantine")
    if schema_signature(source_df.schema) != schema_signature(QUARANTINE_SCHEMA):
        raise MergeGuardError("Unexpected quarantine source schema")

    target_df = spark.table(table)
    require_unique(source_df, ["rejection_id"])
    require_unique(target_df, ["rejection_id"])
    check_quarantine_values(source_df)
    check_quarantine_values(target_df)

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
        raise MergeGuardError("Rejection identity has inconsistent content")

    execute_fixture_merge(
        table, "quarantine", source_df, ["rejection_id"]
    )


print("Guarded fixture MERGE definitions loaded; no writes performed.")

# COMMAND ----------

# CELL 09 — Approved disposable Silver MERGE tests and cleanup
if globals().get("SILVER_FIXTURE_ACTIVE", False):
    raise RuntimeError("Previous Silver fixtures may remain; stop and review")

nonce = uuid.uuid4().hex
SILVER_FIXTURE_NATIONAL = (
    f"workspace.silver._gridlens_runtime_silver_{nonce}_national"
)
SILVER_FIXTURE_QUARANTINE = (
    f"workspace.silver._gridlens_runtime_silver_{nonce}_quarantine"
)

fixture_specs = [
    (SILVER_FIXTURE_NATIONAL, "national", NATIONAL_SCHEMA),
    (SILVER_FIXTURE_QUARANTINE, "quarantine", QUARANTINE_SCHEMA),
]

# Never adopt or clean up a pre-existing table.
for table, _, _ in fixture_specs:
    quoted_fixture(table)
    if spark.catalog.tableExists(table):
        raise RuntimeError(f"Unexpected name collision: {table}")

SILVER_FIXTURE_ACTIVE = True
creation_intents = []
MERGE_TESTS = []
EVIDENCE_09 = {
    "checkpoint": "runtime_silver_fixture_merges_v1",
    "tests_passed": 0,
    "national_rows_before_cleanup": None,
    "quarantine_rows_before_cleanup": None,
    "cleanup": {},
    "persistent_fixture_writes": True,
    "live_table_writes": False,
    "api_requests": False,
    "runtime_notebook_complete": False,
}

print("NATIONAL FIXTURE:", SILVER_FIXTURE_NATIONAL)
print("QUARANTINE FIXTURE:", SILVER_FIXTURE_QUARANTINE)


def check_merge(name, condition):
    if not condition:
        raise AssertionError(name)
    MERGE_TESTS.append(name)


def national_row(record=None, event_id=None, retrieval_hours=0):
    classified = classified_fixture(
        record, event_id, retrieval_hours
    )
    audit, conflicts = canonicalise([classified], "fixture")
    winners = [
        row for row in audit
        if row["canonical_outcome"] == "winner"
    ]
    if conflicts or len(winners) != 1:
        raise MergeGuardError("Fixture did not produce exactly one valid winner")
    return {
        name: winners[0][name] for name in EXPECTED_NATIONAL
    }


def national_df(rows):
    return spark.createDataFrame(rows, NATIONAL_SCHEMA)


def quarantine_source(rows):
    return spark.createDataFrame(rows, QUARANTINE_SCHEMA)


def expect_guard(name, operation, table, role):
    before = fixture_snapshot(table, role)
    try:
        operation()
    except MergeGuardError:
        check_merge(name, fixture_snapshot(table, role) == before)
    else:
        raise AssertionError(f"Expected guard did not fire: {name}")


try:
    for table, role, schema in fixture_specs:
        ddl = ", ".join(
            f"`{field.name}` {field.dataType.simpleString()}"
            + ("" if field.nullable else " NOT NULL")
            for field in schema.fields
        )
        # Track intent before CREATE in case acknowledgement is uncertain.
        creation_intents.append((table, role))
        spark.sql(
            f"CREATE TABLE {quoted_fixture(table)} ({ddl}) USING DELTA"
        )
        require_fixture_destination(table, role)

    nt = SILVER_FIXTURE_NATIONAL
    qt = SILVER_FIXTURE_QUARANTINE

    # 1–2: insert and identical full-row replay.
    initial = national_row(event_id="fixture-initial")
    merge_national_fixture(nt, national_df([initial]))
    check_merge("first insert", fixture_snapshot(nt, "national") == [initial])

    before = fixture_snapshot(nt, "national")
    merge_national_fixture(nt, national_df([initial]))
    check_merge("identical full-row replay", fixture_snapshot(nt, "national") == before)

    # 3: newer valid revision.
    record = deepcopy(BASE_RECORD)
    record["intensity"]["actual"] = 80
    latest = national_row(record, "fixture-newer", 1)
    merge_national_fixture(nt, national_df([latest]))
    check_merge("newer valid revision", fixture_snapshot(nt, "national") == [latest])

    # 4: older incoming row must not regress values/provenance.
    before = fixture_snapshot(nt, "national")
    merge_national_fixture(nt, national_df([initial]))
    check_merge("stale input no-op", fixture_snapshot(nt, "national") == before)

    # 5: equal-time equivalent values preserve the stored provenance.
    equivalent = national_row(record, "fixture-equivalent", 1)
    merge_national_fixture(nt, national_df([equivalent]))
    check_merge("equivalent equal-time provenance preserved", fixture_snapshot(nt, "national") == before)

    # 6: equal-time conflicting values stop before MERGE.
    record["intensity"]["actual"] = 70
    conflict = national_row(record, "fixture-conflict", 1)
    expect_guard(
        "equal-time conflict blocked",
        lambda: merge_national_fixture(nt, national_df([conflict])),
        nt, "national",
    )

    # 7: newer explicit null remains null; forecast never substitutes.
    record["intensity"]["actual"] = None
    newest_null = national_row(record, "fixture-newer-null", 2)
    merge_national_fixture(nt, national_df([newest_null]))
    check_merge(
        "newer null without substitution",
        fixture_snapshot(nt, "national") == [newest_null]
        and newest_null["actual_intensity"] is None,
    )

    # 8: insert another key without altering the unmatched existing key.
    other_record = {
        **deepcopy(BASE_RECORD),
        "from": "2026-01-01T00:30Z",
        "to": "2026-01-01T01:00Z",
    }
    second = national_row(other_record, "fixture-second-key")
    merge_national_fixture(nt, national_df([second]))
    check_merge(
        "second key and unmatched-row preservation",
        fixture_snapshot(nt, "national")
        == sorted([newest_null, second], key=business_key),
    )

    # 9–11: duplicate keys, source schema and value-hash guards.
    expect_guard(
        "duplicate source keys blocked",
        lambda: merge_national_fixture(
            nt, national_df([newest_null, deepcopy(newest_null)])
        ),
        nt, "national",
    )
    expect_guard(
        "source schema mismatch blocked",
        lambda: merge_national_fixture(
            nt, national_df([newest_null]).drop("forecast_intensity")
        ),
        nt, "national",
    )
    bad_hash = {**newest_null, "record_value_sha256": "wrong"}
    expect_guard(
        "business-value hash mismatch blocked",
        lambda: merge_national_fixture(nt, national_df([bad_hash])),
        nt, "national",
    )

    # 12–14: quarantine insert, replay and identity/content collision.
    q_rows = [deepcopy(q_record), deepcopy(q_envelope)]
    merge_quarantine_fixture(qt, quarantine_source(q_rows))
    q_before = fixture_snapshot(qt, "quarantine")
    check_merge(
        "quarantine first insert",
        q_before == sorted(q_rows, key=lambda row: row["rejection_id"]),
    )

    replay = deepcopy(q_rows)
    for row in replay:
        row["processing_run_id"] = "fixture-later-processing"
        row["processed_at_utc"] += timedelta(hours=1)
    merge_quarantine_fixture(qt, quarantine_source(replay))
    check_merge(
        "quarantine replay preserves first processing metadata",
        fixture_snapshot(qt, "quarantine") == q_before,
    )

    collision = deepcopy(q_rows[0])
    collision["raw_record_json"] = "synthetic changed rejection content"
    expect_guard(
        "quarantine identity/content collision blocked",
        lambda: merge_quarantine_fixture(
            qt, quarantine_source([collision])
        ),
        qt, "quarantine",
    )

    # 15–16: honest labels and nonnegative national values.
    wrong_label = {**newest_null, "source_mode": "api"}
    expect_guard(
        "API-labelled row blocked from fixture publication",
        lambda: merge_national_fixture(nt, national_df([wrong_label])),
        nt, "national",
    )

    negative = {**newest_null, "forecast_intensity": -1}
    negative["record_value_sha256"] = hashlib.sha256(
        business_signature(negative).encode("utf-8")
    ).hexdigest()
    expect_guard(
        "negative intensity blocked",
        lambda: merge_national_fixture(nt, national_df([negative])),
        nt, "national",
    )

    # 17–18: quarantine source-key and reason-content guards.
    expect_guard(
        "duplicate rejection IDs blocked",
        lambda: merge_quarantine_fixture(
            qt, quarantine_source([q_rows[0], deepcopy(q_rows[0])])
        ),
        qt, "quarantine",
    )

    bad_reasons = deepcopy(q_rows[0])
    bad_reasons["rejection_reasons"] = [None]
    expect_guard(
        "null rejection reason blocked",
        lambda: merge_quarantine_fixture(
            qt, quarantine_source([bad_reasons])
        ),
        qt, "quarantine",
    )

    EVIDENCE_09["national_rows_before_cleanup"] = len(
        fixture_snapshot(nt, "national")
    )
    EVIDENCE_09["quarantine_rows_before_cleanup"] = len(
        fixture_snapshot(qt, "quarantine")
    )

except Exception as exc:
    EVIDENCE_09["failure_type"] = type(exc).__name__
    EVIDENCE_09["failure_message"] = str(exc)[:700]
    raise

finally:
    EVIDENCE_09["tests_passed"] = len(MERGE_TESTS)

    for table, role in creation_intents:
        try:
            expected_name = (
                SILVER_FIXTURE_NATIONAL
                if role == "national"
                else SILVER_FIXTURE_QUARANTINE
            )
            if table != expected_name:
                raise RuntimeError("Cleanup destination guard failed")

            spark.sql(
                f"DROP TABLE IF EXISTS {quoted_fixture(table)}"
            )
            if spark.catalog.tableExists(table):
                raise RuntimeError("Fixture table remains after DROP")

            EVIDENCE_09["cleanup"][role] = "PASS"
        except Exception as cleanup_exc:
            EVIDENCE_09["cleanup"][role] = "UNRESOLVED"
            EVIDENCE_09[f"{role}_cleanup_error"] = str(cleanup_exc)[:700]

    cleanup_ok = (
        len(EVIDENCE_09["cleanup"]) == len(creation_intents)
        and all(
            value == "PASS"
            for value in EVIDENCE_09["cleanup"].values()
        )
    )
    if cleanup_ok:
        SILVER_FIXTURE_ACTIVE = False

    print("CHECKPOINT EVIDENCE")
    print(json.dumps(EVIDENCE_09, indent=2))

if EVIDENCE_09["cleanup"] != {
    "national": "PASS", "quarantine": "PASS"
}:
    raise RuntimeError("Silver fixture cleanup incomplete; stop and review")

if len(MERGE_TESTS) != 18:
    raise RuntimeError("Silver MERGE test count incomplete")

print("PASS: 18 disposable Silver MERGE tests and cleanup.")

# COMMAND ----------

# CELL 10 — Explicit Silver handoff contract; no default source run
import math
from collections import defaultdict
from datetime import timedelta


def bind_silver_handoff(raw, execution_mode):
    if execution_mode not in ("fixture", "live"):
        raise ValueError("Unknown execution mode")

    required = {
        "source_run_ids_json",
        "publication_start_utc",
        "publication_end_utc",
        "source_mode",
        "orchestration_id",
        "chunk_days",
        "run_anchor_utc",
        "policy_version",
    }
    if set(raw) != required:
        raise ValueError("Handoff keys differ from the supported contract")

    values = {}
    for name in required:
        value = raw[name]
        if (
            not isinstance(value, str)
            or not value.strip()
            or "{{" in value
            or "}}" in value
        ):
            raise ValueError(f"Missing/unresolved handoff field: {name}")
        values[name] = value.strip()

    source_ids = strict_json_loads(values["source_run_ids_json"])
    if (
        not isinstance(source_ids, list)
        or len(source_ids) != 1
        or not isinstance(source_ids[0], str)
        or not re.fullmatch(r"[0-9a-f]{32}", source_ids[0])
    ):
        raise ValueError("Exactly one explicit ingestion UUID is required")

    expected_mode = "fixture" if execution_mode == "fixture" else "api"
    if values["source_mode"] != expected_mode:
        raise ValueError("Source mode/admission mismatch")

    if (
        execution_mode == "live"
        and not re.fullmatch(r"[0-9]+", values["orchestration_id"])
    ):
        raise ValueError("Invalid job orchestration ID")

    if values["policy_version"] != "gridlens_ops_v1":
        raise ValueError("Unsupported quality policy")

    if not re.fullmatch(r"[0-9]+", values["chunk_days"]):
        raise ValueError("Invalid chunk_days")
    chunk_days = int(values["chunk_days"])
    if not 1 <= chunk_days <= 14:
        raise ValueError("chunk_days must be 1–14")

    start = typed_utc(values["publication_start_utc"])
    end = typed_utc(values["publication_end_utc"])
    anchor = typed_utc(values["run_anchor_utc"])

    if start >= end or end - start > timedelta(days=14):
        raise ValueError("Invalid/beyond-cap publication range")

    for bound in (start, end):
        if bound.minute % 30 or bound.second or bound.microsecond:
            raise ValueError("Publication bounds must be half-hour aligned")

    anchor_floor = anchor.replace(
        minute=(anchor.minute // 30) * 30,
        second=0,
        microsecond=0,
    )
    if end > anchor_floor:
        raise ValueError("Publication end exceeds the fixed run anchor")

    windows = []
    cursor = start
    while cursor < end:
        next_cursor = min(cursor + timedelta(days=chunk_days), end)
        windows.append((cursor, next_cursor))
        cursor = next_cursor

    return {
        "execution_mode": execution_mode,
        "source_mode": expected_mode,
        "source_run_ids": source_ids,
        "publication_start": start,
        "publication_end": end,
        "run_anchor_utc": anchor,
        "chunk_days": chunk_days,
        "expected_windows": windows,
        "orchestration_id": values["orchestration_id"],
        "policy_version": values["policy_version"],
    }


print("Silver handoff binder loaded; no source selected or written.")

# COMMAND ----------

# CELL 11 — Complete-run selection from supplied event rows
def select_complete_source_events(events, contract):
    source_ids = set(contract["source_run_ids"])
    selected = [
        normalise_stored(event)
        for event in events
        if event.get("run_id") in source_ids
    ]

    if not selected or len(selected) > 128:
        raise ValueError("Missing/oversized selected run")

    event_ids = []
    request_groups = defaultdict(list)
    body_bytes = 0

    for event in selected:
        for name in (
            "event_id", "run_id", "request_id", "source", "request_url",
        ):
            if not isinstance(event.get(name), str) or not event[name]:
                raise ValueError(f"Missing source identity/provenance: {name}")

        event_ids.append(event["event_id"])

        if (
            event.get("source_mode") != contract["source_mode"]
            or event.get("is_fixture")
            is not (contract["execution_mode"] == "fixture")
        ):
            raise ValueError("Selected run has mixed/wrong source admission")

        for name in (
            "request_from_utc", "request_to_utc",
            "started_at_utc", "finished_at_utc", "stored_at_utc",
        ):
            value = event.get(name)
            if (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset().total_seconds() != 0
            ):
                raise ValueError(f"Invalid source timestamp: {name}")

        left = event["request_from_utc"]
        right = event["request_to_utc"]
        expected_url = (
            "https://api.carbonintensity.org.uk/intensity/"
            + left.strftime("%Y-%m-%dT%H:%MZ")
            + "/"
            + right.strftime("%Y-%m-%dT%H:%MZ")
        )
        if event["request_url"] != expected_url:
            raise ValueError("Request URL/bounds mismatch")

        if event["status"] not in ("succeeded", "failed"):
            raise ValueError("Unknown attempt status")

        raw = event.get("raw_body")
        if raw is None:
            if (
                event.get("raw_text") is not None
                or event.get("response_sha256") is not None
                or event.get("retrieved_at_utc") is not None
                or event.get("event_kind") != "transport_failure"
            ):
                raise ValueError("Bodyless attempt has invalid provenance")
        else:
            if not isinstance(raw, (bytes, bytearray, memoryview)):
                raise ValueError("Invalid response bytes")

            raw = bytes(raw)
            body_bytes += len(raw)

            if (
                hashlib.sha256(raw).hexdigest()
                != event.get("response_sha256")
                or raw.decode("utf-8", errors="replace")
                != event.get("raw_text")
            ):
                raise ValueError("Raw response hash/text mismatch")

            if (
                not isinstance(event.get("retrieved_at_utc"), datetime)
                or event.get("event_kind") != "response"
            ):
                raise ValueError("Response acquisition provenance missing")

        request_groups[event["request_id"]].append(event)

    if len(event_ids) != len(set(event_ids)):
        raise ValueError("Duplicate selected event IDs")
    if body_bytes > 32 * 1024 * 1024:
        raise ValueError("Selected response bytes exceed the inspection cap")

    successful_events = []
    observed_windows = []

    for request_id, attempts in request_groups.items():
        bounds = {
            (event["request_from_utc"], event["request_to_utc"])
            for event in attempts
        }
        if len(bounds) != 1:
            raise ValueError("Request identity reused across different windows")
        observed_windows.append(next(iter(bounds)))

        if any(type(event["attempt_number"]) is not int for event in attempts):
            raise ValueError("Invalid attempt ordinal")

        attempts = sorted(attempts, key=lambda event: event["attempt_number"])
        if (
            len(attempts) > 6
            or [event["attempt_number"] for event in attempts]
            != list(range(1, len(attempts) + 1))
        ):
            raise ValueError("Attempt sequence invalid")

        for event in attempts[:-1]:
            if event["status"] != "failed":
                raise ValueError("Attempt after an earlier success")

            retryable = (
                event.get("failure_kind") == "transport_error"
                and event.get("http_status") is None
            ) or (
                event.get("failure_kind") == "http_error"
                and event.get("http_status") in (408, 429, 500, 502, 503, 504)
            )
            delay = event.get("retry_delay_seconds")
            if (
                not retryable
                or type(delay) not in (int, float)
                or not math.isfinite(delay)
                or not 0 <= delay <= 120
            ):
                raise ValueError("Invalid retained retry transition")

        terminal = attempts[-1]
        if (
            terminal["status"] != "succeeded"
            or terminal.get("http_status") != 200
            or terminal.get("event_kind") != "response"
        ):
            raise ValueError("Selected run contains an incomplete/failed request")

        candidates, diagnostics, exclusion = parse_runtime_event(
            terminal, contract["execution_mode"]
        )
        if diagnostics or exclusion or not candidates:
            raise ValueError(
                "Successful attempt has invalid envelope/admission"
            )

        successful_events.append(terminal)

    if sorted(observed_windows) != sorted(contract["expected_windows"]):
        raise ValueError("Selected run is not an exact complete request plan")

    return sorted(
        successful_events,
        key=lambda event: (
            event["request_from_utc"], event["request_id"]
        ),
    ), {
        "selected_attempt_events": len(selected),
        "selected_success_events": len(successful_events),
        "retained_failed_attempt_events": sum(
            event["status"] == "failed" for event in selected
        ),
        "verified_request_windows": len(observed_windows),
    }


print("Complete-run selector loaded; no source selected or written.")

# COMMAND ----------

# CELL 12 — Synthetic handoff/source-selection tests; no table writes
HANDOFF_TESTS = []
SELECTION_TESTS = []

source_run_id = uuid.uuid4().hex
request_id = uuid.uuid4().hex

TEST_HANDOFF = {
    "source_run_ids_json": json.dumps([source_run_id]),
    "publication_start_utc": "2026-01-01T00:00Z",
    "publication_end_utc": "2026-01-01T00:30Z",
    "source_mode": "fixture",
    "orchestration_id": "fixture-orchestration",
    "chunk_days": "14",
    "run_anchor_utc": "2026-01-02T01:00:00Z",
    "policy_version": "gridlens_ops_v1",
}

contract = bind_silver_handoff(TEST_HANDOFF, "fixture")
if (
    contract["source_run_ids"] != [source_run_id]
    or len(contract["expected_windows"]) != 1
):
    raise AssertionError("Valid handoff binding failed")
HANDOFF_TESTS.append("valid explicit handoff")

for label, override in (
    ("missing source IDs", {"source_run_ids_json": ""}),
    (
        "unresolved source IDs",
        {"source_run_ids_json": "{{tasks.ingest_national.values.ids}}"},
    ),
    (
        "duplicate/multiple source IDs",
        {"source_run_ids_json": json.dumps([source_run_id, source_run_id])},
    ),
    ("wrong source mode", {"source_mode": "api"}),
    (
        "unaligned publication bound",
        {"publication_end_utc": "2026-01-01T00:15Z"},
    ),
    (
        "oversized publication range",
        {"publication_start_utc": "2025-12-01T00:00Z"},
    ),
    ("unknown policy", {"policy_version": "unknown"}),
    (
        "timezone-free run anchor",
        {"run_anchor_utc": "2026-01-02T01:00:00"},
    ),
):
    try:
        bind_silver_handoff({**TEST_HANDOFF, **override}, "fixture")
    except ValueError:
        HANDOFF_TESTS.append(label)
    else:
        raise AssertionError(f"Invalid handoff accepted: {label}")


def source_fixture_event(number, succeeded):
    raw = canonical_json(
        {"data": [BASE_RECORD]}
        if succeeded
        else {"error": "synthetic HTTP 503"}
    ).encode("utf-8")
    when = typed_utc("2026-01-02T00:00Z") + timedelta(seconds=number * 3)

    return {
        "event_id": uuid.uuid4().hex,
        "run_id": source_run_id,
        "request_id": request_id,
        "attempt_number": number,
        "source": "synthetic_source_selection_fixture",
        "source_mode": "fixture",
        "is_fixture": True,
        "event_kind": "response",
        "request_url": (
            "https://api.carbonintensity.org.uk/intensity/"
            "2026-01-01T00:00Z/2026-01-01T00:30Z"
        ),
        "request_from_utc": contract["publication_start"],
        "request_to_utc": contract["publication_end"],
        "started_at_utc": when - timedelta(seconds=1),
        "finished_at_utc": when + timedelta(seconds=1),
        "retrieved_at_utc": when,
        "stored_at_utc": when + timedelta(seconds=2),
        "http_status": 200 if succeeded else 503,
        "raw_body": raw,
        "raw_text": raw.decode("utf-8"),
        "response_sha256": hashlib.sha256(raw).hexdigest(),
        "status": "succeeded" if succeeded else "failed",
        "failure_kind": None if succeeded else "http_error",
        "error": None if succeeded else "synthetic HTTP 503",
        "record_count": 1 if succeeded else None,
        "retry_delay_seconds": None if succeeded else 2.0,
        "response_headers_json": "{}",
    }


events = [
    source_fixture_event(1, False),
    source_fixture_event(2, True),
]
foreign = source_fixture_event(1, True)
foreign["run_id"] = uuid.uuid4().hex
foreign["request_id"] = uuid.uuid4().hex

selected_events, source_metrics = select_complete_source_events(
    events + [foreign], contract
)
if (
    len(selected_events) != 1
    or selected_events[0]["event_id"] != events[1]["event_id"]
    or source_metrics != {
        "selected_attempt_events": 2,
        "selected_success_events": 1,
        "retained_failed_attempt_events": 1,
        "verified_request_windows": 1,
    }
):
    raise AssertionError("Explicit selection/failed-attempt retention failed")
SELECTION_TESTS.append("complete declared run only; failed retry retained")

for label in (
    "duplicate event ID",
    "attempt sequence gap",
    "attempt after earlier success",
    "missing successful terminal attempt",
    "mixed source admission",
    "failed-attempt hash corruption",
    "successful-attempt envelope defect",
    "request URL mismatch",
    "missing planned request window",
):
    altered = deepcopy(events)
    test_contract = contract

    if label == "duplicate event ID":
        altered.append(deepcopy(altered[0]))
    elif label == "attempt sequence gap":
        altered[1]["attempt_number"] = 3
    elif label == "attempt after earlier success":
        altered[0]["status"] = "succeeded"
    elif label == "missing successful terminal attempt":
        altered = altered[:1]
    elif label == "mixed source admission":
        altered[0]["source_mode"] = "api"
    elif label == "failed-attempt hash corruption":
        altered[0]["response_sha256"] = "wrong"
    elif label == "successful-attempt envelope defect":
        raw = b'{"data":[]}'
        altered[1].update({
            "raw_body": raw,
            "raw_text": raw.decode("utf-8"),
            "response_sha256": hashlib.sha256(raw).hexdigest(),
            "record_count": 0,
        })
    elif label == "request URL mismatch":
        altered[0]["request_url"] = "synthetic-wrong-url"
    else:
        # Reconstruct a real two-window plan, but supply only its first window.
        test_contract = bind_silver_handoff({
            **TEST_HANDOFF,
            "publication_end_utc": "2026-01-02T00:30Z",
            "chunk_days": "1",
        }, "fixture")
        first_left, first_right = test_contract["expected_windows"][0]
        for event in altered:
            event["request_from_utc"] = first_left
            event["request_to_utc"] = first_right
            event["request_url"] = (
                "https://api.carbonintensity.org.uk/intensity/"
                + first_left.strftime("%Y-%m-%dT%H:%MZ")
                + "/"
                + first_right.strftime("%Y-%m-%dT%H:%MZ")
            )

    try:
        select_complete_source_events(altered, test_contract)
    except ValueError:
        SELECTION_TESTS.append(label)
    else:
        raise AssertionError(f"Invalid source run accepted: {label}")

# Explicit 25-column Bronze schema for the in-memory fixture roundtrip.
SOURCE_BRONZE_EXPECTED = {
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
source_types = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "boolean": T.BooleanType(),
    "timestamp": T.TimestampType(),
    "binary": T.BinaryType(),
    "double": T.DoubleType(),
}
SOURCE_BRONZE_SCHEMA = T.StructType([
    T.StructField(name, source_types[kind], nullable)
    for name, (kind, nullable) in SOURCE_BRONZE_EXPECTED.items()
])

source_df = spark.createDataFrame(
    events + [foreign], SOURCE_BRONZE_SCHEMA
)
roundtrip_events = [
    row.asDict(recursive=True) for row in source_df.collect()
]
roundtrip_selected, roundtrip_metrics = select_complete_source_events(
    roundtrip_events, contract
)
if (
    roundtrip_metrics != source_metrics
    or [event["event_id"] for event in roundtrip_selected]
    != [event["event_id"] for event in selected_events]
):
    raise AssertionError("Spark roundtrip changed source selection")

if len(HANDOFF_TESTS) != 9 or len(SELECTION_TESTS) != 10:
    raise AssertionError("Handoff/source-selection tests incomplete")

print(json.dumps({
    "checkpoint": "silver_handoff_source_selection_v1",
    "handoff_tests_passed": len(HANDOFF_TESTS),
    "source_selection_tests_passed": len(SELECTION_TESTS),
    "bronze_fixture_rows": len(roundtrip_events),
    **roundtrip_metrics,
    "bronze_spark_roundtrip": "PASS",
    "persistent_writes": False,
    "live_source_selection": False,
    "task_values_emitted": False,
    "runtime_notebook_complete": False,
}, indent=2))

# COMMAND ----------

# CELL 13 — Explicit-table, explicit-run, version-pinned Delta reader
def quoted_source_table(table, execution_mode):
    if execution_mode == "fixture":
        if not re.fullmatch(
            r"workspace\.bronze\._gridlens_runtime_fixture_[0-9a-f]{32}",
            table,
        ):
            raise ValueError("Fixture mode requires a disposable Bronze table")
    elif execution_mode == "live":
        if table != "workspace.bronze.api_responses":
            raise ValueError("Unapproved live Bronze destination")
    else:
        raise ValueError("Unknown execution mode")

    return ".".join(f"`{part}`" for part in table.split("."))


def read_bound_bronze(table, contract, version=None):
    sql_table = quoted_source_table(table, contract["execution_mode"])

    detail = spark.sql(f"DESCRIBE DETAIL {sql_table}").first()
    if detail["format"] != "delta":
        raise ValueError("Source is not Delta")

    if version is None:
        history = spark.sql(
            f"DESCRIBE HISTORY {sql_table} LIMIT 1"
        ).first()
        if history is None:
            raise ValueError("No Delta history available")
        version = int(history["version"])

    if type(version) is not int or version < 0:
        raise ValueError("Delta version must be a nonnegative integer")

    pinned = spark.sql(
        f"SELECT * FROM {sql_table} VERSION AS OF {version}"
    )
    if schema_signature(pinned.schema) != schema_signature(SOURCE_BRONZE_SCHEMA):
        raise ValueError("Pinned Bronze schema differs from the contract")

    selected = pinned.where(
        F.col("run_id").isin(contract["source_run_ids"])
    )
    selected_count = selected.limit(129).count()
    if selected_count == 0 or selected_count > 128:
        raise ValueError("Missing/oversized selected source run")

    body_bytes = selected.agg(
        F.sum(F.length("raw_body")).alias("body_bytes")
    ).first()["body_bytes"] or 0
    if body_bytes > 32 * 1024 * 1024:
        raise ValueError("Selected response bytes exceed the inspection cap")

    return selected, {
        "source_table": table,
        "delta_version": version,
        "selected_attempt_events": selected_count,
        "selected_body_bytes": int(body_bytes),
        "source_run_ids": list(contract["source_run_ids"]),
    }


print("Version-pinned reader loaded; no source selected or written.")

# COMMAND ----------

# CELL 14 — Batch preparation and fixture integrity-gated publication
class FixturePublicationBlocked(MergeGuardError):
    pass


def prepare_silver_batch(
    source_events, contract, processing_run_id, processed_at_utc
):
    require_processing_metadata(processing_run_id, processed_at_utc)

    successful_events, source_metrics = select_complete_source_events(
        source_events, contract
    )

    classified_rows = []
    quarantine_rows = []

    for event in successful_events:
        candidates, diagnostics, exclusion = parse_runtime_event(
            event, contract["execution_mode"]
        )
        if diagnostics or exclusion:
            raise ValueError("Source parsing changed after completeness checks")

        for candidate in candidates:
            classified = validate_candidate(
                candidate,
                contract["publication_start"],
                contract["publication_end"],
            )
            classified_rows.append(classified)
            if classified["classification"] == "invalid":
                quarantine_rows.append(quarantine_record(
                    classified, processing_run_id, processed_at_utc
                ))

        if len(classified_rows) > 100_000:
            raise ValueError("Bounded Silver batch limit exceeded")

    audit_rows, conflicts = canonicalise(
        classified_rows, contract["execution_mode"]
    )
    winners = [
        row for row in audit_rows
        if row["canonical_outcome"] == "winner"
    ]
    national_rows = [
        {name: row[name] for name in EXPECTED_NATIONAL}
        for row in winners
    ]

    return {
        "source_metrics": source_metrics,
        "classified_record_count": len(classified_rows),
        "national_rows": national_rows,
        "quarantine_rows": quarantine_rows,
        "conflicts": conflicts,
        "integrity_gate_passed": (
            bool(national_rows)
            and not quarantine_rows
            and not conflicts
        ),
        # Coverage/freshness policy gates are a separate implementation step.
        "quality_policy_enforced": False,
    }


def publish_integrity_checked_fixture(batch):
    require_fixture_destination(SILVER_FIXTURE_NATIONAL, "national")
    require_fixture_destination(SILVER_FIXTURE_QUARANTINE, "quarantine")

    # Preserve rejection evidence before refusing national publication.
    if batch["quarantine_rows"]:
        merge_quarantine_fixture(
            SILVER_FIXTURE_QUARANTINE,
            spark.createDataFrame(
                batch["quarantine_rows"], QUARANTINE_SCHEMA
            ),
        )

    if not batch["integrity_gate_passed"]:
        raise FixturePublicationBlocked(
            "National publication blocked by rejects/conflicts/no winners"
        )

    merge_national_fixture(
        SILVER_FIXTURE_NATIONAL,
        spark.createDataFrame(batch["national_rows"], NATIONAL_SCHEMA),
    )


print("Silver batch preparation loaded; no source selected or written.")

# COMMAND ----------

# CELL 15 — Disposable storage-to-Silver integration
if globals().get("SILVER_FIXTURE_ACTIVE", False):
    raise RuntimeError("Previous Silver fixtures may remain; resolve cleanup")

nonce = uuid.uuid4().hex
SOURCE_FIXTURE = f"workspace.bronze._gridlens_runtime_fixture_{nonce}"
SILVER_FIXTURE_NATIONAL = (
    f"workspace.silver._gridlens_runtime_silver_{nonce}_national"
)
SILVER_FIXTURE_QUARANTINE = (
    f"workspace.silver._gridlens_runtime_silver_{nonce}_quarantine"
)

source_contract = bind_silver_handoff(TEST_HANDOFF, "fixture")

specs = [
    (SOURCE_FIXTURE, "bronze", SOURCE_BRONZE_SCHEMA),
    (SILVER_FIXTURE_NATIONAL, "national", NATIONAL_SCHEMA),
    (SILVER_FIXTURE_QUARANTINE, "quarantine", QUARANTINE_SCHEMA),
]

def integration_sql_table(table, role):
    return (
        quoted_source_table(table, "fixture")
        if role == "bronze"
        else quoted_fixture(table)
    )

for table, role, _ in specs:
    integration_sql_table(table, role)
    if spark.catalog.tableExists(table):
        raise RuntimeError(f"Unexpected fixture-name collision: {table}")

SILVER_FIXTURE_ACTIVE = True
creation_intents = []
INTEGRATION_TESTS = []
EVIDENCE_15 = {
    "checkpoint": "pinned_source_silver_integration_v1",
    "tests_passed": 0,
    "pinned_source_version": None,
    "latest_source_version": None,
    "source_rows_before_cleanup": None,
    "national_rows_before_cleanup": None,
    "quarantine_rows_before_cleanup": None,
    "cleanup": {},
    "persistent_fixture_writes": True,
    "live_table_writes": False,
    "api_requests": False,
    "actual_cross_task_job": False,
    "quality_policy_enforced": False,
    "runtime_notebook_complete": False,
}

print("SOURCE FIXTURE:", SOURCE_FIXTURE)
print("NATIONAL FIXTURE:", SILVER_FIXTURE_NATIONAL)
print("QUARANTINE FIXTURE:", SILVER_FIXTURE_QUARANTINE)


def integration_check(name, condition):
    if not condition:
        raise AssertionError(name)
    INTEGRATION_TESTS.append(name)


def append_source_fixture(rows):
    # This writer is deliberately tied to this cell's exact fixture.
    quoted_source_table(SOURCE_FIXTURE, "fixture")
    if schema_signature(spark.table(SOURCE_FIXTURE).schema) != schema_signature(
        SOURCE_BRONZE_SCHEMA
    ):
        raise ValueError("Source fixture schema mismatch")
    if any(
        row["source_mode"] != "fixture" or row["is_fixture"] is not True
        for row in rows
    ):
        raise ValueError("Only synthetic fixture rows are allowed")

    spark.createDataFrame(rows, SOURCE_BRONZE_SCHEMA).write \
        .format("delta").mode("append").saveAsTable(SOURCE_FIXTURE)


try:
    for table, role, schema in specs:
        ddl = ", ".join(
            f"`{field.name}` {field.dataType.simpleString()}"
            + ("" if field.nullable else " NOT NULL")
            for field in schema.fields
        )
        creation_intents.append((table, role))
        spark.sql(
            f"CREATE TABLE {integration_sql_table(table, role)} "
            f"({ddl}) USING DELTA"
        )

    # 1. Persist the three baseline events from Cell 12:
    # selected failed retry + selected success + unrelated foreign run.
    append_source_fixture(deepcopy(events + [foreign]))

    pinned_df, pinned_info = read_bound_bronze(
        SOURCE_FIXTURE, source_contract
    )
    EVIDENCE_15["pinned_source_version"] = pinned_info["delta_version"]
    source_rows = [
        row.asDict(recursive=True) for row in pinned_df.collect()
    ]
    integration_check(
        "pinned reader selects only the explicit source run",
        len(source_rows) == 2
        and pinned_info["source_run_ids"] == source_contract["source_run_ids"],
    )

    # 2. Parse, validate and select from the persisted rows.
    batch = prepare_silver_batch(
        source_rows, source_contract,
        "fixture-processing-" + uuid.uuid4().hex,
        datetime.now(timezone.utc),
    )
    integration_check(
        "persisted-source batch preparation",
        batch["integrity_gate_passed"]
        and len(batch["national_rows"]) == 1
        and batch["quarantine_rows"] == []
        and batch["source_metrics"]["retained_failed_attempt_events"] == 1,
    )

    # 3. Publish to fixture Silver and compare complete rows/provenance.
    publish_integrity_checked_fixture(batch)
    national_before = fixture_snapshot(SILVER_FIXTURE_NATIONAL, "national")
    quarantine_before = fixture_snapshot(
        SILVER_FIXTURE_QUARANTINE, "quarantine"
    )
    integration_check(
        "fixture publication reconciles full rows",
        national_before == batch["national_rows"]
        and quarantine_before == [],
    )

    # 4. Add an invalid extra success after the previously terminal success.
    # This changes only the disposable source table.
    late_event = deepcopy(events[1])
    late_event["event_id"] = uuid.uuid4().hex
    late_event["attempt_number"] = 3
    append_source_fixture([late_event])

    pinned_again, pinned_again_info = read_bound_bronze(
        SOURCE_FIXTURE, source_contract,
        version=pinned_info["delta_version"],
    )
    stable_events = [
        row.asDict(recursive=True) for row in pinned_again.collect()
    ]
    _, stable_metrics = select_complete_source_events(
        stable_events, source_contract
    )
    integration_check(
        "explicit pinned version ignores later commits",
        pinned_again_info["delta_version"] == pinned_info["delta_version"]
        and stable_metrics["selected_attempt_events"] == 2,
    )

    # 5. A fresh read must reject that now-invalid attempt sequence.
    latest_df, latest_info = read_bound_bronze(
        SOURCE_FIXTURE, source_contract
    )
    try:
        select_complete_source_events(
            [row.asDict(recursive=True) for row in latest_df.collect()],
            source_contract,
        )
    except ValueError:
        integration_check(
            "latest mutated run rejected without Silver changes",
            latest_info["delta_version"] > pinned_info["delta_version"]
            and fixture_snapshot(SILVER_FIXTURE_NATIONAL, "national")
            == national_before
            and fixture_snapshot(SILVER_FIXTURE_QUARANTINE, "quarantine")
            == quarantine_before,
        )
    else:
        raise AssertionError("Invalid latest attempt sequence was accepted")

    # 6. A different, explicit source run has a valid envelope but a
    # rejected business record. It must not fall back to the earlier run.
    bad_record = deepcopy(BASE_RECORD)
    bad_record["intensity"]["actual"] = -1
    raw = canonical_json({"data": [bad_record]}).encode("utf-8")

    rejected_event = deepcopy(events[1])
    rejected_event.update({
        "event_id": uuid.uuid4().hex,
        "run_id": uuid.uuid4().hex,
        "request_id": uuid.uuid4().hex,
        "attempt_number": 1,
        "raw_body": raw,
        "raw_text": raw.decode("utf-8"),
        "response_sha256": hashlib.sha256(raw).hexdigest(),
        "record_count": 1,
    })
    append_source_fixture([rejected_event])

    rejected_contract = bind_silver_handoff({
        **TEST_HANDOFF,
        "source_run_ids_json": json.dumps([rejected_event["run_id"]]),
    }, "fixture")

    rejected_df, rejected_info = read_bound_bronze(
        SOURCE_FIXTURE, rejected_contract
    )
    rejected_batch = prepare_silver_batch(
        [row.asDict(recursive=True) for row in rejected_df.collect()],
        rejected_contract,
        "fixture-rejected-processing-" + uuid.uuid4().hex,
        datetime.now(timezone.utc),
    )
    integration_check(
        "record rejection produces quarantine and closes integrity gate",
        not rejected_batch["integrity_gate_passed"]
        and rejected_batch["national_rows"] == []
        and len(rejected_batch["quarantine_rows"]) == 1,
    )

    # 7. Retain quarantine evidence, then raise instead of publishing national.
    try:
        publish_integrity_checked_fixture(rejected_batch)
    except FixturePublicationBlocked:
        stored_quarantine = fixture_snapshot(
            SILVER_FIXTURE_QUARANTINE, "quarantine"
        )
        integration_check(
            "quarantine retained while national publication is blocked",
            stored_quarantine == rejected_batch["quarantine_rows"]
            and fixture_snapshot(SILVER_FIXTURE_NATIONAL, "national")
            == national_before,
        )
    else:
        raise AssertionError("Rejected batch was allowed to publish")

    # 8. Confirm the rejection still links to retained raw source evidence.
    raw_match_count = (
        spark.table(SOURCE_FIXTURE)
        .where(F.col("event_id") == rejected_event["event_id"])
        .count()
    )
    integration_check(
        "rejection traces to retained raw Bronze event",
        raw_match_count == 1
        and stored_quarantine[0]["bronze_event_id"]
        == rejected_event["event_id"],
    )

    # 9. Fixture-mode reader cannot point at a live or wrong-domain table.
    try:
        read_bound_bronze(
            "workspace.silver.national_intensity", source_contract
        )
    except ValueError:
        INTEGRATION_TESTS.append("source destination guard")
    else:
        raise AssertionError("Wrong-domain source table was accepted")

    # 10. Final counts and honest fixture labels.
    source_count = spark.table(SOURCE_FIXTURE).count()
    national_rows = fixture_snapshot(SILVER_FIXTURE_NATIONAL, "national")
    quarantine_rows_now = fixture_snapshot(
        SILVER_FIXTURE_QUARANTINE, "quarantine"
    )
    non_fixture_sources = (
        spark.table(SOURCE_FIXTURE)
        .where(
            (F.col("source_mode") != "fixture")
            | (F.col("is_fixture") != True)
        )
        .count()
    )
    integration_check(
        "final isolated counts and fixture labels",
        source_count == 5
        and len(national_rows) == 1
        and len(quarantine_rows_now) == 1
        and non_fixture_sources == 0
        and all(
            row["source_mode"] == "fixture"
            for row in national_rows + quarantine_rows_now
        ),
    )

    EVIDENCE_15["latest_source_version"] = int(
        spark.sql(
            f"DESCRIBE HISTORY "
            f"{quoted_source_table(SOURCE_FIXTURE, 'fixture')} LIMIT 1"
        ).first()["version"]
    )
    EVIDENCE_15["source_rows_before_cleanup"] = source_count
    EVIDENCE_15["national_rows_before_cleanup"] = len(national_rows)
    EVIDENCE_15["quarantine_rows_before_cleanup"] = len(quarantine_rows_now)

except Exception as exc:
    EVIDENCE_15["failure_type"] = type(exc).__name__
    EVIDENCE_15["failure_message"] = str(exc)[:700]
    raise

finally:
    EVIDENCE_15["tests_passed"] = len(INTEGRATION_TESTS)

    expected_tables = {
        "bronze": SOURCE_FIXTURE,
        "national": SILVER_FIXTURE_NATIONAL,
        "quarantine": SILVER_FIXTURE_QUARANTINE,
    }
    for table, role in reversed(creation_intents):
        try:
            if table != expected_tables[role]:
                raise RuntimeError("Cleanup destination guard failed")
            spark.sql(
                f"DROP TABLE IF EXISTS {integration_sql_table(table, role)}"
            )
            if spark.catalog.tableExists(table):
                raise RuntimeError("Fixture table remains after DROP")
            EVIDENCE_15["cleanup"][role] = "PASS"
        except Exception as cleanup_exc:
            EVIDENCE_15["cleanup"][role] = "UNRESOLVED"
            EVIDENCE_15[f"{role}_cleanup_error"] = str(cleanup_exc)[:700]

    if (
        len(EVIDENCE_15["cleanup"]) == len(creation_intents)
        and all(value == "PASS" for value in EVIDENCE_15["cleanup"].values())
    ):
        SILVER_FIXTURE_ACTIVE = False

    print("CHECKPOINT EVIDENCE")
    print(json.dumps(EVIDENCE_15, indent=2))

if EVIDENCE_15["cleanup"] != {
    "bronze": "PASS",
    "national": "PASS",
    "quarantine": "PASS",
}:
    raise RuntimeError("Integration fixture cleanup incomplete; stop and review")

if len(INTEGRATION_TESTS) != 10:
    raise RuntimeError("Integration test count incomplete")

print("PASS: 10 pinned-source/Silver integration tests and cleanup.")