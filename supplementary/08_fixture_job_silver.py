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

# CELL 13 — Strict fixture envelope binding; no Spark access.

FIXTURE_ARTIFACT_VERSION = "gridlens_fixture_artifact_v1"
FIXTURE_ENVELOPE_MAX_BYTES = 16 * 1024


def bind_fixture_artifact(envelope_json):
    if (
        not isinstance(envelope_json, str)
        or not envelope_json.strip()
    ):
        raise ValueError(
            "Explicit nonempty envelope JSON is required"
        )

    if (
        len(envelope_json.encode("utf-8"))
        > FIXTURE_ENVELOPE_MAX_BYTES
    ):
        raise ValueError("Fixture envelope exceeds the size limit")

    envelope = strict_json_loads(envelope_json)

    if (
        not isinstance(envelope, dict)
        or set(envelope) != {
            "envelope_version",
            "handoff",
            "artifact",
        }
    ):
        raise ValueError("Unsupported outer envelope keys/type")

    if envelope["envelope_version"] != FIXTURE_ARTIFACT_VERSION:
        raise ValueError("Unsupported fixture envelope version")

    if not isinstance(envelope["handoff"], dict):
        raise ValueError("Inner handoff must be an object")

    contract = bind_silver_handoff(
        envelope["handoff"],
        "fixture",
    )

    artifact = envelope["artifact"]
    if (
        not isinstance(artifact, dict)
        or set(artifact) != {
            "source_table",
            "delta_version",
            "delta_table_id",
            "fixture_owner",
        }
    ):
        raise ValueError("Unsupported artifact keys/type")

    owner = artifact["fixture_owner"]
    if (
        not isinstance(owner, str)
        or not re.fullmatch(r"[0-9a-f]{32}", owner)
    ):
        raise ValueError("Invalid fixture owner")

    expected_table = (
        "workspace.bronze._gridlens_runtime_fixture_" + owner
    )
    if artifact["source_table"] != expected_table:
        raise ValueError(
            "Source table does not match the allocated owner"
        )

    version = artifact["delta_version"]
    if type(version) is not int or version < 0:
        raise ValueError(
            "Explicit nonnegative integer Delta version required"
        )

    table_id = artifact["delta_table_id"]
    if (
        not isinstance(table_id, str)
        or not re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
            r"[0-9a-f]{4}-[0-9a-f]{12}",
            table_id,
        )
    ):
        raise ValueError("Canonical Delta table UUID required")

    return {
        "envelope_version": envelope["envelope_version"],
        "handoff": deepcopy(envelope["handoff"]),
        "artifact": deepcopy(artifact),
        "contract": contract,
    }


print(
    "Strict fixture envelope binder loaded; "
    "no source read or write."
)

# COMMAND ----------

# CELL 14 — Owned, pinned fixture reader definitions only.
# Loading this cell performs no table access.

def read_fixture_artifact(bound):
    if (
        not isinstance(bound, dict)
        or set(bound) != {
            "envelope_version",
            "handoff",
            "artifact",
            "contract",
        }
    ):
        raise ValueError("Expected a bound fixture artifact")

    # Rebind serialized fields rather than trusting a modified contract.
    checked = bind_fixture_artifact(json.dumps({
        key: bound[key]
        for key in (
            "envelope_version",
            "handoff",
            "artifact",
        )
    }))

    artifact = checked["artifact"]
    contract = checked["contract"]

    sql_table = quoted_source_table(
        artifact["source_table"],
        "fixture",
    )

    def check_identity():
        detail = spark.sql(
            f"DESCRIBE DETAIL {sql_table}"
        ).first().asDict(recursive=True)

        if (
            detail["format"] != "delta"
            or str(detail["id"]) != artifact["delta_table_id"]
            or detail["properties"].get("gridlens.fixture_owner")
            != artifact["fixture_owner"]
        ):
            raise ValueError(
                "Fixture Delta identity/ownership mismatch"
            )

    check_identity()

    selected, metrics = read_bound_bronze(
        artifact["source_table"],
        contract,
        version=artifact["delta_version"],
    )

    # The existing reader checks its bounded selected-run limits first.
    events = [
        row.asDict(recursive=True)
        for row in selected.collect()
    ]

    check_identity()

    if len(events) != metrics["selected_attempt_events"]:
        raise ValueError("Pinned selected-source count changed")

    return events, {
        **metrics,
        "delta_table_id": artifact["delta_table_id"],
        "fixture_owner": artifact["fixture_owner"],
        "envelope_version": checked["envelope_version"],
    }, contract


print(
    "Owned pinned fixture reader loaded; "
    "no source read or write."
)

# COMMAND ----------

# CELL 14 — Owned Bronze cleanup definitions only.
# Loading this cell does not read or drop a table.

PRODUCER_FIXTURE_OWNER_KEY = "gridlens.fixture_owner"


def drop_producer_delta_fixture():
    global RUNTIME_FIXTURE_ACTIVE
    global RUNTIME_FIXTURE_TABLE
    global RUNTIME_FIXTURE_SQL

    intent = globals().get("PRODUCER_FIXTURE_INTENT")
    if not isinstance(intent, dict):
        raise RuntimeError(
            "Missing explicit fixture cleanup intent"
        )

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
            or detail["properties"].get(
                PRODUCER_FIXTURE_OWNER_KEY
            ) != owner
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
    "Owned Bronze cleanup helper loaded; "
    "no cleanup executed."
)

# COMMAND ----------

# Shared fixture-job parameter definitions.
# Must execute BEFORE the consumer entrypoint.

import re


def required_fixture_widget(name):
    # Do not create a blank widget or swallow retrieval errors.
    value = dbutils.widgets.get(name)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Missing fixture job parameter: {name}"
        )

    value = value.strip()

    # Reject a whole unresolved dynamic reference.
    # Ordinary nested JSON closing braces are valid.
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
            or not re.fullmatch(r"[1-9][0-9]{0,31}", raw[name])
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


for name in (
    "required_fixture_widget",
    "validate_fixture_job_context",
    "read_fixture_job_context",
):
    if not callable(globals().get(name)):
        raise RuntimeError(f"Missing parameter helper: {name}")

print(
    "All three fixture-job parameter helpers loaded; "
    "no entrypoint executed."
)

# COMMAND ----------

# Implementation Block 04 — Disposable Silver job checkpoint definitions.
# Insert immediately BEFORE the existing consumer entrypoint.
# Loading this cell performs no persistent reads, writes or cleanup.

def run_job_silver_fixture_checkpoint(batch, contract, consumer_context):
    global SILVER_FIXTURE_ACTIVE
    global SILVER_FIXTURE_NATIONAL
    global SILVER_FIXTURE_QUARANTINE

    if required_fixture_widget("fixture_silver_publication_approved") != "true":
        raise ValueError("Disposable Silver publication requires explicit approval")

    if SILVER_FIXTURE_ACTIVE:
        raise RuntimeError("Existing Silver fixture lifecycle must be resolved")

    if (
        contract["execution_mode"] != "fixture"
        or batch["integrity_gate_passed"] is not True
        or batch["quality_policy_enforced"] is not False
        or len(batch["national_rows"]) != 96
        or batch["quarantine_rows"]
        or batch["conflicts"]
        or any(
            row["source_mode"] != "fixture"
            for row in batch["national_rows"]
        )
    ):
        raise ValueError("Expected the verified clean 96-row fixture batch")

    owner = uuid.uuid4().hex
    targets = {
        role: {
            "table": (
                f"workspace.silver._gridlens_runtime_silver_{owner}_{role}"
            ),
            "delta_table_id": None,
            "create_attempted": False,
        }
        for role in ("national", "quarantine")
    }

    receipt = {
        "checkpoint": "fixture_job_silver_publication_v1",
        "consumer_job_context": dict(consumer_context),
        "owner": owner,
        "targets": targets,
        "write_state": {},
        "checks_passed": [],
        "cleanup": {},
        "quality_policy_enforced": False,
        "quarantine_probe_origin": (
            "in_memory_synthetic_record_not_producer_bronze"
        ),
        "test_status": "NOT_STARTED",
    }

    def owned_identity(role):
        target = targets[role]
        sql_name = quoted_fixture(target["table"])
        detail = spark.sql(
            f"DESCRIBE DETAIL {sql_name}"
        ).first().asDict(recursive=True)

        if (
            target["delta_table_id"] is None
            or detail["format"] != "delta"
            or str(detail["id"]) != target["delta_table_id"]
            or detail["properties"].get("gridlens.fixture_owner") != owner
        ):
            raise RuntimeError(
                "Silver fixture owner/Delta identity mismatch"
            )

        return sql_name

    def guarded_publish(value, operation):
        for role in targets:
            owned_identity(role)

        receipt["write_state"][operation] = "ATTEMPTED"
        publish_integrity_checked_fixture(value)
        receipt["write_state"][operation] = "RETURNED"

    # Check BOTH names before activating or attempting CREATE.
    for target in targets.values():
        if spark.catalog.tableExists(target["table"]):
            raise RuntimeError("Silver fixture name collision; no CREATE")

    SILVER_FIXTURE_NATIONAL = targets["national"]["table"]
    SILVER_FIXTURE_QUARANTINE = targets["quarantine"]["table"]
    SILVER_FIXTURE_ACTIVE = True

    try:
        for role, expected in (
            ("national", EXPECTED_NATIONAL),
            ("quarantine", EXPECTED_QUARANTINE),
        ):
            target = targets[role]
            sql_name = quoted_fixture(target["table"])

            ddl = ", ".join(
                f"`{name}` {kind.upper()}"
                + ("" if nullable else " NOT NULL")
                for name, (kind, nullable) in expected.items()
            )

            target["create_attempted"] = True

            # Log intent BEFORE CREATE, including uncertain outcomes.
            print(json.dumps({
                "silver_fixture_creation_intent": receipt
            }, indent=2))

            spark.sql(
                f"CREATE TABLE {sql_name} ({ddl}) USING DELTA "
                f"TBLPROPERTIES ('gridlens.fixture_owner' = '{owner}')"
            ).collect()

            detail = spark.sql(
                f"DESCRIBE DETAIL {sql_name}"
            ).first().asDict(recursive=True)

            if (
                detail["format"] != "delta"
                or detail["properties"].get("gridlens.fixture_owner") != owner
            ):
                raise RuntimeError(
                    "New Silver fixture identity/owner mismatch"
                )

            target["delta_table_id"] = str(detail["id"])
            owned_identity(role)
            require_fixture_destination(target["table"], role)

        expected_rows = sorted(
            [
                normalise_stored(row)
                for row in batch["national_rows"]
            ],
            key=business_key,
        )

        guarded_publish(batch, "national_first_publication")

        national_before = fixture_snapshot(
            SILVER_FIXTURE_NATIONAL, "national"
        )

        if national_before != expected_rows:
            raise RuntimeError(
                "National full-row first-publication mismatch"
            )

        if fixture_snapshot(
            SILVER_FIXTURE_QUARANTINE, "quarantine"
        ) != []:
            raise RuntimeError(
                "Unexpected quarantine rows for clean producer batch"
            )

        receipt["checks_passed"].append(
            "national_first_publication_full_row_96"
        )

        guarded_publish(batch, "national_identical_replay")

        if fixture_snapshot(
            SILVER_FIXTURE_NATIONAL, "national"
        ) != national_before:
            raise RuntimeError(
                "National identical replay changed full rows"
            )

        receipt["checks_passed"].append(
            "national_identical_replay_full_row"
        )

        # Independent in-memory synthetic record probe.
        # These probe identities are NOT persisted producer Bronze evidence.
        start = contract["publication_start"]

        raw_record = {
            "from": start.isoformat(),
            "to": (start + timedelta(minutes=30)).isoformat(),
            "intensity": {
                "forecast": -1,
                "actual": None,
                "index": "fixture",
            },
        }

        provenance = {
            "bronze_event_id": uuid.uuid4().hex,
            "bronze_run_id": uuid.uuid4().hex,
            "bronze_request_id": uuid.uuid4().hex,
            "source": "gridlens_fixture_probe",
            "source_mode": "fixture",
            "source_retrieved_at_utc": datetime.now(timezone.utc),
            "response_sha256": hashlib.sha256(
                canonical_json(
                    {"data": [raw_record]}
                ).encode("utf-8")
            ).hexdigest(),
        }

        classified = validate_candidate(
            parse_record(raw_record, provenance, 0),
            contract["publication_start"],
            contract["publication_end"],
        )

        if (
            classified["classification"] != "invalid"
            or classified["validation_errors"]
            != ["forecast_intensity:negative"]
        ):
            raise RuntimeError(
                "Synthetic rejection probe classification mismatch"
            )

        rejection = quarantine_record(
            classified,
            "fixture-probe-" + consumer_context["task_run_id"],
            datetime.now(timezone.utc),
        )

        blocked_batch = dict(batch)
        blocked_batch.update({
            "quarantine_rows": [rejection],
            "integrity_gate_passed": False,
        })

        for attempt in range(2):
            probe = deepcopy(blocked_batch)

            if attempt == 1:
                # Replay must retain the FIRST processing metadata.
                probe["quarantine_rows"][0]["processing_run_id"] += "-replay"
                probe["quarantine_rows"][0]["processed_at_utc"] += (
                    timedelta(seconds=1)
                )

            operation = (
                "rejection_first"
                if attempt == 0
                else "rejection_replay"
            )

            try:
                guarded_publish(probe, operation)
            except FixturePublicationBlocked:
                receipt["write_state"][operation] = (
                    "EXPECTED_PUBLICATION_BLOCK"
                )
            else:
                raise RuntimeError(
                    "Reject-bearing probe did not block national publication"
                )

            if fixture_snapshot(
                SILVER_FIXTURE_NATIONAL, "national"
            ) != national_before:
                raise RuntimeError(
                    "Reject-bearing probe changed national rows"
                )

            if fixture_snapshot(
                SILVER_FIXTURE_QUARANTINE, "quarantine"
            ) != [normalise_stored(rejection)]:
                raise RuntimeError(
                    "Quarantine full-row insertion/replay mismatch"
                )

            receipt["checks_passed"].append(
                operation + "_block_and_full_row_readback"
            )

        receipt["national_rows_verified"] = 96
        receipt["quarantine_rows_verified"] = 1
        receipt["test_status"] = "PASSED"

        return receipt

    except Exception as exc:
        receipt["test_status"] = "FAILED"
        receipt["error"] = {
            "type": type(exc).__name__,
            "message": str(exc)[:2000],
        }
        raise

    finally:
        # Attempt BOTH cleanups independently.
        # Unknown Delta identity is unresolved, not permission for a blind DROP.
        for role, target in targets.items():
            try:
                if target["create_attempted"]:
                    sql_name = owned_identity(role)
                    spark.sql(f"DROP TABLE {sql_name}").collect()

                if spark.catalog.tableExists(target["table"]):
                    raise RuntimeError(
                        "Silver fixture still resolves after cleanup"
                    )

                receipt["cleanup"][role] = {
                    "table": target["table"],
                    "result": "ABSENT_CONFIRMED",
                    "absence_scope": "current_account_catalog_lookup",
                }

            except Exception as exc:
                receipt["cleanup"][role] = {
                    "table": target["table"],
                    "result": "UNRESOLVED",
                    "message": str(exc)[:2000],
                }

        cleaned = all(
            row["result"] == "ABSENT_CONFIRMED"
            for row in receipt["cleanup"].values()
        )

        if cleaned:
            SILVER_FIXTURE_ACTIVE = False
            SILVER_FIXTURE_NATIONAL = None
            SILVER_FIXTURE_QUARANTINE = None
        else:
            receipt["test_status"] = "FAILED"

        print(json.dumps(receipt, indent=2))

        if not cleaned:
            raise RuntimeError(
                "Silver fixture cleanup unresolved; "
                "use exact logged identities"
            )


print(
    "Job Silver fixture checkpoint helper loaded; "
    "no tables created or written."
)

# COMMAND ----------

# CELL 16 — Fixture-job consumer entrypoint.
# Read-only Silver preparation; guarded Bronze cleanup.

consumer_context = read_fixture_job_context()

producer_context_json = required_fixture_widget(
    "fixture_producer_context_json"
)
if len(producer_context_json.encode("utf-8")) > 4096:
    raise ValueError("Producer context exceeds size limit")

producer_context = validate_fixture_job_context(
    strict_json_loads(producer_context_json)
)

if (
    producer_context["job_run_id"]
    != consumer_context["job_run_id"]
    or producer_context["task_run_id"]
    != required_fixture_widget("expected_producer_task_run_id")
    or producer_context["task_execution_count"]
    != required_fixture_widget("expected_producer_execution_count")
    or producer_context["task_run_id"]
    == consumer_context["task_run_id"]
):
    raise ValueError(
        "Producer context does not match this job/task dependency"
    )

bound = bind_fixture_artifact(
    required_fixture_widget("fixture_envelope_json")
)

if RUNTIME_FIXTURE_ACTIVE:
    raise RuntimeError(
        "Unexpected active Bronze lifecycle in consumer session"
    )

# Establish cleanup intent from the validated envelope.
# The cleanup helper also checks actual ownership and Delta identity.
artifact = bound["artifact"]

RUNTIME_FIXTURE_TABLE = artifact["source_table"]
RUNTIME_FIXTURE_SQL = quoted_source_table(
    artifact["source_table"],
    "fixture",
)
PRODUCER_FIXTURE_INTENT = {
    "table": RUNTIME_FIXTURE_TABLE,
    "sql_table": RUNTIME_FIXTURE_SQL,
    "owner": artifact["fixture_owner"],
    "delta_table_id": artifact["delta_table_id"],
}
RUNTIME_FIXTURE_ACTIVE = True

evidence = {
    "checkpoint": "fixture_job_consumer_entrypoint_v1",
    "consumer_job_context": consumer_context,
    "producer_job_context": producer_context,
    "publication_executed": False,
    "quality_policy_enforced": False,
    "cleanup": "NOT_ATTEMPTED",
}

try:
    events, reader_metrics, contract = read_fixture_artifact(bound)

    batch = prepare_silver_batch(
        events,
        contract,
        "fixture-consumer-" + consumer_context["task_run_id"],
        datetime.now(timezone.utc),
    )

    expected_keys = {
        (
            contract["publication_start"]
            + timedelta(minutes=30 * i),
            contract["publication_start"]
            + timedelta(minutes=30 * (i + 1)),
        )
        for i in range(96)
    }

    metrics = batch["source_metrics"]

    if (
        metrics != {
            "selected_attempt_events": 4,
            "selected_success_events": 2,
            "retained_failed_attempt_events": 2,
            "verified_request_windows": 2,
        }
        or batch["classified_record_count"] != 96
        or len(batch["national_rows"]) != 96
        or {
            (
                row["interval_start_utc"],
                row["interval_end_utc"],
            )
            for row in batch["national_rows"]
        } != expected_keys
        or batch["quarantine_rows"]
        or batch["conflicts"]
        or batch["integrity_gate_passed"] is not True
        or batch["quality_policy_enforced"] is not False
        or any(
            row["source_mode"] != "fixture"
            for row in batch["national_rows"]
        )
    ):
        raise RuntimeError(
            "Fixture consumer preparation verification failed"
        )

        evidence.update({
        "checkpoint": "fixture_job_consumer_publication_entrypoint_v1",
        "reader_metrics": reader_metrics,
        "source_metrics": metrics,
        "prepared_national_rows": 96,
        "publication_scope": "disposable_silver_only",
        # Until the helper returns, inspect its detailed write-state receipt.
        "publication_executed": None,
        "test_status": "IN_PROGRESS",
    })

    silver_receipt = run_job_silver_fixture_checkpoint(
        batch,
        contract,
        consumer_context,
    )

    evidence.update({
        "silver_fixture_checkpoint": silver_receipt,
        "publication_executed": True,
        "quality_policy_enforced": False,
        "test_status": "PASSED",
    })

except Exception as exc:
    evidence.update({
        "test_status": "FAILED",
        "error_type": type(exc).__name__,
        "error_message": str(exc)[:500],
    })
    raise

finally:
    try:
        evidence["cleanup"] = drop_producer_delta_fixture()

    except Exception as exc:
        evidence["test_status"] = "FAILED"
        evidence["cleanup"] = {
            "result": "UNRESOLVED",
            "message": str(exc)[:500],
        }
        raise

    finally:
        print(json.dumps(evidence, indent=2))