# Databricks notebook source
import sys
import json
import importlib.util
from importlib.metadata import version, PackageNotFoundError

environment = {
    "python_version": sys.version.split()[0],
    "spark_version": spark.version,
    "libraries": {},
}

for package in ("pyspark", "requests", "pandas", "delta"):
    available = importlib.util.find_spec(package) is not None
    try:
        package_version = version(
            "delta-spark" if package == "delta" else package
        )
    except PackageNotFoundError:
        package_version = "not reported"

    environment["libraries"][package] = {
        "import_available": available,
        "version": package_version,
    }

print(json.dumps(environment, indent=2))

# COMMAND ----------

context = spark.sql("""
    SELECT
        current_catalog() AS catalog,
        current_schema() AS schema
""").first().asDict()

print("CURRENT_CONTEXT")
print(json.dumps(context, indent=2))

print("VISIBLE_CATALOGS")
spark.sql("SHOW CATALOGS").show(50, truncate=False)

print("SCHEMAS_IN_CURRENT_CATALOG")
spark.sql("SHOW SCHEMAS").show(50, truncate=False)

# COMMAND ----------

from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import hashlib

# Editable: default to one hour at midnight UTC yesterday.
probe_start = (
    datetime.now(timezone.utc) - timedelta(days=1)
).replace(hour=0, minute=0, second=0, microsecond=0)
probe_end = probe_start + timedelta(hours=1)

start_text = probe_start.strftime("%Y-%m-%dT%H:%MZ")
end_text = probe_end.strftime("%Y-%m-%dT%H:%MZ")
probe_url = (
    f"https://api.carbonintensity.org.uk/intensity/"
    f"{start_text}/{end_text}"
)

api_probe = {
    "request_url": probe_url,
    "request_from": start_text,
    "request_to": end_text,
    "status": "not_started",
}

try:
    request = Request(
        probe_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "GridLens-environment-check/0.1",
        },
    )

    with urlopen(request, timeout=20) as response:
        api_probe["http_status"] = response.status
        raw_payload = response.read()

    api_probe["retrieved_at_utc"] = datetime.now(
        timezone.utc
    ).isoformat()
    api_probe["response_bytes"] = len(raw_payload)
    api_probe["response_sha256"] = hashlib.sha256(
        raw_payload
    ).hexdigest()

    payload = json.loads(raw_payload)
    records = payload.get("data") if isinstance(payload, dict) else None

    if not isinstance(records, list) or not records:
        raise ValueError("Expected a non-empty 'data' list")

    sample = records[0]
    if not isinstance(sample, dict):
        raise ValueError("Expected the first record to be an object")
    if not {"from", "to", "intensity"}.issubset(sample):
        raise ValueError("First record is missing required fields")
    if not isinstance(sample["intensity"], dict):
        raise ValueError("Expected 'intensity' to be an object")

    api_probe.update({
        "status": "passed_reachability_and_basic_shape",
        "record_count": len(records),
        "sample_record": sample,
    })

except HTTPError as exc:
    api_probe.update({
        "status": "failed_http",
        "http_status": exc.code,
    })
except (URLError, TimeoutError) as exc:
    api_probe.update({
        "status": "failed_network",
        "error_type": type(exc).__name__,
    })
except Exception as exc:
    api_probe.update({
        "status": "failed",
        "error_type": type(exc).__name__,
        "error": str(exc)[:300],
    })

print(json.dumps(api_probe, indent=2))

# COMMAND ----------

import json
import uuid

probe_name = f"_gridlens_permission_probe_{uuid.uuid4().hex}"
probe_table = f"`workspace`.`default`.`{probe_name}`"
probe_token = uuid.uuid4().hex

report = {
    "table": f"workspace.default.{probe_name}",
    "create": "not_tested",
    "write": "not_tested",
    "read": "not_tested",
    "delta_format": "not_tested",
    "cleanup": "not_needed",
}
created = False

# Print the exact table name before doing anything.
print("PROBE_TABLE:", report["table"])

try:
    # No IF NOT EXISTS or overwrite: never reuse an existing table.
    spark.sql(f"""
        CREATE TABLE {probe_table} (
            probe_id STRING,
            probe_value INT
        ) USING DELTA
    """)
    created = True
    report["create"] = "passed"

    spark.sql(f"""
        INSERT INTO {probe_table}
        VALUES ('{probe_token}', 42)
    """)
    report["write"] = "passed"

    rows = spark.sql(
        f"SELECT probe_id, probe_value FROM {probe_table}"
    ).collect()

    assert len(rows) == 1, f"Expected 1 row, found {len(rows)}"
    assert rows[0]["probe_id"] == probe_token, "Token mismatch"
    assert rows[0]["probe_value"] == 42, "Value mismatch"
    report["read"] = "passed"

    detail = spark.sql(
        f"DESCRIBE DETAIL {probe_table}"
    ).first().asDict()

    assert detail["format"] == "delta", "Expected Delta format"
    report["delta_format"] = "passed"

except Exception as exc:
    report["error_type"] = type(exc).__name__
    report["error"] = str(exc)[:1000]

finally:
    if created:
        try:
            spark.sql(f"DROP TABLE {probe_table}")
            assert not spark.catalog.tableExists(report["table"]), (
                "Table is still registered after DROP"
            )
            report["cleanup"] = "passed"
        except Exception as exc:
            report["cleanup"] = "failed"
            report["cleanup_error"] = str(exc)[:1000]
            print("MANUAL CLEANUP:", f"DROP TABLE {probe_table};")

checks = ("create", "write", "read", "delta_format", "cleanup")
report["overall"] = (
    "passed"
    if all(report[check] == "passed" for check in checks)
    else "failed"
)

print(json.dumps(report, indent=2))