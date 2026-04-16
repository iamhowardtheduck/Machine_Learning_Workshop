#!/usr/bin/env python3
"""
apm_trace_generator.py — Generates properly linked APM traces for the
LendPath Mortgage ML Workshop.

Unlike the SDG (which generates each document independently), this script
creates coherent trace trees where:
  • Each trace has one root transaction per service call
  • Child spans carry parent.id = their parent transaction.id
  • destination.service.resource is consistent per service pair
  • service.node.name is stable per service instance

This produces a connected APM Service Map and working Trace Waterfall.

LendPath service call topology (edges → service map arrows):
  lendpath-los ──────────────→ lendpath-underwriting
  lendpath-los ──────────────→ lendpath-credit-service
  lendpath-los ──────────────→ lendpath-document-service
  lendpath-los ──────────────→ lendpath-appraisal-service
  lendpath-underwriting ─────→ oracle:1521
  lendpath-underwriting ─────→ kafka:9092
  lendpath-credit-service ───→ oracle:1521
  lendpath-credit-service ───→ https://api.experian.com
  lendpath-credit-service ───→ https://api.equifax.com
  lendpath-document-service ─→ s3.amazonaws.com:443
  lendpath-document-service ─→ oracle:1521
  lendpath-appraisal-service → oracle:1521
  lendpath-appraisal-service → https://api.corelogic.com
  All services ──────────────→ redis:6379  (session cache)

Usage:
    python apm_trace_generator.py \
        --host https://localhost:9200 \
        --user elastic --password changeme \
        --no-verify-ssl \
        [--rate 2]          # traces per second (default: 2)
        [--once]            # generate one batch then exit
"""

import argparse
import random
import secrets
import ssl
import sys
import time
import uuid
from datetime import datetime, timezone

try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    print("ERROR: elasticsearch-py not installed.")
    print("Run: pip install elasticsearch")
    sys.exit(1)


# ─── Service topology ────────────────────────────────────────────────────────
# Each entry defines a service and the downstream calls it can make.
# weight = relative frequency of this transaction type being initiated.

# Each downstream_span tuple:
#   (span_name, span_type, span_subtype,
#    destination_resource,          ← destination.service.resource + service.target.name
#    target_type,                   ← service.target.type  (new field for service map)
#    duration_range_us, probability)
#
# destination.service.resource  — drives service map edge labels  (e.g. "Oracle DB:1521")
# service.target.name           — the downstream service node name in the service map
# service.target.type           — "db", "messaging", "cache", "external", "storage"

SERVICES = {
    "lendpath-los": {
        "node_names":  ["los-app-01", "los-app-02"],
        "transactions": [
            {"name": "POST /api/v1/applications/submit",  "type": "request",   "weight": 25},
            {"name": "GET /api/v1/applications/status",   "type": "request",   "weight": 20},
            {"name": "POST /api/v1/rates/current",        "type": "request",   "weight": 15},
            {"name": "POST /api/v1/documents/upload",     "type": "request",   "weight": 15},
            {"name": "GET /api/v1/payments/escrow",       "type": "request",   "weight": 10},
            {"name": "scheduled-pipeline-check",          "type": "scheduled", "weight": 5},
        ],
        # lendpath-los calls the four downstream app services
        # destination_resource matches service.target.name for app-to-app edges
        "downstream_spans": [
            # (name, type, subtype, destination_resource, target_type, dur_range, prob)
            ("lendpath-underwriting: submit decision",   "external", "http",
             "lendpath-underwriting",     "service", (5000,  80000),  0.6),
            ("lendpath-credit-service: check score",     "external", "http",
             "lendpath-credit-service",   "service", (8000, 120000),  0.5),
            ("lendpath-document-service: store package", "external", "http",
             "lendpath-document-service", "service", (3000,  40000),  0.4),
            ("lendpath-appraisal-service: order report", "external", "http",
             "lendpath-appraisal-service","service", (2000,  20000),  0.3),
            ("Redis GET session",                        "cache",    "redis",
             "Redis:6379",                "cache",   (200,    2000),  0.9),
            ("Redis SET session",                        "cache",    "redis",
             "Redis:6379",                "cache",   (200,    2000),  0.7),
            ("SELECT * FROM loan_applications",          "db",       "oracle",
             "Oracle DB:1521",            "db",      (500,  15000),   0.8),
        ],
    },
    "lendpath-underwriting": {
        "node_names": ["uw-app-01", "uw-app-02"],
        "transactions": [
            {"name": "POST /api/v1/underwriting/decision", "type": "request",   "weight": 30},
            {"name": "GET /api/v1/underwriting/queue",     "type": "request",   "weight": 20},
            {"name": "process-queue-item",                 "type": "messaging", "weight": 15},
        ],
        "downstream_spans": [
            ("SELECT application FROM loan_applications", "db",        "oracle",
             "Oracle DB:1521",  "db",        (1000, 30000), 0.95),
            ("UPDATE application_stage",                  "db",        "oracle",
             "Oracle DB:1521",  "db",        (500,  10000), 0.8),
            ("Kafka produce decision-results",            "messaging", "kafka",
             "Kafka:9092",      "messaging", (200,   5000), 0.7),
            ("Redis GET rate-lock",                       "cache",     "redis",
             "Redis:6379",      "cache",     (200,   2000), 0.6),
        ],
    },
    "lendpath-credit-service": {
        "node_names": ["credit-svc-01"],
        "transactions": [
            {"name": "POST /api/v1/credit/check",  "type": "request", "weight": 40},
            {"name": "GET /api/v1/credit/report",  "type": "request", "weight": 20},
        ],
        "downstream_spans": [
            ("Experian credit bureau API",  "external", "http",
             "Experian API:443",  "external", (50000, 800000), 0.5),
            ("Equifax credit bureau API",   "external", "http",
             "Equifax API:443",   "external", (50000, 800000), 0.5),
            ("INSERT INTO credit_scores",   "db",       "oracle",
             "Oracle DB:1521",    "db",       (500,    8000),  0.9),
            ("SELECT FROM credit_scores",   "db",       "oracle",
             "Oracle DB:1521",    "db",       (300,    5000),  0.8),
            ("Redis GET credit-cache",      "cache",    "redis",
             "Redis:6379",        "cache",    (200,    2000),  0.7),
        ],
    },
    "lendpath-document-service": {
        "node_names": ["doc-svc-01"],
        "transactions": [
            {"name": "POST /api/v1/documents/upload",  "type": "request", "weight": 35},
            {"name": "GET /api/v1/documents/verify",   "type": "request", "weight": 25},
            {"name": "POST /api/v1/documents/package", "type": "request", "weight": 20},
        ],
        "downstream_spans": [
            ("S3 PutObject document",      "storage", "s3",
             "AWS S3:443",     "storage", (2000, 40000), 0.9),
            ("S3 GetObject document",      "storage", "s3",
             "AWS S3:443",     "storage", (1000, 20000), 0.6),
            ("INSERT INTO document_store", "db",      "oracle",
             "Oracle DB:1521", "db",      (500,   8000), 0.8),
            ("SELECT FROM document_store", "db",      "oracle",
             "Oracle DB:1521", "db",      (300,   5000), 0.7),
        ],
    },
    "lendpath-appraisal-service": {
        "node_names": ["appraisal-svc-01"],
        "transactions": [
            {"name": "POST /api/v1/appraisal/order",   "type": "request", "weight": 25},
            {"name": "GET /api/v1/appraisal/results",  "type": "request", "weight": 30},
        ],
        "downstream_spans": [
            ("CoreLogic AVM API",          "external", "http",
             "CoreLogic API:443", "external", (20000, 300000), 0.6),
            ("INSERT INTO appraisals",     "db",       "oracle",
             "Oracle DB:1521",   "db",      (500,  10000),  0.85),
            ("SELECT FROM appraisals",     "db",       "oracle",
             "Oracle DB:1521",   "db",      (300,   5000),  0.75),
            ("Redis GET appraisal-cache",  "cache",    "redis",
             "Redis:6379",       "cache",   (200,   2000),  0.5),
        ],
    },
}

# User pool — stable IDs so geo ML job builds per-user home ranges
USERS = [
    {"id": "LO-001", "name": "James Whitfield",   "roles": ["loan_officer"]},
    {"id": "LO-002", "name": "Maria Santos",       "roles": ["loan_officer"]},
    {"id": "LO-003", "name": "David Kim",          "roles": ["loan_officer"]},
    {"id": "LO-004", "name": "Sarah Mitchell",     "roles": ["loan_officer"]},
    {"id": "LO-005", "name": "Robert Okafor",      "roles": ["loan_officer"]},
    {"id": "PROC-01","name": "Angela Torres",      "roles": ["processor"]},
    {"id": "UW-001", "name": "Thomas Brennan",     "roles": ["underwriter"]},
    {"id": "UW-002", "name": "Linda Chen",         "roles": ["underwriter"]},
    {"id": "ADMIN",  "name": "System Admin",       "roles": ["admin"]},
]

# Stable geo home cities per user — makes geo ML job anomalies detectable
USER_HOME_GEO = {
    "LO-001": {"lat": 40.4406, "lon": -79.9959, "city": "Pittsburgh",    "region": "Pennsylvania",   "country": "US"},
    "LO-002": {"lat": 39.9526, "lon": -75.1652, "city": "Philadelphia",  "region": "Pennsylvania",   "country": "US"},
    "LO-003": {"lat": 39.9612, "lon": -82.9988, "city": "Columbus",      "region": "Ohio",           "country": "US"},
    "LO-004": {"lat": 41.4993, "lon": -81.6944, "city": "Cleveland",     "region": "Ohio",           "country": "US"},
    "LO-005": {"lat": 42.3314, "lon": -83.0458, "city": "Detroit",       "region": "Michigan",       "country": "US"},
    "PROC-01": {"lat": 41.8781, "lon": -87.6298, "city": "Chicago",      "region": "Illinois",       "country": "US"},
    "UW-001": {"lat": 39.7684, "lon": -86.1581, "city": "Indianapolis",  "region": "Indiana",        "country": "US"},
    "UW-002": {"lat": 36.1627, "lon": -86.7816, "city": "Nashville",     "region": "Tennessee",      "country": "US"},
    "ADMIN":  {"lat": 40.4406, "lon": -79.9959, "city": "Pittsburgh",    "region": "Pennsylvania",   "country": "US"},
}

# Anomalous geo locations — injected occasionally to exercise the ML geo job
ANOMALOUS_GEO = [
    {"lat":  6.5244, "lon":  3.3792, "city": "Lagos",      "region": "Lagos",   "country": "NG"},
    {"lat": 55.7558, "lon": 37.6173, "city": "Moscow",     "region": "Moscow",  "country": "RU"},
    {"lat": 39.9042, "lon": 116.4074,"city": "Beijing",    "region": "Beijing", "country": "CN"},
    {"lat": 51.5074, "lon": -0.1278, "city": "London",     "region": "England", "country": "GB"},
    {"lat": 19.4326, "lon": -99.1332,"city": "Mexico City","region": "CDMX",    "country": "MX"},
]


def _hex(n_bytes: int) -> str:
    return secrets.token_hex(n_bytes)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _geo_for_user(user_id: str, anomaly_chance: float = 0.03) -> dict:
    """Return stable home geo with occasional anomalous location."""
    if random.random() < anomaly_chance:
        return random.choice(ANOMALOUS_GEO)
    return USER_HOME_GEO.get(user_id, USER_HOME_GEO["LO-001"])


def _result_for_status(status: int) -> str:
    if status < 300:
        return "HTTP 2xx"
    if status < 400:
        return "HTTP 3xx"
    if status < 500:
        return "HTTP 4xx"
    return "HTTP 5xx"


def _weighted_choice(items: list) -> dict:
    total = sum(i["weight"] for i in items)
    r = random.uniform(0, total)
    cum = 0
    for item in items:
        cum += item["weight"]
        if r <= cum:
            return item
    return items[-1]


def build_base(service_name: str, node_name: str) -> dict:
    """Fields common to every transaction and span document."""
    svc = SERVICES[service_name]
    return {
        "ecs":   {"version": "8.11.0"},
        "data_stream": {"type": "traces", "dataset": "apm", "namespace": "default"},
        "agent": {"name": "java", "version": "1.44.0", "ephemeral_id": _hex(8)},
        "service": {
            "name":        service_name,
            "version":     random.choice(["3.1.0", "3.1.1", "3.2.0", "3.2.1"]),
            "environment": "production",
            "language":    {"name": "Java", "version": "17.0.8"},
            "runtime":     {"name": "Java", "version": "17.0.8"},
            "framework":   {"name": "Spring Boot", "version": "3.1.5"},
            "node":        {"name": node_name},
        },
        "host": {
            "hostname":     node_name,
            "name":         node_name,
            "architecture": "amd64",
            "os":           {"platform": "linux"},
        },
        "process": {"pid": random.randint(1000, 32767), "title": "java"},
        # observer fields — required by Kibana APM UI to recognise documents
        # as coming from an APM Server and render the service map
        "observer": {
            "type":        "apm-server",
            "version":     "8.12.0",
            "version_major": 8,
        },
    }


# App service names that can appear as downstream targets of lendpath-los.
# When lendpath-los calls one of these, we generate a linked child transaction
# for that service within the same trace — this is what Kibana's service map
# requires to draw app-to-app edges. Without a matching transaction doc from
# the downstream service sharing the same trace.id, the edge never appears.
_APP_SERVICE_TARGETS = {
    "lendpath-underwriting",
    "lendpath-credit-service",
    "lendpath-document-service",
    "lendpath-appraisal-service",
}


def _make_child_transaction(child_svc_name: str, trace_id: str,
                             parent_span_id: str, user: dict,
                             geo: dict, ts: str,
                             txn_id: str = None) -> list:
    """
    Generate a transaction + spans for a downstream app service,
    linked into an existing trace via trace_id and parent_span_id.
    This is what makes Kibana draw the edge on the service map:
    the downstream service must have its own transaction doc that
    shares the same trace.id as the caller.
    """
    svc       = SERVICES[child_svc_name]
    node_name = random.choice(svc["node_names"])
    txn_cfg   = _weighted_choice(svc["transactions"])
    txn_id    = txn_id or _hex(8)

    status  = random.choices(
        [200, 201, 400, 404, 500],
        weights=[60, 10, 10, 10, 10]
    )[0]
    outcome = "success" if status < 400 else "failure"

    child_spans   = []
    total_span_us = 0

    for (span_name, span_type, span_subtype,
         dest_resource, target_type, dur_range, probability) in svc["downstream_spans"]:
        if random.random() > probability:
            continue
        dur_us = random.randint(*dur_range)
        total_span_us += dur_us
        span_id = _hex(8)

        child_spans.append({
            **build_base(child_svc_name, node_name),
            "@timestamp":  ts,
            "processor":   {"event": "span", "name": "span"},
            "event":       {"kind": "event", "outcome": "success"},
            "trace":       {"id": trace_id},
            "transaction": {"id": txn_id},
            "parent":      {"id": txn_id},
            "span": {
                "id":       span_id,
                "name":     span_name,
                "type":     span_type,
                "subtype":  span_subtype,
                "action":   ("query"   if span_type == "db"
                             else "request" if span_type in ("external","storage")
                             else "send"),
                "duration": {"us": dur_us},
                # span.destination.service — nested here for Kibana service map
                "destination": {
                    "service": {
                        "name":     dest_resource,
                        "resource": dest_resource,
                        "type":     target_type,
                    }
                },
            },
            # top-level destination for backwards compatibility
            "destination": {
                "service": {
                    "name":     dest_resource,
                    "resource": dest_resource,
                    "type":     target_type,
                }
            },
            "service": {
                **build_base(child_svc_name, node_name)["service"],
                "target": {
                    "name": dest_resource,
                    "type": target_type,
                },
            },
        })

    txn_dur_us = total_span_us + random.randint(500, 5000)
    txn_name   = txn_cfg["name"]
    url_path   = txn_name.split(" ")[-1] if " " in txn_name else "/internal"

    child_txn = {
        **build_base(child_svc_name, node_name),
        "@timestamp":  ts,
        "processor":   {"event": "transaction", "name": "transaction"},
        "event": {
            "kind":     "event",
            "outcome":  outcome,
            "duration": txn_dur_us * 1000,
        },
        "trace":       {"id": trace_id},
        "transaction": {
            "id":       txn_id,
            "name":     txn_name,
            "type":     txn_cfg["type"],
            "result":   _result_for_status(status),
            "sampled":  True,
            "duration": {"us": txn_dur_us},
        },
        # parent.id links this transaction to the calling span in lendpath-los
        "parent": {"id": parent_span_id},
        "http": {
            "request":  {"method": "POST" if "POST" in txn_name else "GET"},
            "response": {"status_code": status},
            "version":  "1.1",
        },
        "url": {
            "full":   f"https://api.lendpath.com{url_path}",
            "path":   url_path,
            "domain": "api.lendpath.com",
        },
        "client": {
            "ip":  f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "geo": {
                "location":         {"lat": geo["lat"], "lon": geo["lon"]},
                "city_name":        geo["city"],
                "region_name":      geo["region"],
                "country_iso_code": geo["country"],
            },
        },
        "user": {"id": user["id"], "name": user["name"], "roles": user["roles"]},
    }

    return [child_txn] + child_spans


def generate_trace(service_name: str, anomaly_chance: float = 0.03) -> list:
    """
    Generate a complete distributed trace.

    For entry-point services (lendpath-los), this produces:
      - Root transaction for lendpath-los
      - Outbound spans to each downstream target
      - For downstream APP services (underwriting, credit, document, appraisal):
        a linked child transaction + their own spans, all sharing the same
        trace.id — this is what Kibana's service map requires to draw
        app-to-app edges between services.
      - Infrastructure spans (Oracle, Redis, Kafka, S3, external APIs) remain
        as spans only — Kibana identifies these by destination.service.resource.

    Returns a list of Elasticsearch bulk action dicts.
    """
    svc         = SERVICES[service_name]
    node_name   = random.choice(svc["node_names"])
    txn_cfg     = _weighted_choice(svc["transactions"])
    user        = random.choice(USERS)
    geo         = _geo_for_user(user["id"], anomaly_chance=anomaly_chance)

    trace_id    = _hex(16)
    txn_id      = _hex(8)
    ts          = _now_iso()

    status      = random.choices(
        [200, 201, 400, 401, 403, 404, 500, 502],
        weights=[50, 10, 8, 5, 3, 6, 5, 3]
    )[0]
    outcome     = "success" if status < 400 else "failure"

    spans         = []
    child_docs    = []   # linked transactions+spans from downstream app services
    total_span_us = 0

    for (span_name, span_type, span_subtype,
         dest_resource, target_type, dur_range, probability) in svc["downstream_spans"]:
        if random.random() > probability:
            continue
        dur_us    = random.randint(*dur_range)
        span_id   = _hex(8)
        total_span_us += dur_us

        span_doc = {
            **build_base(service_name, node_name),
            "@timestamp": ts,
            "timestamp":  {"us": int(datetime.now(timezone.utc).timestamp() * 1_000_000)},
            "processor": {"event": "span", "name": "span"},
            "event":     {"kind": "event", "outcome": "success"},
            "trace":     {"id": trace_id},
            "transaction": {"id": txn_id},
            "parent":    {"id": txn_id},
            "span": {
                "id":       span_id,
                "name":     span_name,
                "type":     span_type,
                "subtype":  span_subtype,
                "action":   ("query"   if span_type == "db"
                             else "request" if span_type in ("external","storage")
                             else "send"),
                "duration": {"us": dur_us},
                # span.destination.service — the field Kibana service map
                # aggregates on to build edges. Must be nested inside span{}.
                "destination": {
                    "service": {
                        "name":     dest_resource,
                        "resource": dest_resource,
                        "type":     target_type,
                    }
                },
            },
            # Also keep top-level destination.service for backwards compatibility
            # with older Kibana versions and other tooling
            "destination": {
                "service": {
                    "name":     dest_resource,
                    "resource": dest_resource,
                    "type":     target_type,
                }
            },
            # service.target — downstream node identity (8.x service map)
            "service": {
                **build_base(service_name, node_name)["service"],
                "target": {
                    "name": dest_resource,
                    "type": target_type,
                },
            },
        }
        spans.append(span_doc)

        # For app-to-app calls: generate a linked child transaction in the
        # downstream service so Kibana can draw the edge on the service map.
        # The child transaction's parent.id points to this span's span_id,
        # creating a proper parent-child chain within the trace.
        if dest_resource in _APP_SERVICE_TARGETS:
            child_txn_id = _hex(8)
            span_doc["child"] = {"id": child_txn_id}
            child_docs.extend(
                _make_child_transaction(
                    dest_resource, trace_id, span_id, user, geo, ts,
                    txn_id=child_txn_id
                )
            )

    txn_dur_us  = total_span_us + random.randint(500, 5000)

    # ── Root transaction document ─────────────────────────────────────────────
    txn_doc = {
        **build_base(service_name, node_name),
        "@timestamp": ts,
        "timestamp":  {"us": int(datetime.now(timezone.utc).timestamp() * 1_000_000)},
        "processor": {"event": "transaction", "name": "transaction"},
        "event": {
            "kind":     "event",
            "outcome":  outcome,
            "duration": txn_dur_us * 1000,   # nanoseconds for event.duration
        },
        "trace":       {"id": trace_id},
        "transaction": {
            "id":       txn_id,
            "name":     txn_cfg["name"],
            "type":     txn_cfg["type"],
            "result":   _result_for_status(status),
            "sampled":  True,
            "duration": {"us": txn_dur_us},
        },
        "http": {
            "request":  {"method": "POST" if "POST" in txn_cfg["name"] else "GET"},
            "response": {"status_code": status},
            "version":  "1.1",
        },
        "url": {
            "full":   f"https://api.lendpath.com{txn_cfg['name'].split(' ')[-1] if ' ' in txn_cfg['name'] else '/internal'}",
            "path":   txn_cfg["name"].split(" ")[-1] if " " in txn_cfg["name"] else "/internal",
            "domain": "api.lendpath.com",
        },
        "client": {
            "ip":  f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "geo": {
                "location":         {"lat": geo["lat"], "lon": geo["lon"]},
                "city_name":        geo["city"],
                "region_name":      geo["region"],
                "country_iso_code": geo["country"],
            },
        },
        "user": {
            "id":    user["id"],
            "name":  user["name"],
            "roles": user["roles"],
        },
        # DFA outlier features
        "labels": {
            "loan_amount":          round(random.uniform(75000, 2500000), 2),
            "session_duration_sec": random.randint(30, 7200),
            "pages_visited":        random.randint(1, 80),
            "docs_downloaded":      random.randint(0, 25),
            "failed_auth_attempts": random.randint(0, 3),
        },
    }

    # Wrap into bulk actions — include linked child transactions from
    # downstream app services so Kibana can draw service map edges
    index = "traces-apm-default"
    docs  = [txn_doc] + spans + child_docs
    return [
        {"_op_type": "create", "_index": index, "_source": doc}
        for doc in docs
    ]


def build_es_client(host: str, user: str, password: str, verify_ssl: bool):
    return Elasticsearch(
        host,
        basic_auth=(user, password),
        verify_certs=verify_ssl,
        ssl_show_warn=False,
        request_timeout=30,
    )



# =============================================================================
# APM METRICS — JVM + System metrics per service node
# =============================================================================
# Emitted to metrics-apm.app.{service}-default (app metrics)
# Per the APM spec, metricset documents use processor.event = "metric"
# Fields required for Kibana APM service dashboards:
#   system.cpu.total.norm.pct    — CPU utilisation 0.0-1.0
#   system.memory.actual.free    — free memory bytes
#   system.memory.total          — total memory bytes
#   jvm.memory.heap.used         — heap used bytes
#   jvm.memory.heap.max          — heap max bytes
#   jvm.memory.heap.committed    — heap committed bytes
#   jvm.memory.non_heap.used     — non-heap used bytes
#   jvm.memory.heap.pool.used    — per-pool heap used
#   jvm.memory.heap.pool.max     — per-pool heap max
#   jvm.memory.heap.pool.committed — per-pool heap committed
#   labels.name                  — GC/pool name label
#   jvm.thread.count             — active thread count
#   jvm.gc.time                  — GC time ms
#   jvm.gc.count                 — GC count
#   jvm.gc.alloc                 — bytes allocated since last GC
#   timestamp.us                 — microsecond timestamp

# JVM memory pool names (Eden, Survivor, Old Gen)
_JVM_POOLS = ["Eden Space", "Survivor Space", "Tenured Gen"]
_GC_NAMES  = ["G1 Young Generation", "G1 Old Generation"]

# Per-service JVM baseline sizes (heap_max_bytes, ram_bytes)
_SVC_JVM = {
    "lendpath-los":              {"heap_max": 2 * 1024**3, "ram": 8 * 1024**3},
    "lendpath-underwriting":     {"heap_max": 2 * 1024**3, "ram": 8 * 1024**3},
    "lendpath-credit-service":   {"heap_max": 1 * 1024**3, "ram": 4 * 1024**3},
    "lendpath-document-service": {"heap_max": 1 * 1024**3, "ram": 4 * 1024**3},
    "lendpath-appraisal-service":{"heap_max": 1 * 1024**3, "ram": 4 * 1024**3},
}


def generate_metrics(service_name: str, ts_iso: str = None) -> list:
    """
    Generate a set of APM metricset documents for one service node.
    Returns a list of bulk action dicts ready for helpers.bulk().

    Emits:
      1 × metricset.name="app" doc  — CPU + memory overview
      N × metricset.name="jvmmetrics" docs  — one per JVM pool
      1 × metricset.name="transaction" summary (optional, lightweight)
    """
    if ts_iso is None:
        ts_iso = _now_iso()

    svc    = SERVICES[service_name]
    jvm    = _SVC_JVM[service_name]
    node   = random.choice(svc["node_names"])
    base   = build_base(service_name, node)
    INDEX  = f"metrics-apm.app.{service_name}-default"

    heap_max       = jvm["heap_max"]
    heap_used      = int(heap_max * random.uniform(0.35, 0.80))
    heap_committed = int(heap_max * random.uniform(heap_used/heap_max, 0.90))
    non_heap_used  = int(random.uniform(80 * 1024**2, 200 * 1024**2))
    ram_total      = jvm["ram"]
    ram_free       = int(ram_total * random.uniform(0.20, 0.60))
    cpu_pct        = round(random.uniform(0.05, 0.75), 4)
    thread_count   = random.randint(20, 120)
    ts_us          = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    actions = []

    # ── 1. App / system metricset ─────────────────────────────────────────────
    app_doc = {
        **base,
        "@timestamp":   ts_iso,
        "timestamp":    {"us": ts_us},
        "processor":    {"event": "metric", "name": "metric"},
        "metricset":    {"name": "app", "interval": "1m"},
        "event":        {"kind": "metric"},
        "system": {
            "cpu": {
                "total": {"norm": {"pct": cpu_pct}}
            },
            "memory": {
                "actual": {"free": ram_free},
                "total":  ram_total,
            },
        },
        "jvm": {
            "memory": {
                "heap": {
                    "used":      heap_used,
                    "max":       heap_max,
                    "committed": heap_committed,
                },
                "non_heap": {
                    "used":      non_heap_used,
                    "committed": int(non_heap_used * random.uniform(1.0, 1.25)),
                },
            },
            "thread": {"count": thread_count},
        },
    }
    actions.append({"_op_type": "create", "_index": INDEX, "_source": app_doc})

    # ── 2. Per-pool JVM metricsets ────────────────────────────────────────────
    remaining_heap = heap_used
    for i, pool_name in enumerate(_JVM_POOLS):
        # Distribute heap_used across pools (Eden gets most)
        if i < len(_JVM_POOLS) - 1:
            pool_used = int(remaining_heap * random.uniform(0.3, 0.6))
        else:
            pool_used = remaining_heap
        remaining_heap -= pool_used

        pool_max       = int(heap_max * random.uniform(0.25, 0.45))
        pool_committed = min(int(pool_used * random.uniform(1.0, 1.3)), pool_max)
        gc_time        = random.randint(0, 150)
        gc_count       = random.randint(0, 25)
        gc_alloc       = int(random.uniform(0, 512 * 1024**2))

        pool_doc = {
            **base,
            "@timestamp":   ts_iso,
            "timestamp":    {"us": ts_us},
            "processor":    {"event": "metric", "name": "metric"},
            "metricset":    {"name": "jvmmetrics", "interval": "1m"},
            "event":        {"kind": "metric"},
            "labels":       {"name": pool_name},
            "jvm": {
                "memory": {
                    "heap": {
                        "pool": {
                            "used":      pool_used,
                            "max":       pool_max,
                            "committed": pool_committed,
                        }
                    },
                },
                "gc": {
                    "time":  gc_time,
                    "count": gc_count,
                    "alloc": gc_alloc,
                },
                "thread": {"count": thread_count},
            },
        }
        actions.append({"_op_type": "create", "_index": INDEX, "_source": pool_doc})

    return actions

def run(host, user, password, verify_ssl, rate, once, purge=False, anomaly_chance=0.03):
    es = build_es_client(host, user, password, verify_ssl)
    try:
        info = es.info()
        print(f"Connected to Elasticsearch {info['version']['number']}")
    except Exception as e:
        print(f"Cannot connect: {e}")
        sys.exit(1)

    if purge:
        print("Purging traces-apm-default (removing stale SDG-generated traces)…")
        try:
            es.indices.delete(index="traces-apm-default", ignore_unavailable=True)
            print("  ✓ Deleted traces-apm-default data stream")
        except Exception as e:
            print(f"  ⚠ Could not delete data stream: {e}")
            print("    Continuing anyway — new linked traces will be added alongside old ones.")
        print()

    service_names = list(SERVICES.keys())
    # Weight initiating services toward lendpath-los (the entry point)
    service_weights = [40, 20, 15, 15, 10]
    interval = 1.0 / rate

    total_docs = 0
    total_traces = 0
    print(f"\nGenerating {rate} trace(s)/sec across {len(service_names)} services.")
    print("Press Ctrl+C to stop.\n")

    metrics_interval = 30   # emit JVM/system metrics every N traces
    try:
        while True:
            # Pick which service initiates this trace
            svc_name = random.choices(service_names, weights=service_weights, k=1)[0]
            actions  = generate_trace(svc_name, anomaly_chance=anomaly_chance)

            # Periodically emit JVM + system metrics for all services
            if total_traces % metrics_interval == 0:
                for msvc in service_names:
                    actions.extend(generate_metrics(msvc))

            try:
                ok, errors = helpers.bulk(es, actions, raise_on_error=False)
                total_docs   += ok
                total_traces += 1
                if errors:
                    print(f"  ⚠ Bulk errors: {errors[:2]}")
            except Exception as e:
                print(f"  ✗ Error indexing trace: {e}")

            if total_traces % 20 == 0:
                print(f"  ✓ {total_traces:,} traces / {total_docs:,} documents indexed")

            if once:
                # Generate one batch of ~100 traces then exit
                if total_traces >= 100:
                    break

            time.sleep(interval)

    except KeyboardInterrupt:
        pass

    print(f"\nDone. {total_traces:,} traces / {total_docs:,} documents indexed.")


def main():
    p = argparse.ArgumentParser(
        description="Generate linked APM traces for the LendPath Mortgage workshop"
    )
    p.add_argument("--host",          default="https://localhost:9200")
    p.add_argument("--user",          default="elastic")
    p.add_argument("--password",      default="changeme")
    p.add_argument("--no-verify-ssl", action="store_true")
    p.add_argument("--rate",          type=float, default=2.0,
                   help="Traces per second (default: 2)")
    p.add_argument("--once",          action="store_true",
                   help="Generate ~100 traces then exit (useful for initial seeding)")
    p.add_argument("--anomaly-chance", type=float, default=0.03,
                   metavar="RATE",
                   help="Geo anomaly injection rate, 0.0–1.0 "
                        "(default: 0.03 = 3%%). Set higher (e.g. 0.30) "
                        "during ML demo sessions to produce strong geo signals.")
    p.add_argument("--purge",         action="store_true",
                   help="Delete all existing documents from traces-apm-default "
                        "before starting. Use this to clear out stale SDG-generated "
                        "unlinked traces that break the APM Service Map.")
    args = p.parse_args()
    run(args.host, args.user, args.password,
        not args.no_verify_ssl, args.rate, args.once, args.purge,
        anomaly_chance=args.anomaly_chance)


if __name__ == "__main__":
    main()
