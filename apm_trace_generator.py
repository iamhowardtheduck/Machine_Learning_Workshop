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
import sys
import time
from datetime import datetime, timezone

try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    print("ERROR: elasticsearch-py not installed.")
    print("Run: pip install elasticsearch")
    sys.exit(1)


# ─── Service topology ────────────────────────────────────────────────────────

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
        "downstream_spans": [
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

# User pool
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

USER_HOME_GEO = {
    "LO-001":  {"lat": 40.4406, "lon": -79.9959, "city": "Pittsburgh",   "region": "Pennsylvania", "country": "US"},
    "LO-002":  {"lat": 39.9526, "lon": -75.1652, "city": "Philadelphia", "region": "Pennsylvania", "country": "US"},
    "LO-003":  {"lat": 39.9612, "lon": -82.9988, "city": "Columbus",     "region": "Ohio",         "country": "US"},
    "LO-004":  {"lat": 41.4993, "lon": -81.6944, "city": "Cleveland",    "region": "Ohio",         "country": "US"},
    "LO-005":  {"lat": 42.3314, "lon": -83.0458, "city": "Detroit",      "region": "Michigan",     "country": "US"},
    "PROC-01": {"lat": 41.8781, "lon": -87.6298, "city": "Chicago",      "region": "Illinois",     "country": "US"},
    "UW-001":  {"lat": 39.7684, "lon": -86.1581, "city": "Indianapolis", "region": "Indiana",      "country": "US"},
    "UW-002":  {"lat": 36.1627, "lon": -86.7816, "city": "Nashville",    "region": "Tennessee",    "country": "US"},
    "ADMIN":   {"lat": 40.4406, "lon": -79.9959, "city": "Pittsburgh",   "region": "Pennsylvania", "country": "US"},
}

ANOMALOUS_GEO = [
    {"lat":  6.5244, "lon":  3.3792, "city": "Lagos",       "region": "Lagos",   "country": "NG"},
    {"lat": 55.7558, "lon": 37.6173, "city": "Moscow",      "region": "Moscow",  "country": "RU"},
    {"lat": 39.9042, "lon": 116.4074,"city": "Beijing",     "region": "Beijing", "country": "CN"},
    {"lat": 51.5074, "lon": -0.1278, "city": "London",      "region": "England", "country": "GB"},
    {"lat": 19.4326, "lon": -99.1332,"city": "Mexico City", "region": "CDMX",    "country": "MX"},
]

# ─── Per-service error pool ───────────────────────────────────────────────────
# Each entry: (exception_type, culprit_fn, message_template, error_code)
# message_template may contain {detail} which is filled with a random value.
_SERVICE_ERRORS = {
    "lendpath-los": [
        ("IllegalStateException",  "LoanPipelineService.submit",
         "Loan application {detail} failed validation: missing borrower SSN", "500"),
        ("TimeoutException",       "UnderwritingClient.submitDecision",
         "Upstream call to lendpath-underwriting timed out after 30000ms",   "503"),
        ("NullPointerException",   "EscrowCalculator.compute",
         "Escrow calculation failed: payment schedule was null",              "500"),
        ("IllegalArgumentException","RateService.getCurrentRate",
         "Invalid loan type '{detail}' passed to rate engine",               "400"),
    ],
    "lendpath-underwriting": [
        ("DataAccessException",    "LoanRepository.findById",
         "Failed to load application {detail} from Oracle: connection reset", "500"),
        ("KafkaProducerException", "DecisionProducer.send",
         "Failed to publish decision event to Kafka topic decision-results",  "500"),
        ("OptimisticLockException","ApplicationService.updateStage",
         "Concurrent update conflict on application {detail}",               "409"),
    ],
    "lendpath-credit-service": [
        ("HttpClientException",    "ExperianClient.fetchReport",
         "Experian API returned HTTP 429 for borrower {detail}",             "429"),
        ("SSLHandshakeException",  "EquifaxClient.connect",
         "TLS handshake failed connecting to Equifax API: certificate expired","503"),
        ("DataIntegrityException", "CreditScoreRepository.insert",
         "Duplicate credit score record for borrower {detail}",             "409"),
    ],
    "lendpath-document-service": [
        ("S3Exception",            "S3DocumentStore.putObject",
         "S3 PutObject failed for key documents/{detail}: Access Denied",    "403"),
        ("DocumentParseException", "PdfProcessor.extract",
         "Unable to parse PDF content for document {detail}",               "422"),
        ("DataAccessException",    "DocumentRepository.findByLoanId",
         "Oracle query timeout fetching documents for loan {detail}",        "504"),
    ],
    "lendpath-appraisal-service": [
        ("HttpClientException",    "CoreLogicClient.requestAvm",
         "CoreLogic AVM API returned HTTP 503 for property {detail}",        "503"),
        ("ValidationException",    "AppraisalService.validateOrder",
         "Appraisal order {detail} missing required property address field", "400"),
        ("DataAccessException",    "AppraisalRepository.save",
         "Oracle INSERT failed for appraisal {detail}: constraint violation", "500"),
    ],
}

# Realistic Java-style stacktrace frames per service
_SERVICE_FRAMES = {
    "lendpath-los": [
        ("com/lendpath/los/service/LoanPipelineService.java", "LoanPipelineService.submit", 142),
        ("com/lendpath/los/controller/ApplicationController.java", "ApplicationController.submitApplication", 87),
        ("com/lendpath/los/filter/AuthFilter.java", "AuthFilter.doFilter", 53),
        ("org/springframework/web/filter/OncePerRequestFilter.java", "OncePerRequestFilter.doFilterInternal", 116),
        ("org/springframework/web/servlet/DispatcherServlet.java", "DispatcherServlet.doDispatch", 1089),
    ],
    "lendpath-underwriting": [
        ("com/lendpath/underwriting/service/UnderwritingService.java", "UnderwritingService.processDecision", 208),
        ("com/lendpath/underwriting/repository/LoanRepository.java", "LoanRepository.findById", 67),
        ("org/springframework/data/jpa/repository/support/SimpleJpaRepository.java", "SimpleJpaRepository.findById", 279),
        ("org/hibernate/internal/SessionImpl.java", "SessionImpl.find", 1138),
    ],
    "lendpath-credit-service": [
        ("com/lendpath/credit/client/ExperianClient.java", "ExperianClient.fetchReport", 95),
        ("com/lendpath/credit/service/CreditCheckService.java", "CreditCheckService.check", 134),
        ("com/lendpath/credit/controller/CreditController.java", "CreditController.checkCredit", 61),
        ("org/springframework/web/servlet/DispatcherServlet.java", "DispatcherServlet.doDispatch", 1089),
    ],
    "lendpath-document-service": [
        ("com/lendpath/documents/store/S3DocumentStore.java", "S3DocumentStore.putObject", 78),
        ("com/lendpath/documents/service/DocumentService.java", "DocumentService.storePackage", 155),
        ("com/lendpath/documents/controller/DocumentController.java", "DocumentController.upload", 49),
        ("org/springframework/web/servlet/DispatcherServlet.java", "DispatcherServlet.doDispatch", 1089),
    ],
    "lendpath-appraisal-service": [
        ("com/lendpath/appraisal/client/CoreLogicClient.java", "CoreLogicClient.requestAvm", 112),
        ("com/lendpath/appraisal/service/AppraisalService.java", "AppraisalService.orderAppraisal", 189),
        ("com/lendpath/appraisal/controller/AppraisalController.java", "AppraisalController.order", 55),
        ("org/springframework/web/servlet/DispatcherServlet.java", "DispatcherServlet.doDispatch", 1089),
    ],
}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _hex(n_bytes: int) -> str:
    return secrets.token_hex(n_bytes)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _ts_us(ts_iso: str = None) -> int:
    """Return microsecond epoch for a given ISO string, or now."""
    if ts_iso:
        try:
            dt = datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            pass
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)



def _geo_for_user(user_id: str, anomaly_chance: float = 0.03) -> dict:
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
    """Fields common to every document (transaction, span, error, metric)."""
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
        "observer": {
            "type":          "apm-server",
            "version":       "8.12.0",
            "version_major": 8,
        },
    }


_APP_SERVICE_TARGETS = {
    "lendpath-underwriting",
    "lendpath-credit-service",
    "lendpath-document-service",
    "lendpath-appraisal-service",
}


# ─── Error document generator ─────────────────────────────────────────────────

def generate_error(
    service_name: str,
    trace_id: str,
    transaction_id: str,
    span_id: str,
    ts_iso: str,
    node_name: str,
    txn_name: str,
    txn_type: str,
    http_method: str = "GET",
    error_rate_override: float = None,
) -> dict:
    """
    Build a single APM error document that matches the structure expected by
    Kibana's APM Error view and Elastic ML error-rate jobs.

    Fields added vs the old approach (mutating transaction docs):
      • data_stream.type = "logs", data_stream.dataset = "apm.error"
      • processor.event = "error"
      • event.dataset = "apm.error"
      • error.id      — unique 32-char hex
      • error.culprit — culprit function name
      NOTE: error.grouping_key and error.grouping_name are scripted/runtime
      fields computed by APM Server — do NOT send them; ES rejects them with
      "Cannot index data directly into a field with a [script] parameter".
      • error.exception[]   — type, code, message, stacktrace frames
      • parent.id           — the span that triggered this error
      • span.id             — same as parent span
      • timestamp.us        — epoch microseconds derived from ts_iso
    """
    errors_for_svc = _SERVICE_ERRORS.get(service_name, _SERVICE_ERRORS["lendpath-los"])
    exc_type, culprit, msg_tpl, exc_code = random.choice(errors_for_svc)

    # Fill in any {detail} placeholder with a realistic-looking value
    detail = random.choice([
        f"APP-{random.randint(100000, 999999)}",
        f"loan_{random.randint(10000, 99999)}",
        f"borrower_{random.randint(1000, 9999)}",
        f"doc_{random.randint(10000, 99999)}",
    ])
    message = msg_tpl.format(detail=detail)

    frames = _SERVICE_FRAMES.get(service_name, _SERVICE_FRAMES["lendpath-los"])
    stacktrace = [
        {
            "filename":              frame[0],
            "function":              frame[1],
            "line":                  {"number": frame[2] + random.randint(-5, 5)},
            "exclude_from_grouping": False,
        }
        for frame in frames
    ]

    base = build_base(service_name, node_name)
    # Error docs go to logs-apm.error-default, not traces-apm-default
    base["data_stream"] = {
        "type":      "logs",
        "dataset":   "apm.error",
        "namespace": "default",
    }

    ts_us = _ts_us(ts_iso)

    return {
        **base,
        "@timestamp": ts_iso,
        "timestamp":  {"us": ts_us},
        "processor":  {"event": "error", "name": "error"},
        "event": {
            "dataset": "apm.error",
            "outcome": "failure",
        },
        "message": message,
        "error": {
            "id":      _hex(16),
            "culprit": culprit,
            "exception": [
                {
                    "type":       exc_type,
                    "code":       exc_code,
                    "message":    message,
                    "stacktrace": stacktrace,
                }
            ],
        },
        "trace":       {"id": trace_id},
        "transaction": {
            "id":   transaction_id,
            "name": txn_name,
            "type": txn_type,
        },
        # parent.id and span.id both point to the owning span
        "parent": {"id": span_id},
        "span":   {"id": span_id},
        "http": {
            "request": {"method": http_method},
        },
    }


# ─── Child transaction builder ────────────────────────────────────────────────

def _make_child_transaction(child_svc_name: str, trace_id: str,
                             parent_span_id: str, user: dict,
                             geo: dict, ts: str,
                             txn_id: str = None,
                             error_rate: float = 0.05) -> list:
    """
    Generate a transaction + spans for a downstream app service,
    linked into an existing trace via trace_id and parent_span_id.
    Emits an error doc when the transaction fails.
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
    last_span_id  = parent_span_id

    for (span_name, span_type, span_subtype,
         dest_resource, target_type, dur_range, probability) in svc["downstream_spans"]:
        if random.random() > probability:
            continue
        dur_us = random.randint(*dur_range)
        total_span_us += dur_us
        span_id = _hex(8)
        last_span_id = span_id

        child_spans.append({
            **build_base(child_svc_name, node_name),
            "@timestamp":  ts,
            "timestamp":   {"us": _ts_us(ts)},
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
                "destination": {
                    "service": {
                        "name":     dest_resource,
                        "resource": dest_resource,
                        "type":     target_type,
                    }
                },
            },
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
    http_method = "POST" if "POST" in txn_name else "GET"

    child_txn = {
        **build_base(child_svc_name, node_name),
        "@timestamp":  ts,
        "timestamp":   {"us": _ts_us(ts)},
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
        "parent": {"id": parent_span_id},
        "http": {
            "request":  {"method": http_method},
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

    docs = [child_txn] + child_spans

    # Emit an error doc when the transaction failed or randomly at error_rate
    if outcome == "failure" or random.random() < error_rate:
        err_doc = generate_error(
            service_name=child_svc_name,
            trace_id=trace_id,
            transaction_id=txn_id,
            span_id=last_span_id,
            ts_iso=ts,
            node_name=node_name,
            txn_name=txn_name,
            txn_type=txn_cfg["type"],
            http_method=http_method,
        )
        docs.append(err_doc)

    return docs


# ─── Root trace generator ─────────────────────────────────────────────────────

def generate_trace(service_name: str, anomaly_chance: float = 0.03,
                   error_rate: float = 0.05,
                   spike_error_rate: float = None,
                   spike_latency_mult: float = None) -> list:
    """
    Generate a complete distributed trace.

    spike_error_rate:   if set, overrides the per-trace error probability
    spike_latency_mult: if set, multiplies all span/transaction durations

    Returns a list of Elasticsearch bulk action dicts.
    Errors go to .ds-logs-apm.error-default-* via data_stream routing.
    All other docs go to traces-apm-default.
    """
    svc         = SERVICES[service_name]
    node_name   = random.choice(svc["node_names"])
    txn_cfg     = _weighted_choice(svc["transactions"])
    user        = random.choice(USERS)
    geo         = _geo_for_user(user["id"], anomaly_chance=anomaly_chance)

    trace_id    = _hex(16)
    txn_id      = _hex(8)
    ts          = _now_iso()
    ts_us       = _ts_us(ts)

    effective_error_rate = spike_error_rate if spike_error_rate is not None else error_rate

    status = random.choices(
        [200, 201, 400, 401, 403, 404, 500, 502],
        weights=[50, 10, 8, 5, 3, 6, 5, 3]
    )[0]
    # During error spikes, heavily bias toward 5xx
    if spike_error_rate is not None and random.random() < spike_error_rate:
        status = random.choices([500, 502, 503, 504], weights=[50, 20, 20, 10])[0]

    outcome = "success" if status < 400 else "failure"

    spans         = []
    child_docs    = []
    total_span_us = 0
    last_span_id  = txn_id

    for (span_name, span_type, span_subtype,
         dest_resource, target_type, dur_range, probability) in svc["downstream_spans"]:
        if random.random() > probability:
            continue
        dur_us = random.randint(*dur_range)
        if spike_latency_mult is not None:
            dur_us = int(dur_us * spike_latency_mult)
        span_id = _hex(8)
        total_span_us += dur_us
        last_span_id   = span_id

        span_doc = {
            **build_base(service_name, node_name),
            "@timestamp": ts,
            "timestamp":  {"us": ts_us},
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
                "destination": {
                    "service": {
                        "name":     dest_resource,
                        "resource": dest_resource,
                        "type":     target_type,
                    }
                },
            },
            "destination": {
                "service": {
                    "name":     dest_resource,
                    "resource": dest_resource,
                    "type":     target_type,
                }
            },
            "service": {
                **build_base(service_name, node_name)["service"],
                "target": {
                    "name": dest_resource,
                    "type": target_type,
                },
            },
        }
        spans.append(span_doc)

        if dest_resource in _APP_SERVICE_TARGETS:
            child_txn_id = _hex(8)
            span_doc["child"] = {"id": child_txn_id}
            child_docs.extend(
                _make_child_transaction(
                    dest_resource, trace_id, span_id, user, geo, ts,
                    txn_id=child_txn_id,
                    error_rate=effective_error_rate,
                )
            )

    txn_dur_us = total_span_us + random.randint(500, 5000)
    if spike_latency_mult is not None:
        txn_dur_us = int(txn_dur_us * spike_latency_mult)

    txn_name    = txn_cfg["name"]
    url_path    = txn_name.split(" ")[-1] if " " in txn_name else "/internal"
    http_method = "POST" if "POST" in txn_name else "GET"

    txn_doc = {
        **build_base(service_name, node_name),
        "@timestamp": ts,
        "timestamp":  {"us": ts_us},
        "processor": {"event": "transaction", "name": "transaction"},
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
        "http": {
            "request":  {"method": http_method},
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
        "user": {
            "id":    user["id"],
            "name":  user["name"],
            "roles": user["roles"],
        },
        "labels": {
            "loan_amount":          round(random.uniform(75000, 2500000), 2),
            "session_duration_sec": random.randint(30, 7200),
            "pages_visited":        random.randint(1, 80),
            "docs_downloaded":      random.randint(0, 25),
            "failed_auth_attempts": random.randint(0, 3),
        },
    }

    # Emit an error doc when the root transaction failed or randomly at error_rate
    root_error_docs = []
    if outcome == "failure" or random.random() < effective_error_rate:
        err_doc = generate_error(
            service_name=service_name,
            trace_id=trace_id,
            transaction_id=txn_id,
            span_id=last_span_id,
            ts_iso=ts,
            node_name=node_name,
            txn_name=txn_name,
            txn_type=txn_cfg["type"],
            http_method=http_method,
        )
        root_error_docs.append(err_doc)

    # Build bulk actions.
    # Error docs have data_stream.type = "logs" so ES routes them to
    # .ds-logs-apm.error-default-* automatically via data stream routing.
    # Everything else goes to traces-apm-default.
    trace_index = "traces-apm-default"
    error_index = "logs-apm.error-default"

    actions = []
    for doc in [txn_doc] + spans + child_docs:
        if doc.get("data_stream", {}).get("type") == "logs":
            actions.append({"_op_type": "create", "_index": error_index, "_source": doc})
        else:
            actions.append({"_op_type": "create", "_index": trace_index, "_source": doc})

    for doc in root_error_docs:
        actions.append({"_op_type": "create", "_index": error_index, "_source": doc})

    return actions


# ─── ES client ────────────────────────────────────────────────────────────────

def build_es_client(host: str, user: str, password: str, verify_ssl: bool):
    return Elasticsearch(
        host,
        basic_auth=(user, password),
        verify_certs=verify_ssl,
        ssl_show_warn=False,
        request_timeout=30,
    )


# ─── Metrics ─────────────────────────────────────────────────────────────────

_JVM_POOLS = ["Eden Space", "Survivor Space", "Tenured Gen"]
_GC_NAMES  = ["G1 Young Generation", "G1 Old Generation"]

_SVC_JVM = {
    "lendpath-los":               {"heap_max": 2 * 1024**3, "ram": 8 * 1024**3},
    "lendpath-underwriting":      {"heap_max": 2 * 1024**3, "ram": 8 * 1024**3},
    "lendpath-credit-service":    {"heap_max": 1 * 1024**3, "ram": 4 * 1024**3},
    "lendpath-document-service":  {"heap_max": 1 * 1024**3, "ram": 4 * 1024**3},
    "lendpath-appraisal-service": {"heap_max": 1 * 1024**3, "ram": 4 * 1024**3},
}


def generate_metrics(service_name: str, ts_iso: str = None) -> list:
    """
    Generate APM metricset documents for one service node.
    Returns a list of bulk action dicts.
    """
    if ts_iso is None:
        ts_iso = _now_iso()

    svc    = SERVICES[service_name]
    jvm    = _SVC_JVM[service_name]
    node   = random.choice(svc["node_names"])
    base   = build_base(service_name, node)
    INDEX  = f"metrics-apm.app.{service_name}-default"
    ts_us  = _ts_us(ts_iso)

    heap_max       = jvm["heap_max"]
    heap_used      = int(heap_max * random.uniform(0.35, 0.80))
    heap_committed = int(heap_max * random.uniform(heap_used / heap_max, 0.90))
    non_heap_used  = int(random.uniform(80 * 1024**2, 200 * 1024**2))
    ram_total      = jvm["ram"]
    ram_free       = int(ram_total * random.uniform(0.20, 0.60))
    cpu_pct        = round(random.uniform(0.05, 0.75), 4)
    thread_count   = random.randint(20, 120)

    actions = []

    app_doc = {
        **base,
        "@timestamp":   ts_iso,
        "timestamp":    {"us": ts_us},
        "processor":    {"event": "metric", "name": "metric"},
        "metricset":    {"name": "app", "interval": "1m"},
        "event":        {"kind": "metric"},
        "system": {
            "cpu":    {"total": {"norm": {"pct": cpu_pct}}},
            "memory": {"actual": {"free": ram_free}, "total": ram_total},
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

    remaining_heap = heap_used
    for i, pool_name in enumerate(_JVM_POOLS):
        if i < len(_JVM_POOLS) - 1:
            pool_used = int(remaining_heap * random.uniform(0.3, 0.6))
        else:
            pool_used = remaining_heap
        remaining_heap -= pool_used

        pool_max       = int(heap_max * random.uniform(0.25, 0.45))
        pool_committed = min(int(pool_used * random.uniform(1.0, 1.3)), pool_max)

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
                    "time":  random.randint(0, 150),
                    "count": random.randint(0, 25),
                    "alloc": int(random.uniform(0, 512 * 1024**2)),
                },
                "thread": {"count": thread_count},
            },
        }
        actions.append({"_op_type": "create", "_index": INDEX, "_source": pool_doc})

    return actions


# ─── Live runner ─────────────────────────────────────────────────────────────

def run(host, user, password, verify_ssl, rate, once, purge=False,
        anomaly_chance=0.03, error_rate=0.05):
    es = build_es_client(host, user, password, verify_ssl)
    try:
        info = es.info()
        print(f"Connected to Elasticsearch {info['version']['number']}")
    except Exception as e:
        print(f"Cannot connect: {e}")
        sys.exit(1)

    if purge:
        print("Purging traces-apm-default…")
        try:
            es.indices.delete(index="traces-apm-default", ignore_unavailable=True)
            print("  ✓ Deleted traces-apm-default data stream")
        except Exception as e:
            print(f"  ⚠ Could not delete data stream: {e}")
        print()

    service_names   = list(SERVICES.keys())
    service_weights = [40, 20, 15, 15, 10]
    interval        = 1.0 / rate

    total_docs   = 0
    total_traces = 0
    print(f"\nGenerating {rate} trace(s)/sec across {len(service_names)} services.")
    print("Press Ctrl+C to stop.\n")

    metrics_interval = 30
    try:
        while True:
            svc_name = random.choices(service_names, weights=service_weights, k=1)[0]
            actions  = generate_trace(svc_name, anomaly_chance=anomaly_chance,
                                      error_rate=error_rate)

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

            if once and total_traces >= 100:
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        pass

    print(f"\nDone. {total_traces:,} traces / {total_docs:,} documents indexed.")


def main():
    p = argparse.ArgumentParser(
        description="Generate linked APM traces for the LendPath Mortgage workshop"
    )
    p.add_argument("--host",           default="https://localhost:9200")
    p.add_argument("--user",           default="elastic")
    p.add_argument("--password",       default="changeme")
    p.add_argument("--no-verify-ssl",  action="store_true")
    p.add_argument("--rate",           type=float, default=2.0)
    p.add_argument("--once",           action="store_true")
    p.add_argument("--anomaly-chance", type=float, default=0.03, metavar="RATE")
    p.add_argument("--error-rate",     type=float, default=0.05, metavar="RATE",
                   help="Baseline fraction of traces that emit an error doc (default: 0.05)")
    p.add_argument("--purge",          action="store_true")
    args = p.parse_args()
    run(args.host, args.user, args.password,
        not args.no_verify_ssl, args.rate, args.once, args.purge,
        anomaly_chance=args.anomaly_chance,
        error_rate=args.error_rate)


if __name__ == "__main__":
    main()
