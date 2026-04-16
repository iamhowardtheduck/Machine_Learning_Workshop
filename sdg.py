#!/usr/bin/env python3
"""
Simple Data Generator (SDG) - Python Edition
Migrated from Java (iamhowardtheduck/SDGv2)
Generates configurable random data and indexes it into Elasticsearch.
"""

import argparse
import ipaddress
import json
import logging
import random
import re
import string
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import yaml
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, NotFoundError

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("sdg")

# ─────────────────────────────────────────────────────────────────────────────
# US State / City data (matches Java implementation)
# ─────────────────────────────────────────────────────────────────────────────
US_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming",
]

US_CITIES = [
    "New York City", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
    "Indianapolis", "San Francisco", "Seattle", "Denver", "Nashville",
    "Oklahoma City", "El Paso", "Washington", "Boston", "Memphis",
    "Louisville", "Portland", "Las Vegas", "Baltimore", "Milwaukee",
    "Albuquerque", "Tucson", "Fresno", "Sacramento", "Mesa",
    "Kansas City", "Atlanta", "Omaha", "Colorado Springs", "Raleigh",
    "Long Beach", "Virginia Beach", "Minneapolis", "Tampa", "New Orleans",
    "Honolulu", "Arlington", "Wichita", "Cleveland", "Bakersfield",
]

COUNTRIES = [
    "United States", "United Kingdom", "Canada", "Australia", "Germany",
    "France", "Japan", "China", "India", "Brazil", "Mexico", "Italy",
    "Spain", "South Korea", "Netherlands", "Russia", "Sweden", "Norway",
    "Denmark", "Finland", "Switzerland", "Austria", "Belgium", "Poland",
    "Czech Republic", "Portugal", "Greece", "Turkey", "Israel", "Argentina",
    "Colombia", "Chile", "Peru", "Venezuela", "South Africa", "Nigeria",
    "Egypt", "Kenya", "Ghana", "Morocco", "Singapore", "Thailand",
    "Indonesia", "Malaysia", "Philippines", "Vietnam", "New Zealand",
    "Ireland", "Scotland", "Wales",
]

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "William", "Barbara", "David", "Elizabeth", "Richard",
    "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
    "Christopher", "Lisa", "Daniel", "Nancy", "Matthew", "Betty",
    "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Dorothy", "Paul", "Kimberly", "Andrew", "Emily",
    "Kenneth", "Donna", "George", "Michelle", "Joshua", "Carol",
    "Kevin", "Amanda", "Brian", "Melissa", "Edward", "Deborah",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts",
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]

HTTP_STATUS_CODES = [
    200, 200, 200, 200, 200,   # weight 200 heavily
    201, 204, 301, 302, 304,
    400, 401, 403, 404, 405,
    500, 502, 503,
]

LOG_LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR", "FATAL"]

MIME_TYPES = [
    "text/html", "application/json", "application/xml", "text/plain",
    "image/jpeg", "image/png", "application/pdf", "text/css",
    "application/javascript", "application/octet-stream",
]

URL_PATHS = [
    "/", "/index.html", "/api/v1/users", "/api/v1/orders", "/api/v2/products",
    "/search", "/login", "/logout", "/dashboard", "/admin",
    "/static/css/main.css", "/static/js/app.js", "/favicon.ico",
    "/api/health", "/api/metrics", "/profile", "/settings",
]

OPERATING_SYSTEMS = [
    "Windows 10", "Windows 11", "macOS 13", "macOS 14",
    "Ubuntu 22.04", "Ubuntu 24.04", "CentOS 7", "RHEL 9",
    "Android 13", "Android 14", "iOS 17", "iOS 16",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Firefox/121.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    "Mozilla/5.0 (Android 13; Mobile; rv:121.0) Gecko/121.0 Firefox/121.0",
    "python-requests/2.31.0",
    "curl/8.4.0",
]

WORDS = [
    "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing",
    "elit", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore",
    "et", "dolore", "magna", "aliqua", "enim", "ad", "minim", "veniam",
    "quis", "nostrud", "exercitation", "ullamco", "laboris", "nisi",
    "aliquip", "ex", "ea", "commodo", "consequat", "duis", "aute", "irure",
    "reprehenderit", "voluptate", "velit", "esse", "cillum", "fugiat",
    "nulla", "pariatur", "excepteur", "sint", "occaecat", "cupidatat",
    "proident", "sunt", "culpa", "qui", "officia", "deserunt", "mollit",
]


# ─────────────────────────────────────────────────────────────────────────────
# Field value generators
# ─────────────────────────────────────────────────────────────────────────────

def _parse_range(range_str: Optional[str], default_min=0, default_max=1_000_000):
    """Parse 'min,max' range string into (min, max) tuple."""
    if not range_str:
        return default_min, default_max
    parts = str(range_str).split(",")
    if len(parts) == 2:
        return float(parts[0].strip()), float(parts[1].strip())
    return default_min, default_max


def gen_int(field: Dict) -> int:
    lo, hi = _parse_range(field.get("range"), 0, 1_000_000)
    return random.randint(int(lo), int(hi))


def gen_long(field: Dict) -> int:
    lo, hi = _parse_range(field.get("range"), 0, 9_999_999_999)
    return random.randint(int(lo), int(hi))


def gen_float(field: Dict) -> float:
    lo, hi = _parse_range(field.get("range"), 0.0, 1_000_000.0)
    return round(random.uniform(lo, hi), 2)


def gen_double(field: Dict) -> float:
    lo, hi = _parse_range(field.get("range"), 0.0, 1_000_000.0)
    return round(random.uniform(lo, hi), 6)


def gen_bool(_field: Dict) -> bool:
    return random.choice([True, False])


def gen_string(field: Dict) -> str:
    length = int(field.get("length", 10))
    chars = field.get("chars", string.ascii_letters + string.digits)
    return "".join(random.choices(chars, k=length))


def gen_words(field: Dict) -> str:
    count = int(field.get("count", random.randint(3, 15)))
    return " ".join(random.choices(WORDS, k=count))


def gen_state(_field: Dict) -> str:
    return random.choice(US_STATES)


def gen_city(_field: Dict) -> str:
    return random.choice(US_CITIES)


def gen_country(_field: Dict) -> str:
    return random.choice(COUNTRIES)


def gen_first_name(_field: Dict) -> str:
    return random.choice(FIRST_NAMES)


def gen_last_name(_field: Dict) -> str:
    return random.choice(LAST_NAMES)


def gen_full_name(_field: Dict) -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def gen_email(_field: Dict) -> str:
    first = random.choice(FIRST_NAMES).lower()
    last = random.choice(LAST_NAMES).lower()
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "company.com",
               "example.org", "outlook.com", "proton.me"]
    return f"{first}.{last}{random.randint(1, 999)}@{random.choice(domains)}"


def gen_phone(_field: Dict) -> str:
    return (f"+1-{random.randint(200,999):03d}-"
            f"{random.randint(100,999):03d}-"
            f"{random.randint(1000,9999):04d}")


def gen_ip(_field: Dict) -> str:
    # avoid reserved ranges for realism
    while True:
        parts = [random.randint(1, 254) for _ in range(4)]
        if parts[0] not in (10, 127, 172, 192):
            return ".".join(str(p) for p in parts)


def gen_ipv6(_field: Dict) -> str:
    return str(ipaddress.IPv6Address(random.randint(0, 2**128 - 1)))


def gen_mac(_field: Dict) -> str:
    return ":".join(f"{random.randint(0, 255):02x}" for _ in range(6))


def gen_uuid(_field: Dict) -> str:
    return str(uuid.uuid4())


def gen_timestamp(field: Dict) -> str:
    """Generate ISO-8601 timestamp. Supports 'range' in minutes back from now."""
    lo, hi = _parse_range(field.get("range"), 0, 60 * 24 * 7)  # default: last week
    minutes_back = random.uniform(lo, hi)
    dt = datetime.now(timezone.utc) - timedelta(minutes=minutes_back)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def gen_date(field: Dict) -> str:
    lo, hi = _parse_range(field.get("range"), 0, 365 * 2)  # default: last 2 years
    days_back = random.randint(int(lo), int(hi))
    dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    return dt.strftime("%Y-%m-%d")


def gen_time(_field: Dict) -> str:
    h = random.randint(0, 23)
    m = random.randint(0, 59)
    s = random.randint(0, 59)
    return f"{h:02d}:{m:02d}:{s:02d}"


def gen_url(_field: Dict) -> str:
    schemes = ["http", "https"]
    tlds = ["com", "org", "net", "io", "co"]
    words = random.choices(WORDS, k=2)
    path = random.choice(URL_PATHS)
    return f"{random.choice(schemes)}://{''.join(words)}.{random.choice(tlds)}{path}"


def gen_http_method(_field: Dict) -> str:
    return random.choice(HTTP_METHODS)


def gen_http_status(_field: Dict) -> int:
    return random.choice(HTTP_STATUS_CODES)


def gen_log_level(_field: Dict) -> str:
    return random.choice(LOG_LEVELS)


def gen_user_agent(_field: Dict) -> str:
    return random.choice(USER_AGENTS)


def gen_mime_type(_field: Dict) -> str:
    return random.choice(MIME_TYPES)


def gen_os(_field: Dict) -> str:
    return random.choice(OPERATING_SYSTEMS)


def gen_ssn(_field: Dict) -> str:
    return (f"{random.randint(100,999):03d}-"
            f"{random.randint(10,99):02d}-"
            f"{random.randint(1000,9999):04d}")


def gen_zip(_field: Dict) -> str:
    return f"{random.randint(10000, 99999):05d}"


def gen_geo_point(_field: Dict) -> Dict:
    lat = round(random.uniform(-90.0, 90.0), 6)
    lon = round(random.uniform(-180.0, 180.0), 6)
    return {"lat": lat, "lon": lon}


def gen_object(field: Dict) -> Dict:
    """Recursively generate an object from nested 'fields' definition."""
    result = {}
    for sub_field in field.get("fields", []):
        result[sub_field["name"]] = generate_value(sub_field)
    return result


def gen_array(field: Dict) -> List:
    """Generate an array of values. Uses 'itemType' and optional 'count'."""
    count = int(field.get("count", random.randint(1, 5)))
    item_field = {"name": "_item", "type": field.get("itemType", "string")}
    item_field.update({k: v for k, v in field.items()
                       if k not in ("name", "type", "count", "itemType")})
    return [generate_value(item_field) for _ in range(count)]


def gen_enum(field: Dict) -> Any:
    """Pick a random value from a 'values' list."""
    values = field.get("values", ["a", "b", "c"])
    return random.choice(values)


def gen_weighted_enum(field: Dict) -> Any:
    """Pick a value based on weights. 'values' list of {value, weight} dicts."""
    items = field.get("values", [{"value": "a", "weight": 1}])
    population = [i["value"] for i in items]
    weights = [i.get("weight", 1) for i in items]
    return random.choices(population, weights=weights, k=1)[0]


def gen_sequence(field: Dict, state: Dict) -> int:
    """Auto-incrementing sequence. State is per-workload-thread."""
    key = field["name"]
    start = int(field.get("start", 1))
    step = int(field.get("step", 1))
    if key not in state:
        state[key] = start
    val = state[key]
    state[key] += step
    return val


def gen_constant(field: Dict) -> Any:
    """Always return the same constant value."""
    return field.get("value", "")


def gen_correlation_id(_field: Dict) -> str:
    return f"corr-{uuid.uuid4().hex[:12]}"


def gen_hostname(_field: Dict) -> str:
    parts = random.choices(WORDS, k=2)
    domains = ["internal", "local", "corp", "example.com"]
    return f"{parts[0]}-{parts[1]}.{random.choice(domains)}"


def gen_port(_field: Dict) -> int:
    return random.choice([80, 443, 8080, 8443, 3306, 5432, 6379, 27017,
                          9200, 9243, 22, 21, 25, 53, 123, 3389,
                          random.randint(1024, 65535)])


def gen_bytes_field(field: Dict) -> int:
    lo, hi = _parse_range(field.get("range"), 100, 10_000_000)
    return random.randint(int(lo), int(hi))


def gen_duration_ms(field: Dict) -> int:
    lo, hi = _parse_range(field.get("range"), 1, 30_000)
    return random.randint(int(lo), int(hi))


# ─────────────────────────────────────────────────────────────────────────────
# Dispatcher
# ─────────────────────────────────────────────────────────────────────────────

GENERATORS = {
    "int":           gen_int,
    "integer":       gen_int,
    "long":          gen_long,
    "float":         gen_float,
    "double":        gen_double,
    "bool":          gen_bool,
    "boolean":       gen_bool,
    "string":        gen_string,
    "word":          gen_words,
    "words":         gen_words,
    "text":          gen_words,
    "state":         gen_state,
    "city":          gen_city,
    "country":       gen_country,
    "firstName":     gen_first_name,
    "first_name":    gen_first_name,
    "lastName":      gen_last_name,
    "last_name":     gen_last_name,
    "fullName":      gen_full_name,
    "full_name":     gen_full_name,
    "name":          gen_full_name,
    "email":         gen_email,
    "phone":         gen_phone,
    "phoneNumber":   gen_phone,
    "ip":            gen_ip,
    "ipv4":          gen_ip,
    "ipv6":          gen_ipv6,
    "mac":           gen_mac,
    "macAddress":    gen_mac,
    "uuid":          gen_uuid,
    "guid":          gen_uuid,
    "timestamp":     gen_timestamp,
    "date":          gen_date,
    "time":          gen_time,
    "url":           gen_url,
    "uri":           gen_url,
    "httpMethod":    gen_http_method,
    "http_method":   gen_http_method,
    "httpStatus":    gen_http_status,
    "http_status":   gen_http_status,
    "logLevel":      gen_log_level,
    "log_level":     gen_log_level,
    "userAgent":     gen_user_agent,
    "user_agent":    gen_user_agent,
    "mimeType":      gen_mime_type,
    "mime_type":     gen_mime_type,
    "os":            gen_os,
    "operatingSystem": gen_os,
    "ssn":           gen_ssn,
    "zip":           gen_zip,
    "zipCode":       gen_zip,
    "zip_code":      gen_zip,
    "geo_point":     gen_geo_point,
    "geoPoint":      gen_geo_point,
    "location":      gen_geo_point,
    "object":        gen_object,
    "nested":        gen_object,
    "array":         gen_array,
    "list":          gen_array,
    "enum":          gen_enum,
    "weightedEnum":  gen_weighted_enum,
    "weighted_enum": gen_weighted_enum,
    "constant":      gen_constant,
    "static":        gen_constant,
    "correlationId": gen_correlation_id,
    "correlation_id": gen_correlation_id,
    "hostname":      gen_hostname,
    "host":          gen_hostname,
    "port":          gen_port,
    "bytes":         gen_bytes_field,
    "duration":      gen_duration_ms,
    "duration_ms":   gen_duration_ms,
}


def generate_value(field: Dict, state: Optional[Dict] = None) -> Any:
    """Generate a single field value based on its type definition."""
    field_type = field.get("type", "string")

    if field_type == "sequence":
        if state is None:
            state = {}
        return gen_sequence(field, state)

    generator = GENERATORS.get(field_type)
    if generator is None:
        log.warning("Unknown field type '%s' for field '%s', using string",
                    field_type, field.get("name", "?"))
        return gen_string(field)

    return generator(field)


def build_document(fields: List[Dict], seq_state: Dict) -> Dict:
    """Build a full document dict from a list of field definitions."""
    doc = {}
    for field in fields:
        key = field["name"]
        # Support nested dot-notation keys  (e.g. "host.ip")
        if "." in key:
            parts = key.split(".")
            container = doc
            for part in parts[:-1]:
                container = container.setdefault(part, {})
            container[parts[-1]] = generate_value(field, seq_state)
        else:
            doc[key] = generate_value(field, seq_state)
    return doc


# ─────────────────────────────────────────────────────────────────────────────
# Peak-time sine-wave multiplier  (matches Java peakTime logic)
# ─────────────────────────────────────────────────────────────────────────────

def _peak_multiplier(peak_time_str: Optional[str]) -> float:
    """
    Returns a sleep multiplier (0.5 – 2.0) based on distance from peak time.
    Near peak → multiplier ~0.5 (faster). Far from peak → multiplier ~2.0.
    """
    if not peak_time_str:
        return 1.0
    try:
        import math
        peak_h, peak_m, peak_s = map(int, peak_time_str.split(":"))
        now = datetime.now()
        peak_seconds = peak_h * 3600 + peak_m * 60 + peak_s
        now_seconds = now.hour * 3600 + now.minute * 60 + now.second
        diff = abs(now_seconds - peak_seconds)
        diff = min(diff, 86400 - diff)          # wrap around midnight
        fraction = diff / 43200.0               # 0 at peak, 1 at anti-peak
        multiplier = 0.5 + 1.5 * math.sin(fraction * math.pi / 2) ** 2
        return multiplier
    except Exception:
        return 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Elasticsearch helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_es_client(cfg: Dict) -> Elasticsearch:
    """Build an Elasticsearch client from the YAML config."""
    scheme = cfg.get("elasticsearchScheme", "https")
    host   = cfg.get("elasticsearchHost", "localhost")
    port   = int(cfg.get("elasticsearchPort", 9200))
    user   = cfg.get("elasticsearchUser", "elastic")
    password = cfg.get("elasticsearchPassword", "")
    api_key_enabled = cfg.get("elasticsearchApiKeyEnabled", False)
    api_key_id     = cfg.get("elasticsearchApiKeyId", "")
    api_key_secret = cfg.get("elasticsearchApiKeySecret", "")

    hosts = [{"host": host, "port": port, "scheme": scheme}]

    if api_key_enabled and api_key_id and api_key_secret:
        client = Elasticsearch(
            hosts,
            api_key=(api_key_id, api_key_secret),
            verify_certs=cfg.get("verifyCerts", True),
            ssl_show_warn=False,
        )
    else:
        client = Elasticsearch(
            hosts,
            http_auth=(user, password),
            verify_certs=cfg.get("verifyCerts", True),
            ssl_show_warn=False,
        )
    return client


def ensure_index(client: Elasticsearch, workload: Dict):
    """Create index if it doesn't exist. Optionally purge on start."""
    index_name = workload["indexName"]
    primary_shards = workload.get("primaryShardCount", 1)
    replica_shards = workload.get("replicaShardCount", 1)
    purge = workload.get("purgeOnStart", False)
    data_stream = workload.get("dataStream", False)

    if purge:
        try:
            if data_stream:
                client.indices.delete_data_stream(name=index_name, ignore_unavailable=True)
            else:
                client.indices.delete(index=index_name, ignore_unavailable=True)
            log.info("Purged existing index/data-stream: %s", index_name)
        except Exception as ex:
            log.warning("Could not purge %s: %s", index_name, ex)

    if data_stream:
        # Data streams are created automatically on first document
        return

    try:
        if not client.indices.exists(index=index_name):
            body = {
                "settings": {
                    "number_of_shards":   primary_shards,
                    "number_of_replicas": replica_shards,
                }
            }
            client.indices.create(index=index_name, body=body)
            log.info("Created index: %s (shards=%d, replicas=%d)",
                     index_name, primary_shards, replica_shards)
    except Exception as ex:
        log.warning("Could not ensure index %s: %s", index_name, ex)


# ─────────────────────────────────────────────────────────────────────────────
# Worker thread
# ─────────────────────────────────────────────────────────────────────────────

class WorkloadWorker(threading.Thread):
    """One thread per workload (or per thread within a workload)."""

    def __init__(self, workload: Dict, client: Elasticsearch,
                 thread_idx: int, stop_event: threading.Event):
        name = f"{workload.get('workloadName','workload')}-t{thread_idx}"
        super().__init__(name=name, daemon=True)
        self.workload    = workload
        self.client      = client
        self.stop_event  = stop_event
        self.seq_state: Dict = {}         # per-thread sequence counters
        self.docs_indexed = 0
        self.errors       = 0

    def run(self):
        workload    = self.workload
        index_name  = workload["indexName"]
        sleep_ms    = int(workload.get("workloadSleep", 1000))
        bulk_depth  = int(workload.get("elasticsearchBulkQueueDepth", 0))
        peak_time   = workload.get("peakTime")
        fields      = workload.get("fields", [])
        data_stream = workload.get("dataStream", False)

        log.info("[%s] Starting — index=%s sleep=%dms bulk=%d",
                 self.name, index_name, sleep_ms, bulk_depth)

        while not self.stop_event.is_set():
            try:
                if bulk_depth > 0:
                    self._send_bulk(index_name, fields, bulk_depth, data_stream)
                else:
                    self._send_single(index_name, fields, data_stream)

                multiplier = _peak_multiplier(peak_time)
                sleep_sec  = (sleep_ms * multiplier) / 1000.0
                self.stop_event.wait(timeout=sleep_sec)

            except ConnectionError as ex:
                log.error("[%s] Connection error: %s — retrying in 5s", self.name, ex)
                self.errors += 1
                self.stop_event.wait(timeout=5)
            except Exception as ex:
                log.error("[%s] Unexpected error: %s", self.name, ex)
                self.errors += 1
                self.stop_event.wait(timeout=2)

        log.info("[%s] Stopped. docs_indexed=%d errors=%d",
                 self.name, self.docs_indexed, self.errors)

    def _send_single(self, index_name, fields, data_stream):
        doc = build_document(fields, self.seq_state)
        # Inject @timestamp if not already present
        if "@timestamp" not in doc:
            doc["@timestamp"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if data_stream:
            self.client.index(index=index_name, body=doc)
        else:
            self.client.index(index=index_name, body=doc)
        self.docs_indexed += 1

    def _send_bulk(self, index_name, fields, bulk_depth, data_stream):
        actions = []
        for _ in range(bulk_depth):
            doc = build_document(fields, self.seq_state)
            if "@timestamp" not in doc:
                doc["@timestamp"] = datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            if data_stream:
                actions.append({"_op_type": "create", "_index": index_name,
                                 "_source": doc})
            else:
                actions.append({"_op_type": "index", "_index": index_name,
                                 "_source": doc})

        success, failed = helpers.bulk(self.client, actions,
                                       raise_on_error=False)
        self.docs_indexed += success
        if failed:
            self.errors += len(failed)
            log.warning("[%s] Bulk errors: %d", self.name, len(failed))


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Simple Data Generator (Python) — indexes random data to Elasticsearch"
    )
    parser.add_argument("config", help="Path to YAML config file")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging level (default: INFO)")
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Load config
    with open(args.config, "r") as fh:
        cfg = yaml.safe_load(fh)

    log.info("Loaded config: %s", args.config)
    log.info("Target: %s://%s:%s",
             cfg.get("elasticsearchScheme", "https"),
             cfg.get("elasticsearchHost", "localhost"),
             cfg.get("elasticsearchPort", 9200))

    # Build client
    client = build_es_client(cfg)

    # Verify connectivity
    try:
        info = client.info()
        log.info("Connected to Elasticsearch %s", info["version"]["number"])
    except Exception as ex:
        log.error("Cannot connect to Elasticsearch: %s", ex)
        sys.exit(1)

    workloads = cfg.get("workloads", [])
    if not workloads:
        log.error("No workloads defined in config.")
        sys.exit(1)

    stop_event = threading.Event()
    workers    = []

    # Setup indexes and spawn threads
    for workload in workloads:
        ensure_index(client, workload)
        thread_count = int(workload.get("workloadThreads", 1))
        for i in range(thread_count):
            w = WorkloadWorker(workload, client, i, stop_event)
            workers.append(w)
            w.start()

    log.info("Started %d worker thread(s). Press Ctrl+C to stop.", len(workers))

    try:
        while True:
            time.sleep(10)
            total_docs = sum(w.docs_indexed for w in workers)
            total_errs = sum(w.errors for w in workers)
            log.info("Stats: docs_indexed=%d errors=%d active_threads=%d",
                     total_docs, total_errs,
                     sum(1 for w in workers if w.is_alive()))
    except KeyboardInterrupt:
        log.info("Shutdown requested…")
        stop_event.set()
        for w in workers:
            w.join(timeout=10)
        total_docs = sum(w.docs_indexed for w in workers)
        total_errs = sum(w.errors for w in workers)
        log.info("Final stats: docs_indexed=%d errors=%d", total_docs, total_errs)
        log.info("Goodbye.")


if __name__ == "__main__":
    main()
