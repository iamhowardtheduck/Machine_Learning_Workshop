#!/usr/bin/env python3
"""
Simple Data Generator (SDG) - Python Edition
Migrated from Java (iamhowardtheduck/SDGv2)
Generates configurable random data and indexes it into Elasticsearch.

Field types match the official supported_fields.md from the original Java app.
27 of 37 types are backed by the Faker library for richer, more realistic data.
The remaining 10 use custom lists (domain-specific or no Faker equivalent).
"""

import argparse
import hashlib
import ipaddress
import logging
import math
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import yaml
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError
from faker import Faker

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("sdg")

# Suppress Elasticsearch HTTP request logs
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)

# Shared Faker instance (thread-safe for reads)
fake = Faker()

# ─────────────────────────────────────────────────────────────────────────────
# Custom data tables
# Used only for types that Faker doesn't cover, or where the Java spec
# mandates a specific dataset (e.g. ancient-gods hostnames).
# ─────────────────────────────────────────────────────────────────────────────

# Ancient gods — matches the Java hostname implementation exactly
HOSTNAMES = [
    "zeus", "hera", "poseidon", "demeter", "athena", "apollo",
    "artemis", "ares", "aphrodite", "hephaestus", "hermes", "dionysus",
    "odin", "thor", "loki", "freya", "tyr", "baldur", "heimdall", "frigg",
    "ra", "osiris", "isis", "horus", "anubis", "thoth", "set", "nut",
    "brahma", "vishnu", "shiva", "indra", "agni", "varuna", "krishna",
]

TEAM_NAMES = [
    "Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot",
    "Phoenix", "Vanguard", "Titan", "Nexus", "Apex", "Zenith",
    "Hydra", "Atlas", "Orion", "Polaris", "Catalyst", "Horizon",
    "Pinnacle", "Velocity",
]

APP_NAMES = [
    "nginx", "apache", "tomcat", "node", "django", "flask", "rails",
    "spring-boot", "express", "fastapi", "haproxy", "varnish",
    "elasticsearch", "kibana", "logstash", "filebeat", "redis", "kafka",
    "rabbitmq", "postgres", "mysql", "mongodb", "cassandra", "consul",
    "vault", "prometheus", "grafana", "jenkins", "gitlab-ci", "airflow",
]

PRODUCT_NAMES = [
    "Widget Pro", "DataSync", "CloudVault", "NetShield", "FlowTracker",
    "LogMaster", "SecureKey", "MetricsPulse", "StreamLine", "CacheBoost",
    "QueryOptimizer", "EventBridge", "TokenGate", "AuditTrail", "PipeRunner",
    "AlertNow", "SnapIndex", "ReplicaSync", "JobQueue", "PolicyEngine",
]

GOT_CHARACTERS = [
    "Jon Snow", "Daenerys Targaryen", "Tyrion Lannister", "Cersei Lannister",
    "Jaime Lannister", "Arya Stark", "Sansa Stark", "Bran Stark",
    "Ned Stark", "Robert Baratheon", "Joffrey Baratheon", "Stannis Baratheon",
    "Melisandre", "Davos Seaworth", "Samwell Tarly", "Brienne of Tarth",
    "Sandor Clegane", "Gregor Clegane", "Petyr Baelish", "Varys",
    "Theon Greyjoy", "Yara Greyjoy", "Margaery Tyrell", "Olenna Tyrell",
    "Oberyn Martell", "Missandei", "Grey Worm", "Jorah Mormont",
]

STAR_TREK_CHARACTERS = [
    "Jean-Luc Picard", "William Riker", "Data", "Geordi La Forge", "Worf",
    "Deanna Troi", "Beverly Crusher", "Wesley Crusher", "Tasha Yar", "Guinan",
    "Miles O'Brien", "Reginald Barclay", "Katherine Pulaski", "Q", "Lwaxana Troi",
    "Ro Laren", "Lore", "Hugh", "Alexander Rozhenko", "K'Ehleyr", "Thomas Riker",
    "Vash", "Gowron", "Martok", "Sela", "Toral", "Ardra", "Nagilum", "Armus",
    "The Traveler", "Boothby", "Admiral Nechayev", "Admiral Haftel", "Admiral Nakamura",
    "Captain Jellico", "Commander Shelby", "Lieutenant Ogawa", "Nurse Alyssa Ogawa",
    "Lieutenant Commander Daren", "Ensign Sito Jaxa", "Simon Tarses", "Lieutenant Selar",
    "Sonya Gomez", "Mot", "Moriarty", "Minuet"
]

CN_FACTS = [
    "Chuck Norris can divide by zero.",
    "Chuck Norris counted to infinity. Twice.",
    "Chuck Norris can slam a revolving door.",
    "Chuck Norris doesn't push the elevator button, the elevator button pushes itself.",
    "Chuck Norris's keyboard has no Ctrl key because Chuck Norris is always in control.",
    "Chuck Norris doesn't need garbage collection because he doesn't call, he collects.",
    "Chuck Norris can write infinite loops and exit them.",
    "Chuck Norris's programs don't have bugs, they have features.",
    "Chuck Norris can make a class that is both abstract and final.",
    "Chuck Norris solved the halting problem. Twice.",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_range(range_str: Optional[str], default_min=0, default_max=1_000_000):
    """Parse 'min,max' range string into (min, max) tuple."""
    if not range_str:
        return default_min, default_max
    parts = str(range_str).split(",")
    if len(parts) == 2:
        return float(parts[0].strip()), float(parts[1].strip())
    return default_min, default_max


def _parse_custom_list(field: Dict) -> List[str]:
    """Parse custom_list: comma-separated string or YAML list."""
    raw = field.get("custom_list", "")
    if isinstance(raw, list):
        return [str(v) for v in raw]
    return [v.strip() for v in str(raw).split(",") if v.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# Field generators
# Backed by Faker where possible; custom lists for the rest.
# ─────────────────────────────────────────────────────────────────────────────

# ── Primitives  (Faker) ───────────────────────────────────────────────────────

def gen_int(field: Dict) -> int:
    lo, hi = _parse_range(field.get("range"), 0, 1_000_000)
    return fake.random_int(min=int(lo), max=int(hi))


def gen_float(field: Dict) -> float:
    lo, hi = _parse_range(field.get("range"), 0.0, 1_000_000.0)
    return fake.pyfloat(right_digits=2, min_value=lo, max_value=hi)


def gen_boolean(_field: Dict) -> bool:
    return fake.boolean()


# ── Names  (Faker) ────────────────────────────────────────────────────────────

def gen_full_name(_field: Dict) -> str:
    return fake.name()


def gen_first_name(_field: Dict) -> str:
    return fake.first_name()


def gen_last_name(_field: Dict) -> str:
    return fake.last_name()


def gen_group(_field: Dict) -> str:
    """Department/group — Faker's bs() gives realistic business unit names."""
    return fake.bs().title()


# ── Names  (custom — no Faker equivalent) ────────────────────────────────────

def gen_team_name(_field: Dict) -> str:
    return random.choice(TEAM_NAMES)


# ── Addresses  (Faker) ────────────────────────────────────────────────────────

def gen_full_address(_field: Dict) -> str:
    return fake.address()


def gen_street_address(_field: Dict) -> str:
    return fake.street_address()


def gen_city(_field: Dict) -> str:
    return fake.city()


def gen_state(_field: Dict) -> str:
    return fake.state()


def gen_zipcode(_field: Dict) -> str:
    return fake.zipcode()


# ── Special numbers  (Faker) ──────────────────────────────────────────────────

def gen_credit_card_number(_field: Dict) -> str:
    return fake.credit_card_number()


def gen_phone_number(_field: Dict) -> str:
    return fake.phone_number()


def gen_ssn(_field: Dict) -> str:
    return fake.ssn()


def gen_uuid(_field: Dict) -> str:
    return fake.uuid4()


def gen_hash(_field: Dict) -> str:
    return fake.md5()


# ── Special numbers  (custom — no Faker equivalent) ───────────────────────────

def gen_product_name(_field: Dict) -> str:
    return random.choice(PRODUCT_NAMES)


# ── Random from lists  (custom — inherently user-defined) ────────────────────

def gen_random_string_from_list(field: Dict) -> str:
    items = _parse_custom_list(field)
    return random.choice(items) if items else ""


def gen_random_integer_from_list(field: Dict) -> int:
    items = _parse_custom_list(field)
    return int(random.choice(items)) if items else 0


def gen_random_float_from_list(field: Dict) -> float:
    items = _parse_custom_list(field)
    return float(random.choice(items)) if items else 0.0


def gen_random_long_from_list(field: Dict) -> int:
    items = _parse_custom_list(field)
    return int(random.choice(items)) if items else 0


# ── Misc  (Faker) ─────────────────────────────────────────────────────────────

def gen_ipv4(_field: Dict) -> str:
    return fake.ipv4_public()


def gen_random_occupation(_field: Dict) -> str:
    return fake.job()


def gen_path(_field: Dict) -> str:
    return fake.file_path(depth=random.randint(1, 4))


def gen_url(_field: Dict) -> str:
    return fake.url()


def gen_mac_address(_field: Dict) -> str:
    return fake.mac_address()


def gen_email(_field: Dict) -> str:
    return fake.email()


def gen_domain(_field: Dict) -> str:
    return fake.domain_name()


def gen_date(field: Dict) -> str:
    """Date string YYYY-MM-DD. range = days back from today (min, max)."""
    lo, hi    = _parse_range(field.get("range"), 0, 365 * 2)
    start_dt  = datetime.now() - timedelta(days=int(hi))
    end_dt    = datetime.now() - timedelta(days=int(lo))
    return fake.date_between(start_date=start_dt.date(),
                             end_date=end_dt.date()).strftime("%Y-%m-%d")


def gen_timezone(_field: Dict) -> str:
    return fake.timezone()


# ── Misc  (custom — no Faker equivalent) ─────────────────────────────────────

def gen_hostname(_field: Dict) -> str:
    """Ancient-gods themed hostname, matching the Java implementation."""
    suffix = random.choice([
        "",
        f"-{random.randint(1, 99):02d}",
        f".{random.choice(['internal', 'local', 'corp'])}",
    ])
    return random.choice(HOSTNAMES) + suffix


def gen_appname(_field: Dict) -> str:
    return random.choice(APP_NAMES)


def gen_random_cn_fact(_field: Dict) -> str:
    return random.choice(CN_FACTS)


def gen_random_got_character(_field: Dict) -> str:
    return random.choice(GOT_CHARACTERS)


def gen_empty(_field: Dict) -> str:
    return ""


# ── Constants ─────────────────────────────────────────────────────────────────

def gen_constant(field: Dict) -> Any:
    return field.get("value", "")


# ─────────────────────────────────────────────────────────────────────────────
# Extra / alias types  (not in official doc; kept for backwards-compat)
# ─────────────────────────────────────────────────────────────────────────────

def gen_long(field: Dict) -> int:
    lo, hi = _parse_range(field.get("range"), 0, 9_999_999_999)
    return fake.random_int(min=int(lo), max=int(hi))


def gen_double(field: Dict) -> float:
    lo, hi = _parse_range(field.get("range"), 0.0, 1_000_000.0)
    return fake.pyfloat(right_digits=6, min_value=lo, max_value=hi)


def gen_timestamp(field: Dict) -> str:
    """ISO-8601 UTC timestamp. range = minutes back from now (min, max)."""
    lo, hi       = _parse_range(field.get("range"), 0, 60 * 24 * 7)
    start_dt     = datetime.now(timezone.utc) - timedelta(minutes=hi)
    end_dt       = datetime.now(timezone.utc) - timedelta(minutes=lo)
    dt           = fake.date_time_between(start_date=start_dt,
                                          end_date=end_dt,
                                          tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def gen_ipv6(_field: Dict) -> str:
    return str(ipaddress.IPv6Address(random.randint(0, 2**128 - 1)))


def gen_geo_point(_field: Dict) -> Dict:
    return {
        "lat": round(random.uniform(-90.0, 90.0), 6),
        "lon": round(random.uniform(-180.0, 180.0), 6),
    }


def gen_sequence(field: Dict, state: Dict) -> int:
    key        = field["name"]
    start      = int(field.get("start", 1))
    step       = int(field.get("step", 1))
    if key not in state:
        state[key] = start
    val         = state[key]
    state[key] += step
    return val


# ─────────────────────────────────────────────────────────────────────────────
# Dispatcher
# Official names (supported_fields.md) listed first; aliases follow.
# ─────────────────────────────────────────────────────────────────────────────

GENERATORS: Dict[str, Any] = {
    # ── Official types (supported_fields.md) ─────────────────────────────
    # Primitives
    "int":                      gen_int,            # Faker
    "float":                    gen_float,          # Faker
    "boolean":                  gen_boolean,        # Faker
    # Names
    "full_name":                gen_full_name,      # Faker
    "first_name":               gen_first_name,     # Faker
    "last_name":                gen_last_name,      # Faker
    "group":                    gen_group,          # Faker (bs())
    "team_name":                gen_team_name,      # custom
    # Addresses
    "full_address":             gen_full_address,   # Faker
    "street_address":           gen_street_address, # Faker
    "city":                     gen_city,           # Faker
    "state":                    gen_state,          # Faker
    "zipcode":                  gen_zipcode,        # Faker
    # Special numbers
    "credit_card_number":       gen_credit_card_number, # Faker
    "phone_number":             gen_phone_number,   # Faker
    "ssn":                      gen_ssn,            # Faker
    "uuid":                     gen_uuid,           # Faker
    "product_name":             gen_product_name,   # custom
    "hash":                     gen_hash,           # Faker (md5)
    # Random from lists
    "random_string_from_list":  gen_random_string_from_list,  # custom
    "random_integer_from_list": gen_random_integer_from_list, # custom
    "random_float_from_list":   gen_random_float_from_list,   # custom
    "random_long_from_list":    gen_random_long_from_list,    # custom
    # Misc
    "ipv4":                     gen_ipv4,               # Faker (public)
    "random_cn_fact":           gen_random_cn_fact,     # custom
    "random_got_character":     gen_random_got_character, # custom
    "random_occupation":        gen_random_occupation,  # Faker (job)
    "empty":                    gen_empty,              # trivial
    "path":                     gen_path,               # Faker
    "hostname":                 gen_hostname,           # custom (ancient gods)
    "appname":                  gen_appname,            # custom
    "url":                      gen_url,                # Faker
    "mac_address":              gen_mac_address,        # Faker
    "email":                    gen_email,              # Faker
    "domain":                   gen_domain,             # Faker
    "date":                     gen_date,               # Faker
    "timezone":                 gen_timezone,           # Faker

    # ── Aliases / extras ─────────────────────────────────────────────────
    "integer":                  gen_int,
    "long":                     gen_long,
    "double":                   gen_double,
    "bool":                     gen_boolean,
    "timestamp":                gen_timestamp,
    "ip":                       gen_ipv4,
    "ipv6":                     gen_ipv6,
    "mac":                      gen_mac_address,
    "guid":                     gen_uuid,
    "phone":                    gen_phone_number,
    "zip":                      gen_zipcode,
    "zip_code":                 gen_zipcode,
    "geo_point":                gen_geo_point,
    "geoPoint":                 gen_geo_point,
    "location":                 gen_geo_point,
    "firstName":                gen_first_name,
    "lastName":                 gen_last_name,
    "fullName":                 gen_full_name,
    "name":                     gen_full_name,
    "host":                     gen_hostname,
    "constant":                 gen_constant,
    "static":                   gen_constant,
}


def generate_value(field: Dict, state: Optional[Dict] = None) -> Any:
    # Constants: 'value' key present, no 'type' key (supported_fields.md spec)
    if "value" in field and "type" not in field:
        return field["value"]

    field_type = field.get("type", "")

    if field_type == "sequence":
        if state is None:
            state = {}
        return gen_sequence(field, state)

    generator = GENERATORS.get(field_type)
    if generator is None:
        log.warning("Unknown field type '%s' for field '%s' — returning empty string",
                    field_type, field.get("name", "?"))
        return ""

    return generator(field)


def build_document(fields: List[Dict], seq_state: Dict) -> Dict:
    """Build a full document dict from a list of field definitions."""
    doc: Dict = {}
    for field in fields:
        key   = field["name"]
        value = generate_value(field, seq_state)
        # Dot-notation → nested dict  e.g. "name.first" → {"name": {"first": …}}
        if "." in key:
            parts     = key.split(".")
            container = doc
            for part in parts[:-1]:
                container = container.setdefault(part, {})
            container[parts[-1]] = value
        else:
            doc[key] = value
    return doc


# ─────────────────────────────────────────────────────────────────────────────
# Peak-time pacing  (uses local host time, matching the Java app)
# ─────────────────────────────────────────────────────────────────────────────

def _peak_multiplier(peak_time_str: Optional[str]) -> float:
    """
    Sine-wave sleep multiplier based on distance from peakTime.
    Near peak → ~0.5 (faster).  Far from peak → ~2.0 (slower).
    Uses local host time — identical to the Java implementation.
    """
    if not peak_time_str:
        return 1.0
    try:
        peak_h, peak_m, peak_s = map(int, str(peak_time_str).split(":"))
        now      = datetime.now()
        peak_sec = peak_h * 3600 + peak_m * 60 + peak_s
        now_sec  = now.hour * 3600 + now.minute * 60 + now.second
        diff     = abs(now_sec - peak_sec)
        diff     = min(diff, 86400 - diff)
        fraction = diff / 43200.0
        return 0.5 + 1.5 * math.sin(fraction * math.pi / 2) ** 2
    except Exception:
        return 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Elasticsearch helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_es_client(cfg: Dict) -> Elasticsearch:
    scheme   = cfg.get("elasticsearchScheme", "https")
    host     = cfg.get("elasticsearchHost", "localhost")
    port     = int(cfg.get("elasticsearchPort", 9200))
    user     = cfg.get("elasticsearchUser", "elastic")
    password = cfg.get("elasticsearchPassword", "")
    api_key_enabled = cfg.get("elasticsearchApiKeyEnabled", False)
    api_key_id      = cfg.get("elasticsearchApiKeyId", "")
    api_key_secret  = cfg.get("elasticsearchApiKeySecret", "")

    hosts = [{"host": host, "port": port, "scheme": scheme}]

    if api_key_enabled and api_key_id and api_key_secret:
        return Elasticsearch(
            hosts,
            api_key=(api_key_id, api_key_secret),
            verify_certs=cfg.get("verifyCerts", True),
            ssl_show_warn=False,
        )
    return Elasticsearch(
        hosts,
        http_auth=(user, password),
        verify_certs=cfg.get("verifyCerts", True),
        ssl_show_warn=False,
    )


def ensure_index(client: Elasticsearch, workload: Dict) -> None:
    index_name     = workload["indexName"]
    primary_shards = workload.get("primaryShardCount", 1)
    replica_shards = workload.get("replicaShardCount", 1)
    purge          = workload.get("purgeOnStart", False)
    data_stream    = workload.get("dataStream", False)

    if purge:
        try:
            if data_stream:
                client.indices.delete_data_stream(name=index_name,
                                                  ignore_unavailable=True)
            else:
                client.indices.delete(index=index_name, ignore_unavailable=True)
            log.info("Purged: %s", index_name)
        except Exception as ex:
            log.warning("Could not purge %s: %s", index_name, ex)

    if data_stream:
        return

    try:
        if not client.indices.exists(index=index_name):
            client.indices.create(index=index_name, body={
                "settings": {
                    "number_of_shards":   primary_shards,
                    "number_of_replicas": replica_shards,
                }
            })
            log.info("Created index: %s (shards=%d, replicas=%d)",
                     index_name, primary_shards, replica_shards)
    except Exception as ex:
        log.warning("Could not ensure index %s: %s", index_name, ex)


# ─────────────────────────────────────────────────────────────────────────────
# Worker thread
# ─────────────────────────────────────────────────────────────────────────────

class WorkloadWorker(threading.Thread):
    def __init__(self, workload: Dict, client: Elasticsearch,
                 thread_idx: int, stop_event: threading.Event):
        # Use indexName directly (no -t# suffix)
        name = workload.get("indexName", "workload")
        super().__init__(name=name, daemon=True)
        self.workload     = workload
        self.client       = client
        self.stop_event   = stop_event
        self.seq_state: Dict = {}
        self.docs_indexed = 0
        self.errors       = 0

    def run(self):
        wl          = self.workload
        index_name  = wl["indexName"]
        sleep_ms    = int(wl.get("workloadSleep", 1000))
        bulk_depth  = int(wl.get("elasticsearchBulkQueueDepth", 0))
        peak_time   = wl.get("peakTime")
        fields      = wl.get("fields", [])
        data_stream = wl.get("dataStream", False)

        while not self.stop_event.is_set():
            try:
                if bulk_depth > 0:
                    self._send_bulk(index_name, fields, bulk_depth, data_stream)
                else:
                    self._send_single(index_name, fields)

                sleep_sec = (sleep_ms * _peak_multiplier(peak_time)) / 1000.0
                self.stop_event.wait(timeout=sleep_sec)

            except ConnectionError as ex:
                log.error("[%s] Connection error: %s — retrying in 5s", self.name, ex)
                self.errors += 1
                self.stop_event.wait(timeout=5)
            except Exception as ex:
                log.error("[%s] Error: %s", self.name, ex)
                self.errors += 1
                self.stop_event.wait(timeout=2)

    def _make_doc(self, fields):
        doc = build_document(fields, self.seq_state)
        if "@timestamp" not in doc:
            doc["@timestamp"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        return doc

    def _send_single(self, index_name, fields):
        self.client.index(index=index_name, body=self._make_doc(fields))
        self.docs_indexed += 1

    def _send_bulk(self, index_name, fields, bulk_depth, data_stream):
        op      = "create" if data_stream else "index"
        actions = [
            {"_op_type": op, "_index": index_name,
             "_source": self._make_doc(fields)}
            for _ in range(bulk_depth)
        ]
        success, failed = helpers.bulk(self.client, actions, raise_on_error=False)
        self.docs_indexed += success
        if failed:
            self.errors += len(failed)
            log.warning("[%s] Bulk errors: %d", self.name, len(failed))


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Simple Data Generator (Python) — streams random data to Elasticsearch"
    )
    parser.add_argument("config", help="Path to YAML config file")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    with open(args.config) as fh:
        cfg = yaml.safe_load(fh)

    log.info("Target: %s://%s:%s",
             cfg.get("elasticsearchScheme", "https"),
             cfg.get("elasticsearchHost", "localhost"),
             cfg.get("elasticsearchPort", 9200))

    client = build_es_client(cfg)
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
    workers: List[WorkloadWorker] = []

    for workload in workloads:
        ensure_index(client, workload)
        for i in range(int(workload.get("workloadThreads", 1))):
            w = WorkloadWorker(workload, client, i, stop_event)
            workers.append(w)
            w.start()

    # Clear screen and display assignment message
    import os
    def clear_and_display():
        os.system('clear' if os.name == 'posix' else 'cls')
        print("\n\n")
        print("=" * 60)
        print("  You are now ready to begin the assignment.")
        print("=" * 60)
        print(f"\n  Workers running: {len(workers)}")
        print(f"  Documents indexed: {sum(w.docs_indexed for w in workers):,}")
        print(f"  Errors: {sum(w.errors for w in workers)}")
        print("\n  Press Ctrl+C to stop.\n")

    try:
        clear_and_display()
        while True:
            time.sleep(5)
            clear_and_display()
    except KeyboardInterrupt:
        print("\n\nShutting down…")
        stop_event.set()
        for w in workers:
            w.join(timeout=10)
        print(f"Final: {sum(w.docs_indexed for w in workers):,} documents indexed, "
              f"{sum(w.errors for w in workers)} errors")


if __name__ == "__main__":
    main()
