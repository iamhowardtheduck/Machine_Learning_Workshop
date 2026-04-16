#!/usr/bin/env python3
"""
sdg-base.py — NetFlow Baseline Data Generator for Elastic ML Workshop

Generates ECS-compliant NetFlow flow records into logs-netflow.log-baseline
to demonstrate anomaly detection baselining, change point detection, and
outlier identification.

Traffic Profile
---------------
Workdays (Mon–Fri, non-holiday):
  - Ramp from 06:00, peak at 12:00–13:00 (lunch hour)
  - Taper from 14:00, low from 20:00
  - Near-zero 22:00–05:00

Weekends / US Federal Holidays:
  - ~25% of weekday volume, steady throughout daylight hours

Spike Weekend:
  - ONE Saturday 3–4 weeks before today gets 4× normal weekend volume
  - Affects bytes AND packet counts — visible as a clear outlier for ML demo

Usage
-----
  python sdg-base.py \\
      --host https://localhost:9200 \\
      --user elastic --password changeme \\
      --no-verify-ssl \\
      --days 30

  # Override the spike weekend (ISO date of the Saturday):
  python sdg-base.py ... --spike-date 2026-03-08

  # List available timezones:
  python sdg-base.py --list-timezones
"""

import argparse
import ipaddress
import os
import random
import sys
import time
import threading
import uuid
from datetime import datetime, timedelta, timezone, date
from queue import Queue, Empty

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import parallel_bulk
except ImportError:
    print("ERROR: elasticsearch-py not installed.  Run: pip install elasticsearch")
    sys.exit(1)

_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
sys.path.insert(0, _HERE)

try:
    from business_calendar import is_us_federal_holiday, is_business_day
    _HAS_CAL = True
except ImportError:
    _HAS_CAL = False

# ── Constants ──────────────────────────────────────────────────────────────────

INDEX        = "logs-netflow.log-baseline"
DEFAULT_DAYS = 30
DEFAULT_TPD  = 50_000     # flows per weekday
MAX_HOURLY   = 10_000     # default hourly cap (overridable)
SPIKE_MULT   = 4.0        # spike weekend multiplier

# Diurnal weights (24 hours) — lunch peak at 12:00
_WORKDAY_WEIGHTS = [
    0.00, 0.00, 0.00, 0.00, 0.00, 0.02,   # 00–05
    0.05, 0.25, 0.55, 0.70, 0.85, 0.95,   # 06–11
    1.00, 0.98, 0.90, 0.85, 0.80, 0.70,   # 12–17
    0.50, 0.30, 0.12, 0.05, 0.02, 0.01,   # 18–23
]
_WEEKEND_WEIGHTS = [
    0.00, 0.00, 0.00, 0.00, 0.00, 0.01,
    0.02, 0.04, 0.06, 0.08, 0.09, 0.10,
    0.10, 0.10, 0.09, 0.08, 0.07, 0.06,
    0.05, 0.04, 0.03, 0.02, 0.01, 0.00,
]

# ── Topology pools ─────────────────────────────────────────────────────────────

_INTERNAL_NETS = [
    "10.10.0.0/20",   # user workstations
    "10.10.16.0/20",  # servers
    "10.10.32.0/22",  # DMZ
    "10.20.0.0/16",   # datacenter
    "172.16.0.0/12",  # management
]
_EXTERNAL_NETS = [
    "203.0.113.0/24",
    "198.51.100.0/24",
    "192.0.2.0/24",
]
_ROUTERS = [
    ("core-router-01",    "10.0.0.1"),
    ("core-router-02",    "10.0.0.2"),
    ("edge-router-01",    "10.0.0.3"),
    ("datacenter-sw-01",  "10.0.0.10"),
]
_ROUTER_WEIGHTS = [35, 35, 20, 10]

_DST_PORTS = [443, 80, 8443, 22, 3306, 5432, 1521, 25, 53, 8080]
_DST_PORT_W = [45, 20, 10,   8,  6,    5,    4,    2,  0,  0 ]
# Fix weights to sum to 100 and cover all ports
_DST_PORT_W = [45, 15, 8, 6, 5, 4, 3, 2, 7, 5]

_PROTOCOLS = [
    ("tcp",  6,  ["https","http","ssh","mysql","postgresql","oracle","smtp"]),
    ("udp",  17, ["dns","ntp","syslog"]),
    ("icmp", 1,  ["icmp"]),
]
_PROTO_W = [75, 20, 5]

_DIRECTIONS = ["inbound", "outbound", "internal"]
_DIRECTION_W = [35, 45, 20]


# ── Timezone helpers ───────────────────────────────────────────────────────────

def _local_tz():
    try:
        import tzlocal
        return tzlocal.get_localzone()
    except Exception:
        offset = -time.timezone if not time.daylight else -time.altzone
        return timezone(timedelta(seconds=offset))

def resolve_tz(name):
    if not name:
        return _local_tz()
    try:
        import zoneinfo
        return zoneinfo.ZoneInfo(name)
    except Exception:
        pass
    try:
        import pytz
        return pytz.timezone(name)
    except Exception:
        pass
    print(f"  ⚠ Unknown timezone {name!r} — using local")
    return _local_tz()

def tz_str(tz):
    return getattr(tz, 'key', getattr(tz, 'zone', str(tz)))

def list_timezones():
    try:
        import zoneinfo
        zones = sorted(zoneinfo.available_timezones())
    except ImportError:
        try:
            import pytz
            zones = sorted(pytz.all_timezones)
        except ImportError:
            print("Install zoneinfo (Python 3.9+) or pytz to list timezones.")
            return
    cols = 3; w = 35
    print(f"\n{len(zones)} timezones available:\n")
    for i in range(0, len(zones), cols):
        print("  " + "".join(f"{z:<{w}}" for z in zones[i:i+cols]))
    print()


# ── IP generation helpers ──────────────────────────────────────────────────────

def _rand_ip_in(cidr):
    net = ipaddress.IPv4Network(cidr, strict=False)
    return str(ipaddress.IPv4Address(
        random.randint(int(net.network_address) + 1,
                       int(net.broadcast_address) - 1)
    ))

def _make_src_dst():
    direction = random.choices(_DIRECTIONS, weights=_DIRECTION_W, k=1)[0]
    if direction == "internal":
        src = _rand_ip_in(random.choice(_INTERNAL_NETS))
        dst = _rand_ip_in(random.choice(_INTERNAL_NETS))
        src_loc = dst_loc = "internal"
    elif direction == "outbound":
        src = _rand_ip_in(random.choice(_INTERNAL_NETS))
        dst = _rand_ip_in(random.choice(_EXTERNAL_NETS))
        src_loc, dst_loc = "internal", "external"
    else:  # inbound
        src = _rand_ip_in(random.choice(_EXTERNAL_NETS))
        dst = _rand_ip_in(random.choice(_INTERNAL_NETS))
        src_loc, dst_loc = "external", "internal"
    return src, dst, src_loc, dst_loc, direction


# ── Flow document builder ──────────────────────────────────────────────────────

def make_flow(ts_iso, spike=False):
    """Build a single ECS NetFlow flow document."""
    src_ip, dst_ip, src_loc, dst_loc, direction = _make_src_dst()
    dst_port = random.choices(_DST_PORTS, weights=_DST_PORT_W, k=1)[0]
    proto_name, iana_num, proto_apps = random.choices(
        _PROTOCOLS, weights=_PROTO_W, k=1)[0]
    app_proto = random.choice(proto_apps)
    router_name, router_ip = random.choices(_ROUTERS, weights=_ROUTER_WEIGHTS, k=1)[0]

    # Bytes — spike multiplier inflates traffic volume
    mult = SPIKE_MULT if spike else 1.0
    src_bytes = int(random.randint(64, 1_500_000) * mult)
    dst_bytes = int(random.randint(64, 5_000_000) * mult)
    src_pkts  = max(1, int(src_bytes / random.randint(500, 1500)))
    dst_pkts  = max(1, int(dst_bytes / random.randint(500, 1500)))
    duration  = random.randint(1_000_000, 300_000_000_000)

    return {
        "@timestamp":          ts_iso,
        "ecs.version":         "8.11.0",
        "event.kind":          "event",
        "event.category":      ["network"],
        "event.type":          ["connection", "end"],
        "event.action":        "netflow_flow",
        "event.dataset":       "netflow.log",
        "event.module":        "netflow",
        "event.duration":      duration,
        "event.start":         ts_iso,
        "event.end":           ts_iso,
        "data_stream": {
            "type":      "logs",
            "dataset":   "netflow.log",
            "namespace": "baseline",
        },
        "source": {
            "ip":       src_ip,
            "port":     random.randint(1024, 65535),
            "bytes":    src_bytes,
            "packets":  src_pkts,
            "locality": src_loc,
        },
        "destination": {
            "ip":       dst_ip,
            "port":     dst_port,
            "bytes":    dst_bytes,
            "packets":  dst_pkts,
            "locality": dst_loc,
        },
        "network": {
            "transport":    proto_name,
            "type":         "ipv4",
            "iana_number":  str(iana_num),
            "bytes":        src_bytes + dst_bytes,
            "packets":      src_pkts + dst_pkts,
            "direction":    direction,
            "protocol":     app_proto,
            "community_id": str(uuid.uuid4()),
        },
        "observer": {
            "type": "router",
            "name": router_name,
            "ip":   [router_ip],
        },
        "netflow": {
            "type": "netflow_flow",
            "exporter": {
                "address":        f"{router_ip}:2055",
                "version":        9,
                "uptime_millis":  random.randint(86_400_000, 2_592_000_000),
            },
            "flow_id":            random.randint(1_000_000, 9_999_999),
            "input":  {"ifindex": random.randint(1, 48)},
            "output": {"ifindex": random.randint(1, 48)},
            "ip_class_of_service": random.choice([0, 8, 16, 24, 32, 40, 48]),
            "tcp_control_bits":   random.choice([2, 4, 16, 24]),
        },
        "related": {"ip": [src_ip, dst_ip]},
        "tags":  ["netflow", "baseline", "lendpath-network"],
        "agent": {
            "type":    "filebeat",
            "name":    "netflow-collector-01",
            "version": "8.14.0",
        },
    }


# ── Volume helpers ─────────────────────────────────────────────────────────────

def _day_factor(d, spike_date):
    """Return the volume multiplier for a given date."""
    if d == spike_date:
        # Spike Saturday — 4× normal weekend volume
        return 0.25 * SPIKE_MULT
    if _HAS_CAL:
        if is_us_federal_holiday(d):
            return 0.10
        if not is_business_day(d):
            return 0.25
    else:
        if d.weekday() >= 5:
            return 0.25
    return 1.0

def _hour_weights(d, spike_date):
    """Return the 24-element hour weight list for a given date."""
    if d == spike_date:
        # Spike day has elevated but still diurnal-shaped traffic
        base = _WEEKEND_WEIGHTS[:]
        return [w * SPIKE_MULT for w in base]
    if _HAS_CAL:
        if not is_business_day(d) or is_us_federal_holiday(d):
            return _WEEKEND_WEIGHTS
    else:
        if d.weekday() >= 5:
            return _WEEKEND_WEIGHTS
    return _WORKDAY_WEIGHTS

def _hour_counts(target, weights, max_hourly):
    """Allocate `target` events across 24 hours respecting weights and cap."""
    total_w = sum(weights)
    counts  = []
    allocated = 0
    for w in weights:
        n = round(target * w / total_w) if total_w > 0 else 0
        n = min(n, max_hourly)
        counts.append(n)
        allocated += n
    # Adjust at peak hour (12)
    diff = target - allocated
    peak = 12
    counts[peak] = min(counts[peak] + diff, max_hourly)
    return counts


# ── Timestamp generator ────────────────────────────────────────────────────────

def timestamps_for_day(day_dt, count, tz, max_hourly, spike_date):
    """Yield `count` ISO timestamp strings for `day_dt` in local `tz`."""
    weights    = _hour_weights(day_dt, spike_date)
    is_spike   = (day_dt == spike_date)
    counts     = _hour_counts(count, weights, max_hourly)
    day_start  = datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=tz)

    for hour, n in enumerate(counts):
        if n <= 0:
            continue
        h_local = day_start + timedelta(hours=hour)
        h_utc   = h_local.astimezone(timezone.utc)
        for _ in range(n):
            ts = h_utc + timedelta(seconds=random.uniform(0, 3599))
            iso = ts.strftime("%Y-%m-%dT%H:%M:%S.") + \
                  f"{ts.microsecond // 1000:03d}Z"
            yield iso, is_spike


# ── Bulk action generator ──────────────────────────────────────────────────────

def action_gen(day_dt, count, tz, max_hourly, spike_date):
    for ts_iso, is_spike in timestamps_for_day(day_dt, count, tz, max_hourly,
                                               spike_date):
        doc = make_flow(ts_iso, spike=is_spike)
        yield {"_op_type": "create", "_index": INDEX, "_source": doc}


# ── Worker ─────────────────────────────────────────────────────────────────────

def day_worker(es, day_dt, count, bulk_size, pb_threads, pb_queue,
               progress_q, tz, max_hourly, spike_date):
    try:
        for ok, info in parallel_bulk(
            es,
            action_gen(day_dt, count, tz, max_hourly, spike_date),
            thread_count=pb_threads, chunk_size=bulk_size,
            queue_size=pb_queue, raise_on_error=False,
            raise_on_exception=False, request_timeout=120,
        ):
            progress_q.put("OK" if ok else f"ERR:{info}")
    except Exception as e:
        progress_q.put(f"ERR:{day_dt}:{e}")


# ── Main backfill ──────────────────────────────────────────────────────────────

def backfill(host, user, password, verify_ssl,
             days, target_per_day, workers, bulk_size,
             pb_threads, pb_queue, tz, max_hourly, spike_date):

    ssl_opts = {"verify_certs": verify_ssl, "ssl_show_warn": False}
    if not verify_ssl:
        ssl_opts["ssl_assert_fingerprint"] = None
    es = Elasticsearch(host, basic_auth=(user, password), **ssl_opts)

    today     = datetime.now(tz).date()
    start_day = today - timedelta(days=days - 1)

    # Auto-select spike weekend: Saturday 3–4 weeks ago if not specified
    if spike_date is None:
        candidate = today - timedelta(days=21)
        # Walk back to the nearest Saturday
        while candidate.weekday() != 5:
            candidate -= timedelta(days=1)
        # Ensure it falls within our backfill window
        if candidate >= start_day:
            spike_date = candidate
        else:
            # Try 2 weeks ago
            candidate = today - timedelta(days=14)
            while candidate.weekday() != 5:
                candidate -= timedelta(days=1)
            spike_date = candidate if candidate >= start_day else None

    tz_name = tz_str(tz)

    # Build day schedule
    schedule = []
    for d in range(days):
        day = start_day + timedelta(days=d)
        factor = _day_factor(day, spike_date)
        count  = max(1, min(round(target_per_day * factor), max_hourly * 24))
        schedule.append((day, count))

    total_flows = sum(c for _, c in schedule)

    print(f"\n{'='*70}")
    print(f"  SDG-Base — NetFlow Baseline Generator")
    print(f"{'='*70}")
    print(f"  Index:          {INDEX}")
    print(f"  Days:           {days}")
    print(f"  Weekday target: {target_per_day:,} flows/day")
    print(f"  Peak cap:       {max_hourly:,} flows/hour")
    print(f"  Timezone:       {tz_name}")
    print(f"  Window:         {start_day}  →  {today}")
    if spike_date:
        print(f"  Spike date:     {spike_date} ({spike_date.strftime('%A')}) "
              f"— {SPIKE_MULT:.0f}× normal volume  ← ML outlier seed")
    else:
        print(f"  Spike date:     (none in window)")
    print(f"  ~Total flows:   {total_flows:,}")
    print()

    # Schedule preview
    print(f"  {'Date':<12} {'Day':<4} {'Type':<10} {'Flows':>10}  {'Note'}")
    print(f"  {'-'*58}")
    for day, count in schedule:
        if _HAS_CAL:
            if is_us_federal_holiday(day):
                dtype = "holiday"
            elif not is_business_day(day):
                dtype = "weekend"
            else:
                dtype = "workday"
        else:
            dtype = "weekend" if day.weekday() >= 5 else "workday"
        note = "◄ SPIKE" if day == spike_date else ""
        print(f"  {str(day):<12} {day.strftime('%a'):<4} {dtype:<10} "
              f"{count:>10,}  {note}")
    print(f"  {'-'*58}")
    print(f"  {'TOTAL':<28} {total_flows:>10,}")
    print(f"\n  Press Ctrl+C to stop.\n")

    progress_q    = Queue()
    start_time    = time.time()
    indexed_total = 0
    error_count   = 0

    def printer():
        nonlocal indexed_total, error_count
        last = time.time()
        while True:
            try:
                item = progress_q.get(timeout=1)
                if item is None:
                    break
                if isinstance(item, str) and item.startswith("ERR:"):
                    error_count += 1
                    if error_count <= 20:
                        print(f"\n  ⚠ {item[4:]}")
                else:
                    indexed_total += 1
                now = time.time()
                if now - last >= 10:
                    elapsed = now - start_time
                    rate    = indexed_total / elapsed if elapsed > 0 else 0
                    pct     = min(indexed_total / total_flows * 100, 100) \
                              if total_flows else 0
                    eta     = (total_flows - indexed_total) / rate \
                              if rate > 0 else 0
                    print(f"  [{pct:5.1f}%] {indexed_total:>10,}/{total_flows:,}"
                          f"  |  {rate:>8,.0f} flows/sec"
                          f"  |  ETA {int(eta//3600):02d}h"
                          f"{int((eta%3600)//60):02d}m"
                          + (f"  | {error_count} errs" if error_count else ""))
                    last = now
            except Empty:
                continue

    t_print = threading.Thread(target=printer, daemon=True)
    t_print.start()

    work_q = Queue()
    for day, count in schedule:
        work_q.put((day, count))

    def worker():
        while True:
            try:
                day_dt, count = work_q.get_nowait()
            except Empty:
                return
            try:
                day_worker(es, day_dt, count, bulk_size, pb_threads, pb_queue,
                           progress_q, tz, max_hourly, spike_date)
            except KeyboardInterrupt:
                return
            except Exception as e:
                progress_q.put(f"ERR:{day_dt}:{e}")
            finally:
                work_q.task_done()

    threads = [threading.Thread(target=worker, daemon=True)
               for _ in range(min(workers, days))]
    for t in threads:
        t.start()
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\n\nStopped early.")

    progress_q.put(None)
    t_print.join(timeout=10)
    elapsed = time.time() - start_time
    rate    = indexed_total / elapsed if elapsed > 0 else 0

    print(f"\n{'='*70}")
    print(f"  NetFlow baseline backfill complete.")
    print(f"  Indexed:  {indexed_total:,} flows   Errors: {error_count:,}")
    print(f"  Elapsed:  {int(elapsed//3600):02d}h"
          f"{int((elapsed%3600)//60):02d}m{int(elapsed%60):02d}s")
    print(f"  Rate:     {rate:,.0f} flows/sec")
    if spike_date:
        print(f"\n  Spike date {spike_date} has {SPIKE_MULT:.0f}× normal weekend "
              f"traffic — use this date as the ML outlier anchor in the demo.")
    print(f"{'='*70}\n")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="SDG-Base: NetFlow baseline generator for Elastic ML baselining demos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard 30-day backfill:
  python sdg-base.py \\
      --host https://localhost:9200 \\
      --user elastic --password changeme --no-verify-ssl \\
      --days 30

  # Set timezone explicitly:
  python sdg-base.py ... --timezone "America/New_York"

  # Override the auto-selected spike weekend:
  python sdg-base.py ... --spike-date 2026-03-08

  # No spike (clean baseline only):
  python sdg-base.py ... --no-spike

  # List available timezones:
  python sdg-base.py --list-timezones
        """
    )
    p.add_argument("--host",           default="https://localhost:9200")
    p.add_argument("--user",           default="elastic")
    p.add_argument("--password",       default="changeme")
    p.add_argument("--no-verify-ssl",  action="store_true")
    p.add_argument("--days",           type=int,   default=DEFAULT_DAYS,
                   help=f"Days of history to generate (default: {DEFAULT_DAYS})")
    p.add_argument("--target-per-day", "--tpd", type=int, default=DEFAULT_TPD,
                   help=f"Weekday flows/day (default: {DEFAULT_TPD:,})")
    p.add_argument("--max-hourly",     type=int,   default=MAX_HOURLY,
                   help=f"Max flows/hour at peak (default: {MAX_HOURLY:,})")
    p.add_argument("--workers",  "-w", type=int,   default=4)
    p.add_argument("--bulk-size","-b", type=int,   default=1000)
    p.add_argument("--pb-threads",     type=int,   default=2)
    p.add_argument("--pb-queue",       type=int,   default=4)
    p.add_argument("--timezone",       default=None, metavar="TZ",
                   help="Timezone for timestamps (default: system local). "
                        "Use --list-timezones to see options.")
    p.add_argument("--spike-date",     default=None, metavar="YYYY-MM-DD",
                   help="Override auto-selected spike Saturday (ISO date). "
                        "Auto-selects a Saturday 3–4 weeks ago if not set.")
    p.add_argument("--no-spike",       action="store_true",
                   help="Disable the spike weekend entirely (clean baseline only)")
    p.add_argument("--list-timezones", action="store_true",
                   help="Print all available timezone names and exit")

    # Read workshop-config.json if present
    _cfg_file = os.path.join(_HERE, "workshop-config.json")
    _cfg = {}
    if os.path.exists(_cfg_file):
        try:
            import json
            _cfg = json.load(open(_cfg_file))
            print(f"  ✓ Loaded config: {_cfg_file}")
        except Exception:
            pass

    args = p.parse_args()

    if args.list_timezones:
        list_timezones()
        sys.exit(0)

    # Back-fill from config where CLI left defaults
    _defaults = {"host": "https://localhost:9200",
                 "user": "elastic", "password": "changeme"}
    if args.host     == _defaults["host"]     and _cfg.get("host"):
        args.host     = _cfg["host"]
    if args.user     == _defaults["user"]     and _cfg.get("user"):
        args.user     = _cfg["user"]
    if args.password == _defaults["password"] and _cfg.get("password"):
        args.password = _cfg["password"]
    if not args.no_verify_ssl and _cfg.get("no_verify_ssl"):
        args.no_verify_ssl = _cfg["no_verify_ssl"]
    if not args.timezone and _cfg.get("timezone"):
        args.timezone = _cfg["timezone"]

    tz = resolve_tz(args.timezone)
    tz_name = tz_str(tz)
    if "utc" in tz_name.lower() or tz_name in ("UTC", "Etc/UTC", "GMT"):
        print(f"  ⚠ Timezone is UTC — if your Kibana browser is in a different "
              f"timezone the lunch peak will appear shifted.")
        print(f"    Use --timezone 'America/New_York' to align with local time.\n")

    spike_date = None
    if not args.no_spike:
        if args.spike_date:
            try:
                spike_date = date.fromisoformat(args.spike_date)
                if spike_date.weekday() != 5:
                    print(f"  ⚠ --spike-date {spike_date} is a "
                          f"{spike_date.strftime('%A')} not a Saturday. "
                          f"Proceeding anyway.")
            except ValueError:
                print(f"ERROR: --spike-date must be YYYY-MM-DD format")
                sys.exit(1)
        # else auto-selected inside backfill()

    backfill(
        host         = args.host,
        user         = args.user,
        password     = args.password,
        verify_ssl   = not args.no_verify_ssl,
        days         = args.days,
        target_per_day = args.target_per_day,
        workers      = args.workers,
        bulk_size    = args.bulk_size,
        pb_threads   = args.pb_threads,
        pb_queue     = args.pb_queue,
        tz           = tz,
        max_hourly   = args.max_hourly,
        spike_date   = spike_date,
    )


if __name__ == "__main__":
    main()
