#!/usr/bin/env python3
"""
backfill_sdg-MLv2-WORKSHOP.py — 7-day SDG backfill with:
  • 10,000 events/hour hard cap at peak
  • Timestamps in user's local timezone (auto-detected or --timezone)
  • 7-day default window
"""

import argparse
import os
import random
import sys
import time
import threading
from datetime import datetime, timedelta, timezone, date
from queue import Queue, Empty

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml not installed.  Run: pip install pyyaml"); sys.exit(1)

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import parallel_bulk
except ImportError:
    print("ERROR: elasticsearch-py not installed."); sys.exit(1)

_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))

if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
for _search in (os.getcwd(), os.path.dirname(os.getcwd())):
    if _search not in sys.path:
        sys.path.append(_search)

def _file_import(modname):
    import importlib.util
    for _p in [_HERE] + sys.path:
        candidate = os.path.join(_p, f"{modname}.py")
        if os.path.exists(candidate):
            spec = importlib.util.spec_from_file_location(modname, candidate)
            mod  = importlib.util.module_from_spec(spec)
            sys.modules[modname] = mod
            spec.loader.exec_module(mod)
            return mod
    return None

if _file_import("business_calendar") is None:
    print(f"ERROR: business_calendar.py not found in {_HERE}")
    sys.exit(1)

from business_calendar import (
    day_volume_factor, is_us_federal_holiday, hour_weights_for_day
)

# ---------------------------------------------------------------------------
# Field compiler — load from backfill_sdg.py via importlib, fall back to
# a faithful inline implementation if the file isn't importable.
# ---------------------------------------------------------------------------
_sdg_mod = _file_import("backfill_sdg")
if _sdg_mod is not None:
    compile_field   = _sdg_mod.compile_field
    make_doc        = _sdg_mod.make_doc
    STREAM_WEIGHTS  = _sdg_mod.STREAM_WEIGHTS
else:

    STREAM_WEIGHTS = {}

    def compile_field(f):
        """Minimal compile_field matching the real backfill_sdg.py logic."""
        name  = f.get("name", "")
        ftype = f.get("type", "value")
        # @timestamp and explicit timestamp type → substituted by make_doc
        if name == "@timestamp" or ftype == "timestamp":
            return (name, None)
        # Static value fields — return a lambda that always returns the value
        if ftype == "value" or "value" in f:
            v = f.get("value")
            return (name, lambda _v=v: _v)
        # Anything else without a generator → skip (return empty string constant)
        return (name, lambda: None)

    def make_doc(compiled, ts):
        doc = {}
        for key, gen in compiled:
            val = ts if gen is None else gen()
            if val is not None:
                parts = key.split(".")
                d = doc
                for p in parts[:-1]:
                    d = d.setdefault(p, {})
                d[parts[-1]] = val
        return doc


# ---------------------------------------------------------------------------
# Timezone helpers
# ---------------------------------------------------------------------------
def resolve_tz(tz_name):
    if not tz_name:
        tz = _local_tz()
        tz_str = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))
        # Warn if the resolved timezone is UTC — on most Linux servers the
        # system timezone is UTC, which means the diurnal peak (1 PM) will be
        # stored as 13:00 UTC and appear shifted in Kibana if the user's
        # browser is in a different timezone.
        if "utc" in tz_str.lower() or tz_str in ("UTC", "Etc/UTC", "GMT"):
            print(f"  ⚠  Timezone is UTC (system default).")
            print(f"     If your Kibana/browser is in a different timezone, the")
            print(f"     diurnal peak will appear shifted in dashboards.")
            print(f"     Re-run with --timezone 'America/New_York' (or your tz)")
            print(f"     to align the peak with your local business hours.")
            print()
        return tz
    try:
        import zoneinfo
        return zoneinfo.ZoneInfo(tz_name)
    except Exception:
        pass
    try:
        import pytz
        return pytz.timezone(tz_name)
    except Exception:
        pass
    return _local_tz()

def _local_tz():
    try:
        import tzlocal
        return tzlocal.get_localzone()
    except Exception:
        offset = -time.timezone if not time.daylight else -time.altzone
        return timezone(timedelta(seconds=offset))

def tz_name(tz):
    return getattr(tz, 'key', getattr(tz, 'zone', str(tz)))


# ---------------------------------------------------------------------------
# Timestamp generator with hourly cap
# ---------------------------------------------------------------------------
def timestamps_for_day_capped(day_dt, count, tz, max_hourly):
    """
    Yield `count` ISO timestamp strings for date `day_dt` in `tz`,
    distributed by the diurnal profile but capped at max_hourly per hour.
    """
    # Build the day start in the user's local timezone
    day_start_local = datetime(day_dt.year, day_dt.month, day_dt.day,
                               tzinfo=tz)
    # Convert to UTC for storage
    day_start_utc = day_start_local.astimezone(timezone.utc)

    weights = hour_weights_for_day(day_dt)
    total_w = sum(weights)

    hour_counts = []
    allocated = 0
    for w in weights:
        n = round(count * w / total_w) if total_w > 0 else 0
        n = min(n, max_hourly)           # apply hourly cap
        hour_counts.append(n)
        allocated += n

    # Apply cap adjustment — peak is hour 13
    hour_counts[13] = min(hour_counts[13] + (count - allocated), max_hourly)

    for hour, n in enumerate(hour_counts):
        if n <= 0:
            continue
        # Add hours in LOCAL time then convert to UTC so that hour 13
        # means 1 PM local, not 1 PM UTC.
        h_start_local = day_start_local + timedelta(hours=hour)
        h_start_utc   = h_start_local.astimezone(timezone.utc)
        for _ in range(n):
            sec = random.uniform(0, 3599)
            ts  = h_start_utc + timedelta(seconds=sec)
            yield ts.strftime("%Y-%m-%dT%H:%M:%S.") + \
                  f"{ts.microsecond // 1000:03d}Z"


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
def action_gen(index_name, compiled, day_dt, count, tz, max_hourly):
    for ts in timestamps_for_day_capped(day_dt, count, tz, max_hourly):
        yield {"_op_type": "create", "_index": index_name,
               "_source": make_doc(compiled, ts)}


def stream_worker(es, index_name, compiled, weekday_target, days,
                  bulk_size, pb_threads, pb_queue, progress_q, tz, max_hourly):
    today     = datetime.now(tz).date()
    start_day = today - timedelta(days=days - 1)
    for d in range(days):
        day   = start_day + timedelta(days=d)
        count = max(1, min(round(weekday_target * day_volume_factor(day)),
                           max_hourly * 24))
        try:
            for ok, info in parallel_bulk(
                es, action_gen(index_name, compiled, day, count, tz, max_hourly),
                thread_count=pb_threads, chunk_size=bulk_size,
                queue_size=pb_queue, raise_on_error=False,
                raise_on_exception=False, request_timeout=120,
            ):
                progress_q.put("OK" if ok else f"ERR:{info}")
        except Exception as e:
            progress_q.put(f"ERR:{index_name}:{e}")


# ---------------------------------------------------------------------------
# Main backfill entry point
# ---------------------------------------------------------------------------
def backfill(host, user, password, verify_ssl, config_path,
             days, target_per_day, workers, bulk_size,
             pb_threads, pb_queue, tz, max_hourly):

    ssl_opts = {"verify_certs": verify_ssl, "ssl_show_warn": False}
    if not verify_ssl:
        ssl_opts["ssl_assert_fingerprint"] = None
    es = Elasticsearch(host, basic_auth=(user, password), **ssl_opts)

    if not os.path.exists(config_path):
        print(f"ERROR: config not found: {config_path}"); sys.exit(1)
    with open(config_path) as fh:
        cfg = yaml.safe_load(fh)

    stream_fields = {}
    for w in cfg.get("workloads", []):
        idx = w.get("indexName", "")
        if idx and idx not in stream_fields:
            stream_fields[idx] = w.get("fields", [])

    compiled_streams = {idx: [compile_field(f) for f in fields]
                        for idx, fields in stream_fields.items()}
    total_weight   = sum(STREAM_WEIGHTS.get(i, 1.0) for i in stream_fields)
    stream_targets = {
        idx: max(1, round(target_per_day * STREAM_WEIGHTS.get(idx, 1.0) / total_weight))
        for idx in stream_fields
    }

    today     = datetime.now(tz).date()
    start_day = today - timedelta(days=days - 1)
    total_docs = sum(
        min(round(t * day_volume_factor(start_day + timedelta(days=d))),
            max_hourly * 24)
        for t in stream_targets.values()
        for d in range(days)
    )

    print(f"\n{'='*64}")
    print(f"  LendPath ML Workshop v2 — SDG Historical Backfill")
    print(f"{'='*64}")
    print(f"  Days:              {days}")
    print(f"  Weekday target:    {target_per_day:>12,} docs/day")
    print(f"  Peak cap:          {max_hourly:>12,} events/hour")
    print(f"  ~Total docs:       {total_docs:>12,}")
    print(f"  Streams:           {len(stream_fields)}")
    print(f"  Timezone:          {tz_name(tz)}")
    print(f"  Window:            {start_day}  →  {today}")
    print(f"\n  Press Ctrl+C to stop.\n")

    progress_q = Queue()
    start_time = time.time()
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
                    pct     = min(indexed_total / total_docs * 100, 100) if total_docs else 0
                    eta     = (total_docs - indexed_total) / rate if rate > 0 else 0
                    print(f"  [{pct:5.1f}%] {indexed_total:>10,}/{total_docs:,}"
                          f"  |  {rate:>8,.0f} docs/sec"
                          f"  |  ETA {int(eta//3600):02d}h{int((eta%3600)//60):02d}m"
                          + (f"  | {error_count} errs" if error_count else ""))
                    last = now
            except Empty:
                continue

    t_print = threading.Thread(target=printer, daemon=True)
    t_print.start()
    work_q = Queue()
    for idx in stream_fields:
        work_q.put(idx)

    def worker():
        while True:
            try:
                idx = work_q.get_nowait()
            except Empty:
                return
            try:
                stream_worker(es, idx, compiled_streams[idx],
                              stream_targets[idx], days, bulk_size,
                              pb_threads, pb_queue, progress_q,
                              tz, max_hourly)
            except KeyboardInterrupt:
                return
            except Exception as e:
                progress_q.put(f"ERR:{idx}:{e}")
            finally:
                work_q.task_done()

    threads = [threading.Thread(target=worker, daemon=True)
               for _ in range(min(workers, len(stream_fields)))]
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
    print(f"\n{'='*64}")
    print(f"  SDG backfill complete.")
    print(f"  Indexed: {indexed_total:,}   Errors: {error_count:,}")
    print(f"  Elapsed: {int(elapsed//3600):02d}h"
          f"{int((elapsed%3600)//60):02d}m{int(elapsed%60):02d}s")
    print(f"  Rate:    {rate:,.0f} docs/sec")
    print(f"{'='*64}\n")


def main():
    p = argparse.ArgumentParser(
        description="SDG historical backfill v2 — 7 days, 10k/hr cap, local timezone"
    )
    p.add_argument("--host",               default="https://localhost:9200")
    p.add_argument("--user",               default="elastic")
    p.add_argument("--password",           default="changeme")
    p.add_argument("--no-verify-ssl",      action="store_true")
    p.add_argument("--days",               type=int, default=7)
    p.add_argument("--target-per-day", "--tpd", type=int, default=56_000)
    p.add_argument("--workers", "-w",      type=int, default=6)
    p.add_argument("--bulk-size", "-b",    type=int, default=1000)
    p.add_argument("--parallel-bulk-threads", "--pb-threads", type=int, default=2)
    p.add_argument("--parallel-bulk-queue",   "--pb-queue",   type=int, default=4)
    p.add_argument("--config",             default="mortgage-workshop.yml")
    p.add_argument("--timezone",           default=None, metavar="TZ")
    p.add_argument("--max-hourly",         type=int, default=10_000)
    args = p.parse_args()
    tz = resolve_tz(args.timezone)
    backfill(
        args.host, args.user, args.password, not args.no_verify_ssl,
        args.config, args.days, args.target_per_day,
        args.workers, args.bulk_size,
        args.parallel_bulk_threads, args.parallel_bulk_queue,
        tz, args.max_hourly,
    )

if __name__ == "__main__":
    main()
