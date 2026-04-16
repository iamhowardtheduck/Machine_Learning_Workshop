#!/usr/bin/env python3
"""
backfill_apm-MLv2-WORKSHOP.py — 7-day APM trace backfill with:
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
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import parallel_bulk
except ImportError:
    print("ERROR: elasticsearch-py not installed."); sys.exit(1)

# Resolve the directory this script physically lives in — works even when
# launched as a subprocess with a relative __file__ path.
_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))

# Insert the script's own directory first — this is where apm_trace_generator.py
# must live alongside this script.
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# Also try cwd and parent of cwd as fallbacks
for _search in (os.getcwd(), os.path.dirname(os.getcwd())):
    if _search not in sys.path:
        sys.path.append(_search)

# Attempt direct file-based import as final fallback regardless of sys.path
def _import_apm_trace_generator():
    import importlib.util
    candidate = os.path.join(_HERE, "apm_trace_generator.py")
    if os.path.exists(candidate):
        spec = importlib.util.spec_from_file_location("apm_trace_generator", candidate)
        mod  = importlib.util.module_from_spec(spec)
        sys.modules["apm_trace_generator"] = mod
        spec.loader.exec_module(mod)
        return mod
    # Search all sys.path entries
    for _p in sys.path:
        candidate = os.path.join(_p, "apm_trace_generator.py")
        if os.path.exists(candidate):
            spec = importlib.util.spec_from_file_location("apm_trace_generator", candidate)
            mod  = importlib.util.module_from_spec(spec)
            sys.modules["apm_trace_generator"] = mod
            spec.loader.exec_module(mod)
            return mod
    return None

_apm_mod = _import_apm_trace_generator()
if _apm_mod is None:
    print(f"ERROR: apm_trace_generator.py not found.")
    print(f"  Script directory: {_HERE}")
    print(f"  Searched sys.path:")
    for _p in sys.path:
        print(f"    {'✓' if os.path.exists(os.path.join(_p, 'apm_trace_generator.py')) else '✗'} {_p}")
    print(f"  Ensure apm_trace_generator.py is in the same directory as this script.")
    sys.exit(1)

from apm_trace_generator import SERVICES, generate_trace, generate_metrics

try:
    from business_calendar import day_volume_factor, hour_weights_for_day
except ImportError:
    print("ERROR: business_calendar.py not found."); sys.exit(1)

INDEX = "traces-apm-default"


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

def tz_name_str(tz):
    return getattr(tz, 'key', getattr(tz, 'zone', str(tz)))


# ---------------------------------------------------------------------------
# Trace action generator with timezone + hourly cap
# ---------------------------------------------------------------------------
def trace_action_gen(day_dt, traces_per_day, tz, max_hourly):
    """
    Yield bulk action dicts for all traces in one day.
    Timestamps are generated in `tz` and stored as UTC ISO strings.
    Volume is capped at max_hourly per hour.
    """
    service_names   = list(SERVICES.keys())
    service_weights = [40, 20, 15, 15, 10]

    day_start_local = datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=tz)
    day_start_utc   = day_start_local.astimezone(timezone.utc)

    weights  = hour_weights_for_day(day_dt)
    total_w  = sum(weights)

    hour_counts = []
    allocated   = 0
    for w in weights:
        n = round(traces_per_day * w / total_w) if total_w > 0 else 0
        n = min(n, max_hourly)
        hour_counts.append(n)
        allocated += n
    hour_counts[13] = min(hour_counts[13] + (traces_per_day - allocated), max_hourly)

    for hour, n in enumerate(hour_counts):
        if n <= 0:
            continue
        # Add hours in LOCAL time then convert to UTC so that hour 13
        # means 1 PM local, not 1 PM UTC.
        h_start_local = day_start_local + timedelta(hours=hour)
        h_start_utc   = h_start_local.astimezone(timezone.utc)

        for _ in range(n):
            ts_utc  = h_start_utc + timedelta(seconds=random.uniform(0, 3599))
            ts_iso  = ts_utc.strftime("%Y-%m-%dT%H:%M:%S.") + \
                      f"{ts_utc.microsecond // 1000:03d}Z"
            svc_name = random.choices(service_names, weights=service_weights, k=1)[0]

            actions = generate_trace(svc_name)
            for action in actions:
                doc = action["_source"]
                doc["@timestamp"] = ts_iso
                yield {"_op_type": "create", "_index": INDEX, "_source": doc}

            # Emit metrics ~2% of the time
            if random.random() < 0.02:
                for msvc in service_names:
                    for action in generate_metrics(msvc, ts_iso=ts_iso):
                        yield action


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
def day_worker(es, day_dt, traces_per_day, bulk_size, pb_threads, pb_queue,
               progress_q, tz, max_hourly):
    try:
        for ok, info in parallel_bulk(
            es,
            trace_action_gen(day_dt, traces_per_day, tz, max_hourly),
            thread_count=pb_threads, chunk_size=bulk_size,
            queue_size=pb_queue, raise_on_error=False,
            raise_on_exception=False, request_timeout=120,
        ):
            progress_q.put("OK" if ok else f"ERR:{info}")
    except Exception as e:
        progress_q.put(f"ERR:day {day_dt}:{e}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def backfill(host, user, password, verify_ssl, days, traces_per_day,
             workers, bulk_size, pb_threads, pb_queue, tz, max_hourly):

    ssl_opts = {"verify_certs": verify_ssl, "ssl_show_warn": False}
    if not verify_ssl:
        ssl_opts["ssl_assert_fingerprint"] = None
    es = Elasticsearch(host, basic_auth=(user, password), **ssl_opts)

    today     = datetime.now(tz).date()
    start_day = today - timedelta(days=days - 1)

    total_traces = sum(
        min(round(traces_per_day * day_volume_factor(start_day + timedelta(days=d))),
            max_hourly * 24)
        for d in range(days)
    )
    total_docs = total_traces * 6   # ~6 docs per trace

    print(f"\n{'='*64}")
    print(f"  LendPath ML Workshop v2 — APM Historical Backfill")
    print(f"{'='*64}")
    print(f"  Days:              {days}")
    print(f"  Weekday target:    {traces_per_day:>12,} traces/day  (~{traces_per_day*6:,} docs)")
    print(f"  Peak cap:          {max_hourly:>12,} events/hour")
    print(f"  ~Total traces:     {total_traces:>12,}")
    print(f"  ~Total docs:       {total_docs:>12,}")
    print(f"  Timezone:          {tz_name_str(tz)}")
    print(f"  Window:            {start_day}  →  {today}")
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
    for d in range(days):
        day      = start_day + timedelta(days=d)
        day_vol  = min(round(traces_per_day * day_volume_factor(day)), max_hourly * 24)
        work_q.put((day, day_vol))

    def worker():
        while True:
            try:
                day_dt, vol = work_q.get_nowait()
            except Empty:
                return
            try:
                day_worker(es, day_dt, vol, bulk_size, pb_threads, pb_queue,
                           progress_q, tz, max_hourly)
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
    print(f"\n{'='*64}")
    print(f"  APM backfill complete.")
    print(f"  Indexed: {indexed_total:,}   Errors: {error_count:,}")
    print(f"  Elapsed: {int(elapsed//3600):02d}h"
          f"{int((elapsed%3600)//60):02d}m{int(elapsed%60):02d}s")
    print(f"  Rate:    {rate:,.0f} docs/sec")
    print(f"{'='*64}\n")


def main():
    p = argparse.ArgumentParser(
        description="APM historical backfill v2 — 7 days, 10k/hr cap, local timezone"
    )
    p.add_argument("--host",               default="https://localhost:9200")
    p.add_argument("--user",               default="elastic")
    p.add_argument("--password",           default="changeme")
    p.add_argument("--no-verify-ssl",      action="store_true")
    p.add_argument("--days",               type=int, default=7)
    p.add_argument("--traces-per-day", "--tpd", type=int, default=4_000)
    p.add_argument("--workers", "-w",      type=int, default=4)
    p.add_argument("--bulk-size", "-b",    type=int, default=300)
    p.add_argument("--parallel-bulk-threads", "--pb-threads", type=int, default=2)
    p.add_argument("--parallel-bulk-queue",   "--pb-queue",   type=int, default=4)
    p.add_argument("--timezone",           default=None, metavar="TZ")
    p.add_argument("--max-hourly",         type=int, default=10_000)
    args = p.parse_args()
    tz = resolve_tz(args.timezone)
    backfill(
        args.host, args.user, args.password, not args.no_verify_ssl,
        args.days, args.traces_per_day,
        args.workers, args.bulk_size,
        args.parallel_bulk_threads, args.parallel_bulk_queue,
        tz, args.max_hourly,
    )

if __name__ == "__main__":
    main()
