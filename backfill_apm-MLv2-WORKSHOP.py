#!/usr/bin/env python3
"""
backfill_apm-MLv2-WORKSHOP.py — 7-day APM trace backfill with:
  • 10,000 events/hour hard cap at peak
  • Timestamps in user's local timezone (auto-detected or --timezone)
  • 7-day default window
  • Anomaly spike injection via --spike-manifest:
      volume     → multiplies trace count per spike hour
      error_rate → passed as spike_error_rate into generate_trace()
      latency    → passed as spike_latency_mult into generate_trace()
  • Error docs routed to logs-apm.error-default (data_stream.type = "logs")
"""

import argparse
import json
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

_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))

if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
for _search in (os.getcwd(), os.path.dirname(os.getcwd())):
    if _search not in sys.path:
        sys.path.append(_search)

def _import_apm_trace_generator():
    import importlib.util
    candidate = os.path.join(_HERE, "apm_trace_generator.py")
    if os.path.exists(candidate):
        spec = importlib.util.spec_from_file_location("apm_trace_generator", candidate)
        mod  = importlib.util.module_from_spec(spec)
        sys.modules["apm_trace_generator"] = mod
        spec.loader.exec_module(mod)
        return mod
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
    sys.exit(1)

from apm_trace_generator import SERVICES, generate_trace, generate_metrics

try:
    from business_calendar import day_volume_factor, hour_weights_for_day
except ImportError:
    print("ERROR: business_calendar.py not found."); sys.exit(1)

TRACE_INDEX = "traces-apm-default"
ERROR_INDEX = "logs-apm.error-default"


# ---------------------------------------------------------------------------
# Spike manifest loader
# ---------------------------------------------------------------------------
def load_spike_manifest(path):
    if not path:
        return []
    try:
        with open(path) as fh:
            data = json.load(fh)
        spikes = data.get("spikes", [])
        if spikes:
            print(f"  ✓ Spike manifest loaded: {len(spikes)} spike window(s)")
            for s in spikes:
                active = ", ".join(s.get("types", []))
                print(f"    hours {s['start_hour']:>4}–{s['end_hour']:<4}  [{active}]  "
                      f"vol ×{s.get('volume_mult',1):.1f}  "
                      f"err {s.get('error_rate',0)*100:.0f}%  "
                      f"lat ×{s.get('latency_mult',1):.1f}")
        return spikes
    except Exception as e:
        print(f"  ⚠ Could not load spike manifest {path!r}: {e}")
        return []


def spike_at_hour(spikes, hour_offset):
    for s in spikes:
        if s["start_hour"] <= hour_offset < s["end_hour"]:
            return s
    return None


# ---------------------------------------------------------------------------
# Timezone helpers
# ---------------------------------------------------------------------------
def resolve_tz(tz_name):
    if not tz_name:
        tz = _local_tz()
        tz_str = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))
        if "utc" in tz_str.lower() or tz_str in ("UTC", "Etc/UTC", "GMT"):
            print(f"  ⚠  Timezone is UTC (system default).")
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
# Trace action generator — spike-aware, correct index routing
# ---------------------------------------------------------------------------
def trace_action_gen(day_dt, traces_per_day, tz, max_hourly,
                     spikes=None, day_offset=0, base_error_rate=0.05):
    """
    Yield bulk action dicts for all traces in one day.

    Spike behaviour:
      volume     → hour trace count × spike.volume_mult (cap bypassed)
      error_rate → passed as spike_error_rate to generate_trace()
      latency    → passed as spike_latency_mult to generate_trace()

    Index routing (per doc):
      data_stream.type == "logs"  → logs-apm.error-default
      everything else             → traces-apm-default
    """
    spikes          = spikes or []
    service_names   = list(SERVICES.keys())
    service_weights = [40, 20, 15, 15, 10]

    day_start_local = datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=tz)

    weights  = hour_weights_for_day(day_dt)
    total_w  = sum(weights)

    # Per-hour trace counts with volume spike multiplier
    hour_counts = []
    allocated   = 0
    for hour, w in enumerate(weights):
        abs_hour = day_offset * 24 + hour
        spike    = spike_at_hour(spikes, abs_hour)
        base_n   = round(traces_per_day * w / total_w) if total_w > 0 else 0
        if spike and "volume" in spike.get("types", []):
            n = round(base_n * spike["volume_mult"])
        else:
            n = min(base_n, max_hourly)
        hour_counts.append(n)
        allocated += n

    # Remainder into peak hour 13
    remainder  = traces_per_day - allocated
    abs_peak   = day_offset * 24 + 13
    peak_spike = spike_at_hour(spikes, abs_peak)
    if peak_spike and "volume" in peak_spike.get("types", []):
        hour_counts[13] = hour_counts[13] + remainder
    else:
        hour_counts[13] = min(hour_counts[13] + remainder, max_hourly)

    for hour, n in enumerate(hour_counts):
        if n <= 0:
            continue
        abs_hour     = day_offset * 24 + hour
        active_spike = spike_at_hour(spikes, abs_hour)
        spike_types  = active_spike.get("types", []) if active_spike else []

        h_start_local = day_start_local + timedelta(hours=hour)
        h_start_utc   = h_start_local.astimezone(timezone.utc)

        # Spike params passed directly into generate_trace
        spike_error_rate   = active_spike.get("error_rate") \
                             if active_spike and "error_rate" in spike_types else None
        spike_latency_mult = active_spike.get("latency_mult") \
                             if active_spike and "latency" in spike_types else None

        for _ in range(n):
            ts_utc = h_start_utc + timedelta(seconds=random.uniform(0, 3599))
            ts_iso = ts_utc.strftime("%Y-%m-%dT%H:%M:%S.") + \
                     f"{ts_utc.microsecond // 1000:03d}Z"
            ts_us  = int(ts_utc.timestamp() * 1_000_000)

            svc_name = random.choices(service_names, weights=service_weights, k=1)[0]

            actions = generate_trace(
                svc_name,
                error_rate=base_error_rate,
                spike_error_rate=spike_error_rate,
                spike_latency_mult=spike_latency_mult,
            )

            # Stamp every doc with the backfill timestamp — generate_trace
            # uses datetime.now() internally so we correct it here.
            for action in actions:
                doc = action["_source"]
                doc["@timestamp"] = ts_iso
                if "timestamp" in doc:
                    doc["timestamp"]["us"] = ts_us
                # Route to correct index
                ds_type = doc.get("data_stream", {}).get("type", "traces")
                action["_index"] = ERROR_INDEX if ds_type == "logs" else TRACE_INDEX
                yield action

            # Metrics ~2% of the time
            if random.random() < 0.02:
                for msvc in service_names:
                    for action in generate_metrics(msvc, ts_iso=ts_iso):
                        doc = action["_source"]
                        doc["@timestamp"] = ts_iso
                        if "timestamp" in doc:
                            doc["timestamp"]["us"] = ts_us
                        yield action


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
def day_worker(es, day_dt, traces_per_day, bulk_size, pb_threads, pb_queue,
               progress_q, tz, max_hourly, spikes=None, day_offset=0,
               base_error_rate=0.05):
    try:
        for ok, info in parallel_bulk(
            es,
            trace_action_gen(day_dt, traces_per_day, tz, max_hourly,
                             spikes=spikes, day_offset=day_offset,
                             base_error_rate=base_error_rate),
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
             workers, bulk_size, pb_threads, pb_queue, tz, max_hourly,
             spikes=None, base_error_rate=0.05):

    spikes = spikes or []
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
    total_docs = total_traces * 6

    all_spike_types = sorted({t for s in spikes for t in s.get("types", [])})
    print(f"\n{'='*64}")
    print(f"  LendPath ML Workshop v2 — APM Historical Backfill")
    print(f"{'='*64}")
    print(f"  Days:              {days}")
    print(f"  Weekday target:    {traces_per_day:>12,} traces/day  (~{traces_per_day*6:,} docs)")
    print(f"  Peak cap:          {max_hourly:>12,} events/hour"
          + (" (bypassed during volume spikes)" if any("volume" in s.get("types",[]) for s in spikes) else ""))
    print(f"  ~Total traces:     {total_traces:>12,}")
    print(f"  ~Total docs:       {total_docs:>12,}")
    print(f"  Base error rate:   {base_error_rate*100:.1f}%")
    print(f"  Spike windows:     {len(spikes)}"
          + (f"  [{', '.join(all_spike_types)}]" if all_spike_types else ""))
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
        day     = start_day + timedelta(days=d)
        day_vol = min(round(traces_per_day * day_volume_factor(day)), max_hourly * 24)
        work_q.put((day, day_vol, d))

    def worker():
        while True:
            try:
                day_dt, vol, d_offset = work_q.get_nowait()
            except Empty:
                return
            try:
                day_worker(es, day_dt, vol, bulk_size, pb_threads, pb_queue,
                           progress_q, tz, max_hourly,
                           spikes=spikes, day_offset=d_offset,
                           base_error_rate=base_error_rate)
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
    p.add_argument("--base-error-rate",    type=float, default=0.05, metavar="RATE",
                   help="Baseline fraction of traces that emit error docs (default: 0.05)")
    p.add_argument("--spike-manifest",     default=None, metavar="PATH",
                   help="Path to anomaly_spikes.json written by the orchestrator")
    args = p.parse_args()
    tz     = resolve_tz(args.timezone)
    spikes = load_spike_manifest(args.spike_manifest)
    backfill(
        args.host, args.user, args.password, not args.no_verify_ssl,
        args.days, args.traces_per_day,
        args.workers, args.bulk_size,
        args.parallel_bulk_threads, args.parallel_bulk_queue,
        tz, args.max_hourly,
        spikes=spikes,
        base_error_rate=args.base_error_rate,
    )

if __name__ == "__main__":
    main()
