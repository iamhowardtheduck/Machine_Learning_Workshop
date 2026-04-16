#!/usr/bin/env python3
"""
backfill_apm.py — Historical APM trace backfill for the LendPath ML Workshop.

Generates 30 days of properly linked APM traces (transactions + spans) with
historical timestamps. Volume is 30% of the overall daily budget:

  Weekday target: 6,250 traces/day  (~37,500 docs at ~6 docs/trace)
  Weekend/holiday: ≤30% of weekday  (~1,875 traces → ~11,250 docs)

Service map fields included per span:
  destination.service.resource  — e.g. "Oracle DB:1521", "Kafka:9092"
  service.target.name           — same value; identifies the downstream node
  service.target.type           — "db", "messaging", "cache", "external", "storage"

Imports the full service topology from apm_trace_generator.py so historical
and live traces are structurally identical.

Usage:
    python backfill_apm.py \\
        --host https://localhost:9200 \\
        --user elastic --password changeme \\
        --no-verify-ssl \\
        [--days 30] \\
        [--traces-per-day 6250] \\
        [--workers 6] \\
        [--bulk-size 500]
"""

import argparse
import os
import random
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from queue import Queue, Empty

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import parallel_bulk
except ImportError:
    print("ERROR: elasticsearch-py not installed.  Run: pip install elasticsearch")
    sys.exit(1)

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

try:
    from apm_trace_generator import (
        SERVICES, USERS, USER_HOME_GEO,
        _hex, _geo_for_user, _result_for_status, _weighted_choice, build_base,
        generate_trace, generate_metrics
    )
except ImportError as e:
    print(f"ERROR: cannot import from apm_trace_generator.py: {e}")
    print(f"  Ensure apm_trace_generator.py is in: {_HERE}")
    sys.exit(1)

try:
    from business_calendar import (
        timestamps_for_day, day_volume_factor, is_us_federal_holiday
    )
except ImportError:
    print("ERROR: business_calendar.py not found. It must be in the same directory.")
    sys.exit(1)


def _ts_iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def trace_action_gen(day_dt, traces_per_day):
    """
    Generator yielding bulk action dicts for all traces in one day.

    Delegates entirely to generate_trace() from apm_trace_generator.py
    so all field logic — including child.id, service.target, span.destination,
    child transactions, and observer fields — stays in one place.
    Timestamps are overridden with historically distributed values.
    """
    INDEX         = "traces-apm-default"
    service_names = list(SERVICES.keys())
    svc_weights   = [40, 20, 15, 15, 10]

    day_start  = datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=timezone.utc)

    from business_calendar import hour_weights_for_day
    weights  = hour_weights_for_day(day_dt)
    total_w  = sum(weights)
    hour_counts = []
    allocated = 0
    for w in weights:
        n = round(traces_per_day * w / total_w) if total_w > 0 else 0
        hour_counts.append(n)
        allocated += n
    hour_counts[13] += traces_per_day - allocated

    for hour, n in enumerate(hour_counts):
        if n <= 0:
            continue
        h_start = (day_start + timedelta(hours=hour)).timestamp()

        for _ in range(n):
            ts_dt    = datetime.fromtimestamp(h_start + random.uniform(0, 3599),
                                              tz=timezone.utc)
            ts_iso   = _ts_iso(ts_dt)
            svc_name = random.choices(service_names, weights=svc_weights, k=1)[0]

            # generate_trace() produces a complete correctly-structured trace
            # including child transactions, child.id, service.target, and
            # span.destination nested correctly for Kibana service map.
            # We override @timestamp on every doc to use our historical value.
            actions = generate_trace(svc_name)
            for action in actions:
                doc = action["_source"]
                doc["@timestamp"] = ts_iso
                yield {"_op_type": "create", "_index": INDEX, "_source": doc}

            # Emit JVM + system metrics once per service every ~50 traces
            # so the APM service dashboards have metric data to display
            if random.random() < 0.02:   # ~2% of traces → ~1 metric set per 50 traces
                for msvc in service_names:
                    for action in generate_metrics(msvc, ts_iso=ts_iso):
                        yield action


def day_worker(es, traces_per_day, days, pb_threads, pb_queue,
               bulk_size, progress_q, work_q):
    end_day   = datetime.now(timezone.utc).date()
    start_day = end_day - timedelta(days=days - 1)
    while True:
        try:
            d = work_q.get_nowait()
        except Empty:
            return
        day   = start_day + timedelta(days=d)
        count = max(1, round(traces_per_day * day_volume_factor(day)))
        try:
            for ok, info in parallel_bulk(
                es, trace_action_gen(day, count),
                thread_count=pb_threads, chunk_size=bulk_size,
                queue_size=pb_queue, raise_on_error=False,
                raise_on_exception=False, request_timeout=120,
            ):
                if ok:
                    progress_q.put(1)
                else:
                    err = (info.get("create") or {}).get("error", {})
                    if err.get("type") != "version_conflict_engine_exception":
                        progress_q.put(f"ERR:{day}:{err.get('type','?')}")
        except Exception as e:
            progress_q.put(f"ERR:{day}:{e}")
        finally:
            work_q.task_done()


def backfill(host, user, password, verify_ssl, days, traces_per_day,
             workers, bulk_size, pb_threads, pb_queue):
    es = Elasticsearch(host, basic_auth=(user, password), verify_certs=verify_ssl,
                       ssl_show_warn=False, request_timeout=120, max_retries=3,
                       retry_on_timeout=True)
    try:
        info = es.info()
        print(f"Connected to Elasticsearch {info['version']['number']}")
    except Exception as e:
        print(f"Cannot connect: {e}"); sys.exit(1)

    avg_spans = sum(
        len([s for s in SERVICES[sn]["downstream_spans"] if s[6] > 0.5])
        for sn in SERVICES
    ) / len(SERVICES)
    avg_docs = 1 + avg_spans

    end_day   = datetime.now(timezone.utc).date()
    start_day = end_day - timedelta(days=days - 1)
    total_traces_est = sum(
        round(traces_per_day * day_volume_factor(start_day + timedelta(days=d)))
        for d in range(days)
    )
    total_docs_est = int(total_traces_est * avg_docs)

    print(f"\n{'='*62}")
    print(f"  LendPath ML Workshop — APM Historical Trace Backfill")
    print(f"{'='*62}")
    print(f"  Days:              {days}")
    print(f"  Weekday traces/day:{traces_per_day:>12,}  (~{round(traces_per_day*avg_docs):,} docs, 30% of budget)")
    print(f"  Weekend/holiday:   ≤{round(traces_per_day*0.30):>10,} traces/day")
    print(f"  ~Total docs:       {total_docs_est:>12,}")
    print(f"  Day workers:       {min(workers, days)}")
    print(f"  pb threads/worker: {pb_threads}")

    holidays = [start_day + timedelta(days=d) for d in range(days)
                if is_us_federal_holiday(start_day + timedelta(days=d))]
    if holidays:
        print(f"\n  Federal holidays (→ reduced volume):")
        for h in holidays: print(f"    {h}  ({h.strftime('%A')})")
    else:
        print(f"\n  No US Federal holidays in this {days}-day window.")
    print(f"\n  Press Ctrl+C to stop.\n")

    progress_q = Queue(); start_time = time.time()
    indexed_total = 0; error_count = 0

    def printer():
        nonlocal indexed_total, error_count
        last = time.time()
        while True:
            try:
                item = progress_q.get(timeout=1)
                if item is None: break
                if isinstance(item, str) and item.startswith("ERR:"):
                    error_count += 1
                    if error_count <= 20: print(f"\n  ⚠ {item[4:]}")
                else:
                    indexed_total += 1
                now = time.time()
                if now - last >= 10:
                    elapsed = now - start_time
                    rate    = indexed_total / elapsed if elapsed > 0 else 0
                    pct     = min(indexed_total / total_docs_est * 100, 100)
                    eta     = (total_docs_est - indexed_total) / rate if rate > 0 else 0
                    print(f"  [{pct:5.1f}%] {indexed_total:>10,}/~{total_docs_est:,}"
                          f"  |  {rate:>8,.0f} docs/sec"
                          f"  |  ETA {int(eta//3600):02d}h{int((eta%3600)//60):02d}m"
                          + (f"  | {error_count} errs" if error_count else ""))
                    last = now
            except Empty: continue

    t_print = threading.Thread(target=printer, daemon=True); t_print.start()
    work_q  = Queue()
    for d in range(days): work_q.put(d)

    threads = [threading.Thread(
        target=day_worker,
        args=(es, traces_per_day, days, pb_threads, pb_queue,
              bulk_size, progress_q, work_q),
        daemon=True) for _ in range(min(workers, days))]
    for t in threads: t.start()
    try:
        for t in threads: t.join()
    except KeyboardInterrupt: print("\n\nStopped early.")

    progress_q.put(None); t_print.join(timeout=10)
    elapsed = time.time() - start_time; rate = indexed_total / elapsed if elapsed > 0 else 0
    print(f"\n{'='*62}")
    print(f"  APM backfill complete.")
    print(f"  Indexed: {indexed_total:,}   Errors: {error_count:,}")
    print(f"  Elapsed: {int(elapsed//3600):02d}h{int((elapsed%3600)//60):02d}m{int(elapsed%60):02d}s")
    print(f"  Rate:    {rate:,.0f} docs/sec")
    print(f"{'='*62}\n")


def main():
    p = argparse.ArgumentParser(description="APM historical backfill — 30% of 100-150k/day with US business calendar")
    p.add_argument("--host",     default="https://localhost:9200")
    p.add_argument("--user",     default="elastic")
    p.add_argument("--password", default="changeme")
    p.add_argument("--no-verify-ssl", action="store_true")
    p.add_argument("--days",          type=int, default=30)
    p.add_argument("--traces-per-day","--tpd", type=int, default=250_000,
                   help="Weekday APM traces/day — 30%% of overall budget (default: 250,000 → ~1,500,000 docs)")
    p.add_argument("--workers",    "-w", type=int, default=6)
    p.add_argument("--bulk-size",  "-b", type=int, default=500)
    p.add_argument("--parallel-bulk-threads","--pb-threads", type=int, default=4)
    p.add_argument("--parallel-bulk-queue",  "--pb-queue",   type=int, default=8)
    args = p.parse_args()

    backfill(args.host, args.user, args.password, not args.no_verify_ssl,
             args.days, args.traces_per_day, args.workers, args.bulk_size,
             args.parallel_bulk_threads, args.parallel_bulk_queue)

if __name__ == "__main__": main()
