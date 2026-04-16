#!/usr/bin/env python3
"""
backfill_sdg.py — Historical SDG data backfill for the LendPath ML Workshop.

Generates 30 days of historical ECS data across all 20 SDG data streams.

Volume (70% of the overall 100k-150k/day budget):
  Weekdays (Mon-Fri, non-holiday): target_per_day docs  (default 87,500)
  Weekends + US Federal Holidays:  ≤30% of weekday target (~26,250 docs)

Diurnal pattern (1 PM peak, 9-5 business window):
  Workday: ramps from 9 AM, peaks at 1 PM, tapers by 6 PM, zero overnight
  Reduced: same shape at 30% volume

Usage:
    python backfill_sdg.py \\
        --host https://localhost:9200 \\
        --user elastic --password changeme \\
        --no-verify-ssl \\
        [--days 30] \\
        [--target-per-day 87500] \\
        [--workers 8] \\
        [--bulk-size 2000] \\
        [--config mortgage-workshop.yml]
"""

import argparse
import hashlib
import os
import random
import sys
import time
import threading
import uuid
from datetime import datetime, timedelta, timezone
from queue import Queue, Empty

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml not installed.  Run: pip install pyyaml")
    sys.exit(1)

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import parallel_bulk
except ImportError:
    print("ERROR: elasticsearch-py not installed.  Run: pip install elasticsearch")
    sys.exit(1)

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
try:
    from business_calendar import (
        timestamps_for_day, day_volume_factor, is_us_federal_holiday
    )
except ImportError:
    print("ERROR: business_calendar.py not found. It must be in the same directory.")
    sys.exit(1)

STREAM_WEIGHTS = {
    "logs-nginx.access-mortgage":          12.0,
    "logs-aws.vpcflow-mortgage":           12.0,
    "logs-haproxy.log-mortgage":           11.0,
    "logs-mortgage.applications-default":   9.0,
    "logs-aws.waf-mortgage":               8.0,
    "logs-coredns.log-mortgage":           8.0,
    "logs-mortgage.audit-default":         7.0,
    "logs-ping_one.audit-mortgage":        6.0,
    "logs-akamai.siem-mortgage":           6.0,
    "logs-nginx.error-mortgage":           5.0,
    "logs-oracle.database_audit-mortgage": 4.0,
    "metrics-nginx.stubstatus-mortgage":   2.0,
    "metrics-mortgage.services-default":   2.0,
    "metrics-mortgage.hosts-default":      2.0,
    "metrics-haproxy.stat-mortgage":       1.5,
    "metrics-haproxy.info-mortgage":       1.0,
    "metrics-kafka.broker-mortgage":       1.0,
    "metrics-kafka.partition-mortgage":    0.5,
    "metrics-oracle.sysmetric-mortgage":   0.5,
    "metrics-oracle.tablespace-mortgage":  0.5,
}

POOL  = 65536
_MASK = POOL - 1
_IPS   = [f"{random.randint(1,254)}.{random.randint(0,255)}."
           f"{random.randint(0,255)}.{random.randint(1,254)}"
           for _ in range(POOL)]
_UUIDS = [uuid.uuid4().hex for _ in range(POOL)]
_HASHES= [hashlib.md5(uuid.uuid4().bytes).hexdigest()[:16] for _ in range(POOL)]
_GEOS  = [{"lat": round(random.uniform(25,50),4), "lon": round(random.uniform(-125,-65),4)}
          for _ in range(POOL)]
_CITIES= ["Pittsburgh","Philadelphia","Columbus","Cleveland","Chicago",
          "Indianapolis","Nashville","Atlanta","Dallas","Houston",
          "Phoenix","Los Angeles","Seattle","Denver","Boston"]
_STATES= ["Pennsylvania","Ohio","Illinois","Indiana","Tennessee",
          "Georgia","Texas","Arizona","California","Washington",
          "Colorado","Massachusetts","Michigan","Florida","New York"]
_NAMES = ["James Whitfield","Maria Santos","David Kim","Sarah Mitchell",
          "Robert Okafor","Angela Torres","Thomas Brennan","Linda Chen",
          "Michael Chang","Jennifer Lopez","William Foster","Patricia Green"]

_pidx = 0; _plock = threading.Lock()
def _pi():
    global _pidx
    with _plock:
        i = _pidx; _pidx = (_pidx+1) & _MASK
    return i

def _parse_range(s):
    p = str(s).split(",")
    return (float(p[0].strip()), float(p[1].strip())) if len(p)==2 else (0.0,100.0)

def compile_field(f):
    name = f.get("name",""); ftype = f.get("type","value")
    if name == "@timestamp" or ftype == "timestamp": return (name, None)
    if ftype == "value" or "value" in f:
        v = f.get("value"); return (name, lambda _v=v: _v)
    if ftype in ("random_string_from_list","random_integer_from_list"):
        raw = f.get("custom_list","")
        choices = raw if isinstance(raw,list) else [c.strip().strip('"') for c in str(raw).split(",") if c.strip()]
        if not choices: return (name, lambda: None)
        if ftype == "random_integer_from_list":
            ic=[int(c) for c in choices]; return (name, lambda ic=ic: random.choice(ic))
        return (name, lambda c=choices: random.choice(c))
    if ftype == "uuid": return (name, lambda: _UUIDS[_pi()])
    if ftype == "hash": return (name, lambda: _HASHES[_pi()])
    if ftype in ("int","integer"):
        lo,hi=_parse_range(f.get("range","0,100")); return (name, lambda l=int(lo),h=int(hi): random.randint(l,h))
    if ftype == "long":
        lo,hi=_parse_range(f.get("range","0,1000000")); return (name, lambda l=int(lo),h=int(hi): random.randint(l,h))
    if ftype == "float":
        lo,hi=_parse_range(f.get("range","0,1")); return (name, lambda l=lo,h=hi: round(random.uniform(l,h),4))
    if ftype == "boolean": return (name, lambda: random.random()<0.5)
    if ftype == "ipv4": return (name, lambda: _IPS[_pi()])
    if ftype == "geo_point": return (name, lambda: _GEOS[_pi()])
    if ftype == "city": return (name, lambda: random.choice(_CITIES))
    if ftype == "state": return (name, lambda: random.choice(_STATES))
    if ftype == "full_name": return (name, lambda: random.choice(_NAMES))
    v = f.get("value"); return (name, lambda _v=v: _v)

def _set(doc, key, val):
    parts=key.split("."); cur=doc
    for p in parts[:-1]:
        if p not in cur: cur[p]={}
        cur=cur[p]
    cur[parts[-1]]=val

def make_doc(compiled, ts):
    doc={}
    for key,gen in compiled:
        val=ts if gen is None else gen()
        if val is not None: _set(doc,key,val)
    return doc

def action_gen(index_name, compiled, day_dt, count):
    for ts in timestamps_for_day(day_dt, count):
        yield {"_op_type":"create","_index":index_name,
               "_source":make_doc(compiled,ts)}

def stream_worker(es, index_name, compiled, weekday_target, days,
                  bulk_size, pb_threads, pb_queue, progress_q):
    end_day   = datetime.now(timezone.utc).date()
    start_day = end_day - timedelta(days=days-1)
    for d in range(days):
        day   = start_day + timedelta(days=d)
        count = max(1, round(weekday_target * day_volume_factor(day)))
        try:
            for ok, info in parallel_bulk(
                es, action_gen(index_name, compiled, day, count),
                thread_count=pb_threads, chunk_size=bulk_size,
                queue_size=pb_queue, raise_on_error=False,
                raise_on_exception=False, request_timeout=120,
            ):
                if ok:
                    progress_q.put(1)
                else:
                    err=(info.get("create") or {}).get("error",{})
                    if err.get("type") != "version_conflict_engine_exception":
                        progress_q.put(f"ERR:{index_name}:{err.get('type','?')}")
        except Exception as e:
            progress_q.put(f"ERR:{index_name}:{e}")

def backfill(host, user, password, verify_ssl, config_path,
             days, target_per_day, workers, bulk_size, pb_threads, pb_queue):
    es = Elasticsearch(host, basic_auth=(user,password), verify_certs=verify_ssl,
                       ssl_show_warn=False, request_timeout=120, max_retries=3,
                       retry_on_timeout=True)
    try:
        info = es.info()
        print(f"Connected to Elasticsearch {info['version']['number']}")
    except Exception as e:
        print(f"Cannot connect: {e}"); sys.exit(1)

    if not os.path.exists(config_path):
        print(f"ERROR: config not found: {config_path}"); sys.exit(1)
    with open(config_path) as fh:
        cfg = yaml.safe_load(fh)

    stream_fields = {}
    for w in cfg.get("workloads",[]):
        idx = w.get("indexName","")
        if idx and idx not in stream_fields:
            stream_fields[idx] = w.get("fields",[])

    compiled_streams = {idx:[compile_field(f) for f in fields]
                        for idx,fields in stream_fields.items()}
    total_weight  = sum(STREAM_WEIGHTS.get(i,1.0) for i in stream_fields)
    stream_targets= {idx: max(1,round(target_per_day*STREAM_WEIGHTS.get(idx,1.0)/total_weight))
                     for idx in stream_fields}

    end_day   = datetime.now(timezone.utc).date()
    start_day = end_day - timedelta(days=days-1)
    total_docs = sum(
        sum(round(t * day_volume_factor(start_day + timedelta(days=d))) for d in range(days))
        for t in stream_targets.values()
    )

    print(f"\n{'='*62}")
    print(f"  LendPath ML Workshop — SDG Historical Backfill")
    print(f"{'='*62}")
    print(f"  Days:              {days}")
    print(f"  Weekday target:    {target_per_day:>12,} docs/day  (70% of budget)")
    print(f"  Weekend/holiday:   ≤{round(target_per_day*0.30):>10,} docs/day  (30%)")
    print(f"  ~Total docs:       {total_docs:>12,}")
    print(f"  Streams:           {len(stream_fields)}")

    holidays = [start_day+timedelta(days=d) for d in range(days)
                if is_us_federal_holiday(start_day+timedelta(days=d))]
    if holidays:
        print(f"\n  Federal holidays in window (→ reduced volume):")
        for h in holidays: print(f"    {h}  ({h.strftime('%A')})")
    else:
        print(f"\n  No US Federal holidays in this {days}-day window.")
    print(f"\n  Press Ctrl+C to stop.\n")

    progress_q=Queue(); start_time=time.time()
    indexed_total=0; error_count=0

    def printer():
        nonlocal indexed_total, error_count
        last=time.time()
        while True:
            try:
                item=progress_q.get(timeout=1)
                if item is None: break
                if isinstance(item,str) and item.startswith("ERR:"):
                    error_count+=1
                    if error_count<=20: print(f"\n  ⚠ {item[4:]}")
                else:
                    indexed_total+=1
                now=time.time()
                if now-last>=10:
                    elapsed=now-start_time; rate=indexed_total/elapsed if elapsed>0 else 0
                    pct=min(indexed_total/total_docs*100,100)
                    eta=(total_docs-indexed_total)/rate if rate>0 else 0
                    print(f"  [{pct:5.1f}%] {indexed_total:>10,}/{total_docs:,}"
                          f"  |  {rate:>8,.0f} docs/sec"
                          f"  |  ETA {int(eta//3600):02d}h{int((eta%3600)//60):02d}m"
                          +(f"  | {error_count} errs" if error_count else ""))
                    last=now
            except Empty: continue

    t_print=threading.Thread(target=printer,daemon=True); t_print.start()
    work_q=Queue()
    for idx in stream_fields: work_q.put(idx)

    def worker():
        while True:
            try: idx=work_q.get_nowait()
            except Empty: return
            try:
                stream_worker(es,idx,compiled_streams[idx],stream_targets[idx],
                              days,bulk_size,pb_threads,pb_queue,progress_q)
            except KeyboardInterrupt: return
            except Exception as e: progress_q.put(f"ERR:{idx}:{e}")
            finally: work_q.task_done()

    threads=[threading.Thread(target=worker,daemon=True)
             for _ in range(min(workers,len(stream_fields)))]
    for t in threads: t.start()
    try:
        for t in threads: t.join()
    except KeyboardInterrupt: print("\n\nStopped early.")

    progress_q.put(None); t_print.join(timeout=10)
    elapsed=time.time()-start_time; rate=indexed_total/elapsed if elapsed>0 else 0
    print(f"\n{'='*62}")
    print(f"  SDG backfill complete.")
    print(f"  Indexed: {indexed_total:,}   Errors: {error_count:,}")
    print(f"  Elapsed: {int(elapsed//3600):02d}h{int((elapsed%3600)//60):02d}m{int(elapsed%60):02d}s")
    print(f"  Rate:    {rate:,.0f} docs/sec")
    print(f"{'='*62}\n")

def main():
    p=argparse.ArgumentParser(description="SDG historical backfill — 100-150k events/day with US business calendar")
    p.add_argument("--host",default="https://localhost:9200")
    p.add_argument("--user",default="elastic")
    p.add_argument("--password",default="changeme")
    p.add_argument("--no-verify-ssl",action="store_true")
    p.add_argument("--days",type=int,default=30)
    p.add_argument("--target-per-day","--tpd",type=int,default=3_500_000,
                   help="Weekday SDG docs/day — 70%% of overall budget (default: 3,500,000)")
    p.add_argument("--workers","-w",type=int,default=8)
    p.add_argument("--bulk-size","-b",type=int,default=2000)
    p.add_argument("--parallel-bulk-threads","--pb-threads",type=int,default=4)
    p.add_argument("--parallel-bulk-queue","--pb-queue",type=int,default=8)
    p.add_argument("--config",default="mortgage-workshop.yml")
    args=p.parse_args()
    backfill(args.host,args.user,args.password,not args.no_verify_ssl,
             args.config,args.days,args.target_per_day,args.workers,
             args.bulk_size,args.parallel_bulk_threads,args.parallel_bulk_queue)

if __name__=="__main__": main()
