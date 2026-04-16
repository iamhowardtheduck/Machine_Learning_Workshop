#!/usr/bin/env python3
"""
backfill_all-MLv2-WORKSHOP.py — 7-day historical backfill, then immediate
                                  handoff to real-time generators.

Key differences from the original:
  • 7 days only (not 30)
  • Peak volume capped at 10,000 events/hour (bypassable by anomaly spikes)
  • Timestamps generated in the user's local timezone (auto-detected,
    overridable with --timezone)
  • After backfill, run_workshop.py starts automatically from where
    the backfill left off (--then-run is the expected default)
  • ML job definitions default to the MLv2 files (shorter bucket spans)
  • Anomaly spike injection: random volume, error-rate, and latency spikes
    injected into the historical timeline via --spike-* flags

Spike flags (all optional):
  --spike-count N          Number of anomaly spikes to inject (default: 0)
  --spike-volume-mult X    Volume multiplier at spike peak (default: 5.0)
  --spike-duration-hrs H   Duration of each spike in hours (default: 2)
  --spike-error-rate X     Fraction of APM traces that become errors during
                           spike (0.0–1.0, default: 0.25)
  --spike-latency-mult X   APM latency multiplier during spike (default: 4.0)
  --spike-seed N           Random seed for reproducible spike placement
  --spike-types TYPE...    Which spike types to inject: volume, error_rate,
                           latency (default: all three)
  --spike-cap-override     Allow spikes to exceed the --max-hourly cap
                           (default: True when using spikes)

Usage:
    python backfill_all-MLv2-WORKSHOP.py \\
        --host https://localhost:9200 \\
        --user elastic --password changeme --no-verify-ssl \\
        --then-run

    # Inject 3 random anomaly spikes:
    python backfill_all-MLv2-WORKSHOP.py ... \\
        --spike-count 3 --spike-volume-mult 8 --spike-duration-hrs 3 \\
        --spike-error-rate 0.4 --spike-latency-mult 6 --spike-cap-override

    # Volume-only spikes, reproducible placement:
    python backfill_all-MLv2-WORKSHOP.py ... \\
        --spike-count 2 --spike-types volume --spike-seed 42

    # Override timezone:
    python backfill_all-MLv2-WORKSHOP.py ... --timezone "America/New_York"

    # List available timezones:
    python backfill_all-MLv2-WORKSHOP.py --list-timezones
"""

import argparse
import os
import subprocess
import sys
import time
import threading
import signal
import random
import json
from datetime import date, datetime, timedelta, timezone

_HERE  = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable

sys.path.insert(0, _HERE)
try:
    from business_calendar import (
        day_volume_factor, is_us_federal_holiday, is_business_day
    )
    _CAL = True
except ImportError:
    _CAL = False

# Maximum events per hour at peak (10,000 cap) — bypassed by spike-cap-override
MAX_HOURLY = 10_000
# 7-day default
DEFAULT_DAYS = 7
# Daily budget split: 70% SDG, 30% APM
DEFAULT_SDG_TPD    = 56_000
DEFAULT_APM_TRACES = 4_000

# ── Spike defaults ────────────────────────────────────────────────────────────
SPIKE_DEFAULT_COUNT        = 0
SPIKE_DEFAULT_VOLUME_MULT  = 5.0
SPIKE_DEFAULT_DURATION_HRS = 2
SPIKE_DEFAULT_ERROR_RATE   = 0.25
SPIKE_DEFAULT_LATENCY_MULT = 4.0
SPIKE_ALL_TYPES            = ["volume", "error_rate", "latency"]


# ── Timezone helpers ──────────────────────────────────────────────────────────

def get_local_tz():
    try:
        import tzlocal
        return tzlocal.get_localzone()
    except ImportError:
        pass
    try:
        import time as _t
        offset_sec = -_t.timezone if not _t.daylight else -_t.altzone
        return timezone(timedelta(seconds=offset_sec))
    except Exception:
        return timezone.utc


def resolve_tz(tz_name):
    if not tz_name:
        return get_local_tz()
    try:
        import zoneinfo
        return zoneinfo.ZoneInfo(tz_name)
    except ImportError:
        pass
    try:
        import pytz
        return pytz.timezone(tz_name)
    except ImportError:
        pass
    print(f"  ⚠ Could not load timezone {tz_name!r} — zoneinfo/pytz not installed.")
    print(f"    Falling back to local system timezone.")
    return get_local_tz()


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
    col_w = 35
    cols  = 3
    print(f"\nAvailable timezones ({len(zones)} total):\n")
    for i in range(0, len(zones), cols):
        row = zones[i:i+cols]
        print("  " + "".join(f"{z:<{col_w}}" for z in row))
    print()


# ── Spike generation ──────────────────────────────────────────────────────────

def generate_spikes(days, spike_count, spike_duration_hrs, spike_volume_mult,
                    spike_error_rate, spike_latency_mult, spike_types,
                    spike_seed=None):
    """
    Generate a list of spike descriptor dicts, each with:
      start_hour  : int — hour offset from backfill start (0 = first hour)
      end_hour    : int — exclusive end hour
      types       : list of str — which anomaly types are active
      volume_mult : float
      error_rate  : float
      latency_mult: float

    Spikes are placed randomly across the backfill window, with a minimum
    gap of spike_duration_hrs between them to avoid overlaps.
    """
    if spike_count == 0:
        return []

    rng          = random.Random(spike_seed)
    total_hours  = days * 24
    # Keep spikes away from the very first and last hour
    placeable    = list(range(1, total_hours - spike_duration_hrs - 1))
    spikes       = []
    blocked      = set()

    attempts = 0
    while len(spikes) < spike_count and attempts < spike_count * 200:
        attempts += 1
        if not placeable:
            break
        h = rng.choice(placeable)
        # Check no overlap with existing spikes (+ buffer of 1hr on each side)
        if any(h in blocked for _ in [1]):
            continue
        if h in blocked:
            continue
        spikes.append({
            "start_hour":   h,
            "end_hour":     h + spike_duration_hrs,
            "types":        list(spike_types),
            "volume_mult":  spike_volume_mult,
            "error_rate":   spike_error_rate,
            "latency_mult": spike_latency_mult,
        })
        # Block this window + buffer
        for bh in range(max(0, h - spike_duration_hrs),
                        min(total_hours, h + spike_duration_hrs * 2 + 1)):
            blocked.add(bh)
        # Remove blocked hours from placeable
        placeable = [x for x in placeable if x not in blocked]

    spikes.sort(key=lambda s: s["start_hour"])
    return spikes


def spike_at_hour(spikes, hour_offset):
    """Return the active spike dict for a given hour offset, or None."""
    for s in spikes:
        if s["start_hour"] <= hour_offset < s["end_hour"]:
            return s
    return None


def print_spike_schedule(spikes, days, tz):
    if not spikes:
        print("  No anomaly spikes configured.")
        return

    today     = date.today()
    start_day = today - timedelta(days=days - 1)
    tz_name   = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))

    print(f"\n  {'#':<4} {'Date':<12} {'Time window':<22} {'Types':<30} "
          f"{'Vol ×':>6} {'Err%':>6} {'Lat ×':>6}")
    print(f"  {'-'*88}")

    for i, s in enumerate(spikes, 1):
        day_offset  = s["start_hour"] // 24
        hour_start  = s["start_hour"] % 24
        hour_end    = s["end_hour"] % 24 if s["end_hour"] % 24 != 0 else 24
        spike_date  = start_day + timedelta(days=day_offset)
        types_str   = ", ".join(s["types"])
        vol_str     = f"{s['volume_mult']:.1f}×" if "volume" in s["types"] else "—"
        err_str     = f"{s['error_rate']*100:.0f}%" if "error_rate" in s["types"] else "—"
        lat_str     = f"{s['latency_mult']:.1f}×" if "latency" in s["types"] else "—"
        window_str  = f"{hour_start:02d}:00–{hour_end:02d}:00"
        print(f"  {i:<4} {str(spike_date):<12} {window_str:<22} {types_str:<30} "
              f"{vol_str:>6} {err_str:>6} {lat_str:>6}")

    print(f"  {'-'*88}")
    print(f"  {len(spikes)} spike(s) across {days} days  (timezone: {tz_name})")


def write_spike_manifest(spikes, days):
    """
    Write spikes to a JSON manifest so sub-scripts can read it.
    Returns the manifest path (or None if no spikes).
    """
    if not spikes:
        return None
    manifest_path = os.path.join(_HERE, "anomaly_spikes.json")
    with open(manifest_path, "w") as f:
        json.dump({
            "days":   days,
            "spikes": spikes,
        }, f, indent=2)
    return manifest_path


# ── Output helpers ────────────────────────────────────────────────────────────

def stream_output(proc, prefix, logfile):
    with open(logfile, "w") as lf:
        for line in proc.stdout:
            txt = line.rstrip()
            if txt:
                print(f"  [{prefix}] {txt}", flush=True)
                lf.write(txt + "\n")


def schedule_preview(days, sdg_weekday, apm_weekday, tz, spikes):
    today     = date.today()
    start_day = today - timedelta(days=days - 1)
    tz_name   = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))
    print(f"\n  {'Date':<12} {'Day':<4} {'Type':<9} {'SDG docs':>10} "
          f"{'APM traces':>11} {'Total':>10} {'Peak /hr':>10} {'Spikes':>8}")
    print(f"  {'-'*80}")
    grand = 0
    for d in range(days):
        day    = start_day + timedelta(days=d)
        factor = day_volume_factor(day) if _CAL else 1.0
        sdg    = round(sdg_weekday * factor)
        apm    = round(apm_weekday * factor)
        apm_d  = apm * 6
        total  = sdg + apm_d
        peak_hr = min(round(total * 0.13), MAX_HOURLY)
        grand  += total
        dtype  = ("HOLIDAY" if (_CAL and is_us_federal_holiday(day))
                  else "weekend" if day.weekday() >= 5
                  else "workday")
        # Count spikes on this day
        day_start_hr = d * 24
        day_end_hr   = day_start_hr + 24
        day_spikes   = [s for s in spikes
                        if s["start_hour"] < day_end_hr
                        and s["end_hour"] > day_start_hr]
        spike_str = f"⚡ ×{len(day_spikes)}" if day_spikes else ""
        print(f"  {str(day):<12} {day.strftime('%a'):<4} {dtype:<9} "
              f"{sdg:>10,} {apm:>11,} {total:>10,} {peak_hr:>10,} {spike_str:>8}")
    print(f"  {'-'*80}")
    print(f"  {'TOTAL':<47} {grand:>10,}")
    print(f"  Max events/hour at peak: {MAX_HOURLY:,} (cap — bypassed during spikes)")


# ── Main backfill runner ──────────────────────────────────────────────────────

def run_backfill(host, user, password, verify_ssl,
                 days, sdg_target, apm_traces, tz,
                 sdg_workers, apm_workers,
                 sdg_bulk, apm_bulk,
                 sdg_pb_threads, apm_pb_threads,
                 sdg_config, then_run,
                 max_hourly=MAX_HOURLY,
                 sdg_script_path=None,
                 bootstrap_script=None,
                 kibana_host=None,
                 job_files=None,
                 # ── spike params ──
                 spikes=None,
                 spike_cap_override=True):

    spikes = spikes or []
    run_script = os.path.join(_HERE, "run_workshop.py")

    # Resolve APM backfill script
    apm_script = os.path.join(_HERE, "backfill_apm-MLv2-WORKSHOP.py")
    if not os.path.exists(apm_script):
        apm_script = os.path.join(_HERE, "backfill_apm.py")
    if not os.path.exists(apm_script):
        print(f"ERROR: APM backfill script not found in {_HERE}")
        sys.exit(1)

    # Resolve SDG backfill script
    if sdg_script_path and os.path.exists(sdg_script_path):
        sdg_script = sdg_script_path
    else:
        sdg_script = os.path.join(_HERE, "backfill_sdg-MLv2-WORKSHOP.py")
        if not os.path.exists(sdg_script):
            sdg_script = os.path.join(_HERE, "backfill_sdg.py")
        if not os.path.exists(sdg_script):
            print(f"ERROR: SDG backfill script not found in {_HERE}")
            sys.exit(1)

    for s in (sdg_script, apm_script):
        if not os.path.exists(s):
            print(f"ERROR: {s} not found"); sys.exit(1)

    # Write spike manifest for sub-scripts to consume
    manifest_path = write_spike_manifest(spikes, days)

    tz_name = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))
    common  = ["--host", host, "--user", user, "--password", password] \
              + (["--no-verify-ssl"] if not verify_ssl else []) \
              + ["--timezone", tz_name]

    # Effective hourly cap: unlimited (sys.maxsize proxy) when spike_cap_override
    # is set and spikes are present; otherwise honour max_hourly.
    effective_cap = 999_999_999 if (spikes and spike_cap_override) else max_hourly

    sdg_cmd = [PYTHON, sdg_script] + common + [
        "--days",                  str(days),
        "--target-per-day",        str(sdg_target),
        "--workers",               str(sdg_workers),
        "--bulk-size",             str(sdg_bulk),
        "--parallel-bulk-threads", str(sdg_pb_threads),
        "--config",                sdg_config,
        "--max-hourly",            str(effective_cap),
    ]
    apm_cmd = [PYTHON, apm_script] + common + [
        "--days",                  str(days),
        "--traces-per-day",        str(apm_traces),
        "--workers",               str(apm_workers),
        "--bulk-size",             str(apm_bulk),
        "--parallel-bulk-threads", str(apm_pb_threads),
        "--max-hourly",            str(effective_cap),
    ]

    # Pass spike manifest path to sub-scripts if they support it
    if manifest_path:
        sdg_cmd += ["--spike-manifest", manifest_path]
        apm_cmd += ["--spike-manifest", manifest_path]

    print(f"\n{'='*68}")
    print(f"  LendPath ML Workshop v2 — {days}-Day Historical Backfill")
    print(f"{'='*68}")
    print(f"  Days:              {days}")
    print(f"  SDG weekday/day:   {sdg_target:,} docs")
    print(f"  APM weekday/day:   {apm_traces:,} traces (~{apm_traces*6:,} docs)")
    print(f"  Peak cap:          {max_hourly:,} events/hour"
          + (" (bypassed during spikes)" if spikes and spike_cap_override else ""))
    print(f"  Timezone:          {tz_name}")
    print(f"  Target:            {host}")

    if spikes:
        print(f"\n  Anomaly spikes:    {len(spikes)} injected")
        spike_type_labels = sorted({t for s in spikes for t in s["types"]})
        print(f"  Spike types:       {', '.join(spike_type_labels)}")
        if manifest_path:
            print(f"  Spike manifest:    {os.path.basename(manifest_path)}")

    if _CAL:
        schedule_preview(days, sdg_target, apm_traces, tz, spikes)

    if spikes:
        print_spike_schedule(spikes, days, tz)

    print(f"\n  Sub-scripts:")
    print(f"    SDG: {os.path.basename(sdg_script)}")
    print(f"    APM: {os.path.basename(apm_script)}")
    print(f"\n  Launching SDG and APM backfill in parallel…\n")

    procs = []; threads = []; start = time.time()

    for cmd, prefix, logname in [
        (sdg_cmd, "SDG", "backfill_sdg.log"),
        (apm_cmd, "APM", "backfill_apm.log"),
    ]:
        logpath = os.path.join(_HERE, logname)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, cwd=_HERE)
        procs.append(proc)
        t = threading.Thread(target=stream_output,
                             args=(proc, prefix, logpath), daemon=True)
        t.start(); threads.append(t)
        print(f"  ✓ {prefix} started  (PID {proc.pid})  →  {logname}")

    print()

    def shutdown(signum=None, frame=None):
        print("\n\nInterrupted — stopping…")
        for p in procs:
            try: p.terminate()
            except: pass
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for p in procs: p.wait()
    for t in threads: t.join(timeout=5)

    elapsed = time.time() - start
    h, rem  = divmod(int(elapsed), 3600)
    m, s    = divmod(rem, 60)

    print(f"\n{'='*68}")
    print(f"  Backfill complete.  Elapsed: {h:02d}h{m:02d}m{s:02d}s")
    print(f"  SDG exit: {procs[0].returncode}   APM exit: {procs[1].returncode}")

    if procs[0].returncode != 0 or procs[1].returncode != 0:
        print("  ⚠ One or more processes had errors.")
        print("    Review backfill_sdg.log and backfill_apm.log.")
    else:
        print(f"  ✓ {days} days of historical data indexed.")
        if spikes:
            print(f"  ✓ {len(spikes)} anomaly spike(s) injected into timeline.")
        print("  ✓ Starting live generators now (continuing from present)…")
    print(f"{'='*68}\n")

    # ── Post-backfill sequence ────────────────────────────────────────────────
    bs = bootstrap_script
    if bs is None:
        for _bname in ["bootstrap-MLv2-WORKSHOP.py", "bootstrap.py"]:
            _bp = os.path.join(_HERE, _bname)
            if os.path.exists(_bp):
                bs = _bp
                break

    tz_str = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))
    bs_common = ["--host", host, "--user", user, "--password", password] \
                + (["--no-verify-ssl"] if not verify_ssl else []) \
                + (["--kibana-host", kibana_host] if kibana_host else []) \
                + (["--skip-kibana"] if not kibana_host else []) \
                + ["--timezone", tz_str, "--skip-tz-picker"]

    if job_files:
        bs_common += ["--job-files"] + job_files

    if bs and os.path.exists(bs):
        print(f"\n{'='*68}")
        print(f"  Post-Backfill Automation")
        print(f"{'='*68}")
        print(f"  Bootstrap: {os.path.basename(bs)}")
        print()

        print("▸ Step 1/3 — Starting AD datafeeds…")
        try:
            result = subprocess.run(
                [PYTHON, bs] + bs_common + ["--skip-kibana"], cwd=_HERE)
            if result.returncode == 0:
                print("  ✓ AD datafeeds started")
            else:
                print(f"  ⚠ AD datafeed start returned exit code {result.returncode}")
        except Exception as e:
            print(f"  ⚠ Could not start AD datafeeds: {e}")

        print()
        print("▸ Step 2/3 — Creating and starting DFA jobs…")
        try:
            result = subprocess.run(
                [PYTHON, bs] + bs_common + ["--create-dfa", "--skip-kibana"],
                cwd=_HERE)
            if result.returncode == 0:
                print("  ✓ DFA jobs created and started")
            else:
                print(f"  ⚠ DFA job creation returned exit code {result.returncode}")
        except Exception as e:
            print(f"  ⚠ Could not create DFA jobs: {e}")

        print(f"\n{'='*68}\n")
    else:
        print(f"\n  ⚠ bootstrap script not found — skipping AD/DFA automation.")
        print(f"    Run manually:")
        print(f"      python bootstrap.py ...")
        print(f"      python bootstrap.py --create-dfa ...")
        print()

    if then_run:
        if not os.path.exists(run_script):
            print(f"WARNING: run_workshop.py not found.")
            return
        run_common = ["--host", host, "--user", user, "--password", password] \
                     + (["--no-verify-ssl"] if not verify_ssl else [])
        if sdg_script_path:
            run_common += ["--sdg-script", sdg_script_path]
        print("▸ Step 3/3 — Starting live generators (continuing from present)…")
        try:
            subprocess.run([PYTHON, run_script] + run_common, cwd=_HERE)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        run_common = f"--host {host} --user {user} --password {password}" \
                     + (" --no-verify-ssl" if not verify_ssl else "")
        print(f"  Next:  python run_workshop.py {run_common}\n")


# ── Config loader ─────────────────────────────────────────────────────────────

def _load_workshop_config():
    for search in [
        os.path.dirname(os.path.realpath(os.path.abspath(__file__))),
        os.getcwd(),
    ]:
        p = os.path.join(search, "workshop-config.json")
        if os.path.exists(p):
            try:
                import json as _json
                cfg = _json.load(open(p))
                print(f"  ✓ Loaded config: {p}")
                return cfg
            except Exception:
                pass
    return {}


# ── Argument parsing ──────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="LendPath ML Workshop v2 — 7-day backfill with anomaly spike injection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic backfill, then go live:
  python backfill_all-MLv2-WORKSHOP.py \\
      --host https://localhost:9200 \\
      --user elastic --password changeme --no-verify-ssl --then-run

  # 3 random anomaly spikes (volume + error-rate + latency):
  python backfill_all-MLv2-WORKSHOP.py ... \\
      --spike-count 3 --spike-volume-mult 8 --spike-duration-hrs 3 \\
      --spike-error-rate 0.4 --spike-latency-mult 6

  # Volume-only spikes, reproducible placement:
  python backfill_all-MLv2-WORKSHOP.py ... \\
      --spike-count 2 --spike-types volume --spike-seed 42

  # List all available timezones:
  python backfill_all-MLv2-WORKSHOP.py --list-timezones
        """
    )

    # ── Connection ─────────────────────────────────────────────────────────────
    p.add_argument("--host",          default="https://localhost:9200")
    p.add_argument("--user",          default="elastic")
    p.add_argument("--password",      default="changeme")
    p.add_argument("--no-verify-ssl", action="store_true")

    # ── Backfill config ────────────────────────────────────────────────────────
    p.add_argument("--days",          type=int, default=DEFAULT_DAYS,
                   help=f"Days of history to generate (default: {DEFAULT_DAYS})")
    p.add_argument("--sdg-target",    type=int, default=DEFAULT_SDG_TPD)
    p.add_argument("--apm-traces",    type=int, default=DEFAULT_APM_TRACES)
    p.add_argument("--sdg-workers",   type=int, default=6)
    p.add_argument("--apm-workers",   type=int, default=4)
    p.add_argument("--sdg-bulk",      type=int, default=1000)
    p.add_argument("--apm-bulk",      type=int, default=300)
    p.add_argument("--sdg-pb-threads",type=int, default=2)
    p.add_argument("--apm-pb-threads",type=int, default=2)
    p.add_argument("--sdg-config",    default="mortgage-workshop.yml")
    p.add_argument("--sdg-script",    default=None, metavar="PATH")
    p.add_argument("--max-hourly",    type=int, default=MAX_HOURLY,
                   help=f"Normal peak cap (default: {MAX_HOURLY:,}). "
                        "Bypassed during spikes when --spike-cap-override is set.")
    p.add_argument("--timezone",      default=None, metavar="TZ")
    p.add_argument("--list-timezones",action="store_true")
    p.add_argument("--bootstrap-script", default=None, metavar="PATH")
    p.add_argument("--kibana-host",   default=None, metavar="URL")
    p.add_argument("--job-files",     nargs="+", default=None, metavar="FILE")
    p.add_argument("--then-run",      action="store_true", default=True)
    p.add_argument("--no-then-run",   action="store_false", dest="then_run")

    # ── Anomaly spike flags ────────────────────────────────────────────────────
    spike = p.add_argument_group(
        "anomaly spikes",
        "Inject randomised anomaly windows into the historical timeline.\n"
        "Sub-scripts receive spike windows via anomaly_spikes.json manifest."
    )
    spike.add_argument("--spike-count", type=int, default=SPIKE_DEFAULT_COUNT,
                       metavar="N",
                       help="Number of anomaly spikes to inject (default: 0 = none)")
    spike.add_argument("--spike-volume-mult", type=float,
                       default=SPIKE_DEFAULT_VOLUME_MULT, metavar="X",
                       help=f"Volume multiplier at spike peak (default: {SPIKE_DEFAULT_VOLUME_MULT}×)")
    spike.add_argument("--spike-duration-hrs", type=int,
                       default=SPIKE_DEFAULT_DURATION_HRS, metavar="H",
                       help=f"Duration of each spike in hours (default: {SPIKE_DEFAULT_DURATION_HRS})")
    spike.add_argument("--spike-error-rate", type=float,
                       default=SPIKE_DEFAULT_ERROR_RATE, metavar="X",
                       help=f"APM error fraction during spike, 0–1 (default: {SPIKE_DEFAULT_ERROR_RATE})")
    spike.add_argument("--spike-latency-mult", type=float,
                       default=SPIKE_DEFAULT_LATENCY_MULT, metavar="X",
                       help=f"APM latency multiplier during spike (default: {SPIKE_DEFAULT_LATENCY_MULT}×)")
    spike.add_argument("--spike-types", nargs="+",
                       choices=SPIKE_ALL_TYPES, default=SPIKE_ALL_TYPES,
                       metavar="TYPE",
                       help=f"Spike types: {', '.join(SPIKE_ALL_TYPES)} (default: all)")
    spike.add_argument("--spike-seed", type=int, default=None, metavar="N",
                       help="Random seed for reproducible spike placement")
    spike.add_argument("--spike-cap-override", action="store_true", default=True,
                       help="Allow spikes to exceed --max-hourly cap (default: True)")
    spike.add_argument("--no-spike-cap-override", action="store_false",
                       dest="spike_cap_override",
                       help="Keep spikes within --max-hourly cap")

    # ── Load saved config, parse, back-fill ───────────────────────────────────
    _cfg = _load_workshop_config()
    if _cfg:
        _defaults = {
            "host": "https://localhost:9200", "user": "elastic",
            "password": "changeme", "kibana_host": None,
            "no_verify_ssl": False, "timezone": None, "job_files": None,
        }
        args = p.parse_args()
        if args.host     == _defaults["host"]     and _cfg.get("host"):
            args.host     = _cfg["host"]
        if args.user     == _defaults["user"]     and _cfg.get("user"):
            args.user     = _cfg["user"]
        if args.password == _defaults["password"] and _cfg.get("password"):
            args.password = _cfg["password"]
        if not args.no_verify_ssl and _cfg.get("no_verify_ssl"):
            args.no_verify_ssl = _cfg["no_verify_ssl"]
        if not getattr(args, "kibana_host", None) and _cfg.get("kibana_host"):
            args.kibana_host = _cfg["kibana_host"]
        if not getattr(args, "timezone", None) and _cfg.get("timezone"):
            args.timezone = _cfg["timezone"]
        if not getattr(args, "job_files", None) and _cfg.get("job_files"):
            args.job_files = _cfg["job_files"]
    else:
        args = p.parse_args()
        print("  ℹ  No workshop-config.json found — using CLI args only.")
        print("     Run bootstrap.py first to save connection settings.")
        print()

    if args.list_timezones:
        list_timezones()
        sys.exit(0)

    tz = resolve_tz(args.timezone)
    tz_name = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))
    print(f"\n  Detected timezone: {tz_name}")

    # ── Generate spikes ───────────────────────────────────────────────────────
    spikes = generate_spikes(
        days=args.days,
        spike_count=args.spike_count,
        spike_duration_hrs=args.spike_duration_hrs,
        spike_volume_mult=args.spike_volume_mult,
        spike_error_rate=args.spike_error_rate,
        spike_latency_mult=args.spike_latency_mult,
        spike_types=args.spike_types,
        spike_seed=args.spike_seed,
    )

    run_backfill(
        host=args.host, user=args.user, password=args.password,
        verify_ssl=not args.no_verify_ssl,
        days=args.days, sdg_target=args.sdg_target, apm_traces=args.apm_traces,
        tz=tz, max_hourly=args.max_hourly,
        sdg_workers=args.sdg_workers, apm_workers=args.apm_workers,
        sdg_bulk=args.sdg_bulk, apm_bulk=args.apm_bulk,
        sdg_pb_threads=args.sdg_pb_threads, apm_pb_threads=args.apm_pb_threads,
        sdg_config=args.sdg_config, then_run=args.then_run,
        sdg_script_path=args.sdg_script,
        bootstrap_script=args.bootstrap_script,
        kibana_host=args.kibana_host,
        job_files=args.job_files,
        spikes=spikes,
        spike_cap_override=args.spike_cap_override,
    )


if __name__ == "__main__":
    main()
