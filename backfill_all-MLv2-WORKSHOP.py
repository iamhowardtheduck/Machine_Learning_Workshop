#!/usr/bin/env python3
"""
backfill_all-MLv2-WORKSHOP.py — 7-day historical backfill, then immediate
                                  handoff to real-time generators.

Key differences from the original:
  • 7 days only (not 30)
  • Peak volume capped at 10,000 events/hour
  • Timestamps generated in the user's local timezone (auto-detected,
    overridable with --timezone)
  • After backfill, run_workshop.py starts automatically from where
    the backfill left off (--then-run is the expected default)
  • ML job definitions default to the MLv2 files (shorter bucket spans)

Usage:
    python backfill_all-MLv2-WORKSHOP.py \\
        --host https://localhost:9200 \\
        --user elastic --password changeme --no-verify-ssl \\
        --then-run

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

# Maximum events per hour at peak (10,000 cap)
MAX_HOURLY = 10_000
# 7-day default
DEFAULT_DAYS = 7
# Daily budget split: 70% SDG, 30% APM
# With 10k/hr peak and ~8 business hours, weekday max = ~80,000/day
# 70% = 56,000 SDG, 30% APM traces = ~4,000 (× ~6 docs = ~24,000 APM docs)
DEFAULT_SDG_TPD    = 56_000
DEFAULT_APM_TRACES = 4_000


def get_local_tz():
    """Detect the local system timezone, return a tzinfo object."""
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
    """Resolve a timezone name string to a tzinfo object."""
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
    # Fallback — try UTC offset like "UTC+5" or "Etc/GMT-5"
    print(f"  ⚠ Could not load timezone {tz_name!r} — zoneinfo/pytz not installed.")
    print(f"    Falling back to local system timezone.")
    return get_local_tz()


def list_timezones():
    """Print all available timezone names."""
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


def stream_output(proc, prefix, logfile):
    with open(logfile, "w") as lf:
        for line in proc.stdout:
            txt = line.rstrip()
            if txt:
                print(f"  [{prefix}] {txt}", flush=True)
                lf.write(txt + "\n")


def schedule_preview(days, sdg_weekday, apm_weekday, tz):
    today     = date.today()
    start_day = today - timedelta(days=days - 1)
    tz_name   = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))
    print(f"\n  {'Date':<12} {'Day':<4} {'Type':<9} {'SDG docs':>10} "
          f"{'APM traces':>11} {'Total':>10} {'Peak /hr':>10}")
    print(f"  {'-'*70}")
    grand = 0
    for d in range(days):
        day    = start_day + timedelta(days=d)
        factor = day_volume_factor(day) if _CAL else 1.0
        sdg    = round(sdg_weekday * factor)
        apm    = round(apm_weekday * factor)
        apm_d  = apm * 6
        total  = sdg + apm_d
        # Approx peak hour = 13% of daily (based on 1PM weight)
        peak_hr = min(round(total * 0.13), MAX_HOURLY)
        grand  += total
        dtype  = ("HOLIDAY" if (_CAL and is_us_federal_holiday(day))
                  else "weekend" if day.weekday() >= 5
                  else "workday")
        print(f"  {str(day):<12} {day.strftime('%a'):<4} {dtype:<9} "
              f"{sdg:>10,} {apm:>11,} {total:>10,} {peak_hr:>10,}")
    print(f"  {'-'*70}")
    print(f"  {'TOTAL':<47} {grand:>10,}")
    print(f"  Max events/hour at peak: {MAX_HOURLY:,} (hard cap)")


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
                 job_files=None):

    run_script = os.path.join(_HERE, "run_workshop.py")

    # Resolve APM backfill script — v2 preferred, original as fallback
    apm_script = os.path.join(_HERE, "backfill_apm-MLv2-WORKSHOP.py")
    if not os.path.exists(apm_script):
        apm_script = os.path.join(_HERE, "backfill_apm.py")
    if not os.path.exists(apm_script):
        print(f"ERROR: APM backfill script not found in {_HERE}")
        print(f"  Tried: backfill_apm-MLv2-WORKSHOP.py, backfill_apm.py")
        sys.exit(1)

    # Resolve SDG backfill script
    # sdg_script_path may be an explicit override from --sdg-script
    if sdg_script_path and os.path.exists(sdg_script_path):
        sdg_script = sdg_script_path
    else:
        sdg_script = os.path.join(_HERE, "backfill_sdg-MLv2-WORKSHOP.py")
        if not os.path.exists(sdg_script):
            sdg_script = os.path.join(_HERE, "backfill_sdg.py")
        if not os.path.exists(sdg_script):
            print(f"ERROR: SDG backfill script not found in {_HERE}")
            print(f"  Tried: backfill_sdg-MLv2-WORKSHOP.py, backfill_sdg.py")
            sys.exit(1)

    for s in (sdg_script, apm_script):
        if not os.path.exists(s):
            print(f"ERROR: {s} not found"); sys.exit(1)

    tz_name = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))
    common  = ["--host", host, "--user", user, "--password", password] \
              + (["--no-verify-ssl"] if not verify_ssl else []) \
              + ["--timezone", tz_name]

    sdg_cmd = [PYTHON, sdg_script] + common + [
        "--days",                  str(days),
        "--target-per-day",        str(sdg_target),
        "--workers",               str(sdg_workers),
        "--bulk-size",             str(sdg_bulk),
        "--parallel-bulk-threads", str(sdg_pb_threads),
        "--config",                sdg_config,
        "--max-hourly",            str(max_hourly),
    ]
    apm_cmd = [PYTHON, apm_script] + common + [
        "--days",                  str(days),
        "--traces-per-day",        str(apm_traces),
        "--workers",               str(apm_workers),
        "--bulk-size",             str(apm_bulk),
        "--parallel-bulk-threads", str(apm_pb_threads),
        "--max-hourly",            str(max_hourly),
    ]

    print(f"\n{'='*68}")
    print(f"  LendPath ML Workshop v2 — {days}-Day Historical Backfill")
    print(f"{'='*68}")
    print(f"  Days:              {days}")
    print(f"  SDG weekday/day:   {sdg_target:,} docs")
    print(f"  APM weekday/day:   {apm_traces:,} traces (~{apm_traces*6:,} docs)")
    print(f"  Peak cap:          {max_hourly:,} events/hour")
    print(f"  Timezone:          {tz_name}")
    print(f"  Target:            {host}")

    if _CAL:
        schedule_preview(days, sdg_target, apm_traces, tz)

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
    for t in threads:  t.join(timeout=5)

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
        print("  ✓ Starting live generators now (continuing from present)…")
    print(f"{'='*68}\n")

    # ── Post-backfill sequence ────────────────────────────────────────────────
    # Resolve bootstrap script
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

    # ── Step A: Start AD datafeeds ───────────────────────────────────────────
    if bs and os.path.exists(bs):
        print(f"\n{'='*68}")
        print(f"  Post-Backfill Automation")
        print(f"{'='*68}")
        print(f"  Bootstrap: {os.path.basename(bs)}")
        print()

        print("▸ Step 1/3 — Starting AD datafeeds…")
        try:
            result = subprocess.run(
                [PYTHON, bs] + bs_common + ["--skip-kibana"],
                cwd=_HERE
            )
            if result.returncode == 0:
                print("  ✓ AD datafeeds started")
            else:
                print(f"  ⚠ AD datafeed start returned exit code {result.returncode}")
        except Exception as e:
            print(f"  ⚠ Could not start AD datafeeds: {e}")

        # ── Step B: Create and start DFA jobs ─────────────────────────────────
        print()
        print("▸ Step 2/3 — Creating and starting DFA jobs…")
        try:
            result = subprocess.run(
                [PYTHON, bs] + bs_common + ["--create-dfa", "--skip-kibana"],
                cwd=_HERE
            )
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
        print(f"      python bootstrap.py ...  (then start datafeeds manually in Kibana)")
        print(f"      python bootstrap.py --create-dfa ...")
        print()

    # ── Step C: Start live generators ────────────────────────────────────────
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



def _load_workshop_config():
    """Load connection config saved by bootstrap.py."""
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


def main():
    p = argparse.ArgumentParser(
        description="LendPath ML Workshop v2 — 7-day backfill with 10k/hr cap",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic run — backfill 7 days then go live:
  python backfill_all-MLv2-WORKSHOP.py \\
      --host https://localhost:9200 \\
      --user elastic --password changeme --no-verify-ssl \\
      --then-run

  # Override timezone:
  python backfill_all-MLv2-WORKSHOP.py ... --timezone "America/Chicago"

  # List all available timezones:
  python backfill_all-MLv2-WORKSHOP.py --list-timezones
        """
    )
    p.add_argument("--host",          default="https://localhost:9200")
    p.add_argument("--user",          default="elastic")
    p.add_argument("--password",      default="changeme")
    p.add_argument("--no-verify-ssl", action="store_true")
    p.add_argument("--days",          type=int,  default=DEFAULT_DAYS,
                   help=f"Days of history to generate (default: {DEFAULT_DAYS})")
    p.add_argument("--sdg-target",    type=int,  default=DEFAULT_SDG_TPD,
                   help=f"Weekday SDG docs/day (default: {DEFAULT_SDG_TPD:,})")
    p.add_argument("--apm-traces",    type=int,  default=DEFAULT_APM_TRACES,
                   help=f"Weekday APM traces/day (default: {DEFAULT_APM_TRACES:,})")
    p.add_argument("--sdg-workers",   type=int,  default=6)
    p.add_argument("--apm-workers",   type=int,  default=4)
    p.add_argument("--sdg-bulk",      type=int,  default=1000)
    p.add_argument("--apm-bulk",      type=int,  default=300)
    p.add_argument("--sdg-pb-threads",type=int,  default=2)
    p.add_argument("--apm-pb-threads",type=int,  default=2)
    p.add_argument("--sdg-config",    default="mortgage-workshop.yml")
    p.add_argument("--sdg-script",    default=None, metavar="PATH",
                   help="Path to the SDG entry-point script (e.g. sdg-prime.py). "
                        "Passed to run_workshop.py after backfill completes. "
                        "Auto-detected from workshop directory if not set.")
    p.add_argument("--max-hourly",    type=int, default=MAX_HOURLY,
                   help=f"Max events/hour at peak (default: {MAX_HOURLY:,}). "
                        "Passed through to sub-scripts as a constant.")
    p.add_argument("--timezone",      default=None, metavar="TZ",
                   help="Timezone for timestamp generation, e.g. 'America/New_York'. "
                        "Defaults to system local time. Use --list-timezones to see all options.")
    p.add_argument("--list-timezones", action="store_true",
                   help="Print all available timezone names and exit")
    p.add_argument("--bootstrap-script", default=None, metavar="PATH",
                   help="Path to bootstrap.py to run after backfill. "
                        "Auto-detected if not set.")
    p.add_argument("--kibana-host",    default=None, metavar="URL",
                   help="Kibana URL for bootstrap post-backfill steps "
                        "(e.g. http://localhost:5601). Skips Kibana if not set.")
    p.add_argument("--job-files",      nargs="+", default=None, metavar="FILE",
                   help="ML job definition files passed to bootstrap. "
                        "Defaults to bootstrap's own defaults.")
    p.add_argument("--then-run",       action="store_true", default=True,
                   help="Start live generators immediately after backfill (default: True)")
    p.add_argument("--no-then-run",   action="store_false", dest="then_run",
                   help="Do not start live generators after backfill")

    # Load previously saved bootstrap config and use as defaults for
    # any arg the user didn't explicitly pass on the command line.
    _cfg = _load_workshop_config()
    if _cfg:
        # argparse doesn't support post-parse defaults elegantly, so we
        # patch the namespace directly for args left at their default values.
        _defaults = {
            "host":           "https://localhost:9200",
            "user":           "elastic",
            "password":       "changeme",
            "kibana_host":    None,
            "no_verify_ssl":  False,
            "timezone":       None,
            "job_files":      None,
        }
        # Parse first so explicit CLI args take priority
        args = p.parse_args()
        # Then back-fill from config only where the user left the default
        if args.host          == _defaults["host"]         and _cfg.get("host"):
            args.host          = _cfg["host"]
        if args.user          == _defaults["user"]         and _cfg.get("user"):
            args.user          = _cfg["user"]
        if args.password      == _defaults["password"]     and _cfg.get("password"):
            args.password      = _cfg["password"]
        if not args.no_verify_ssl and _cfg.get("no_verify_ssl"):
            args.no_verify_ssl = _cfg["no_verify_ssl"]
        if not getattr(args, "kibana_host", None) and _cfg.get("kibana_host"):
            args.kibana_host   = _cfg["kibana_host"]
        if not getattr(args, "timezone", None) and _cfg.get("timezone"):
            args.timezone      = _cfg["timezone"]
        if not getattr(args, "job_files", None) and _cfg.get("job_files"):
            args.job_files     = _cfg["job_files"]
    else:
        args = p.parse_args()
        if not _cfg:
            print("  ℹ  No workshop-config.json found — using CLI args only.")
            print("     Run bootstrap.py first to save connection settings.")
            print()


    if args.list_timezones:
        list_timezones()
        sys.exit(0)

    tz = resolve_tz(args.timezone)
    tz_name = getattr(tz, 'key', getattr(tz, 'zone', str(tz)))
    print(f"\n  Detected timezone: {tz_name}")

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
    )


if __name__ == "__main__":
    main()
