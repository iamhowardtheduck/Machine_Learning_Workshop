#!/usr/bin/env python3
"""
run_workshop.py — Launches both the SDG and APM trace generator together.

Runs sdg-prime.py and apm_trace_generator.py as child processes with a
shared stop signal (Ctrl+C). Both processes write to the same Elasticsearch
cluster. Status is printed on a configurable interval.

Usage:
    python run_workshop.py \\
        --host https://localhost:9200 \\
        --user elastic --password changeme \\
        --no-verify-ssl \\
        [--sdg-config mortgage-workshop.yml] \\
        [--apm-rate 2]            # traces/sec (default: 2) \\
        [--purge-apm]             # wipe stale APM traces before starting \\
        [--status-interval 30]    # status print interval in seconds (default: 10) \\
        [--no-restart]            # don't auto-restart crashed processes \\
        [--sdg-only]              # run SDG only, skip APM generator \\
        [--apm-only]              # run APM generator only, skip SDG \\
        [--log-dir /tmp/logs]     # directory for sdg.log and apm.log \\
        [--anomaly-chance 0.30]   # geo anomaly injection rate (default: 0.03 = 3%)
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
import time
import signal
import yaml


def tail_log(path, n=5):
    """Return the last n non-empty lines of a log file."""
    try:
        with open(path) as f:
            lines = [l.rstrip() for l in f.readlines() if l.strip()]
        return lines[-n:] if lines else []
    except Exception:
        return []


def _patch_sdg_config(sdg_config, host, user, password, verify_ssl, script_dir):
    """
    Write a temporary copy of the SDG YAML with connection settings overridden
    by the values passed on the command line. Returns the path to the temp file.
    The caller is responsible for deleting it.
    """
    config_path = (os.path.join(script_dir, sdg_config)
                   if not os.path.isabs(sdg_config) else sdg_config)

    with open(config_path) as fh:
        cfg = yaml.safe_load(fh)

    m      = re.match(r"(?:(https?)://)?([^:/]+)(?::(\d+))?", host)
    scheme = m.group(1) or "https"
    hostname = m.group(2)
    port   = int(m.group(3)) if m.group(3) else 9200

    cfg["elasticsearchScheme"]   = scheme
    cfg["elasticsearchHost"]     = hostname
    cfg["elasticsearchPort"]     = port
    cfg["elasticsearchUser"]     = user
    cfg["elasticsearchPassword"] = password
    cfg["verifyCerts"]           = verify_ssl

    fd, tmp_path = tempfile.mkstemp(
        suffix=".yml", prefix=".sdg_patched_", dir=script_dir
    )
    with os.fdopen(fd, "w") as fh:
        yaml.dump(cfg, fh, default_flow_style=False, allow_unicode=True)
    return tmp_path


def run(host, user, password, verify_ssl,
        sdg_config, apm_rate, purge_apm,
        status_interval, no_restart,
        sdg_only, apm_only,
        log_dir, anomaly_chance):

    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir    = log_dir or script_dir
    os.makedirs(log_dir, exist_ok=True)

    sdg_script   = os.path.join(script_dir, "sdg-prime.py")
    apm_script   = os.path.join(script_dir, "apm_trace_generator.py")
    sdg_log_path = os.path.join(log_dir, "sdg.log")
    apm_log_path = os.path.join(log_dir, "apm.log")
    python       = sys.executable

    # Validate requested scripts exist
    if not apm_only and not os.path.exists(sdg_script):
        print(f"ERROR: {sdg_script} not found"); sys.exit(1)
    if not sdg_only and not os.path.exists(apm_script):
        print(f"ERROR: {apm_script} not found"); sys.exit(1)

    # ── Patch SDG YAML ────────────────────────────────────────────────────────
    tmp_config = None
    if not apm_only:
        tmp_config = _patch_sdg_config(
            sdg_config, host, user, password, verify_ssl, script_dir
        )

    # ── Build command lists ───────────────────────────────────────────────────
    sdg_cmd = [python, sdg_script, tmp_config] if not apm_only else None

    apm_cmd = None
    if not sdg_only:
        apm_cmd = [
            python, apm_script,
            "--host",     host,
            "--user",     user,
            "--password", password,
            "--rate",     str(apm_rate),
        ]
        if not verify_ssl:
            apm_cmd.append("--no-verify-ssl")
        if purge_apm:
            apm_cmd.append("--purge")
        if anomaly_chance is not None:
            apm_cmd += ["--anomaly-chance", str(anomaly_chance)]

    # ── Banner ────────────────────────────────────────────────────────────────
    mode = ("SDG only" if sdg_only else
            "APM only" if apm_only else
            "SDG + APM")

    print("\n" + "=" * 62)
    print("  LendPath ML Workshop — Data Generators")
    print("=" * 62)
    print(f"\n  Mode:            {mode}")
    if not apm_only:
        print(f"  SDG config:      {sdg_config}  (patched with CLI credentials)")
    if not sdg_only:
        print(f"  APM rate:        {apm_rate} trace(s)/sec")
        print(f"  Anomaly chance:  {anomaly_chance*100:.0f}% geo anomaly injection"
              if anomaly_chance is not None else
              f"  Anomaly chance:  3% (default — use --anomaly-chance to adjust)")
        print(f"  Purge APM:       {'yes — stale traces deleted first' if purge_apm else 'no'}")
    print(f"  Auto-restart:    {'disabled (--no-restart)' if no_restart else 'enabled'}")
    print(f"  Status interval: every {status_interval}s")
    print(f"  Target:          {host}")
    print(f"\n  Logs:")
    if not apm_only:  print(f"    SDG  →  {sdg_log_path}")
    if not sdg_only:  print(f"    APM  →  {apm_log_path}")
    print("\n  Press Ctrl+C to stop.\n")

    # ── Launch ────────────────────────────────────────────────────────────────
    sdg_log  = open(sdg_log_path,  "w") if not apm_only else None
    apm_log  = open(apm_log_path,  "w") if not sdg_only else None

    sdg_proc = subprocess.Popen(
        sdg_cmd, stdout=sdg_log, stderr=subprocess.STDOUT, cwd=script_dir
    ) if not apm_only else None

    apm_proc = subprocess.Popen(
        apm_cmd, stdout=apm_log, stderr=subprocess.STDOUT, cwd=script_dir
    ) if not sdg_only else None

    if sdg_proc: print(f"  ✓ SDG started          (PID {sdg_proc.pid})")
    if apm_proc: print(f"  ✓ APM trace generator  (PID {apm_proc.pid})")
    print()

    # ── Shutdown handler ──────────────────────────────────────────────────────
    def shutdown(signum=None, frame=None):
        print("\n\nShutting down…")
        for proc in (sdg_proc, apm_proc):
            if proc:
                try: proc.terminate()
                except Exception: pass
        for proc in (sdg_proc, apm_proc):
            if proc:
                try: proc.wait(timeout=8)
                except subprocess.TimeoutExpired: proc.kill()
        for f in (sdg_log, apm_log):
            if f:
                try: f.close()
                except Exception: pass
        if tmp_config:
            try: os.unlink(tmp_config)
            except Exception: pass
        print("Both generators stopped.")
        print(f"\nLogs saved to: {log_dir}\n")
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── Status loop ───────────────────────────────────────────────────────────
    tick      = 0
    elapsed   = 0

    while True:
        time.sleep(status_interval)
        elapsed += status_interval
        tick    += 1

        sdg_alive = sdg_proc.poll() is None if sdg_proc else None
        apm_alive = apm_proc.poll() is None if apm_proc else None

        # Build status line
        parts = [f"[{elapsed:>5}s]"]
        if sdg_alive is not None:
            parts.append(f"SDG: {'running' if sdg_alive else 'STOPPED ⚠'}")
        if apm_alive is not None:
            parts.append(f"APM: {'running' if apm_alive else 'STOPPED ⚠'}")
        print("  " + "  |  ".join(parts))

        # Show latest APM progress line
        if not sdg_only:
            for line in tail_log(apm_log_path, 2):
                if any(k in line.lower() for k in ("traces", "error", "connected")):
                    print(f"          APM → {line}")

        # Show latest SDG error if any
        if not apm_only:
            for line in tail_log(sdg_log_path, 3):
                if "error" in line.lower() or "exception" in line.lower():
                    print(f"          SDG → {line}")

        # ── Auto-restart (unless --no-restart) ────────────────────────────────
        if no_restart:
            # Exit if either process dies
            if sdg_proc and not sdg_alive:
                print("\n  SDG exited and --no-restart is set. Stopping.")
                shutdown()
            if apm_proc and not apm_alive:
                print("\n  APM trace generator exited and --no-restart is set. Stopping.")
                shutdown()
        else:
            if sdg_proc and not sdg_alive:
                print("  ⚠ SDG exited — restarting…")
                sdg_log.close()
                sdg_log  = open(sdg_log_path, "a")
                sdg_proc = subprocess.Popen(
                    sdg_cmd, stdout=sdg_log,
                    stderr=subprocess.STDOUT, cwd=script_dir
                )
                print(f"  ✓ SDG restarted (PID {sdg_proc.pid})")

            if apm_proc and not apm_alive:
                print("  ⚠ APM trace generator exited — restarting…")
                apm_log.close()
                apm_log  = open(apm_log_path, "a")
                apm_proc = subprocess.Popen(
                    apm_cmd, stdout=apm_log,
                    stderr=subprocess.STDOUT, cwd=script_dir
                )
                print(f"  ✓ APM trace generator restarted (PID {apm_proc.pid})")


def main():
    p = argparse.ArgumentParser(
        description="Run SDG and APM trace generator for the LendPath ML Workshop",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard run (both generators):
  python run_workshop.py --host https://localhost:9200 \\
      --user elastic --password changeme --no-verify-ssl

  # First run — purge stale APM traces for a clean service map:
  python run_workshop.py ... --purge-apm

  # Demo mode — inject geo anomalies at 30% for visible ML signals:
  python run_workshop.py ... --anomaly-chance 0.30

  # APM-only run (backfill already covered SDG streams):
  python run_workshop.py ... --apm-only

  # SDG-only run (APM backfill is sufficient for now):
  python run_workshop.py ... --sdg-only

  # Quieter status output — print every 60s instead of 10s:
  python run_workshop.py ... --status-interval 60

  # Demo/debug — crash is a crash, don't hide it with auto-restart:
  python run_workshop.py ... --no-restart

  # Custom log location:
  python run_workshop.py ... --log-dir /var/log/lendpath
        """
    )

    # ── Connection ────────────────────────────────────────────────────────────
    p.add_argument("--host",           default="https://localhost:9200")
    p.add_argument("--user",           default="elastic")
    p.add_argument("--password",       default="changeme")
    p.add_argument("--no-verify-ssl",  action="store_true")

    # ── Generator config ──────────────────────────────────────────────────────
    p.add_argument("--sdg-config",     default="mortgage-workshop.yml",
                   help="SDG YAML config file (default: mortgage-workshop.yml)")
    p.add_argument("--apm-rate",       type=float, default=2.0,
                   help="APM traces per second (default: 2)")
    p.add_argument("--purge-apm",      action="store_true",
                   help="Delete traces-apm-default before starting — clears stale "
                        "unlinked traces that break the APM Service Map. "
                        "Use on first run if services appear disconnected.")
    p.add_argument("--anomaly-chance", type=float, default=None,
                   metavar="RATE",
                   help="Geo anomaly injection rate for APM traces, 0.0–1.0 "
                        "(default: 0.03 = 3%%). Set to 0.30 during ML demos "
                        "to produce strong geo anomaly signals quickly.")

    # ── Run mode ──────────────────────────────────────────────────────────────
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--sdg-only",    action="store_true",
                      help="Run the SDG only — skip the APM trace generator. "
                           "Useful when APM data is already backfilled.")
    mode.add_argument("--apm-only",    action="store_true",
                      help="Run the APM trace generator only — skip the SDG. "
                           "Useful for demos focused on APM / Service Map.")

    # ── Behaviour ─────────────────────────────────────────────────────────────
    p.add_argument("--status-interval", type=int, default=10,
                   metavar="SECONDS",
                   help="How often to print the status line in seconds "
                        "(default: 10). Use 60 for quieter output during demos.")
    p.add_argument("--no-restart",     action="store_true",
                   help="Do not auto-restart crashed generators. The script will "
                        "exit if either process dies. Recommended during demos "
                        "so failures are visible rather than silently hidden.")
    p.add_argument("--log-dir",        default=None,
                   metavar="DIR",
                   help="Directory for sdg.log and apm.log "
                        "(default: same directory as run_workshop.py)")

    args = p.parse_args()

    run(
        host             = args.host,
        user             = args.user,
        password         = args.password,
        verify_ssl       = not args.no_verify_ssl,
        sdg_config       = args.sdg_config,
        apm_rate         = args.apm_rate,
        purge_apm        = args.purge_apm,
        status_interval  = args.status_interval,
        no_restart       = args.no_restart,
        sdg_only         = args.sdg_only,
        apm_only         = args.apm_only,
        log_dir          = args.log_dir,
        anomaly_chance   = args.anomaly_chance,
    )


if __name__ == "__main__":
    main()
