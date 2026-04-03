#!/usr/bin/env python3
"""
download_iodb.py
─────────────────────────────────────────────────────────────────────────────
Single-script downloader for all IO databases required by:
  · fastinfra_mrio_benchmark.py   (pymrio)
  · run_all_db.py                 (pymrio)
  · mario_analysis/fastinfra_mario.py   (mario via pymrio bridge)
  · iopy_analysis/fastinfra_iopy.py     (iopy — auto-download, fixed cache)

All pymrio/mario databases are stored in:  ./input_iodb/<db_name>/
iopy stores to its own internal cache; this script creates a symlink at:
  ./input_iodb/iopy_cache → <iopy_package>/.temp_data

After downloading, update the DB_PATHS dict in each script:
  DB_PATHS = {
      "exiobase": "./input_iodb/exiobase",
      "wiod":     "./input_iodb/wiod",
      "oecd":     "./input_iodb/oecd",
      "figaro":   "./input_iodb/figaro",   # mario only
      "eora26":   "./input_iodb/eora26",   # if credentials provided below
  }

Databases
─────────────────────────────────────────────────────────────────────────────
  DB              Library     Source              Size (approx)
  ─────────────── ─────────── ─────────────────── ──────────────
  EXIOBASE 3.8    pymrio      Zenodo              ~900 MB (pxp ixi)
  WIOD 2013       pymrio      wiod.org            ~350 MB (all years)
  OECD ICIO v2021 pymrio      OECD Zenodo         ~200 MB (2018 only)
  FIGARO 2018     mario       Eurostat            ~250 MB
  Eora26          pymrio      worldmrio.com       ~2 GB   (needs account)
  iopy OECD 2021  iopy        OECD (auto)         ~95 MB  (cached already)
  iopy ExioBase   iopy        Zenodo (auto)       ~729 MB (cached already)

Usage
─────
  python3 download_iodb.py                      # download all (skip Eora26)
  python3 download_iodb.py --skip-large         # skip ExioBase + Eora26
  python3 download_iodb.py --only exiobase wiod # download specific DBs only
  python3 download_iodb.py --eora-email you@x.com --eora-password pw  # +Eora26
"""

import argparse
import os
import sys
import shutil
import time
from pathlib import Path

# ── Parse CLI arguments ───────────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description="Download all IO databases for FAST-Infra analysis scripts."
)
parser.add_argument(
    "--skip-large", action="store_true",
    help="Skip downloads >500 MB (EXIOBASE, Eora26)"
)
parser.add_argument(
    "--only", nargs="+", metavar="DB",
    choices=["exiobase", "wiod", "oecd", "figaro", "eora26", "iopy"],
    help="Download only the specified databases"
)
parser.add_argument(
    "--eora-email", default=None,
    help="Eora26 account email (worldmrio.com)"
)
parser.add_argument(
    "--eora-password", default=None,
    help="Eora26 account password"
)
parser.add_argument(
    "--exiobase-year", type=int, default=2018,
    help="EXIOBASE year to download (default: 2018)"
)
parser.add_argument(
    "--oecd-year", type=int, default=2018,
    help="OECD ICIO year to download (default: 2018)"
)
parser.add_argument(
    "--wiod-year", type=int, default=2014,
    help="WIOD year to download (default: 2014)"
)
parser.add_argument(
    "--figaro-year", type=int, default=2018,
    help="FIGARO year to download (default: 2018)"
)
args = parser.parse_args()

# ── Output folder ─────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent.resolve()
IODB_DIR  = BASE_DIR / "input_iodb"
IODB_DIR.mkdir(exist_ok=True)

# Subfolder paths
PATHS = {
    "exiobase": IODB_DIR / "exiobase",
    "wiod":     IODB_DIR / "wiod",
    "oecd":     IODB_DIR / "oecd",
    "figaro":   IODB_DIR / "figaro",
    "eora26":   IODB_DIR / "eora26",
    "iopy":     IODB_DIR / "iopy_cache",
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def _header(title):
    print(f"\n{'═'*64}")
    print(f"  {title}")
    print(f"{'═'*64}")

def _ok(msg):    print(f"  [OK]     {msg}")
def _skip(msg):  print(f"  [SKIP]   {msg}")
def _warn(msg):  print(f"  [WARN]   {msg}")
def _error(msg): print(f"  [ERROR]  {msg}")
def _info(msg):  print(f"  [INFO]   {msg}")

def _should_run(db_name):
    if args.only:
        return db_name in args.only
    return True

def _timer(t0):
    elapsed = time.time() - t0
    return f"{elapsed:.0f}s" if elapsed < 60 else f"{elapsed/60:.1f}min"

# ─────────────────────────────────────────────────────────────────────────────
# 1. EXIOBASE 3  (pymrio + mario)
# ─────────────────────────────────────────────────────────────────────────────
def download_exiobase():
    _header("EXIOBASE 3  (pymrio / mario)")

    if args.skip_large:
        _skip("EXIOBASE skipped — use --skip-large=False or omit --skip-large to download (~900 MB)")
        return False

    if not _should_run("exiobase"):
        _skip("Not in --only list")
        return False

    try:
        import pymrio
    except ImportError:
        _error("pymrio not installed — run: pip install pymrio")
        return False

    folder = PATHS["exiobase"]
    folder.mkdir(exist_ok=True)

    # Check if already downloaded
    existing = list(folder.glob("*.zip")) + list(folder.glob("*.csv"))
    if existing:
        _skip(f"Already downloaded ({len(existing)} files in {folder})")
        _info("Delete the folder to force re-download.")
        return True

    _info(f"Downloading EXIOBASE 3  year={args.exiobase_year}  system=pxp → {folder}")
    _info("This may take 10–20 minutes depending on connection speed.")
    t0 = time.time()

    try:
        pymrio.download_exiobase3(
            storage_folder=str(folder),
            years=[args.exiobase_year],
            system="pxp",
            overwrite_existing=False,
        )
        _ok(f"EXIOBASE 3 downloaded in {_timer(t0)}")
        return True
    except Exception as exc:
        _error(f"EXIOBASE download failed: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 2. WIOD 2013  (pymrio + mario)
# ─────────────────────────────────────────────────────────────────────────────
def download_wiod():
    _header("WIOD 2013 release  (pymrio / mario)")

    if not _should_run("wiod"):
        _skip("Not in --only list")
        return False

    try:
        import pymrio
    except ImportError:
        _error("pymrio not installed")
        return False

    folder = PATHS["wiod"]
    folder.mkdir(exist_ok=True)

    existing = list(folder.glob("*.zip")) + list(folder.glob("*.xlsx"))
    if existing:
        _skip(f"Already downloaded ({len(existing)} files in {folder})")
        return True

    _info(f"Downloading WIOD 2013  year={args.wiod_year} → {folder}")
    _info("Note: pymrio only supports the 2013 release (2016 lacks extensions).")
    t0 = time.time()

    try:
        pymrio.download_wiod2013(
            storage_folder=str(folder),
            years=[args.wiod_year],
            overwrite_existing=False,
        )
        _ok(f"WIOD 2013 downloaded in {_timer(t0)}")
        return True
    except Exception as exc:
        _error(f"WIOD download failed: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 3. OECD ICIO v2021  (pymrio + mario)
# ─────────────────────────────────────────────────────────────────────────────
def download_oecd():
    _header("OECD ICIO v2021  (pymrio / mario)")

    if not _should_run("oecd"):
        _skip("Not in --only list")
        return False

    try:
        import pymrio
    except ImportError:
        _error("pymrio not installed")
        return False

    folder = PATHS["oecd"]
    folder.mkdir(exist_ok=True)

    existing = list(folder.glob("*.zip")) + list(folder.glob("*.csv"))
    if existing:
        _skip(f"Already downloaded ({len(existing)} files in {folder})")
        return True

    _info(f"Downloading OECD ICIO v2021  year={args.oecd_year} → {folder}")
    t0 = time.time()

    try:
        pymrio.download_oecd(
            storage_folder=str(folder),
            version="v2021",
            years=[args.oecd_year],
            overwrite_existing=False,
        )
        _ok(f"OECD ICIO downloaded in {_timer(t0)}")
        return True
    except Exception as exc:
        _error(f"OECD download failed: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 4. FIGARO 2018  (mario)
# ─────────────────────────────────────────────────────────────────────────────
def download_figaro():
    _header("FIGARO 2018  (mario)")

    if not _should_run("figaro"):
        _skip("Not in --only list")
        return False

    try:
        import mario
    except ImportError:
        _error("mario not installed — run: pip install mariopy")
        return False

    folder = PATHS["figaro"]
    folder.mkdir(exist_ok=True)

    existing = list(folder.glob("*.csv")) + list(folder.glob("*.zip"))
    if existing:
        _skip(f"Already downloaded ({len(existing)} files in {folder})")
        return True

    _info(f"Downloading FIGARO {args.figaro_year} → {folder}")
    t0 = time.time()

    success = True
    for fmt, label in [
        ("ind-by-ind", "ixi"),
        ("prod-by-prod", "pxp"),
    ]:
        sub = folder / label
        sub.mkdir(exist_ok=True)
        _info(f"  Downloading FIGARO IOT {args.figaro_year} {label}...")
        try:
            mario.download_figaro(
                table="IOT",
                year=args.figaro_year,
                path=str(sub),
                format=fmt,
            )
            _ok(f"  FIGARO {label} downloaded")
        except Exception as exc:
            err_str = str(exc)
            if "zip" in err_str.lower() or "404" in err_str or "tokeniz" in err_str:
                _warn(f"  FIGARO {label} unavailable — Eurostat moved flat-file hosting (HTTP 404).")
                _warn(f"  New location: https://data.jrc.ec.europa.eu/collection/id-00403")
                _warn(f"  mario and iopy have not yet been updated for the new URL / Parquet format.")
            else:
                _error(f"  FIGARO {label} failed: {exc}")
            success = False

    if success:
        _ok(f"FIGARO downloaded in {_timer(t0)}")
    else:
        _warn("FIGARO skipped — upstream URL change. See docs/iodb_download_guide.md.")
    return False  # not available regardless


# ─────────────────────────────────────────────────────────────────────────────
# 5. Eora26  (pymrio + mario — credentials required)
# ─────────────────────────────────────────────────────────────────────────────
def download_eora26():
    _header("Eora26  (pymrio / mario — registration required)")

    if not _should_run("eora26"):
        _skip("Not in --only list")
        return False

    if not args.eora_email or not args.eora_password:
        _warn("Eora26 requires a free academic account at https://worldmrio.com/login.jsp")
        _warn("Re-run with:  --eora-email your@email.com --eora-password yourpassword")
        _info("Eora26 contributes the 'developing-country bias' DB profile in run_all_db.py.")
        return False

    try:
        import pymrio
    except ImportError:
        _error("pymrio not installed")
        return False

    folder = PATHS["eora26"]
    folder.mkdir(exist_ok=True)

    existing = list(folder.glob("*.zip")) + list(folder.glob("*.txt"))
    if existing:
        _skip(f"Already downloaded ({len(existing)} files in {folder})")
        return True

    _info(f"Downloading Eora26 (year 2015) → {folder}")
    _info("Note: ~2 GB download, may take 30+ minutes.")
    t0 = time.time()

    try:
        pymrio.download_eora26(
            storage_folder=str(folder),
            email=args.eora_email,
            password=args.eora_password,
            years=[2015],
            overwrite_existing=False,
        )
        _ok(f"Eora26 downloaded in {_timer(t0)}")
        return True
    except Exception as exc:
        _error(f"Eora26 download failed: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 6. iopy databases  (OECD 2021, ExioBase 3.81)
# ─────────────────────────────────────────────────────────────────────────────
def download_iopy():
    _header("iopy databases  (OECD 2021, ExioBase 3.81  — auto-download)")

    if not _should_run("iopy"):
        _skip("Not in --only list")
        return False

    try:
        import iopy
        from iopy.core.globals import DATA_FOLDER as IOPY_CACHE
    except ImportError:
        _error("iopy not installed — run: pip install git+https://github.com/WWakker/iopy.git")
        return False

    _info(f"iopy downloads to its own fixed cache: {IOPY_CACHE}")

    # Create symlink from input_iodb/iopy_cache → iopy's cache
    link_path = PATHS["iopy"]
    if link_path.is_symlink():
        _skip(f"Symlink already exists: {link_path} → {os.readlink(link_path)}")
    elif link_path.exists():
        _skip(f"{link_path} exists (directory, not symlink)")
    else:
        try:
            link_path.symlink_to(IOPY_CACHE)
            _ok(f"Created symlink: {link_path} → {IOPY_CACHE}")
        except Exception as exc:
            _warn(f"Could not create symlink ({exc}); iopy data is at {IOPY_CACHE}")

    # Check what's already cached
    from pathlib import Path as _Path
    cache_path = _Path(IOPY_CACHE)
    if cache_path.exists():
        cached = [f.name for f in cache_path.iterdir() if not f.name.startswith("_")]
        if cached:
            _info(f"Already cached ({len(cached)} files): {', '.join(cached[:4])}{'...' if len(cached)>4 else ''}")

    # ── iopy OECD 2021 ────────────────────────────────────────────────────
    _info("Downloading iopy OECD 2021 (year=2018) if not cached...")
    if args.skip_large:
        _skip("Skipping ExioBase (large) — OECD will still be downloaded")

    t0 = time.time()
    try:
        _iopy_db = iopy.OECD(version="2021", year=2018)
        _ok(f"iopy OECD 2021 ready in {_timer(t0)}")
    except Exception as exc:
        _error(f"iopy OECD 2021 failed: {exc}")

    # ── iopy OECD 2022-small ───────────────────────────────────────────────
    _info("Downloading iopy OECD 2022-small (year=2018) if not cached...")
    try:
        t0 = time.time()
        _iopy_db = iopy.OECD(version="2022-small", year=2018)
        _ok(f"iopy OECD 2022-small ready in {_timer(t0)}")
    except Exception as exc:
        _warn(f"iopy OECD 2022-small failed (known URL issue): {exc}")

    # ── iopy ExioBase 3.81 ixi ────────────────────────────────────────────
    if not args.skip_large:
        _info("Downloading iopy ExioBase 3.81 ixi (year=2018) if not cached...")
        _info("This is a ~730 MB download and may take 10–30 minutes.")
        try:
            t0 = time.time()
            _iopy_db = iopy.ExioBase(version="3.81", year=2018, kind="industry-by-industry")
            _ok(f"iopy ExioBase 3.81 ixi ready in {_timer(t0)}")
        except Exception as exc:
            _error(f"iopy ExioBase 3.81 ixi failed: {exc}")

        # ── iopy ExioBase 3.81 pxp ────────────────────────────────────────
        _info("Downloading iopy ExioBase 3.81 pxp (year=2018) if not cached...")
        try:
            t0 = time.time()
            _iopy_db = iopy.ExioBase(version="3.81", year=2018, kind="product-by-product")
            _ok(f"iopy ExioBase 3.81 pxp ready in {_timer(t0)}")
        except Exception as exc:
            _error(f"iopy ExioBase 3.81 pxp failed: {exc}")
    else:
        _skip("ExioBase 3.81 skipped (--skip-large)")

    # ── iopy FIGARO ───────────────────────────────────────────────────────
    _info("Downloading iopy FIGARO 2022 ixi (year=2018) if not cached...")
    try:
        t0 = time.time()
        _iopy_db = iopy.Figaro(version="2022", year=2018, kind="industry-by-industry")
        _ok(f"iopy FIGARO 2022 ixi ready in {_timer(t0)}")
    except Exception as exc:
        _warn(f"iopy FIGARO 2022 ixi failed (known Eurostat format issue): {exc}")

    _info("Downloading iopy FIGARO 2022 pxp (year=2018) if not cached...")
    try:
        t0 = time.time()
        _iopy_db = iopy.Figaro(version="2022", year=2018, kind="product-by-product")
        _ok(f"iopy FIGARO 2022 pxp ready in {_timer(t0)}")
    except Exception as exc:
        _warn(f"iopy FIGARO 2022 pxp failed (known Eurostat format issue): {exc}")

    return True


# ─────────────────────────────────────────────────────────────────────────────
# 7. Summary & path reference
# ─────────────────────────────────────────────────────────────────────────────
def print_summary(results):
    _header("Download summary")

    db_info = {
        "exiobase": ("EXIOBASE 3 pxp",          "pymrio + mario",    "fastinfra_mrio_benchmark.py, run_all_db.py, fastinfra_mario.py"),
        "wiod":     ("WIOD 2013",                "pymrio + mario",    "fastinfra_mrio_benchmark.py, run_all_db.py, fastinfra_mario.py"),
        "oecd":     ("OECD ICIO v2021",          "pymrio + mario",    "fastinfra_mrio_benchmark.py, run_all_db.py, fastinfra_mario.py"),
        "figaro":   ("FIGARO 2018 ixi+pxp",      "mario",             "fastinfra_mario.py"),
        "eora26":   ("Eora26 (needs account)",   "pymrio + mario",    "fastinfra_mrio_benchmark.py, run_all_db.py, fastinfra_mario.py"),
        "iopy":     ("iopy cache (OECD+ExioBase)","iopy",              "iopy_analysis/fastinfra_iopy.py"),
    }

    print(f"\n  {'Database':<28} {'Status':<10} {'Used by'}")
    print(f"  {'─'*28} {'─'*10} {'─'*40}")
    for db, (label, lib, scripts) in db_info.items():
        status = "OK" if results.get(db) else ("SKIPPED" if results.get(db) is False else "FAILED")
        icon   = "✓" if status == "OK" else ("○" if status == "SKIPPED" else "✗")
        print(f"  {icon} {label:<27} {lib:<20} → {scripts}")

    print(f"\n  DB_PATHS for scripts (set in each analysis script):")
    print(f"  ─────────────────────────────────────────────────")
    for db, path in PATHS.items():
        if db == "iopy":
            continue
        exists = "✓" if path.exists() and any(path.iterdir()) else "○ (empty/missing)"
        print(f"  \"{db}\": \"{path}\",  {exists}")

    print(f"\n  iopy cache: {PATHS['iopy']} (symlink to iopy internal cache)")
    print(f"\n  Scripts to update after downloading:")
    print(f"    /home/deuler/s4_data/fastinfra_mrio_benchmark.py  → DB_PATHS / MRIO_PATH")
    print(f"    /home/deuler/s4_data/run_all_db.py                → DB_PATHS dict")
    print(f"    /home/deuler/s4_data/mario_analysis/fastinfra_mario.py → DB_PATHS dict")
    print(f"    iopy_analysis/fastinfra_iopy.py does not need updating (auto-download)")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\nFAST-Infra IO Database Downloader")
    print(f"Output folder: {IODB_DIR}")
    if args.skip_large:
        print("Mode: --skip-large  (EXIOBASE + Eora26 skipped)")
    if args.only:
        print(f"Mode: --only {', '.join(args.only)}")
    print()

    results = {}

    results["exiobase"] = download_exiobase()
    results["wiod"]     = download_wiod()
    results["oecd"]     = download_oecd()
    results["figaro"]   = download_figaro()
    results["eora26"]   = download_eora26()
    results["iopy"]     = download_iopy()

    print_summary(results)
