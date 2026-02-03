#!/usr/bin/env python3
"""Merge Normattiva (norme_in_vigore) + Camera (sparql_query) into one table.

Usage:
    python merge.py 2025 10
"""

import argparse
import csv
import subprocess
import sys
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def run(script: str, args: list[str]) -> None:
    """Run a sibling script, streaming its output to the console."""
    cmd = [sys.executable, script] + args
    print(f"\n{'=' * 60}")
    print(f"  Running: {' '.join(cmd)}")
    print("=" * 60 + "\n")
    subprocess.run(cmd, check=True)


def latest_csv(directory: Path, glob: str) -> Path:
    """Return the most-recently-modified CSV matching *glob* inside *directory*."""
    candidates = sorted(directory.glob(glob), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError(f"No CSV matching '{glob}' in {directory}")
    return candidates[-1]


def load_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def norm_uri(uri: str) -> str:
    """Normalise http/https so both sides can be compared."""
    return uri.replace("http://", "https://").strip() if uri else ""


# ---------------------------------------------------------------------------
# merge logic  –  full outer join on normattiva_uri (http/https normalised)
# ---------------------------------------------------------------------------

HEADERS = ["normattiva_uri", "camera_uri", "senato_uri"]


def merge(norme: list[dict], camera: list[dict]) -> list[dict]:
    # Index camera rows by normalised normattiva_uri
    camera_by_uri: dict[str, dict] = {}
    for row in camera:
        key = norm_uri(row.get("normattiva_uri", ""))
        if key:
            camera_by_uri[key] = row

    merged: list[dict] = []
    matched_keys: set[str] = set()

    # left side: every Normattiva norm
    for n in norme:
        key = norm_uri(n.get("normattiva_uri", ""))
        c = camera_by_uri.get(key)
        if key and c:
            matched_keys.add(key)
        merged.append({
            "normattiva_uri": n.get("normattiva_uri", ""),
            "camera_uri":     c.get("atto", "")      if c else "",
            "senato_uri":     c.get("senato_uri", "") if c else "",
        })

    # right-only: camera acts whose normattiva_uri didn't match any norm
    for c in camera:
        key = norm_uri(c.get("normattiva_uri", ""))
        if key in matched_keys or not key:
            continue
        merged.append({
            "normattiva_uri": c.get("normattiva_uri", ""),
            "camera_uri":     c.get("atto", ""),
            "senato_uri":     c.get("senato_uri", ""),
        })

    return merged


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Merge Normattiva + Camera per anno/mese")
    parser.add_argument("anno", type=int, help="Anno (es. 2025)")
    parser.add_argument("mese", type=int, help="Mese (1-12)")
    args = parser.parse_args()
    anno, mese = args.anno, args.mese
    mese_str = f"{mese:02d}"

    # --- 1. run upstream scripts ---
    run("norme_in_vigore.py", [str(anno), str(mese)])
    run("sparql_query.py",    [str(anno), str(mese)])

    # --- 2. load their latest CSVs ---
    norme_dir  = Path("output/norme_in_vigore")
    camera_dir = Path("output")

    norme_csv  = latest_csv(norme_dir,  f"norme_{anno}{mese_str}_[!r]*.csv")
    camera_csv = latest_csv(camera_dir, f"leg_19_app_def_{anno}_{mese_str}_*.csv")

    print(f"\n  Normattiva CSV : {norme_csv}")
    print(f"  Camera CSV     : {camera_csv}\n")

    norme  = load_csv(norme_csv)
    camera = load_csv(camera_csv)

    # --- 3. merge ---
    merged = merge(norme, camera)

    # --- 4. stats ---
    n_matched   = sum(1 for r in merged if r["normattiva_uri"] and r["camera_uri"])
    n_norm_only = sum(1 for r in merged if r["normattiva_uri"] and not r["camera_uri"])
    n_cam_only  = sum(1 for r in merged if not r["normattiva_uri"] and r["camera_uri"])

    print("=" * 60)
    print(f"  Merge summary  {anno}/{mese_str}")
    print("=" * 60)
    print(f"  Normattiva rows : {len(norme)}")
    print(f"  Camera rows     : {len(camera)}")
    print(f"  Matched         : {n_matched}")
    print(f"  Normattiva only : {n_norm_only}")
    print(f"  Camera only     : {n_cam_only}")
    print(f"  Total merged    : {len(merged)}\n")

    # --- 5. save ---
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path   = output_dir / f"merged_{anno}_{mese_str}_{timestamp}.csv"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(merged)

    print(f"  Merged CSV saved: {out_path}\n")

    # --- 6. pretty-print table ---
    w = 85  # column width for URIs
    print(f"  {'normattiva_uri':<{w}} {'camera_uri':<{w}} {'senato_uri'}")
    print(f"  {'-'*w} {'-'*w} {'-'*w}")
    for r in merged:
        print(f"  {r['normattiva_uri']:<{w}} {r['camera_uri']:<{w}} {r['senato_uri']}")


if __name__ == "__main__":
    main()
