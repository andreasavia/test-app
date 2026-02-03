#!/usr/bin/env python3
"""
matching_v2 — Normattiva norm listing.

Prompts for anno / mese, fetches norms via ricerca/avanzata,
filters to the requested month, and prints a full table.
"""

import json
import re
import requests
from pathlib import Path

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS  = {"Content-Type": "application/json"}


def fetch_norms(anno: int, mese: int) -> list[dict]:
    """Paginate through ricerca/avanzata for the given anno + mese."""
    url   = f"{BASE_URL}/ricerca/avanzata"
    atti  = []
    pagina = 1
    while True:
        payload = {
            "orderType": "vecchio",
            "annoProvvedimento": anno,
            "meseProvvedimento": mese,
            "paginazione": {
                "paginaCorrente": pagina,
                "numeroElementiPerPagina": 100,
            },
        }
        print(f"  fetching pagina {pagina}...")
        r = requests.post(url, json=payload, headers=HEADERS, timeout=60)
        r.raise_for_status()
        batch = r.json().get("listaAtti", [])
        if not batch:
            break
        atti.extend(batch)
        pagina += 1
    return atti


def clean_titolo(raw: str | None) -> str:
    """Collapse whitespace and strip the trailing codice in parentheses."""
    if not raw:
        return ""
    text = re.sub(r'\s+', ' ', raw).strip()
    text = re.sub(r'\s*\(\d+[A-Z]\d+\)\s*$', '', text)   # e.g. (25G00006)
    return text.strip("[] ")


def main():
    anno = int(input("Anno (es. 2025): "))
    mese = int(input("Mese (1-12):     "))
    month_prefix = f"{anno}-{mese:02d}"

    print(f"\n  Fetching norms from Normattiva for {anno}/{mese:02d}...")
    all_norms = fetch_norms(anno, mese)

    # Keep only norms whose dataGU falls in the requested month
    norms = sorted(
        [a for a in all_norms if a.get("dataGU", "").startswith(month_prefix)],
        key=lambda a: (a.get("dataGU", ""), a.get("numeroProvvedimento", "0")),
    )

    # --- print table ---
    print("\n" + "=" * 100)
    print(f"  NORMS PUBLISHED IN {month_prefix}  ({len(norms)} total)")
    print("=" * 100)
    print(f"  {'#':<4} {'Codice':<14} {'dataGU':<12} {'Descrizione':<42} {'Titolo'}")
    print(f"  {'-'*4} {'-'*14} {'-'*12} {'-'*42} {'-'*60}")

    for idx, a in enumerate(norms, 1):
        codice      = a.get("codiceRedazionale", "")
        dgu         = a.get("dataGU", "")
        descrizione = a.get("descrizioneAtto", "")
        titolo      = clean_titolo(a.get("titoloAtto"))
        print(f"  {idx:<4} {codice:<14} {dgu:<12} {descrizione:<42} {titolo[:60]}")

    print("=" * 100)

    # --- save ---
    output_dir = Path("output/matching")
    output_dir.mkdir(parents=True, exist_ok=True)
    month_tag = month_prefix.replace("-", "")
    out_file  = output_dir / f"norms_{month_tag}.json"
    out_file.write_text(json.dumps(norms, indent=2, ensure_ascii=False))
    print(f"\n  Saved: {out_file}")


if __name__ == "__main__":
    main()
