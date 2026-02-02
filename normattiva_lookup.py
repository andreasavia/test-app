#!/usr/bin/env python3
"""Normattiva OpenData API Lookup Script - Query Italian legislation details."""

import json
import re
import requests
from pathlib import Path
from datetime import datetime

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS = {"Content-Type": "application/json"}


def search_simple(query: str, page: int = 1, page_size: int = 10) -> dict:
    """Search legislation using ricerca semplice endpoint."""
    url = f"{BASE_URL}/ricerca/semplice"
    payload = {
        "testoRicerca": query,
        "orderType": "recente",
        "paginazione": {
            "paginaCorrente": page,
            "numeroElementiPerPagina": page_size
        }
    }

    print(f"Searching: '{query}'")
    response = requests.post(url, json=payload, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def search_advanced(title: str = None, text: str = None, year: int = None) -> dict:
    """Search legislation using ricerca avanzata endpoint."""
    url = f"{BASE_URL}/ricerca/avanzata"
    payload = {
        "orderType": "recente",
        "paginazione": {
            "paginaCorrente": 1,
            "numeroElementiPerPagina": 10
        }
    }

    if title:
        payload["titoloRicerca"] = title
    if text:
        payload["testoRicerca"] = text
    if year:
        payload["filtriMap"] = {"anno_provvedimento": year}

    print(f"Advanced search: title='{title}', year={year}")
    response = requests.post(url, json=payload, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def get_act_detail(codice_redazionale: str, data_gu: str = None) -> dict:
    """Get full act details using dettaglio-atto endpoint."""
    url = f"{BASE_URL}/atto/dettaglio-atto"
    payload = {"codiceRedazionale": codice_redazionale}
    if data_gu:
        payload["dataGU"] = data_gu

    print(f"Fetching details for: {codice_redazionale}")
    response = requests.post(url, json=payload, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def save_results(label: str, data: dict, output_dir: Path) -> None:
    """Save results to a JSON file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"{label}_{timestamp}.json"

    with output_file.open('w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"  ✓ Saved: {output_file}")


def main():
    output_dir = Path("output/normattiva")

    print("=" * 60)
    print("Normattiva OpenData API Lookup")
    print("Act: ac19_2393 - Semplificazione normativa (S. 1192)")
    print("=" * 60 + "\n")

    # 1. Simple search for the act by title keywords
    print("--- Step 1: Simple Search ---")
    search_results = search_simple("semplificazione normativa deleghe governo")
    save_results("search_simple", search_results, output_dir)
    print(f"  Results: {json.dumps(search_results, indent=2, ensure_ascii=False)[:500]}\n")

    # 2. Advanced search filtering by title and year (approved Nov 2025)
    print("--- Step 2: Advanced Search ---")
    advanced_results = search_advanced(
        title="semplificazione normativa deleghe governo",
        year=2025
    )
    save_results("search_advanced", advanced_results, output_dir)
    print(f"  Results: {json.dumps(advanced_results, indent=2, ensure_ascii=False)[:500]}\n")

    # 3. Find the target act (ac19_2393) by matching title keywords, then fetch detail
    print("--- Step 3: Act Detail ---")
    target_keywords = ["semplificazione normativa", "deleghe al governo"]
    for result_set in [advanced_results, search_results]:
        for atto in result_set.get("listaAtti", []):
            titolo = re.sub(r'\s+', ' ', atto.get("titoloAtto", "")).lower()
            if all(kw in titolo for kw in target_keywords):
                codice = atto.get("codiceRedazionale")
                data_gu = atto.get("dataGU")
                print(f"  Matched: {codice} | {atto.get('titoloAtto', '')[:80]}...")
                detail = get_act_detail(codice, data_gu)
                save_results(f"detail_{codice}", detail, output_dir)
                print(f"\n  Detail:\n{json.dumps(detail, indent=2, ensure_ascii=False)[:1000]}\n")
                break
        else:
            continue
        break

    print("=" * 60)
    print("Lookup complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
