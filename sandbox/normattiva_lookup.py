#!/usr/bin/env python3
"""Normattiva OpenData API Lookup Script - Query Italian legislation details."""

import json
import requests
from pathlib import Path
from datetime import datetime

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS = {"Content-Type": "application/json"}



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

    # ac19_2393 -> Legge 10 novembre 2025, n. 167
    codice_redazionale = "25G00174"
    data_gu = "2025-11-14"

    print("=" * 60)
    print("Normattiva OpenData API Lookup")
    print(f"codiceRedazionale: {codice_redazionale} | dataGU: {data_gu}")
    print("=" * 60 + "\n")

    detail = get_act_detail(codice_redazionale, data_gu)
    save_results(f"detail_{codice_redazionale}", detail, output_dir)

    atto = detail.get("data", {}).get("atto", {})
    print(f"  Titolo: {atto.get('titolo')}")
    print(f"  Sotto titolo: {atto.get('sottoTitolo', '').strip()}\n")

    print("=" * 60)
    print("Lookup complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
