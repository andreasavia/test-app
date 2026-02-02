#!/usr/bin/env python3
"""Normattiva - Norme in vigore in un periodo specificato."""

import csv
import json
import requests
from pathlib import Path
from datetime import datetime

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS = {"Content-Type": "application/json"}


def get_norme_aggiornati(data_inizio: str, data_fine: str) -> dict:
    """Fetch norms updated/in effect in the given date range via ricerca/aggiornati."""
    url = f"{BASE_URL}/ricerca/aggiornati"
    payload = {
        "dataInizioAggiornamento": data_inizio,
        "dataFineAggiornamento": data_fine
    }

    print(f"Fetching norme aggiornati: {data_inizio} -> {data_fine}")
    response = requests.post(url, json=payload, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return response.json()


def save_to_csv(atti: list, output_path: Path) -> None:
    """Save list of atti to CSV."""
    if not atti:
        print("No results to save.")
        return

    with output_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=atti[0].keys(), extrasaction='ignore')
        writer.writeheader()
        writer.writerows(atti)

    print(f"  ✓ CSV saved: {output_path}")


def save_to_json(data: dict, output_path: Path) -> None:
    """Save raw API response as JSON."""
    with output_path.open('w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"  ✓ JSON saved: {output_path}")


def main():
    # Novembre 2025
    data_inizio = "2025-11-01T00:00:00Z"
    data_fine = "2025-11-30T23:59:59Z"

    output_dir = Path("output/norme_in_vigore")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 60)
    print("Norme in vigore - Novembre 2025")
    print("=" * 60 + "\n")

    results = get_norme_aggiornati(data_inizio, data_fine)

    # Save raw JSON
    save_to_json(results, output_dir / f"norme_nov2025_raw_{timestamp}.json")

    # Extract and save atti list
    atti = results.get("listaAtti", [])
    print(f"  Totale norme: {len(atti)}\n")

    if atti:
        save_to_csv(atti, output_dir / f"norme_nov2025_{timestamp}.csv")

        print("\n  Prime 10 norme:")
        print(f"  {'codice':<14} {'data GU':<12} {'descrizione'}")
        print(f"  {'-'*14} {'-'*12} {'-'*50}")
        for atto in atti[:10]:
            print(f"  {atto.get('codiceRedazionale', ''):<14} "
                  f"{atto.get('dataGU', ''):<12} "
                  f"{atto.get('descrizioneAtto', '')}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
