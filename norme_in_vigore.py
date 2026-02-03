#!/usr/bin/env python3
"""Normattiva - Norme in vigore in un periodo specificato."""

import csv
import json
import requests
from pathlib import Path
from datetime import datetime

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS = {"Content-Type": "application/json"}

# denominazioneAtto  →  segmento URN di normattiva.it
URN_TIPO = {
    "LEGGE":                                        "legge",
    "DECRETO-LEGGE":                                "decreto-legge",
    "DECRETO LEGISLATIVO":                          "decreto-legislativo",
    "DECRETO DEL PRESIDENTE DELLA REPUBBLICA":      "decreto:presidente:repubblica",
    "DECRETO DEL PRESIDENTE DEL CONSIGLIO DEI MINISTRI": "decreto:presidente:consiglio-dei-ministri",
    "DECRETO":                                      "decreto",
}


def normattiva_uri(atto: dict) -> str:
    """Build the normattiva.it N2Ls URI for an atto, or empty string if type unknown."""
    tipo = URN_TIPO.get(atto.get("denominazioneAtto", ""))
    if not tipo:
        return ""
    data = atto.get("dataEmanazione", "")[:10]   # "2025-10-03T…" → "2025-10-03"
    numero = atto.get("numeroProvvedimento", "")
    if not data or not numero:
        return ""
    return f"https://www.normattiva.it/uri-res/N2Ls?urn:nir:stato:{tipo}:{data};{numero}"


def ricerca_avanzata(anno: int, mese: int, pagina: int = 1, per_pagina: int = 100) -> dict:
    """Ricerca avanzata filtrata per anno e mese di emanazione."""
    url = f"{BASE_URL}/ricerca/avanzata"
    payload = {
        "orderType": "vecchio",
        "annoProvvedimento": anno,
        "meseProvvedimento": mese,
        "paginazione": {
            "paginaCorrente": pagina,
            "numeroElementiPerPagina": per_pagina
        }
    }

    print(f"Ricerca avanzata: anno={anno}, mese={mese}, pagina={pagina}")
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
    anno = 2025
    mese = 10

    output_dir = Path("output/norme_in_vigore")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 60)
    print(f"Norme emanate - {anno}/{mese:02d}")
    print("=" * 60 + "\n")

    # Pagina tutte le risultati
    atti = []
    pagina = 1
    while True:
        results = ricerca_avanzata(anno, mese, pagina=pagina)
        batch = results.get("listaAtti", [])
        if not batch:
            break
        atti.extend(batch)
        print(f"  Pagina {pagina}: {len(batch)} risultati")
        pagina += 1

    # Enrich each atto with the normattiva.it URI
    for atto in atti:
        atto["normattiva_uri"] = normattiva_uri(atto)

    # Save raw JSON (ultima risposta) e CSV completo
    save_to_json({"listaAtti": atti}, output_dir / f"norme_{anno}{mese:02d}_raw_{timestamp}.json")
    print(f"\n  Totale norme: {len(atti)}\n")

    if atti:
        save_to_csv(atti, output_dir / f"norme_{anno}{mese:02d}_{timestamp}.csv")

        print("\n  Prime 10 norme:")
        print(f"  {'codice':<14} {'data GU':<12} {'descrizione':<45} {'normattiva_uri'}")
        print(f"  {'-'*14} {'-'*12} {'-'*45} {'-'*70}")
        for atto in atti[:10]:
            print(f"  {atto.get('codiceRedazionale', ''):<14} "
                  f"{atto.get('dataGU', ''):<12} "
                  f"{atto.get('descrizioneAtto', ''):<45} "
                  f"{atto.get('normattiva_uri', '')}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
