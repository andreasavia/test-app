#!/usr/bin/env python3
"""
download_norm.py — Download the full text of an Italian norm from Normattiva.

Flow:
  1. Fetch the normattiva.it page for the given URN/URL
     → extract codiceRedazionale + dataGU
  2. POST /atto/dettaglio-atto (article 1, no dataVigenza)
     → read articoloDataInizioVigenza (entry-into-force date)
  3. POST /atto/dettaglio-atto for each article with that vigenza
     → collect full text + raw JSON
  4. Save HTML + JSON to ./download/

Usage examples:
  python download_norm.py "urn:nir:stato:legge:2026-01-07;1"
  python download_norm.py "https://www.normattiva.it/uri-res/N2Ls?urn:nir:stato:legge:2026-01-22;8"
"""

import argparse
import os
import re
import json
import requests

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
NORMATTIVA_BASE = "https://www.normattiva.it/uri-res/N2Ls?"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "download")


# ---------------------------------------------------------------------------
# Resolve URN → codiceRedazionale + dataGU via the normattiva.it page
# ---------------------------------------------------------------------------

def resolve_urn(urn):
    """Fetch the normattiva.it page for the URN and extract codiceRedazionale + dataGU."""
    if urn.startswith("urn:"):
        url = NORMATTIVA_BASE + urn
    else:
        url = urn

    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    resp.raise_for_status()

    # The page contains links like: caricaAKN?dataGU=20260107&codiceRedaz=25G00211&...
    m = re.search(r"dataGU=(\d{8})&(?:amp;)?codiceRedaz=([A-Z0-9]+)", resp.text)
    if not m:
        raise RuntimeError(f"Could not extract codiceRedazionale/dataGU from {url}")

    data_gu_raw, codice = m.groups()
    data_gu = f"{data_gu_raw[:4]}-{data_gu_raw[4:6]}-{data_gu_raw[6:8]}"
    return codice, data_gu


# ---------------------------------------------------------------------------
# dettaglio-atto
# ---------------------------------------------------------------------------

def yyyymmdd_to_iso(raw):
    """Convert '20260128' → '2026-01-28'."""
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def lookup_vigenza(codice, data_gu):
    """Call dettaglio-atto for article 1 (no dataVigenza) and return the entry-into-force date."""
    resp = requests.post(
        f"{BASE_URL}/atto/dettaglio-atto",
        json={"codiceRedazionale": codice, "dataGU": data_gu, "idArticolo": 1},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    resp.raise_for_status()
    raw = resp.json().get("data", {}).get("atto", {}).get("articoloDataInizioVigenza")
    if not raw:
        raise RuntimeError("articoloDataInizioVigenza not found in dettaglio-atto response")
    return yyyymmdd_to_iso(raw)


def fetch_all_articles(codice_redazionale, data_gu, data_vigenza):
    """Iterate idArticolo 1, 2, … until 404; return (metadata, [html_per_articolo], [raw_responses])."""
    url = f"{BASE_URL}/atto/dettaglio-atto"
    metadata = None
    articles = []
    raw_responses = []

    for art_id in range(1, 10000):   # upper bound is just a safety cap
        body = {
            "codiceRedazionale": codice_redazionale,
            "dataGU": data_gu,
            "idArticolo": art_id,
            "dataVigenza": data_vigenza,
        }

        resp = requests.post(url, json=body, headers={"Content-Type": "application/json"}, timeout=60)
        if resp.status_code == 404:
            break
        resp.raise_for_status()

        data = resp.json()
        atto = data.get("data", {}).get("atto", {})
        if metadata is None:
            metadata = atto
        articles.append(atto.get("articoloHtml", ""))
        raw_responses.append(data)
        print(f"    article {art_id} fetched ({len(atto.get('articoloHtml', ''))} chars)")

    return metadata or {}, articles, raw_responses


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save_html(metadata, articles, filename, vigenza):
    """Write all articles into a single HTML file."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    titolo = metadata.get("titolo", "")
    sotto = metadata.get("sottoTitolo", "")

    html = (
        "<!DOCTYPE html>\n<html lang=\"it\">\n<head>\n"
        f"<meta charset=\"utf-8\">\n<title>{titolo}</title>\n"
        "</head>\n<body>\n"
        f"<h1>{titolo}</h1>\n"
        f"<p><em>{sotto}</em></p>\n"
        f"<p><strong>Vigenza:</strong> {vigenza}</p>\n"
        + "\n".join(articles) +
        "\n</body>\n</html>\n"
    )

    dest = os.path.join(DOWNLOAD_DIR, filename)
    with open(dest, "w", encoding="utf-8") as f:
        f.write(html)
    return dest


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Download the full text of an Italian norm from Normattiva.",
        epilog="Output is saved to ./download/"
    )
    parser.add_argument("urn",
                        help="Normattiva URN or URL (e.g. urn:nir:stato:legge:2026-01-07;1 "
                             "or https://www.normattiva.it/uri-res/N2Ls?urn:nir:stato:legge:...)")
    args = parser.parse_args()

    # --- resolve URN → codice + dataGU ---
    print("[1/4] Resolving URN...")
    codice, data_gu = resolve_urn(args.urn)
    print(f"  codiceRedazionale={codice}  dataGU={data_gu}\n")

    # --- look up entry-into-force date ---
    print("[2/4] Looking up vigenza...")
    vigenza = lookup_vigenza(codice, data_gu)
    print(f"  articoloDataInizioVigenza={vigenza}\n")

    # --- fetch all articles ---
    print("[3/4] Fetching all articles...")
    metadata, articles, raw_responses = fetch_all_articles(codice, data_gu, data_vigenza=vigenza)
    titolo = metadata.get("titolo", "unknown")
    print(f"  Title: {titolo}  ({len(articles)} articles)\n")

    # --- save ---
    safe_name = re.sub(r"[^\w]", "_", codice)
    base_name = f"{safe_name}_{vigenza}"

    print("[4/4] Saving...")
    html_path = save_html(metadata, articles, f"{base_name}.html", vigenza=vigenza)
    print(f"  HTML: {html_path} ({os.path.getsize(html_path):,} bytes)")

    json_path = os.path.join(DOWNLOAD_DIR, f"{base_name}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(raw_responses, f, ensure_ascii=False, indent=2)
    print(f"  JSON: {json_path} ({os.path.getsize(json_path):,} bytes)")


if __name__ == "__main__":
    main()
