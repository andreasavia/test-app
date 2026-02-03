#!/usr/bin/env python3
"""
download_norm.py — Download the full text of an Italian norm from Normattiva.

Flow:
  1. Parse the Normattiva URN  → tipo, date, numero
  2. POST /ricerca/avanzata    → locate the act, get codiceRedazionale + dataGU
  3. POST /atto/dettaglio-atto → fetch each article (idArticolo 1, 2, … until 404)
  4. Save concatenated HTML to ./download/

Usage examples:
  python download_norm.py "urn:nir:stato:legge:2026-01-07;1" --vigenza 2026-02-03
  python download_norm.py "https://www.normattiva.it/uri-res/N2Ls?urn:nir:stato:legge:2026-01-07;1" --vigenza 2026-02-03
"""

import argparse
import os
import re
import sys
import requests

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "download")

# Maps URN type tokens to denominazioneAtto values used by the API
URN_TYPE_TO_DENOMINAZIONE = {
    "legge": "LEGGE",
    "decreto-legge": "DECRETO-LEGGE",
    "decreto-legislativo": "DECRETO LEGISLATIVO",
    "decreto-presidente-repubblica": "DECRETO DEL PRESIDENTE DELLA REPUBBLICA",
    "decreto-presidente-consiglio-ministri": "DECRETO DEL PRESIDENTE DEL CONSIGLIO DEI MINISTRI",
    "decreto-ministeriale": "DECRETO MINISTERIALE",
}


# ---------------------------------------------------------------------------
# URN parsing
# ---------------------------------------------------------------------------

def parse_urn(urn):
    """Parse urn:nir:stato:<tipo>:<YYYY-MM-DD>;<numero> → dict."""
    m = re.search(r"urn:nir:stato:([^:]+):(\d{4}-\d{2}-\d{2});(\S+)", urn)
    if not m:
        raise ValueError(f"Cannot parse URN: {urn}")
    tipo_raw, data_str, numero_raw = m.groups()
    year, month, day = (int(x) for x in data_str.split("-"))
    # numero may have suffixes like ~art1; strip them
    numero = numero_raw.split("~")[0]
    return {
        "tipo_urn": tipo_raw,
        "denominazione": URN_TYPE_TO_DENOMINAZIONE.get(tipo_raw, tipo_raw.upper()),
        "anno": year,
        "mese": month,
        "giorno": day,
        "numero": numero,
        "data_str": data_str,
    }


# ---------------------------------------------------------------------------
# Ricerca → dettaglio-atto → save
# ---------------------------------------------------------------------------

def search_act(params):
    """POST ricerca/avanzata — find the act matching the parsed params."""
    url = f"{BASE_URL}/ricerca/avanzata"
    body = {
        "orderType": "vecchio",
        "annoProvvedimento": params["anno"],
        "meseProvvedimento": params["mese"],
        "paginazione": {"paginaCorrente": 1, "numeroElementiPerPagina": 50},
    }
    if params.get("denominazione"):
        body["denominazioneAtto"] = params["denominazione"]

    resp = requests.post(url, json=body, headers={"Content-Type": "application/json"}, timeout=60)
    resp.raise_for_status()
    return resp.json().get("listaAtti", [])


def find_act(params):
    """Narrow search results to the single act matching numero + giorno."""
    atti = search_act(params)
    numero = params.get("numero")
    giorno = params.get("giorno")

    for atto in atti:
        if numero and str(atto.get("numeroProvvedimento")) != str(numero):
            continue
        if giorno and str(atto.get("giornoProvvedimento")) != str(giorno):
            continue
        return atto

    # Fallback: print what we got so the user can adjust
    print(f"  WARNING: no exact match for numero={numero} giorno={giorno}")
    print(f"  Found {len(atti)} acts. First few:")
    for a in atti[:5]:
        print(f"    numero={a.get('numeroProvvedimento')}  giorno={a.get('giornoProvvedimento')}  "
              f"codice={a.get('codiceRedazionale')}  desc={a.get('descrizioneAtto')}")
    return None


def fetch_all_articles(codice_redazionale, data_gu, data_vigenza=None):
    """Iterate idArticolo 1, 2, … until 404; return (metadata, [html_per_articolo])."""
    url = f"{BASE_URL}/atto/dettaglio-atto"
    metadata = None          # filled from the first successful response
    articles = []

    for art_id in range(1, 10000):   # upper bound is just a safety cap
        body = {"codiceRedazionale": codice_redazionale, "idArticolo": art_id}
        if data_gu:
            body["dataGU"] = data_gu
        if data_vigenza:
            body["dataVigenza"] = data_vigenza

        resp = requests.post(url, json=body, headers={"Content-Type": "application/json"}, timeout=60)
        if resp.status_code == 404:
            break                # no more articles
        resp.raise_for_status()

        atto = resp.json().get("data", {}).get("atto", {})
        if metadata is None:
            metadata = atto      # titolo, sottoTitolo, etc. from first article
        articles.append(atto.get("articoloHtml", ""))
        print(f"    article {art_id} fetched ({len(atto.get('articoloHtml', ''))} chars)")

    return metadata or {}, articles


def save_html(metadata, articles, filename):
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
    parser.add_argument("--vigenza", required=True,
                        help="Version in force on this date YYYY-MM-DD")
    args = parser.parse_args()

    params = parse_urn(args.urn)
    print(f"Parsed URN → {params}\n")
    if args.vigenza:
        params["vigenza"] = args.vigenza

    # --- search → fetch → save ---
    print("[1/3] Searching for the act...")
    atto = find_act(params)
    if not atto:
        sys.exit(1)
    codice = atto["codiceRedazionale"]
    data_gu = atto.get("dataGU")
    desc = atto.get("descrizioneAtto", "")
    print(f"  Found: {desc}  (codice={codice}, dataGU={data_gu})\n")

    print("[2/3] Fetching all articles...")
    metadata, articles = fetch_all_articles(codice, data_gu, data_vigenza=params.get("vigenza"))
    titolo = metadata.get("titolo", "unknown")
    print(f"  Title: {titolo}  ({len(articles)} articles)\n")

    safe_name = re.sub(r"[^\w]", "_", codice)
    filename = f"{safe_name}.html"

    print("[3/3] Saving...")
    path = save_html(metadata, articles, filename)
    print(f"\nDone. Saved to: {path} ({os.path.getsize(path):,} bytes)")


if __name__ == "__main__":
    main()
