#!/usr/bin/env python3
"""
matching_v2 — Normattiva norm listing + Camera numero search.

Prompts for anno / mese, fetches norms via ricerca/avanzata,
filters to the requested month, then for each norm queries Camera
dei Deputati for acts whose titolo contains "n. <numeroProvvedimento>".
All hits are kept and listed — no filtering or disambiguation.

CSV output is one row per hit (norm fields repeated); norms with
zero hits get a single row with empty Camera columns.
"""

import csv
import json
import re
import requests
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON

BASE_URL      = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
CAMERA_SPARQL = "http://dati.camera.it/sparql"
HEADERS       = {"Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Normattiva helpers
# ---------------------------------------------------------------------------

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


def classify_norm_type(descrizione: str) -> str:
    """Coarse norm-type bucket from descrizioneAtto."""
    for t in ("LEGGE", "DECRETO LEGISLATIVO", "DECRETO-LEGGE",
              "DECRETO DEL PRESIDENTE DELLA REPUBBLICA",
              "DECRETO DEL PRESIDENTE DEL CONSIGLIO DEI MINISTRI", "DECRETO"):
        if descrizione.startswith(t):
            return t
    return "ALTRO"


TIPO_SHORT = {
    "LEGGE":                                            "LEGGE",
    "DECRETO-LEGGE":                                    "D.L.",
    "DECRETO LEGISLATIVO":                              "D.Lgs.",
    "DECRETO DEL PRESIDENTE DELLA REPUBBLICA":          "DPR",
    "DECRETO DEL PRESIDENTE DEL CONSIGLIO DEI MINISTRI": "DPCM",
    "DECRETO":                                          "DECRETO",
    "ALTRO":                                            "ALTRO",
}


# ---------------------------------------------------------------------------
# Camera SPARQL
# ---------------------------------------------------------------------------

def camera_search_by_numero(anno_numero: str) -> list[dict]:
    """All Camera acts (leg 19) whose titolo contains e.g. '2025, n. 179'.

    Returns a deduplicated list (by atto URI) of plain dicts with keys:
        atto, numero, titolo.
    """
    sparql = SPARQLWrapper(CAMERA_SPARQL)
    sparql.setQuery(f'''
        PREFIX ocd: <http://dati.camera.it/ocd/>
        PREFIX dc:  <http://purl.org/dc/elements/1.1/>

        SELECT DISTINCT ?atto ?numero ?titolo {{
            ?atto a ocd:atto;
                dc:identifier ?numero;
                ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>;
                dc:title ?titolo .
            FILTER(CONTAINS(?titolo, "{anno_numero}"))
        }} ORDER BY ?numero
    ''')
    sparql.setReturnFormat(SPARQL_JSON)
    bindings = sparql.query().convert()["results"]["bindings"]

    # deduplicate by atto URI
    seen, result = set(), []
    for b in bindings:
        uri = b["atto"]["value"]
        if uri not in seen:
            seen.add(uri)
            result.append({k: v["value"] for k, v in b.items()})
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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

    # --- query Camera for every norm ---
    print(f"\n  Querying Camera for {len(norms)} norms...")
    # hits_by_codice: codice -> list of hit dicts
    hits_by_codice: dict[str, list[dict]] = {}
    for i, a in enumerate(norms):
        codice = a.get("codiceRedazionale", "")
        desc   = a.get("descrizioneAtto", "")
        # extract "2025, n. 179" from e.g. "LEGGE 1 dicembre 2025, n. 179"
        m = re.search(r'(\d{4},\s*n\.\s*\d+)', desc)
        if not m:
            hits_by_codice[codice] = []
            continue
        anno_numero = m.group(1)
        print(f"    [{i+1}/{len(norms)}] {anno_numero:<12} {desc}", end=" … ", flush=True)
        hits = camera_search_by_numero(anno_numero)
        hits_by_codice[codice] = hits
        print(f"{len(hits)} hits")

    # --- print full table (one block per norm, all hits listed) ---
    print("\n" + "=" * 140)
    print(f"  NORMS PUBLISHED IN {month_prefix}  ({len(norms)} total)")
    print("=" * 140)
    print(f"  {'#':<4} {'Codice':<14} {'Type':<8} {'Descrizione':<50} {'hits':<5}")
    print(f"  {'-'*4} {'-'*14} {'-'*8} {'-'*50} {'-'*5}")

    # flat rows for CSV / JSON  (one row per hit; zero-hit norms get one row)
    table = []

    for idx, a in enumerate(norms, 1):
        codice      = a.get("codiceRedazionale", "")
        descrizione = a.get("descrizioneAtto", "")
        numero_prov = a.get("numeroProvvedimento", "")
        norm_type   = classify_norm_type(descrizione)
        tipo_short  = TIPO_SHORT.get(norm_type, norm_type)
        hits        = hits_by_codice.get(codice, [])

        # norm header row
        print(f"  {idx:<4} {codice:<14} {tipo_short:<8} {descrizione:<50} {len(hits)}")

        if hits:
            for h in hits:
                print(f"       └─ {h['numero']:<8} {h['atto']}")
                table.append({
                    "codice": codice,
                    "dataGU": a.get("dataGU", ""),
                    "descrizione": descrizione,
                    "norm_type": norm_type,
                    "numeroProvvedimento": numero_prov,
                    "camera_act": h["numero"],
                    "camera_uri": h["atto"],
                })
        else:
            table.append({
                "codice": codice,
                "dataGU": a.get("dataGU", ""),
                "descrizione": descrizione,
                "norm_type": norm_type,
                "numeroProvvedimento": numero_prov,
                "camera_act": None,
                "camera_uri": None,
            })

    print("=" * 140)
    total_hits = sum(len(hits_by_codice.get(a.get("codiceRedazionale", ""), [])) for a in norms)
    print(f"  {len(norms)} norms   {total_hits} Camera hits total")

    # --- save ---
    output_dir = Path("output/matching")
    output_dir.mkdir(parents=True, exist_ok=True)
    month_tag  = month_prefix.replace("-", "")

    out_json = output_dir / f"norms_{month_tag}.json"
    out_json.write_text(json.dumps(table, indent=2, ensure_ascii=False))
    print(f"  Saved: {out_json}")

    csv_file = output_dir / f"norms_{month_tag}.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["codice", "dataGU", "descrizione", "norm_type",
                         "numeroProvvedimento", "camera_act", "camera_uri"])
        for row in table:
            writer.writerow([
                row["codice"],
                row["dataGU"],
                row["descrizione"],
                row["norm_type"],
                row["numeroProvvedimento"],
                row["camera_act"] or "",
                row["camera_uri"] or "",
            ])
    print(f"  Saved: {csv_file}")


if __name__ == "__main__":
    main()
