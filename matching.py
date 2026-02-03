#!/usr/bin/env python3
"""
Match Normattiva laws back to Camera dei Deputati acts.

Two matching strategies identified:

Strategy A — decreto-legge reference (most reliable):
  Normattiva sottoTitolo contains e.g. "decreto-legge 3 ottobre 2025, n. 145"
  Camera titolo contains the same reference.
  -> extract the DL number+date from Normattiva, FILTER Camera titolo by it.

Strategy B — approval date:
  Normattiva law date (e.g. "18 novembre 2025") = Camera fase "Legge" dataIter (20251118)
  -> query Camera for fase containing "Legge" on that date.
"""

import json
import re
import requests
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON

NORMATTIVA_BASE = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
CAMERA_SPARQL = "http://dati.camera.it/sparql"
HEADERS = {"Content-Type": "application/json"}


def get_normattiva_detail(codice_redazionale: str, data_gu: str) -> dict:
    """Fetch full act detail from Normattiva."""
    payload = {"codiceRedazionale": codice_redazionale, "dataGU": data_gu}
    r = requests.post(f"{NORMATTIVA_BASE}/atto/dettaglio-atto",
                      json=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def extract_decreto_legge_ref(text: str) -> dict | None:
    """Extract decreto-legge date and number from text.

    e.g. "decreto-legge 3 ottobre 2025, n. 145"
      -> {"numero": "145", "data_str": "3 ottobre 2025"}
    """
    m = re.search(
        r'decreto-legge\s+(\d+\s+\w+\s+\d{4}),\s*n\.\s*(\d+)',
        text, re.IGNORECASE
    )
    if m:
        return {"data_str": m.group(1), "numero": m.group(2)}
    return None


def extract_law_date(titolo: str) -> str | None:
    """Extract YYYYMMDD from Normattiva titolo like 'LEGGE 18 novembre 2025, n. 173'.

    Returns Camera-style date string e.g. '20251118'.
    """
    MESI = {"gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
            "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
            "settembre": "09", "ottobre": "10", "novembre": "11", "dicembre": "12"}

    m = re.search(r'(\d+)\s+(\w+)\s+(\d{4})', titolo)
    if m:
        day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
        month = MESI.get(month_name)
        if month:
            return f"{year}{month}{int(day):02d}"
    return None


def camera_search_by_dl_ref(dl_numero: str) -> list:
    """Strategy A: search Camera for acts whose titolo contains decreto-legge n. X."""
    sparql = SPARQLWrapper(CAMERA_SPARQL)
    sparql.setQuery(f'''
        PREFIX ocd: <http://dati.camera.it/ocd/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>

        SELECT ?atto ?numero ?titolo ?fase ?dataIter {{
            ?atto a ocd:atto;
                dc:identifier ?numero;
                ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>;
                dc:title ?titolo;
                ocd:rif_statoIter ?statoIter .
            ?statoIter dc:title ?fase ; dc:date ?dataIter .
            FILTER(CONTAINS(LCASE(?titolo), "decreto-legge"))
            FILTER(CONTAINS(?titolo, "n. {dl_numero}"))
            FILTER(CONTAINS(LCASE(?fase), "legge"))
        }} ORDER BY ?dataIter
    ''')
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]


def camera_search_by_date(date_str: str) -> list:
    """Strategy B: search Camera for acts reaching fase 'Legge' on a given date."""
    sparql = SPARQLWrapper(CAMERA_SPARQL)
    sparql.setQuery(f'''
        PREFIX ocd: <http://dati.camera.it/ocd/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>

        SELECT ?atto ?numero ?titolo ?fase ?dataIter {{
            ?atto a ocd:atto;
                dc:identifier ?numero;
                ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>;
                dc:title ?titolo;
                ocd:rif_statoIter ?statoIter .
            ?statoIter dc:title ?fase ; dc:date ?dataIter .
            FILTER(CONTAINS(LCASE(?fase), "legge"))
            FILTER(?dataIter = "{date_str}")
        }} ORDER BY ?numero
    ''')
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]


def main():
    output_dir = Path("output/matching")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Target: Legge 18 novembre 2025, n. 173 (conversione DL 145/2025)
    codice = "25G00182"
    data_gu = "2025-11-20"

    print("=" * 70)
    print("MATCHING: Normattiva -> Camera dei Deputati")
    print(f"Target: codiceRedazionale={codice}, dataGU={data_gu}")
    print("=" * 70 + "\n")

    # 1. Fetch Normattiva detail
    print("--- 1. Normattiva detail ---")
    detail = get_normattiva_detail(codice, data_gu)
    atto = detail.get("data", {}).get("atto", {})
    titolo = atto.get("titolo", "")
    sotto_titolo = atto.get("sottoTitolo", "").strip()
    print(f"  titolo:      {titolo}")
    print(f"  sottoTitolo: {sotto_titolo}\n")

    # 2. Extract references
    print("--- 2. Extract references ---")
    dl_ref = extract_decreto_legge_ref(sotto_titolo)
    law_date = extract_law_date(titolo)
    print(f"  decreto-legge ref: {dl_ref}")
    print(f"  law date (Camera format): {law_date}\n")

    # 3. Strategy A — match by decreto-legge number
    print("--- 3. Strategy A: match by decreto-legge numero ---")
    if dl_ref:
        hits_a = camera_search_by_dl_ref(dl_ref["numero"])
        for h in hits_a:
            vals = {k: v["value"] for k, v in h.items()}
            print(f"  ac19_{vals['numero']:<6} fase: {vals['fase']:<60} dataIter: {vals['dataIter']}")
    else:
        print("  No decreto-legge ref found.")
        hits_a = []

    # 4. Strategy B — match by date
    print(f"\n--- 4. Strategy B: match by date {law_date} ---")
    if law_date:
        hits_b = camera_search_by_date(law_date)
        for h in hits_b:
            vals = {k: v["value"] for k, v in h.items()}
            print(f"  ac19_{vals['numero']:<6} fase: {vals['fase']:<60} dataIter: {vals['dataIter']}")
    else:
        print("  Could not extract law date.")
        hits_b = []

    # 5. Save all results
    output = {
        "normattiva": {"codice": codice, "dataGU": data_gu, "titolo": titolo, "sottoTitolo": sotto_titolo},
        "extracted": {"decreto_legge_ref": dl_ref, "law_date": law_date},
        "strategy_a_results": [{k: v["value"] for k, v in h.items()} for h in hits_a],
        "strategy_b_results": [{k: v["value"] for k, v in h.items()} for h in hits_b],
    }
    out_file = output_dir / "matching_25G00182.json"
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\n  ✓ Saved: {out_file}")

    print("\n" + "=" * 70)
    print("Done!")
    print("=" * 70)


if __name__ == "__main__":
    main()
