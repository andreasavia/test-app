#!/usr/bin/env python3
"""
Batch matching: Normattiva laws -> Camera dei Deputati acts.

Reads the norm list produced by norme_in_vigore.py, filters to LEGGEs
(ordinary laws that always pass through Camera), then for each norm runs
two matching strategies, computes their intersection, and refines any
remaining ambiguities via keyword overlap against the Camera titolo.

Strategy A — decreto-legge reference (applies to conversion laws):
  Extract "decreto-legge <date>, n. <numero>" from Normattiva sottoTitolo.
  FILTER Camera titolo by that DL numero.

Strategy B — approval date:
  Extract law date from Normattiva titolo (e.g. "18 novembre 2025" -> 20251118).
  Query Camera for acts reaching fase containing "legge" on that date.

Confidence levels:
  exact      — A ∩ B yields exactly one unique act  (or single strategy yields one)
  ambiguous  — intersection has > 1 candidate
  no_match   — no candidate found

Usage:
    python matching.py   # prompts for anno / mese, fetches norms live
"""

import html
import json
import re
import requests
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON

NORMATTIVA_BASE = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
CAMERA_SPARQL = "http://dati.camera.it/sparql"
HEADERS = {"Content-Type": "application/json"}

# Only ordinary LEGGEs always originate as Camera acts.
# D.Lgs are government decrees (based on a prior legge delega) — skip them.
CAMERA_TYPES = {"LEGGE"}


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
    """Extract YYYYMMDD from titolo like 'LEGGE 18 novembre 2025, n. 173'.

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


def flatten_hits(hits: list) -> list[dict]:
    """Flatten SPARQL bindings to plain dicts and deduplicate by atto URI."""
    seen = set()
    result = []
    for h in hits:
        uri = h["atto"]["value"]
        if uri not in seen:
            seen.add(uri)
            result.append({k: v["value"] for k, v in h.items()})
    return result


def intersect(hits_a: list[dict], hits_b: list[dict]) -> list[dict]:
    """Return acts present in both hit lists (by atto URI)."""
    uris_b = {h["atto"] for h in hits_b}
    return [h for h in hits_a if h["atto"] in uris_b]


def _significant_words(text: str) -> set[str]:
    """Decode HTML, collapse whitespace, return words >= 4 chars (stop-word proxy)."""
    return set(re.findall(r'\b\w{4,}\b', re.sub(r'\s+', ' ', html.unescape(text)).lower()))


def refine_by_keywords(sotto_titolo: str, candidates: list[dict]) -> list[dict]:
    """When multiple candidates survive A∩B, pick those with best keyword overlap.

    Compares significant words in Normattiva sottoTitolo against each Camera titolo.
    Returns only the candidate(s) tied at the highest score.
    """
    if len(candidates) <= 1:
        return candidates
    norm_words = _significant_words(sotto_titolo)
    scored = [(c, len(norm_words & _significant_words(c["titolo"]))) for c in candidates]
    max_score = max(s for _, s in scored)
    return [c for c, s in scored if s == max_score]


def match_norm(codice: str, data_gu: str) -> dict:
    """Full matching pipeline for a single norm."""
    detail = get_normattiva_detail(codice, data_gu)
    atto = detail.get("data", {}).get("atto", {})
    titolo = atto.get("titolo", "")
    sotto_titolo = atto.get("sottoTitolo", "").strip()

    dl_ref = extract_decreto_legge_ref(sotto_titolo)
    law_date = extract_law_date(titolo)

    hits_a = flatten_hits(camera_search_by_dl_ref(dl_ref["numero"])) if dl_ref else []
    hits_b = flatten_hits(camera_search_by_date(law_date)) if law_date else []

    # Intersection if both strategies fired; otherwise fall back to whichever ran
    if hits_a and hits_b:
        matched = intersect(hits_a, hits_b)
    elif hits_a:
        matched = hits_a
    else:
        matched = hits_b

    # Keyword refinement: disambiguate same-date candidates via title overlap
    if len(matched) > 1:
        matched = refine_by_keywords(sotto_titolo, matched)

    if len(matched) == 1:
        confidence = "exact"
    elif len(matched) > 1:
        confidence = "ambiguous"
    else:
        confidence = "no_match"

    return {
        "codice": codice,
        "dataGU": data_gu,
        "titolo": titolo,
        "dl_ref": dl_ref,
        "law_date": law_date,
        "strategy_a": hits_a,
        "strategy_b": hits_b,
        "matched": matched,
        "confidence": confidence,
    }


def classify_norm_type(descrizione: str) -> str:
    """Classify norm into a coarse type bucket."""
    for t in ("LEGGE", "DECRETO LEGISLATIVO", "DECRETO-LEGGE",
              "DECRETO DEL PRESIDENTE DELLA REPUBBLICA",
              "DECRETO DEL PRESIDENTE DEL CONSIGLIO DEI MINISTRI", "DECRETO"):
        if descrizione.startswith(t):
            return t
    return "ALTRO"


def fetch_norms(anno: int, mese: int) -> list[dict]:
    """Call Normattiva ricerca/avanzata and paginate through all results."""
    url = f"{NORMATTIVA_BASE}/ricerca/avanzata"
    atti = []
    pagina = 1
    while True:
        payload = {
            "orderType": "vecchio",
            "annoProvvedimento": anno,
            "meseProvvedimento": mese,
            "paginazione": {
                "paginaCorrente": pagina,
                "numeroElementiPerPagina": 100
            }
        }
        print(f"  Normattiva ricerca/avanzata: anno={anno}, mese={mese}, pagina={pagina}")
        r = requests.post(url, json=payload, headers=HEADERS, timeout=60)
        r.raise_for_status()
        batch = r.json().get("listaAtti", [])
        if not batch:
            break
        atti.extend(batch)
        pagina += 1
    return atti


def main():
    anno = int(input("Anno (es. 2025): "))
    mese = int(input("Mese (1-12):     "))
    month_prefix = f"{anno}-{mese:02d}"

    print(f"\n  Fetching norms from Normattiva for {anno}/{mese:02d}...")
    all_norms = fetch_norms(anno, mese)
    print(f"  Normattiva returned {len(all_norms)} norms total")

    # Filter to LEGGEs published in the requested month
    targets = [
        a for a in all_norms
        if a.get("dataGU", "").startswith(month_prefix)
        and classify_norm_type(a.get("descrizioneAtto", "")) in CAMERA_TYPES
    ]
    targets.sort(key=lambda a: a.get("dataGU", ""))

    output_dir = Path("output/matching")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print(f"BATCH MATCHING: Normattiva -> Camera dei Deputati  [{month_prefix}]")
    print(f"LEGGEs to match: {len(targets)}")
    print("=" * 70)

    results = []
    for i, norm in enumerate(targets):
        codice = norm["codiceRedazionale"]
        data_gu = norm["dataGU"]
        desc = norm.get("descrizioneAtto", "")
        print(f"\n[{i+1}/{len(targets)}] {codice}  {data_gu}  {desc}")

        result = match_norm(codice, data_gu)
        results.append(result)

        if result["confidence"] == "exact":
            act = result["matched"][0]
            print(f"  ✓ {act['numero']:<6} — {act['titolo'].strip()[:80]}")
        elif result["confidence"] == "ambiguous":
            print(f"  ~ AMBIGUOUS ({len(result['matched'])} candidates)")
            for m in result["matched"]:
                print(f"    {m['numero']:<6} — {m['titolo'].strip()[:70]}")
        else:
            print(f"  ✗ no match")

    # --- summary table ---
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  {'Codice':<14} {'Titolo (Normattiva)':<50} {'Camera act':<12} {'Conf.'}")
    print(f"  {'-'*14} {'-'*50} {'-'*12} {'-'*10}")
    for r in results:
        camera_act = r["matched"][0]["numero"] if r["matched"] else "—"
        print(f"  {r['codice']:<14} {r['titolo'][:50]:<50} {camera_act:<12} {r['confidence']}")

    exact   = sum(1 for r in results if r["confidence"] == "exact")
    ambig   = sum(1 for r in results if r["confidence"] == "ambiguous")
    nomatch = sum(1 for r in results if r["confidence"] == "no_match")
    print(f"\n  exact: {exact}   ambiguous: {ambig}   no_match: {nomatch}")

    # --- save ---
    month_tag = month_prefix.replace("-", "")
    out_file = output_dir / f"batch_matching_{month_tag}.json"
    out_file.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\n  ✓ Saved: {out_file}")
    print("=" * 70)


if __name__ == "__main__":
    main()
