#!/usr/bin/env python3
"""
ricerca_normattiva.py — Search Italian norms by year/month via ricerca/avanzata.

Usage:
  python ricerca_normattiva.py 2026 1
"""

import argparse
import csv
import json
import html as html_module
import re
import requests
from pathlib import Path
from datetime import datetime

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS = {"Content-Type": "application/json"}
OUTPUT_DIR = Path("normattiva")
VAULT_DIR = Path("vault")
NORMATTIVA_SITE = "https://www.normattiva.it"

# denominazioneAtto  →  segmento URN di normattiva.it
URN_TIPO = {
    "COSTITUZIONE":                                 "costituzione",
    "DECRETO":                                      "decreto",
    "DECRETO DEL CAPO DEL GOVERNO":                 "decreto:capo:governo",
    "DECRETO DEL CAPO DEL GOVERNO, PRIMO MINISTRO SEGRETARIO DI STATO": "decreto:capo:governo:primo-ministro-segretario-di-stato",
    "DECRETO DEL CAPO PROVVISORIO DELLO STATO":     "decreto:capo-provvisorio:stato",
    "DECRETO DEL DUCE":                             "decreto:duce",
    "DECRETO DEL DUCE DEL FASCISMO, CAPO DEL GOVERNO": "decreto:duce:fascismo:capo:governo",
    "DECRETO DEL PRESIDENTE DEL CONSIGLIO DEI MINISTRI": "decreto:presidente:consiglio-dei-ministri",
    "DECRETO DEL PRESIDENTE DELLA REPUBBLICA":      "decreto:presidente:repubblica",
    "DECRETO-LEGGE":                                "decreto-legge",
    "DECRETO-LEGGE LUOGOTENENZIALE":                "decreto-legge-luogotenenziale",
    "DECRETO LEGISLATIVO":                          "decreto-legislativo",
    "DECRETO LEGISLATIVO DEL CAPO PROVVISORIO DELLO STATO": "decreto-legislativo:capo-provvisorio:stato",
    "DECRETO LEGISLATIVO LUOGOTENENZIALE":          "decreto-legislativo-luogotenenziale",
    "DECRETO LEGISLATIVO PRESIDENZIALE":            "decreto-legislativo-presidenziale",
    "DECRETO LUOGOTENENZIALE":                      "decreto-luogotenenziale",
    "DECRETO MINISTERIALE":                         "decreto-ministeriale",
    "DECRETO PRESIDENZIALE":                        "decreto-presidenziale",
    "DECRETO REALE":                                "decreto-reale",
    "DELIBERAZIONE":                                "deliberazione",
    "DETERMINAZIONE DEL COMMISSARIO PER LE FINANZE": "determinazione:commissario:finanze",
    "DETERMINAZIONE DEL COMMISSARIO PER LA PRODUZIONE BELLICA": "determinazione:commissario:produzione-bellica",
    "DETERMINAZIONE INTERCOMMISSARIALE":            "determinazione-intercommissariale",
    "LEGGE":                                        "legge",
    "LEGGE COSTITUZIONALE":                         "legge-costituzionale",
    "ORDINANZA":                                    "ordinanza",
    "REGIO DECRETO":                                "regio-decreto",
    "REGIO DECRETO-LEGGE":                          "regio-decreto-legge",
    "REGIO DECRETO LEGISLATIVO":                    "regio-decreto-legislativo",
    "REGOLAMENTO":                                  "regolamento",
}

# One column per approfondimento type (order preserved in CSV)
APPROFONDIMENTO_COLUMNS = [
    "atti_aggiornati",
    "atti_correlati",
    "lavori_preparatori",
    "aggiornamenti_atto",
    "note_atto",
    "relazioni",
    "aggiornamenti_titolo",
    "aggiornamenti_struttura",
    "atti_parlamentari",
    "atti_attuativi",
]

# display text on the N2Ls page (lowercase)  →  column name
TEXT_TO_COLUMN = {
    "atti aggiornati":              "atti_aggiornati",
    "atti correlati":               "atti_correlati",
    "lavori preparatori":           "lavori_preparatori",
    "aggiornamenti all'atto":       "aggiornamenti_atto",
    "note atto":                    "note_atto",
    "relazioni":                    "relazioni",
    "aggiornamenti al titolo":      "aggiornamenti_titolo",
    "aggiornamenti alla struttura": "aggiornamenti_struttura",
    "atti parlamentari":            "atti_parlamentari",
    "atti attuativi":               "atti_attuativi",
}


def normattiva_uri(atto: dict) -> str:
    """Build the normattiva.it N2Ls URI for an atto, or empty string if type unknown."""
    tipo = URN_TIPO.get(atto.get("denominazioneAtto", ""))
    if not tipo:
        return ""
    data = atto.get("dataEmanazione", "")[:10]   # "2026-01-03T…" → "2026-01-03"
    numero = atto.get("numeroProvvedimento", "")
    if not data or not numero:
        return ""
    return f"https://www.normattiva.it/uri-res/N2Ls?urn:nir:stato:{tipo}:{data};{numero}"


def extract_links(html):
    """Extract normattiva / senato / camera links from an approfondimento HTML fragment."""
    links = []
    for m in re.finditer(r'href="([^"]*)"', html):
        href = m.group(1).replace("&amp;", "&")
        if href.startswith("/atto/"):
            href = NORMATTIVA_SITE + href
        if any(x in href for x in ("caricaDettaglioAtto", "senato.it", "camera.it")):
            if href not in links:
                links.append(href)
    return links


def fetch_approfondimenti(session, uri):
    """Load the N2Ls page, find active approfondimento endpoints, fetch and parse links.
    Returns dict: {column_name: "link1; link2; ...", "gu_link": "..."} for all APPROFONDIMENTO_COLUMNS."""
    result = {col: "" for col in APPROFONDIMENTO_COLUMNS}
    result["gu_link"] = ""

    try:
        resp = session.get(uri, timeout=30)
        resp.raise_for_status()
    except Exception:
        return result

    # Extract GU link (gazzettaufficiale.it)
    gu_match = re.search(r'href="(https?://www\.gazzettaufficiale\.it/[^"]+)"', resp.text)
    if gu_match:
        result["gu_link"] = gu_match.group(1).replace("&amp;", "&")

    # Find every <a> that has a data-href; match its text to a column
    for m in re.finditer(r'<a\s[^>]*data-href="([^"]+)"[^>]*>\s*(.*?)\s*</a>', resp.text, re.DOTALL):
        data_href = m.group(1).replace("&amp;", "&")
        text = html_module.unescape(re.sub(r'\s+', ' ', m.group(2)).strip().lower())

        col = TEXT_TO_COLUMN.get(text)
        if not col:
            continue

        try:
            sub = session.get(NORMATTIVA_SITE + data_href, timeout=30)
            sub.raise_for_status()
        except Exception:
            continue
        if "Sessione Scaduta" in sub.text:
            continue

        links = extract_links(sub.text)
        if links:
            result[col] = "\n".join(links)

    return result


def ricerca_avanzata(anno: int, mese: int, pagina: int = 1, per_pagina: int = 100) -> dict:
    """POST ricerca/avanzata filtrata per anno e mese di emanazione."""
    payload = {
        "annoProvvedimento": anno,
        "meseProvvedimento": mese,
        "paginazione": {
            "paginaCorrente": str(pagina),
            "numeroElementiPerPagina": str(per_pagina),
        },
    }
    resp = requests.post(f"{BASE_URL}/ricerca/avanzata", json=payload, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()


def save_csv(atti: list, path: Path) -> None:
    if not atti:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=atti[0].keys(), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(atti)
    print(f"  CSV:  {path} ({len(atti)} rows)")


def save_json(data, path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  JSON: {path}")


def save_markdown(atti: list, vault_dir: Path) -> None:
    """Save each atto as a markdown file for Obsidian, organized by year/month/number."""
    if not atti:
        return
    vault_dir.mkdir(parents=True, exist_ok=True)

    for atto in atti:
        codice = atto.get("codiceRedazionale", "unknown")
        descrizione = atto.get("descrizioneAtto", codice)
        titolo = atto.get("titoloAtto", "").strip().strip("[]").strip()
        numero_provv = atto.get("numeroProvvedimento", "0")
        tipo = atto.get("denominazioneAtto", "")
        data_gu = atto.get("dataGU", "")
        numero_gu = atto.get("numeroGU", "")
        data_emanazione = atto.get("dataEmanazione", "")[:10]
        uri = atto.get("normattiva_uri", "")

        # Parse year/month from dataEmanazione
        try:
            eman_date = datetime.strptime(data_emanazione, "%Y-%m-%d")
            year = str(eman_date.year)
            month = f"{eman_date.month:02d}"
        except ValueError:
            year = "unknown"
            month = "00"

        # Create folder: vault/YYYY/MM/numero/
        norm_dir = vault_dir / year / month / str(numero_provv)
        norm_dir.mkdir(parents=True, exist_ok=True)

        # Main markdown file
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', descrizione)
        filepath = norm_dir / f"{safe_filename}.md"

        lines = []
        # YAML frontmatter only
        lines.append("---")
        lines.append(f"codice-redazionale: {codice}")
        lines.append(f"tipo: {tipo}")
        lines.append(f"numero-atto: {numero_provv}")
        lines.append(f"data-emanazione: {data_emanazione}")
        lines.append(f"data-gu: {data_gu}")
        lines.append(f"numero-gu: {numero_gu}")
        if uri:
            lines.append(f"normattiva-urn: {uri}")
        # GU link extracted from page
        gu_link = atto.get("gu_link", "")
        if gu_link:
            lines.append(f"gu-link: {gu_link}")
        lines.append(f"titolo-atto: \"{titolo}\"")
        lines.append(f"descrizione-atto: \"{descrizione}\"")
        # Add all approfondimenti as metadata
        for col in APPROFONDIMENTO_COLUMNS:
            content = atto.get(col, "")
            if content:
                col_name = col.replace("_", "-")
                lines.append(f"{col_name}:")
                for link in content.split("\n"):
                    if link.strip():
                        lines.append(f"  - {link.strip()}")
        lines.append("---")

        with filepath.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    print(f"  Vault: {vault_dir}/ ({len(atti)} norms)")


def main():
    parser = argparse.ArgumentParser(
        description="Search norms on Normattiva by year and month.",
        epilog="Output is saved to normattiva/ and vault/",
    )
    parser.add_argument("anno", type=int, help="Year (e.g. 2026)")
    parser.add_argument("mese", type=int, help="Month (1-12)")
    args = parser.parse_args()

    # Validate
    if not (1 <= args.mese <= 12):
        parser.error(f"mese must be 1-12, got: {args.mese}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_range = f"{args.anno}_{args.mese:02d}"

    print("=" * 60)
    print(f"Ricerca normattiva: {args.anno}/{args.mese:02d}")
    print("=" * 60 + "\n")

    # Paginate all results
    atti = []
    pagina = 1
    while True:
        print(f"  Pagina {pagina}...")
        results = ricerca_avanzata(args.anno, args.mese, pagina=pagina)
        batch = results.get("listaAtti", [])
        if not batch:
            break
        atti.extend(batch)
        print(f"    {len(batch)} risultati")
        pagina += 1

    # Enrich with normattiva.it URI
    for atto in atti:
        atto["normattiva_uri"] = normattiva_uri(atto)

    print(f"\n  Totale norme: {len(atti)}\n")

    # Fetch approfondimenti for each atto
    print("[Fetching approfondimenti]")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    for i, atto in enumerate(atti):
        uri = atto.get("normattiva_uri")
        if not uri:
            for col in APPROFONDIMENTO_COLUMNS:
                atto[col] = ""
            continue
        print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}...", end=" ", flush=True)
        appro = fetch_approfondimenti(session, uri)
        atto.update(appro)
        populated = [col for col in APPROFONDIMENTO_COLUMNS if appro[col]]
        print(f"{', '.join(populated) if populated else 'nessuno'}")

    # Save
    print("[Saving]")
    save_json({"listaAtti": atti}, OUTPUT_DIR / f"ricerca_{safe_range}_raw_{timestamp}.json")
    save_csv(atti, OUTPUT_DIR / f"ricerca_{safe_range}_{timestamp}.csv")
    save_markdown(atti, VAULT_DIR)

    # Preview
    if atti:
        print(f"\n  Prime 10 norme:")
        print(f"  {'codice':<14} {'dataGU':<12} {'descrizione':<45} {'approfondimenti (colonne nel CSV)'}")
        print(f"  {'-'*14} {'-'*12} {'-'*45} {'-'*60}")
        for atto in atti[:10]:
            populated = [col for col in APPROFONDIMENTO_COLUMNS if atto.get(col)]
            print(f"  {atto.get('codiceRedazionale', ''):<14} "
                  f"{atto.get('dataGU', ''):<12} "
                  f"{atto.get('descrizioneAtto', ''):<45} "
                  f"{', '.join(populated)}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
