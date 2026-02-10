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
import xml.etree.ElementTree as ET
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


def fetch_camera_metadata(session, camera_url: str) -> dict:
    """Fetch and parse metadata from a camera.it RDF endpoint.
    Returns dict with camera-atto, legislatura, natura, data-presentazione, iniziativa-dei-deputati."""
    result = {}

    # Parse URL to extract legislatura and atto number
    # URL format: http://www.camera.it/uri-res/N2Ls?urn:camera-it:parlamento:scheda.progetto.legge:camera;19.legislatura;1621
    url_match = re.search(r'(\d+)\.legislatura;(\d+)', camera_url)
    if not url_match:
        return result

    legislatura = url_match.group(1)
    atto_num = url_match.group(2)
    result["legislatura"] = legislatura
    result["camera-atto"] = f"C. {atto_num}"

    rdf_url = f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{atto_num}"
    result["camera-atto-iri"] = rdf_url

    # Request RDF/XML format explicitly
    try:
        resp = session.get(rdf_url, headers={"Accept": "application/rdf+xml"}, timeout=30)
        resp.raise_for_status()
        rdf_text = resp.text
    except Exception:
        return result

    # Parse RDF/XML
    try:
        root = ET.fromstring(rdf_text)
    except ET.ParseError:
        return result

    # Namespaces used in the RDF
    ns = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'ocd': 'http://dati.camera.it/ocd/',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'foaf': 'http://xmlns.com/foaf/0.1/',
    }

    # Find any Description element (the main one)
    atto_elem = None
    for desc in root.findall('.//rdf:Description', ns):
        about = desc.get(f"{{{ns['rdf']}}}about", "")
        if f"ac{legislatura}_{atto_num}" in about:
            atto_elem = desc
            break

    if atto_elem is None:
        # Fallback: try first Description
        descriptions = root.findall('.//rdf:Description', ns)
        if descriptions:
            atto_elem = descriptions[0]

    if atto_elem is None:
        return result

    # Extract natura (dc:type)
    tipo_elem = atto_elem.find('dc:type', ns)
    if tipo_elem is not None and tipo_elem.text:
        result["camera-natura"] = tipo_elem.text.strip()

    # Extract iniziativa (ocd:iniziativa) - Governo or Parlamentare
    iniziativa_elem = atto_elem.find('ocd:iniziativa', ns)
    if iniziativa_elem is not None and iniziativa_elem.text:
        result["camera-iniziativa"] = iniziativa_elem.text.strip()

    # Extract presentation date (dc:date) - format YYYYMMDD
    date_elem = atto_elem.find('dc:date', ns)
    if date_elem is not None and date_elem.text:
        raw_date = date_elem.text.strip()
        # Convert YYYYMMDD to readable format
        if len(raw_date) == 8 and raw_date.isdigit():
            try:
                dt = datetime.strptime(raw_date, "%Y%m%d")
                months = ["", "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
                          "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre"]
                result["camera-data-presentazione"] = f"{dt.day} {months[dt.month]} {dt.year}"
            except ValueError:
                result["camera-data-presentazione"] = raw_date
        else:
            result["camera-data-presentazione"] = raw_date

    # Extract PDF link (dc:relation)
    pdf_links = []
    for relation_elem in atto_elem.findall('dc:relation', ns):
        resource = relation_elem.get(f"{{{ns['rdf']}}}resource", "")
        if resource and resource.endswith('.pdf'):
            pdf_links.append(resource)
    if pdf_links:
        result["camera-pdf"] = pdf_links

    # Extract creator (first signer) - dc:creator contains name directly
    deputies = []
    for creator_elem in atto_elem.findall('dc:creator', ns):
        if creator_elem.text:
            name = creator_elem.text.strip()
            if not any(d["name"] == name for d in deputies):
                deputies.append({"name": name, "link": ""})

    # Get primo_firmatario URIs (handle both resource URIs and blank nodes)
    primo_uris = []
    for primo_elem in atto_elem.findall('ocd:primo_firmatario', ns):
        # Try direct resource URI first (parliamentary bills)
        resource = primo_elem.get(f"{{{ns['rdf']}}}resource", "")
        if resource:
            primo_uris.append(resource)
        else:
            # Try blank node (government bills)
            node_id = primo_elem.get(f"{{{ns['rdf']}}}nodeID", "")
            if node_id:
                persona_uri = resolve_blank_node(root, node_id, ns)
                if persona_uri:
                    primo_uris.append(persona_uri)

    # Fetch groups for all primo_firmatario
    for i, dep in enumerate(deputies):
        if i < len(primo_uris):
            group = fetch_parliamentary_group(session, primo_uris[i], ns, legislatura)
            if group:
                dep["group"] = group

    # Extract additional signers (dc:contributor contains names, ocd:altro_firmatario has URIs)
    contributors = []
    for contrib_elem in atto_elem.findall('dc:contributor', ns):
        if contrib_elem.text:
            contributors.append(contrib_elem.text.strip())

    altro_firmatari = []
    for altro_elem in atto_elem.findall('ocd:altro_firmatario', ns):
        resource = altro_elem.get(f"{{{ns['rdf']}}}resource", "")
        if resource:
            altro_firmatari.append(resource)
        else:
            # Try blank node
            node_id = altro_elem.get(f"{{{ns['rdf']}}}nodeID", "")
            if node_id:
                persona_uri = resolve_blank_node(root, node_id, ns)
                if persona_uri:
                    altro_firmatari.append(persona_uri)

    # Match contributors with their groups
    for i, name in enumerate(contributors):
        if not any(d["name"] == name for d in deputies):
            group = ""
            if i < len(altro_firmatari):
                group = fetch_parliamentary_group(session, altro_firmatari[i], ns, legislatura)
            deputies.append({"name": name, "group": group})

    if deputies:
        result["camera-firmatari"] = deputies

    # Extract relatori (rapporteurs)
    relatori_refs = []
    for rel_elem in atto_elem.findall('ocd:rif_relatore', ns):
        resource = rel_elem.get(f"{{{ns['rdf']}}}resource", "")
        if resource:
            relatori_refs.append(resource)

    if relatori_refs:
        relatori = fetch_relatori_names(session, relatori_refs, ns)
        if relatori:
            result["camera-relatori"] = relatori

    # Fetch HTML page for votazione-finale and potentially override with HTML-based firmatari
    html_text = None
    try:
        html_resp = session.get(camera_url, timeout=30)
        html_resp.raise_for_status()
        html_text = html_resp.text

        # Extract final vote
        voto_match = re.search(r'href="([^"]*votazioni[^"]*schedaVotazione[^"]*)"', html_text)
        if voto_match:
            link = voto_match.group(1).replace("&amp;", "&")
            if not link.startswith("http"):
                link = "https://www.camera.it" + link
            result["camera-votazione-finale"] = link
    except Exception:
        pass

    # For government bills, parse HTML to get ministerial roles instead of groups
    if html_text and result.get("camera-iniziativa") == "Governo":
        html_firmatari = parse_html_firmatari(html_text, legislatura)
        if html_firmatari:
            result["camera-firmatari"] = html_firmatari

    return result


def parse_html_firmatari(html: str, legislatura: str) -> list:
    """Parse firmatari from HTML page for government bills."""
    firmatari = []

    # Look for <div class="iniziativa"> for government bills
    iniziativa_match = re.search(r'<div class="iniziativa">(.*?)</div>', html, re.DOTALL)
    if not iniziativa_match:
        return firmatari

    section = iniziativa_match.group(1)

    # Extract each person with their role
    # Pattern: <a href="...idPersona=123">NAME</a></span> (<em>ROLE</em>)
    pattern = r'<a\s+href="[^"]*idPersona=(\d+)"[^>]*>([^<]+)</a>\s*</span>\s*\(<em>([^<]+)</em>\)'

    for match in re.finditer(pattern, section):
        person_id = match.group(1)
        name = re.sub(r'\s+', ' ', match.group(2)).strip()
        role = match.group(3).strip()

        firmatari.append({
            "name": name,
            "role": role,
            "link": f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx?sezione=deputati&tipoDoc=schedaDeputato&idlegislatura={legislatura}&idPersona={person_id}"
        })

    return firmatari


def fetch_relatori_names(session, relatori_refs: list, ns: dict) -> list:
    """Fetch relatore names from their RDF URIs."""
    relatori = []
    for ref in relatori_refs:
        try:
            resp = session.get(ref, headers={"Accept": "application/rdf+xml"}, timeout=10)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            # Find the Description with dc:creator (the relatore name)
            for desc in root.findall('.//rdf:Description', ns):
                creator = desc.find('dc:creator', ns)
                if creator is not None and creator.text:
                    name = creator.text.strip()
                    if name and name not in relatori:
                        relatori.append(name)
                    break
        except Exception:
            continue
    return relatori


def resolve_blank_node(root, node_id: str, ns: dict) -> str:
    """Resolve a blank node to get the persona/deputato URI."""
    for desc in root.findall('.//rdf:Description', ns):
        desc_node_id = desc.get(f"{{{ns['rdf']}}}nodeID", "")
        if desc_node_id == node_id:
            # Found the blank node, look for ocd:rif_persona
            rif_persona = desc.find('ocd:rif_persona', ns)
            if rif_persona is not None:
                resource = rif_persona.get(f"{{{ns['rdf']}}}resource", "")
                if resource:
                    return resource
    return ""


def fetch_parliamentary_group(session, person_uri: str, ns: dict, legislatura: str = "19") -> str:
    """Fetch parliamentary group abbreviation from person/deputato RDF."""
    try:
        # If persona.rdf URI, convert to deputato.rdf URI
        # persona.rdf/p50204 → deputato.rdf/d50204_19
        if 'persona.rdf' in person_uri:
            person_match = re.search(r'/p(\d+)', person_uri)
            if person_match:
                person_id = person_match.group(1)
                person_uri = f"http://dati.camera.it/ocd/deputato.rdf/d{person_id}_{legislatura}"

        resp = session.get(person_uri, headers={"Accept": "application/rdf+xml"}, timeout=10)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)

        # Look for gruppo parlamentare reference
        gruppo_uri = None
        for desc in root.findall('.//rdf:Description', ns):
            gruppo_elem = desc.find('ocd:rif_gruppoParlamentare', ns)
            if gruppo_elem is not None:
                gruppo_uri = gruppo_elem.get(f"{{{ns['rdf']}}}resource", "")
                if gruppo_uri:
                    break

        if not gruppo_uri:
            return ""

        # Fetch the group RDF to get the abbreviation
        gruppo_resp = session.get(gruppo_uri, headers={"Accept": "application/rdf+xml"}, timeout=10)
        gruppo_resp.raise_for_status()
        gruppo_root = ET.fromstring(gruppo_resp.text)

        # Find the main Description for this group
        for desc in gruppo_root.findall('.//rdf:Description', ns):
            about = desc.get(f"{{{ns['rdf']}}}about", "")
            if about == gruppo_uri:
                # Try ocd:sigla first
                sigla = desc.find('ocd:sigla', ns)
                if sigla is not None and sigla.text:
                    return sigla.text.strip()

                # Parse abbreviation from rdfs:label
                # Format: "FULL NAME (ABBREVIATION) (DATE"
                label = desc.find('rdfs:label', ns)
                if label is not None and label.text:
                    label_text = label.text.strip()
                    # Extract abbreviation from parentheses
                    match = re.search(r'\(([A-Z\-]+)\)\s*\(', label_text)
                    if match:
                        return match.group(1)
                    # Fallback: return full label
                    return label_text
    except Exception:
        pass
    return ""


def build_scheda_link(resource_uri: str, legislatura: str) -> str:
    """Build scheda deputato link from resource URI."""
    # Extract person ID from URI like:
    # - http://dati.camera.it/ocd/deputato.rdf/d50204_19 (deputato)
    # - http://dati.camera.it/ocd/persona.rdf/p50204 (persona)

    # Try deputato format: d{personId}_{legislatura}
    match = re.search(r'/d(\d+)_\d+', resource_uri)
    if match:
        person_id = match.group(1)
        return f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx?sezione=deputati&tipoDoc=schedaDeputato&idlegislatura={legislatura}&idPersona={person_id}"

    # Try persona format: p{personId}
    match = re.search(r'/p(\d+)', resource_uri)
    if match:
        person_id = match.group(1)
        return f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx?sezione=deputati&tipoDoc=schedaDeputato&idlegislatura={legislatura}&idPersona={person_id}"

    return resource_uri


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
        # Build normattiva-link
        if data_gu and codice:
            normattiva_link = f"https://www.normattiva.it/atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta={data_gu}&atto.codiceRedazionale={codice}"
            lines.append(f"normattiva-link: {normattiva_link}")
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

        # Camera metadata (from lavori preparatori RDF)
        if atto.get("legislatura"):
            lines.append(f"camera-legislatura: {atto.get('legislatura')}")
        if atto.get("camera-atto"):
            lines.append(f"camera-atto: {atto.get('camera-atto')}")
        if atto.get("camera-atto-iri"):
            lines.append(f"camera-atto-iri: {atto.get('camera-atto-iri')}")
        if atto.get("camera-natura"):
            lines.append(f"camera-natura: \"{atto.get('camera-natura')}\"")
        if atto.get("camera-iniziativa"):
            lines.append(f"camera-iniziativa: \"{atto.get('camera-iniziativa')}\"")
        if atto.get("camera-data-presentazione"):
            lines.append(f"camera-data-presentazione: \"{atto.get('camera-data-presentazione')}\"")
        if atto.get("camera-pdf"):
            lines.append("camera-pdf:")
            for pdf in atto.get("camera-pdf", []):
                lines.append(f"  - {pdf}")
        if atto.get("camera-firmatari"):
            lines.append("camera-firmatari:")
            for dep in atto.get("camera-firmatari", []):
                if dep.get('role'):
                    # Government bill: show ministerial role
                    lines.append(f"  - \"{dep['name']} - {dep['role']}\"")
                elif dep.get('group'):
                    # Parliamentary bill: show parliamentary group
                    lines.append(f"  - \"{dep['name']} - {dep['group']}\"")
                else:
                    lines.append(f"  - \"{dep['name']}\"")
        if atto.get("camera-relatori"):
            lines.append("camera-relatori:")
            for rel in atto.get("camera-relatori", []):
                lines.append(f"  - \"{rel}\"")
        if atto.get("camera-votazione-finale"):
            lines.append(f"camera-votazione-finale: {atto.get('camera-votazione-finale')}")

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

    # Fetch camera.it metadata from lavori_preparatori
    print("[Fetching camera.it metadata]")
    for i, atto in enumerate(atti):
        lavori = atto.get("lavori_preparatori", "")
        camera_links = [l for l in lavori.split("\n") if "camera.it" in l]
        if camera_links:
            print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}...", end=" ", flush=True)
            camera_meta = fetch_camera_metadata(session, camera_links[0])
            atto.update(camera_meta)
            print(f"legislatura {camera_meta.get('legislatura', '?')}, {camera_meta.get('camera-atto', '?')}")
        else:
            print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}... no camera.it link")

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
