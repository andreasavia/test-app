#!/usr/bin/env python3
"""SPARQL Query Script - Run SPARQL queries against endpoints."""

import csv
import re
import requests
from datetime import datetime
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON

NORMATTIVA_LINK_RE = re.compile(
    r'http://www\.normattiva\.it/uri-res/N2Ls\?urn:nir:stato:[^"\'<>\s]+'
)


def run_query(endpoint: str, query: str) -> dict:
    """Execute a SPARQL query against the given endpoint."""
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


def print_results(results: dict) -> None:
    """Print query results in a readable format."""
    bindings = results.get("results", {}).get("bindings", [])

    if not bindings:
        print("No results found.")
        return

    # Get column headers
    headers = list(bindings[0].keys())
    print(" | ".join(headers))
    print("-" * (len(" | ".join(headers)) + 10))

    # Print rows
    for row in bindings:
        values = [row.get(h, {}).get("value", "") for h in headers]
        print(" | ".join(values))


def save_to_csv(results: dict, output_path: Path) -> None:
    """Save query results to a CSV file."""
    bindings = results.get("results", {}).get("bindings", [])

    if not bindings:
        print("No results to save.")
        return

    # Get column headers
    headers = list(bindings[0].keys())

    # Write to CSV
    with output_path.open('w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

        for row in bindings:
            values = [row.get(h, {}).get("value", "") for h in headers]
            writer.writerow(values)

    print(f"\nResults saved to: {output_path}")


def fetch_normattiva_links(url: str) -> list[str]:
    """Fetch a camera.it page and extract all normattiva.it N2Ls URIs."""
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        return list(dict.fromkeys(NORMATTIVA_LINK_RE.findall(r.text)))  # unique, order-preserved
    except Exception as e:
        print(f"[warn] {e}")
        return []


def main():
    # Query Italian Chamber of Deputies for approved acts from 19th Legislature (Nov 2025)
    endpoint = "http://dati.camera.it/sparql"

    query = """
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?atto ?numero ?iniziativa ?presentazione ?titolo ?isReferencedBy (MAX(?dataApprovazione) AS ?dataApprovazione) {
        {
            SELECT DISTINCT ?atto {
                ?atto a ocd:atto;
                    ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>;
                    ocd:rif_statoIter ?statoIter .
                ?statoIter dc:title ?fase ; dc:date ?dataIter .
                FILTER(CONTAINS(LCASE(?fase), "approvato"))
                FILTER(REGEX(?dataIter, "^202510"))
            }
        }
        ?atto ocd:iniziativa ?iniziativa;
            dc:identifier ?numero;
            dc:date ?presentazione;
            dc:title ?titolo .
        OPTIONAL { ?atto dcterms:isReferencedBy ?isReferencedBy . }
        OPTIONAL {
            ?votazione a ocd:votazione; ocd:rif_attoCamera ?atto;
                ocd:approvato "1"^^xsd:integer;
                dc:date ?dataApprovazione .
        }
    } GROUP BY ?atto ?numero ?iniziativa ?presentazione ?titolo ?isReferencedBy
      ORDER BY ?presentazione
    """

    print(f"Querying: {endpoint}\n")
    print("Query:")
    print(query)
    print("\nExecuting query...")

    results = run_query(endpoint, query)
    bindings = results.get("results", {}).get("bindings", [])
    print(f"Total results: {len(bindings)}")

    # --- enrich each row with normattiva links from the isReferencedBy page ---
    print(f"\n  Fetching Normattiva links from Camera pages...")
    for i, b in enumerate(bindings):
        numero  = b.get("numero", {}).get("value", "")
        ref_url = b.get("isReferencedBy", {}).get("value", "")
        if ref_url:
            print(f"    [{i+1}/{len(bindings)}] {numero:<6} {ref_url}", end=" … ", flush=True)
            links = fetch_normattiva_links(ref_url)
            b["normattiva_uri"] = {"value": "; ".join(links), "type": "uri"}
            print(f"{len(links)} link(s)")
        else:
            b["normattiva_uri"] = {"value": "", "type": "literal"}

    # Create output directory
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"legislatura_19_approved_nov2025_{timestamp}.csv"

    # Save results to CSV
    save_to_csv(results, output_file)


if __name__ == "__main__":
    main()
