#!/usr/bin/env python3
"""SPARQL Query Script - Run SPARQL queries against endpoints."""

import csv
from datetime import datetime
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON


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


def main():
    # Query Italian Chamber of Deputies for approved acts from 19th Legislature (Nov 2025)
    endpoint = "http://dati.camera.it/sparql"

    query = """
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    select distinct ?atto ?numero ?iniziativa ?presentazione ?titolo ?fase ?dataIter ?dataApprovazione {
        ?atto a ocd:atto;
            ocd:iniziativa ?iniziativa;
            dc:identifier ?numero;
            ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>;
            dc:date ?presentazione;
            dc:title ?titolo; ocd:rif_statoIter ?statoIter .
        ?statoIter  dc:title ?fase ; dc:date ?dataIter .
        FILTER(CONTAINS(LCASE(?fase), "approvato"))
        FILTER(REGEX(?dataIter, "^202511"))
        OPTIONAL{
            ?votazione a ocd:votazione; ocd:rif_attoCamera ?atto;
                ocd:approvato "1"^^xsd:integer;
                dc:date ?dataApprovazione.
        }
    } ORDER BY ?presentazione ?dataIter
    """

    print(f"Querying: {endpoint}\n")
    print("Query:")
    print(query)
    print("\nExecuting query...")

    results = run_query(endpoint, query)

    # Create output directory
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"legislatura_19_approved_nov2025_{timestamp}.csv"

    # Save results to CSV
    save_to_csv(results, output_file)

    # Print summary
    bindings = results.get("results", {}).get("bindings", [])
    print(f"Total results: {len(bindings)}")


if __name__ == "__main__":
    main()
