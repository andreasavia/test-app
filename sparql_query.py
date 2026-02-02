#!/usr/bin/env python3
"""SPARQL Query Script - Run SPARQL queries against endpoints."""

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


def main():
    # Example: Query DBpedia for programming languages
    endpoint = "https://dbpedia.org/sparql"

    query = """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?language ?name
    WHERE {
        ?language a dbo:ProgrammingLanguage .
        ?language rdfs:label ?name .
        FILTER (lang(?name) = 'en')
    }
    LIMIT 10
    """

    print(f"Querying: {endpoint}\n")
    print("Query:")
    print(query)
    print("\nResults:")
    print("=" * 50)

    results = run_query(endpoint, query)
    print_results(results)


if __name__ == "__main__":
    main()
