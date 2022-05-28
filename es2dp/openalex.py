import gzip
import json
from typing import Dict, List

OA_SAMPLE_DATA = "./data/openalex/works/sample.jsonl.gz"


def inverted_index_to_string(inverted_index: Dict[str, List[int]]) -> str:
    """Reconstruct text from inverted index"""
    idx_word = []
    for word, indices in inverted_index.items():
        for idx in indices:
            idx_word.append([idx, word])
    idx_word.sort()
    return ' '.join((word for _, word in idx_word))


def extract_authors(authorships: List[Dict]) -> List[str]:
    """Return list of author display names from authorship"""
    authors = []
    for author in authorships:
        if author.get("author") and author["author"].get("display_name"):
            authors.append(author["author"]["display_name"])
    return authors


def extract_urls(record: Dict) -> List[str]:
    urls = []
    # Open Access
    if record.get("open_access") and record["open_access"].get("oa_url"):
        urls.append(record["open_access"]["oa_url"])
    # Host Venue
    if record.get("host_venue") and record["host_venue"].get("url"):
        urls.append(record["host_venue"]["url"])
    # Alt Host Venues
    for alt_host in record.get("alternate_host_venues", []):
        if alt_host.get("url"):
            urls.append(alt_host["url"])
    return urls


def main():
    """
    Exploring OpenAlex sample data and parsing
    """
    with gzip.open(OA_SAMPLE_DATA, "r") as file:
        for line in file:
            record = json.loads(line)

            if record.get("abstract_inverted_index"):
                abstract = inverted_index_to_string(record["abstract_inverted_index"])
            else:
                # Drop records without Abstracts
                continue

            if record.get("title"):
                title = inverted_index_to_string(record["title"])
            else:
                # Drop records without Titles
                continue

            authors = extract_authors(record.get("authorships"))
            pub_year = record.get("publication_year")
            doi = record.get("publication_year")
            cited_by_count = record.get("cited_by_count")
            urls = extract_urls(record)
            # ...


if __name__ == "__main__":
    main()
