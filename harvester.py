#!/usr/bin/env python3
"""OAI-PMH Metadata Harvester for UPI Repository.

Harvests Dublin Core metadata records from the UPI Institutional Repository
(repository.upi.edu) via OAI-PMH protocol. Supports resumption tokens for
complete harvesting of large collections.

Usage:
    python harvester.py [--output OUTPUT_DIR] [--format FORMAT]

Author: upi-metadata-quality
License: MIT
"""

import os
import csv
import json
import time
import logging
import argparse
from datetime import datetime
from lxml import etree
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL = "https://repository.upi.edu/cgi/oai2"
METADATA_PREFIX = "oai_dc"
OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DC_NS = "http://purl.org/dc/elements/1.1/"
NAMESPACES = {
    "oai": OAI_NS,
    "dc": DC_NS,
}

DC_ELEMENTS = [
    "title", "creator", "subject", "description", "publisher",
    "contributor", "date", "type", "format", "identifier",
    "source", "language", "relation", "coverage", "rights",
]


def harvest_records(base_url=BASE_URL, metadata_prefix=METADATA_PREFIX,
                    from_date=None, until_date=None, set_spec=None,
                    delay=1.0):
    """Harvest all records from an OAI-PMH endpoint.

    Handles resumption tokens automatically for iterating through
    the complete collection.

    Parameters
    ----------
    base_url : str
        OAI-PMH base URL.
    metadata_prefix : str
        Metadata format (default: oai_dc).
    from_date : str, optional
        Start date filter (YYYY-MM-DD).
    until_date : str, optional
        End date filter (YYYY-MM-DD).
    set_spec : str, optional
        OAI set to harvest.
    delay : float
        Seconds to wait between requests (default: 1.0).

    Yields
    ------
    dict
        Parsed metadata record.
    """
    params = {
        "verb": "ListRecords",
        "metadataPrefix": metadata_prefix,
    }
    if from_date:
        params["from"] = from_date
    if until_date:
        params["until"] = until_date
    if set_spec:
        params["set"] = set_spec

    total = 0
    resumption_token = None

    while True:
        if resumption_token:
            req_params = {"verb": "ListRecords",
                          "resumptionToken": resumption_token}
        else:
            req_params = params

        try:
            response = requests.get(base_url, params=req_params, timeout=60)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error("Request failed: %s. Retrying in 10s...", e)
            time.sleep(10)
            continue

        root = etree.fromstring(response.content)

        error = root.find(".//oai:error", NAMESPACES)
        if error is not None:
            code = error.get("code", "unknown")
            if code == "noRecordsMatch":
                logger.info("No records match the given criteria.")
                return
            logger.error("OAI-PMH error [%s]: %s", code, error.text)
            return

        records = root.findall(".//oai:record", NAMESPACES)
        for record in records:
            header = record.find("oai:header", NAMESPACES)
            status = header.get("status", "")
            if status == "deleted":
                continue

            parsed = parse_record(header, record)
            if parsed:
                total += 1
                yield parsed

        if total % 1000 == 0 and total > 0:
            logger.info("Harvested %d records so far...", total)

        token_elem = root.find(".//oai:resumptionToken", NAMESPACES)
        if token_elem is not None and token_elem.text:
            resumption_token = token_elem.text.strip()
            logger.info("Resumption token received. Total so far: %d", total)
            time.sleep(delay)
        else:
            break

    logger.info("Harvesting complete. Total records: %d", total)


def parse_record(header, record):
    """Parse a single OAI-PMH record into a flat dictionary.

    Parameters
    ----------
    header : lxml.etree.Element
        OAI record header.
    record : lxml.etree.Element
        Full OAI record element.

    Returns
    -------
    dict or None
        Parsed record with all DC elements, or None if metadata missing.
    """
    oai_id = header.findtext("oai:identifier", default="", namespaces=NAMESPACES)
    datestamp = header.findtext("oai:datestamp", default="", namespaces=NAMESPACES)
    set_specs = [s.text for s in header.findall("oai:setSpec", NAMESPACES) if s.text]

    metadata = record.find(".//oai:metadata", NAMESPACES)
    if metadata is None:
        return None

    parsed = {
        "oai_identifier": oai_id,
        "datestamp": datestamp,
        "setSpecs": ";".join(set_specs),
    }

    for element in DC_ELEMENTS:
        values = metadata.findall(f".//dc:{element}", NAMESPACES)
        text_values = [v.text.strip() for v in values if v.text and v.text.strip()]
        parsed[element] = " || ".join(text_values) if text_values else ""

    parsed["year"] = extract_year(parsed.get("date", ""))

    return parsed


def extract_year(date_str):
    """Extract a four-digit year from a date string.

    Parameters
    ----------
    date_str : str
        Date string, possibly containing multiple dates separated by ||.

    Returns
    -------
    str
        Four-digit year, or empty string if not found.
    """
    import re
    for part in date_str.split(" || "):
        match = re.search(r"(\d{4})", part.strip())
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2100:
                return str(year)
    return ""


def save_csv(records, filepath):
    """Save harvested records to a CSV file.

    Parameters
    ----------
    records : list of dict
        Parsed metadata records.
    filepath : str
        Output CSV file path.
    """
    if not records:
        logger.warning("No records to save.")
        return

    fieldnames = ["oai_identifier", "datestamp"] + DC_ELEMENTS + ["setSpecs", "year"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    logger.info("Saved %d records to %s", len(records), filepath)


def save_json(records, filepath):
    """Save harvested records to a JSON file.

    Parameters
    ----------
    records : list of dict
        Parsed metadata records.
    filepath : str
        Output JSON file path.
    """
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("Saved %d records to %s", len(records), filepath)


def generate_harvest_report(records):
    """Generate a summary report of the harvest.

    Parameters
    ----------
    records : list of dict
        Parsed metadata records.

    Returns
    -------
    dict
        Harvest report with statistics.
    """
    year_dist = {}
    for r in records:
        y = r.get("year", "unknown")
        if not y:
            y = "unknown"
        year_dist[y] = year_dist.get(y, 0) + 1

    report = {
        "harvest_date": datetime.now().isoformat(),
        "base_url": BASE_URL,
        "metadata_prefix": METADATA_PREFIX,
        "total_records": len(records),
        "year_distribution": dict(sorted(year_dist.items())),
        "dc_elements_available": DC_ELEMENTS,
    }
    return report


def main():
    parser = argparse.ArgumentParser(
        description="OAI-PMH Metadata Harvester for UPI Repository"
    )
    parser.add_argument(
        "--output", "-o", default="output",
        help="Output directory (default: output)"
    )
    parser.add_argument(
        "--format", "-f", choices=["csv", "json", "both"], default="both",
        help="Output format (default: both)"
    )
    parser.add_argument(
        "--from-date", default=None,
        help="Harvest records from this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--until-date", default=None,
        help="Harvest records until this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Delay between requests in seconds (default: 1.0)"
    )
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    logger.info("Starting OAI-PMH harvest from %s", BASE_URL)
    records = list(harvest_records(
        from_date=args.from_date,
        until_date=args.until_date,
        delay=args.delay,
    ))

    if not records:
        logger.warning("No records harvested.")
        return

    if args.format in ("csv", "both"):
        save_csv(records, os.path.join(args.output, "all_harvested_records.csv"))
    if args.format in ("json", "both"):
        save_json(records, os.path.join(args.output, "all_harvested_records.json"))

    report = generate_harvest_report(records)
    report_path = os.path.join(args.output, "harvest_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info("Harvest report saved to %s", report_path)


if __name__ == "__main__":
    main()
