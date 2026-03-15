#!/usr/bin/env python3
"""Three-Tier Metadata Quality Analysis.

Implements the Bruce & Hillmann (2004) three-tier analytical framework
for assessing Dublin Core metadata quality from OAI-PMH harvested records.

Tier 1: Completeness (field population rates)
Tier 2: Consistency (date format, author names, title casing)
Tier 3: Descriptive Richness (abstract, contributor, PID availability)

Usage:
    python analysis.py --input OUTPUT_DIR/all_harvested_records.csv

Author: upi-metadata-quality
License: MIT
"""

import csv
import json
import re
import argparse
import logging
from collections import Counter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DC_ELEMENTS = [
    "title", "creator", "subject", "description", "publisher",
    "contributor", "date", "type", "format", "identifier",
    "source", "language", "relation", "coverage", "rights",
]


def load_records(filepath):
    """Load harvested records from a CSV file."""
    records = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    logger.info("Loaded %d records from %s", len(records), filepath)
    return records


def tier1_completeness(records):
    """Tier 1: Compute completeness rates for each DC element.

    Completeness is defined as the percentage of records with a
    non-empty value for each Dublin Core element.

    Parameters
    ----------
    records : list of dict

    Returns
    -------
    dict
        Element-level completeness percentages.
    """
    n = len(records)
    if n == 0:
        return {}

    results = {}
    for element in DC_ELEMENTS:
        filled = sum(1 for r in records if r.get(element, "").strip())
        results[element] = {
            "count": filled,
            "total": n,
            "percentage": round(filled / n * 100, 2),
        }

    return results


def tier2_consistency(records):
    """Tier 2: Assess consistency metrics.

    Evaluates:
    - Date format consistency (ISO 8601 compliance)
    - Author name patterns (full name vs. placeholder)
    - Title casing (ALL CAPS vs. standard)

    Parameters
    ----------
    records : list of dict

    Returns
    -------
    dict
        Consistency analysis results.
    """
    n = len(records)
    if n == 0:
        return {}

    # Date format analysis
    iso_dates = 0
    other_dates = 0
    empty_dates = 0
    for r in records:
        date_val = r.get("date", "").strip()
        if not date_val:
            empty_dates += 1
        else:
            first_date = date_val.split(" || ")[0].strip()
            if re.match(r"^\d{4}(-\d{2}(-\d{2})?)?$", first_date):
                iso_dates += 1
            else:
                other_dates += 1

    # Author name pattern analysis
    full_names = 0
    placeholder_names = 0
    empty_names = 0
    for r in records:
        creator = r.get("creator", "").strip()
        if not creator:
            empty_names += 1
        else:
            first_creator = creator.split(" || ")[0].strip()
            if is_placeholder_name(first_creator):
                placeholder_names += 1
            else:
                full_names += 1

    # Title casing analysis
    all_caps = 0
    mixed_case = 0
    empty_titles = 0
    for r in records:
        title = r.get("title", "").strip()
        if not title:
            empty_titles += 1
        else:
            first_title = title.split(" || ")[0].strip()
            if first_title == first_title.upper() and len(first_title) > 5:
                all_caps += 1
            else:
                mixed_case += 1

    dated = n - empty_dates
    named = n - empty_names
    titled = n - empty_titles

    return {
        "date_format": {
            "iso_8601": iso_dates,
            "other_format": other_dates,
            "empty": empty_dates,
            "iso_percentage": round(iso_dates / dated * 100, 2) if dated else 0,
        },
        "author_names": {
            "full_name": full_names,
            "placeholder": placeholder_names,
            "empty": empty_names,
            "full_name_percentage": round(full_names / named * 100, 2) if named else 0,
            "placeholder_percentage": round(placeholder_names / named * 100, 2) if named else 0,
        },
        "title_casing": {
            "all_caps": all_caps,
            "mixed_case": mixed_case,
            "empty": empty_titles,
            "all_caps_percentage": round(all_caps / titled * 100, 2) if titled else 0,
        },
    }


def is_placeholder_name(name):
    """Check if an author name matches a placeholder pattern.

    Placeholder patterns include:
    - Dash after comma: "Suherman, -"
    - Repeated family name: "Suherman, Suherman"
    - Single character after comma: "Suherman, S"

    Parameters
    ----------
    name : str

    Returns
    -------
    bool
    """
    if "," not in name:
        return False

    parts = name.split(",", 1)
    family = parts[0].strip()
    given = parts[1].strip() if len(parts) > 1 else ""

    if not given or given == "-":
        return True
    if given.lower() == family.lower():
        return True
    if len(given) == 1:
        return True
    if re.match(r"^[A-Z]\.$", given):
        return True

    return False


def tier3_richness(records):
    """Tier 3: Evaluate descriptive richness.

    Assesses availability of elements beyond core Dublin Core that
    are required for MODS/ETD-MS interoperability:
    - Substantive abstracts (dc:description)
    - Advisor information (dc:contributor)
    - Persistent identifiers (DOI, ORCID)
    - Degree type, language, URL identifiers

    Parameters
    ----------
    records : list of dict

    Returns
    -------
    dict
        Descriptive richness results.
    """
    n = len(records)
    if n == 0:
        return {}

    has_abstract = 0
    has_contributor = 0
    has_doi = 0
    has_orcid = 0
    has_http_url = 0
    has_language = 0
    has_type = 0

    for r in records:
        desc = r.get("description", "").strip()
        if desc and len(desc) > 50:
            has_abstract += 1

        if r.get("contributor", "").strip():
            has_contributor += 1

        identifier = r.get("identifier", "").strip()
        if "doi.org" in identifier or "10." in identifier:
            has_doi += 1
        if "orcid.org" in identifier:
            has_orcid += 1
        if "http" in identifier:
            has_http_url += 1

        if r.get("language", "").strip():
            has_language += 1
        if r.get("type", "").strip():
            has_type += 1

    return {
        "substantive_abstract": {
            "count": has_abstract,
            "percentage": round(has_abstract / n * 100, 2),
        },
        "contributor_advisor": {
            "count": has_contributor,
            "percentage": round(has_contributor / n * 100, 2),
        },
        "persistent_identifier": {
            "doi": {"count": has_doi, "percentage": round(has_doi / n * 100, 2)},
            "orcid": {"count": has_orcid, "percentage": round(has_orcid / n * 100, 2)},
            "http_url": {"count": has_http_url, "percentage": round(has_http_url / n * 100, 2)},
        },
        "language": {
            "count": has_language,
            "percentage": round(has_language / n * 100, 2),
        },
        "document_type": {
            "count": has_type,
            "percentage": round(has_type / n * 100, 2),
        },
    }


def year_distribution(records):
    """Compute year-by-year record distribution."""
    years = Counter(r.get("year", "unknown") for r in records)
    return dict(sorted(years.items()))


def run_analysis(records):
    """Run the complete three-tier analysis.

    Parameters
    ----------
    records : list of dict

    Returns
    -------
    dict
        Complete analysis results.
    """
    logger.info("Running Tier 1: Completeness analysis...")
    t1 = tier1_completeness(records)

    logger.info("Running Tier 2: Consistency analysis...")
    t2 = tier2_consistency(records)

    logger.info("Running Tier 3: Descriptive richness analysis...")
    t3 = tier3_richness(records)

    dist = year_distribution(records)

    results = {
        "total_records": len(records),
        "year_distribution": dist,
        "tier1_completeness": t1,
        "tier2_consistency": t2,
        "tier3_descriptive_richness": t3,
    }

    return results


def print_summary(results):
    """Print a human-readable summary of the analysis."""
    print("\n" + "=" * 60)
    print("THREE-TIER METADATA QUALITY ANALYSIS")
    print("Based on Bruce & Hillmann (2004) Framework")
    print("=" * 60)
    print(f"\nTotal records analysed: {results['total_records']}")

    print("\n--- TIER 1: COMPLETENESS ---")
    for elem, data in results["tier1_completeness"].items():
        print(f"  {elem:15s}: {data['percentage']:6.2f}% ({data['count']}/{data['total']})")

    print("\n--- TIER 2: CONSISTENCY ---")
    t2 = results["tier2_consistency"]
    print(f"  ISO 8601 dates:     {t2['date_format']['iso_percentage']:.2f}%")
    print(f"  Full author names:  {t2['author_names']['full_name_percentage']:.2f}%")
    print(f"  Placeholder names:  {t2['author_names']['placeholder_percentage']:.2f}%")
    print(f"  ALL CAPS titles:    {t2['title_casing']['all_caps_percentage']:.2f}%")

    print("\n--- TIER 3: DESCRIPTIVE RICHNESS ---")
    t3 = results["tier3_descriptive_richness"]
    print(f"  Substantive abstract: {t3['substantive_abstract']['percentage']:.2f}%")
    print(f"  Contributor/Advisor:  {t3['contributor_advisor']['percentage']:.2f}%")
    print(f"  DOI:                  {t3['persistent_identifier']['doi']['percentage']:.2f}%")
    print(f"  HTTP URL:             {t3['persistent_identifier']['http_url']['percentage']:.2f}%")
    print(f"  Language:             {t3['language']['percentage']:.2f}%")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Three-Tier Metadata Quality Analysis (Bruce & Hillmann, 2004)"
    )
    parser.add_argument(
        "--input", "-i", required=True,
        help="Path to harvested CSV file"
    )
    parser.add_argument(
        "--output", "-o", default="output/three_tier_analysis.json",
        help="Output JSON file path"
    )
    args = parser.parse_args()

    records = load_records(args.input)
    results = run_analysis(records)
    print_summary(results)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info("Analysis results saved to %s", args.output)


if __name__ == "__main__":
    main()
