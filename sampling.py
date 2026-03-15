#!/usr/bin/env python3
"""Stratified Random Sampling for Metadata Audit.

Draws a stratified random sample from harvested OAI-PMH records,
stratified by publication year. Designed to support the quantitative
strand of the convergent mixed-methods research design.

Usage:
    python sampling.py --input output/all_harvested_records.csv \
        --per-year 100 --start 2015 --end 2025

Author: upi-metadata-quality
License: MIT
"""

import csv
import random
import argparse
import logging
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_records(filepath):
    """Load records from CSV file."""
    records = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    logger.info("Loaded %d records from %s", len(records), filepath)
    return records


def stratified_sample(records, per_year=100, start_year=2015,
                      end_year=2025, seed=42):
    """Draw a stratified random sample by year.

    Parameters
    ----------
    records : list of dict
        Full set of harvested records.
    per_year : int
        Number of records to sample per year stratum (default: 100).
    start_year : int
        First year to include (default: 2015).
    end_year : int
        Last year to include (default: 2025).
    seed : int
        Random seed for reproducibility (default: 42).

    Returns
    -------
    list of dict
        Sampled records.
    dict
        Sampling report with counts per stratum.
    """
    random.seed(seed)

    by_year = defaultdict(list)
    for r in records:
        year_str = r.get("year", "").strip()
        if not year_str:
            continue
        try:
            year = int(year_str)
        except ValueError:
            continue
        if start_year <= year <= end_year:
            by_year[year].append(r)

    sample = []
    report = {}

    for year in range(start_year, end_year + 1):
        population = by_year.get(year, [])
        pop_size = len(population)

        if pop_size == 0:
            logger.warning("Year %d: no records found. Skipping.", year)
            report[str(year)] = {
                "population": 0,
                "sampled": 0,
                "note": "no records available",
            }
            continue

        n = min(per_year, pop_size)
        selected = random.sample(population, n)
        sample.extend(selected)

        report[str(year)] = {
            "population": pop_size,
            "sampled": n,
        }
        logger.info(
            "Year %d: sampled %d from %d records", year, n, pop_size
        )

    logger.info("Total sampled: %d records", len(sample))
    return sample, report


def save_sample(sample, filepath):
    """Save the stratified sample to CSV."""
    if not sample:
        logger.warning("No records to save.")
        return

    fieldnames = sample[0].keys()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sample)

    logger.info("Saved %d sampled records to %s", len(sample), filepath)


def main():
    parser = argparse.ArgumentParser(
        description="Stratified random sampling for metadata audit"
    )
    parser.add_argument(
        "--input", "-i", required=True,
        help="Path to full harvested CSV file"
    )
    parser.add_argument(
        "--output", "-o", default="output/stratified_sample.csv",
        help="Output CSV file path"
    )
    parser.add_argument(
        "--per-year", type=int, default=100,
        help="Records per year stratum (default: 100)"
    )
    parser.add_argument(
        "--start", type=int, default=2015,
        help="Start year (default: 2015)"
    )
    parser.add_argument(
        "--end", type=int, default=2025,
        help="End year (default: 2025)"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    args = parser.parse_args()

    records = load_records(args.input)
    sample, report = stratified_sample(
        records,
        per_year=args.per_year,
        start_year=args.start,
        end_year=args.end,
        seed=args.seed,
    )

    save_sample(sample, args.output)

    print("\nSampling Report:")
    print("-" * 40)
    total_pop = 0
    total_samp = 0
    for year, info in sorted(report.items()):
        pop = info["population"]
        samp = info["sampled"]
        total_pop += pop
        total_samp += samp
        note = info.get("note", "")
        if note:
            print(f"  {year}: {samp:4d} / {pop:6d}  ({note})")
        else:
            print(f"  {year}: {samp:4d} / {pop:6d}")
    print("-" * 40)
    print(f"  Total: {total_samp:4d} / {total_pop:6d}")


if __name__ == "__main__":
    main()
