# OAI-PMH Metadata Harvester for UPI Repository

Python scripts for harvesting and analysing Dublin Core metadata records from the [UPI Institutional Repository](https://repository.upi.edu) via OAI-PMH protocol.

These scripts support the research methodology described in:

> **The Metadata Quality Paradox in Institutional ETD Repositories: Structural Patterns from 92,701 Dublin Core Records**

## Data Source

- **Repository**: UPI Repository (repository.upi.edu), EPrints 3.4.6
- **OAI-PMH Endpoint**: `https://repository.upi.edu/cgi/oai2`
- **Metadata Format**: `oai_dc` (Dublin Core)
- **Total Records**: 92,701

## Scripts

| Script | Description |
|---|---|
| `harvester.py` | OAI-PMH harvester with resumption token support |
| `analysis.py` | Three-tier metadata quality analysis (Bruce & Hillmann, 2004) |
| `sampling.py` | Stratified random sampling by publication year |

## Installation

```bash
git clone https://github.com/upi-metadata-quality/oai-pmh-harvester.git
cd oai-pmh-harvester
pip install -r requirements.txt
```

## Usage

### Step 1: Harvest all records

```bash
python harvester.py --output output --format both
```

This harvests all records from the UPI Repository OAI-PMH endpoint and saves them as CSV and JSON files in the `output/` directory.

### Step 2: Draw stratified sample

```bash
python sampling.py --input output/all_harvested_records.csv \
    --output output/stratified_sample.csv \
    --per-year 100 --start 2015 --end 2025 --seed 42
```

Draws 100 records per year (2015-2025) using stratified random sampling with a fixed seed for reproducibility.

### Step 3: Run three-tier analysis

```bash
python analysis.py --input output/all_harvested_records.csv \
    --output output/three_tier_analysis.json
```

Performs the complete Bruce & Hillmann (2004) three-tier analysis:

- **Tier 1 (Completeness)**: Field population rates for all 15 DC elements
- **Tier 2 (Consistency)**: ISO 8601 date compliance, author name patterns, title casing
- **Tier 3 (Descriptive Richness)**: Abstract availability, contributor/advisor, persistent identifiers

## Analytical Framework

The three-tier framework is adapted from Bruce & Hillmann (2004):

| Tier | Dimension | Metrics |
|---|---|---|
| 1 | Completeness | Percentage of records with non-empty values per element |
| 2 | Consistency | Date format (ISO 8601), author names (full vs placeholder), title casing |
| 3 | Descriptive Richness | Abstracts, advisor info, DOI/ORCID, language, degree type |

### Placeholder Name Detection

Author names are classified as placeholder if `dc:creator` contains:
- A dash after a comma (e.g., `Suherman, -`)
- Repetition of the family name (e.g., `Suherman, Suherman`)
- A single character after a comma (e.g., `Suherman, S`)

## Output Files

| File | Description |
|---|---|
| `all_harvested_records.csv` | Complete harvest (all records, 19 columns) |
| `all_harvested_records.json` | Complete harvest in JSON format |
| `harvest_report.json` | Harvest metadata and year distribution |
| `stratified_sample.csv` | Stratified sample (100 per year) |
| `three_tier_analysis.json` | Full three-tier analysis results |

## Requirements

- Python 3.8+
- `requests` >= 2.28.0
- `lxml` >= 4.9.0

## License

MIT License. See [LICENSE](LICENSE) for details.

## Citation

If you use these scripts in your research, please cite the associated article and this repository.
