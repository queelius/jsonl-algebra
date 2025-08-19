# Scripts

Utility scripts for jsonl-algebra development and testing.

## generate_dataset.py

A comprehensive data generation script that creates realistic JSONL datasets for testing.

### Features
- Generates realistic person records with:
  - Names (using Faker library)
  - Ages with realistic distribution
  - Addresses (cities, states)
  - Timestamps
  - Nested attributes
  - Relationships between records

### Usage

```bash
# Generate default dataset
python scripts/generate_dataset.py

# Generate with specific parameters (requires checking script for available options)
python scripts/generate_dataset.py --records 1000 --output test_data/large_dataset.jsonl
```

### Dependencies
- Requires `faker` library for realistic data generation
- Install with: `pip install faker`

### Output Format
The script generates JSONL files with configurable schemas suitable for testing various jsonl-algebra operations including joins, groupby, and nested field access.