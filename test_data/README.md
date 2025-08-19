# Test Data

This directory contains sample JSONL files used for testing and development of jsonl-algebra operations.

## Files

### Basic Test Data
- `test_data1.jsonl` - Simple id/name records with duplicates
- `test_data2.jsonl` - Additional id/name records for set operations
- `temp_union.jsonl` - Union operation test data
- `data.jsonl` - Records with id, name, and age fields
- `users.jsonl` - Basic user records (id, name)
- `orders.jsonl` - Order records with user relationships

### Nested Structure Examples
- `nested-names.jsonl` - Deeply nested person records (person.name.first/last)
- `users-nested.jsonl` - Users with nested attributes

### Complex Datasets
- `people.jsonl` - Large dataset with realistic person records (52KB)
- `companies.jsonl` - Company records for join operations

### Individual JSON Files (json_files/)
- `item-0.json` through `item-9.json` - Individual JSON files that can be combined into JSONL
- Useful for testing operations that convert between JSON and JSONL formats
- Each file contains nested structures with various data types

## Usage Examples

```bash
# Basic selection
ja select 'age > 25' data.jsonl

# Join users with orders
ja join users.jsonl orders.jsonl --on id=user_id

# Work with nested data
ja select-path '$.person.age > 25' nested-names.jsonl

# Set operations
ja union test_data1.jsonl test_data2.jsonl
ja intersection test_data1.jsonl test_data2.jsonl
ja difference test_data1.jsonl test_data2.jsonl

# Distinct records
ja distinct test_data1.jsonl

# Group by operations
ja groupby category --agg count,sum:amount orders.jsonl

# Working with individual JSON files (if supported)
# Combine multiple JSON files into JSONL
cat test_data/json_files/*.json | jq -c . > combined.jsonl
```

## Generating Test Data

Use the script in `scripts/generate_dataset.py` to create custom test datasets with configurable parameters.