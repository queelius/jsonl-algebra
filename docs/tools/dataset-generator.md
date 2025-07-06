# Dataset Generator

The `ja-generate-dataset` command creates synthetic datasets specifically designed to showcase jsonl-algebra's capabilities. It generates two related JSONL files with realistic nested structures and relationships.

## Quick Start

```bash
# Install with dataset support
pip install "jsonl-algebra[dataset]"

# Generate default dataset (20 companies, 100 people)
ja-generate-dataset

# Generate larger dataset
ja-generate-dataset --num-companies 50 --num-people 1000

# Generate to specific directory
ja-generate-dataset --output-dir examples/
```

## Generated Data Structure

### Companies (`companies.jsonl`)

Each company record contains:

```json
{
  "id": "uuid-string",
  "name": "Company Name Inc",
  "industry": "Technology",
  "headquarters": {
    "city": "San Francisco",
    "state": "CA", 
    "country": "USA"
  },
  "size": 1500,
  "founded": 2010
}
```

**Industries**: Technology, Healthcare, Finance, Education, Retail, Manufacturing, Consulting, Entertainment, Transportation, Real Estate, Construction, Agriculture, Energy, Media

### People (`people.jsonl`)

Each person record contains:

```json
{
  "id": "uuid-string",
  "created_at": "2023-05-15T10:30:00Z",
  "status": "active",
  "household_id": "shared-uuid-for-family",
  "person": {
    "name": {
      "first": "Sarah",
      "last": "Johnson"
    },
    "age": 32,
    "gender": "female",
    "email": "sarah.johnson@gmail.com",
    "phone": "555-123-4567",
    "location": {
      "city": "San Francisco",
      "state": "CA",
      "country": "USA"
    },
    "interests": ["hiking", "photography", "cooking"],
    "job": {
      "title": "Software Engineer", 
      "company_name": "Tech Solutions Inc",
      "salary": 95000.0
    }
  }
}
```

**Key Features**:

- **Household relationships**: People sharing `household_id` have the same last name and location
- **Employment relationships**: `person.job.company_name` links to company `name` field
- **Nested structures**: Rich nested JSON perfect for testing ja's navigation capabilities
- **Realistic distributions**: Age, gender, salary, and location follow realistic patterns

## Command Line Options

```bash
ja-generate-dataset [OPTIONS]

Data Generation:
  --num-companies N        Number of companies (default: 20)
  --num-people N          Number of people (default: 100)  
  --max-household-size N  Max people per household (default: 5)

Randomness Control:
  --deterministic         Use fixed seed for reproducible data (default)
  --random               Use random seed for varied output
  --seed N               Custom seed for deterministic mode (default: 42)

Output Control:
  --output-dir PATH       Output directory (default: current directory)
  --companies-file PATH   Custom companies file path
  --people-file PATH      Custom people file path
```

## Example Workflows

### Basic Exploration

```bash
# Generate sample data
ja-generate-dataset --num-companies 10 --num-people 50

# Explore the data
ja people.jsonl --head 3 --pretty
ja companies.jsonl --select name,industry,size --head 5

# Count people by company
ja people.jsonl --group-by person.job.company_name --count
```

### Nested Data Operations

```bash
# Extract names and ages
ja people.jsonl --select person.name,person.age

# Filter by age and location  
ja people.jsonl --where 'person.age > 30' --select person.name,person.location.state

# Group by nested fields
ja people.jsonl --group-by person.location.state,person.gender --count
```

### Relational Operations

```bash
# Join people with their companies
ja people.jsonl --join companies.jsonl --on 'person.job.company_name = name' \\
  --select person.name,name,industry,person.job.salary

# Find high earners in tech companies
ja people.jsonl --join companies.jsonl --on 'person.job.company_name = name' \\
  --where 'industry = "Technology" and person.job.salary > 100000' \\
  --select person.name,name,person.job.salary
```

### Aggregation Examples

```bash
# Average salary by industry
ja people.jsonl --join companies.jsonl --on 'person.job.company_name = name' \\
  --group-by industry --agg 'avg(person.job.salary)'

# Company size distribution
ja companies.jsonl --group-by industry --agg 'avg(size),count(*)'

# Household statistics
ja people.jsonl --group-by household_id --agg 'count(*),avg(person.age)' \\
  --select '*,_count as household_size,_avg_person_age as avg_age'
```

## Testing and Development

The dataset generator is designed for both documentation examples and unit testing:

### Deterministic Mode (Default)

- Always produces the same output for given parameters
- Perfect for examples, documentation, and reproducible tests
- Uses seed 42 by default

### Random Mode

- Generates varied data for stress testing
- Useful for property-based testing
- Each run produces different realistic data

### Usage in Tests

```python
# In test files
import subprocess
import tempfile
import os

def setup_test_data():
    \"\"\"Generate deterministic test data.\"\"\"
    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.run([
            "python", "scripts/generate_dataset.py",
            "--deterministic", "--seed", "123",
            "--num-companies", "5", "--num-people", "25", 
            "--output-dir", tmpdir
        ])
        return os.path.join(tmpdir, "people.jsonl"), os.path.join(tmpdir, "companies.jsonl")
```

## Data Relationships

The generated datasets are designed to demonstrate common data patterns:

```text
Companies (1) → (N) People
  company.name = person.job.company_name

Households (1) → (N) People  
  household_id groups people with shared:
  - person.name.last (family name)
  - person.location (shared address)

Geographic Hierarchy:
  Country → State → City
  All records use USA with realistic state/city combinations
```

## Dependencies

- **Core**: Works with Python standard library only
- **Enhanced**: Install `faker` for richer company names and data variety

  ```bash
  pip install "jsonl-algebra[dataset]"  # Includes faker
  ```

Without faker, the generator uses built-in data pools that still produce realistic, varied output.

## Tips

1. **Start small**: Use `--num-people 20 --num-companies 5` for initial exploration
2. **Use deterministic mode**: For examples and tests where consistency matters  
3. **Scale gradually**: Large datasets (10K+ records) are great for performance testing
4. **Combine with ja**: The suggested commands in the output are a great starting point
5. **Explore relationships**: Use joins to see how nested structures connect across files

The generated data is specifically crafted to showcase ja's strengths in handling real-world nested JSON data patterns.
