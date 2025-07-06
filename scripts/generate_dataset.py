#!/usr/bin/env python3
"""
Dataset Generator for JSONL Algebra

This script generates synthetic datasets for testing and demonstrating ja (jsonl-algebra).
It creates two related datasets:
- people.jsonl: Person records with nested structure, household relationships, and employment
- companies.jsonl: Company records with industry and location information

The datasets are designed to showcase ja's capabilities for:
- Nested JSON manipulation
- Relational operations (joins)
- Grouping and aggregation
- Filtering and transformation

Data Structure:
- People have nested person.name, person.location, person.job fields
- People belong to households (shared last name, location)
- People work for companies (foreign key relationship)
- Companies have nested headquarters.city/state structure

Usage:
    python scripts/generate_dataset.py --help
    python scripts/generate_dataset.py --num-people 1000 --num-companies 50
    python scripts/generate_dataset.py --deterministic --output-dir examples/
"""

import json
import random
import uuid
import argparse
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Optional dependency for richer fake data
try:
    from faker import Faker
    HAS_FAKER = True
except ImportError:
    HAS_FAKER = False

# ─── Constants and Configuration ─────────────────────────────────────────────

# Default seed for reproducible datasets
DEFAULT_SEED = 42

# Static data pools - these provide consistent fake data even without faker
STATUSES = ["active", "inactive", "pending"]
GENDERS = ["male", "female", "nonbinary"]
INTERESTS = [
    "reading", "hiking", "cycling", "cooking", "traveling", "gaming",
    "photography", "yoga", "sports", "music", "gardening", "drawing",
    "art", "writing", "dancing", "swimming", "running", "coding"
]

# Common job titles across industries
COMMON_JOBS = [
    "Software Engineer", "Data Scientist", "Product Manager", "Designer",
    "Marketing Manager", "Sales Manager", "Operations Manager", "Consultant",
    "Nurse", "Teacher", "Accountant", "Lawyer", "Electrician", "Plumber",
    "Graphic Designer", "Writer", "Analyst", "Researcher", "Administrator"
]

# Industries for companies
INDUSTRIES = [
    "Technology", "Healthcare", "Finance", "Education", "Retail", 
    "Manufacturing", "Consulting", "Entertainment", "Transportation",
    "Real Estate", "Construction", "Agriculture", "Energy", "Media"
]

# US states and cities (simplified dataset)
STATES = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
CITIES_BY_STATE = {
    "CA": ["Los Angeles", "San Francisco", "San Diego", "Sacramento"],
    "NY": ["New York", "Buffalo", "Rochester", "Syracuse"],
    "TX": ["Houston", "Dallas", "Austin", "San Antonio"],
    "FL": ["Miami", "Orlando", "Tampa", "Jacksonville"],
    "IL": ["Chicago", "Springfield", "Peoria", "Rockford"],
    "PA": ["Philadelphia", "Pittsburgh", "Harrisburg", "Allentown"],
    "OH": ["Columbus", "Cleveland", "Cincinnati", "Toledo"],
    "GA": ["Atlanta", "Augusta", "Columbus", "Savannah"],
    "NC": ["Charlotte", "Raleigh", "Greensboro", "Durham"],
    "MI": ["Detroit", "Grand Rapids", "Warren", "Sterling Heights"]
}

# Common last names for household generation
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green"
]

# First names by gender
FIRST_NAMES = {
    "male": [
        "James", "Robert", "John", "Michael", "William", "David", "Richard",
        "Joseph", "Thomas", "Christopher", "Charles", "Daniel", "Matthew",
        "Anthony", "Mark", "Donald", "Steven", "Paul", "Andrew", "Joshua"
    ],
    "female": [
        "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara",
        "Susan", "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty",
        "Helen", "Sandra", "Donna", "Carol", "Ruth", "Sharon", "Michelle"
    ],
    "nonbinary": [
        "Alex", "Taylor", "Jordan", "Casey", "Riley", "Avery", "Quinn",
        "River", "Sage", "Phoenix", "Rowan", "Skyler", "Cameron", "Drew"
    ]
}

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com"]

# ─── Helper Functions ────────────────────────────────────────────────────────

def setup_randomness(seed: Optional[int] = None, deterministic: bool = True) -> None:
    """Initialize random number generators with optional seeding."""
    if deterministic:
        actual_seed = seed or DEFAULT_SEED
        random.seed(actual_seed)
        if HAS_FAKER:
            Faker.seed(actual_seed)
        print(f"🎲 Using deterministic seed: {actual_seed}")
    else:
        print("🎲 Using random seed")


def random_timestamp(days_back: int = 5 * 365) -> str:
    """Generate a random ISO timestamp within the last N days."""
    dt = datetime.utcnow() - timedelta(days=random.randint(0, days_back))
    return dt.isoformat() + "Z"


def biased_age() -> int:
    """Generate age with realistic distribution: young adults, middle-aged, etc."""
    # Age clusters: 0-17 (15%), 18-35 (40%), 36-60 (30%), 61-85 (15%)
    age_groups = [
        random.randint(0, 17),    # Children/teens
        random.randint(18, 35),   # Young adults
        random.randint(36, 60),   # Middle-aged
        random.randint(61, 85),   # Seniors
    ]
    return random.choices(age_groups, weights=[0.15, 0.4, 0.3, 0.15])[0]


def generate_phone() -> str:
    """Generate a realistic US phone number."""
    return f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


def generate_email(first: str, last: str) -> str:
    """Generate a realistic email address."""
    domain = random.choice(EMAIL_DOMAINS)
    # Sometimes add numbers to handle duplicates
    suffix = f"{random.randint(1, 99)}" if random.random() < 0.3 else ""
    return f"{first.lower()}.{last.lower()}{suffix}@{domain}"


def fake_company_name() -> str:
    """Generate a company name (with or without faker)."""
    if HAS_FAKER:
        fake = Faker()
        return fake.company()
    else:
        # Simple company name generator
        prefixes = ["Tech", "Global", "Digital", "Metro", "Prime", "Elite", "Smart"]
        bases = ["Solutions", "Systems", "Services", "Corp", "Industries", "Group", "Labs"]
        suffixes = ["LLC", "Inc", "Co", "Corp", "Ltd"]
        
        name_parts = [random.choice(prefixes), random.choice(bases)]
        if random.random() < 0.7:  # 70% chance of suffix
            name_parts.append(random.choice(suffixes))
        
        return " ".join(name_parts)


# ─── Data Generators ─────────────────────────────────────────────────────────

def generate_companies(n: int) -> List[Dict[str, Any]]:
    """Generate a list of company records."""
    companies = []
    
    for _ in range(n):
        company_id = str(uuid.uuid4())
        state = random.choice(STATES)
        city = random.choice(CITIES_BY_STATE[state])
        
        company = {
            "id": company_id,
            "name": fake_company_name(),
            "industry": random.choice(INDUSTRIES),
            "headquarters": {
                "city": city,
                "state": state,
                "country": "USA"
            },
            "size": random.randint(50, 20000),
            "founded": random.randint(1950, 2020)
        }
        companies.append(company)
    
    return companies


def generate_people(
    total_people: int, 
    max_per_household: int, 
    company_names: List[str]
) -> List[Dict[str, Any]]:
    """Generate a list of person records organized by households."""
    people = []
    count = 0
    
    while count < total_people:
        # Create a household
        household_id = str(uuid.uuid4())
        household_size = random.randint(1, max_per_household)
        
        # Don't exceed target count
        household_size = min(household_size, total_people - count)
        
        # Shared household attributes
        shared_last_name = random.choice(LAST_NAMES)
        shared_state = random.choice(STATES)
        shared_city = random.choice(CITIES_BY_STATE[shared_state])
        
        # Generate people in this household
        for _ in range(household_size):
            gender = random.choices(GENDERS, weights=[0.45, 0.45, 0.10])[0]
            first_name = random.choice(FIRST_NAMES[gender])
            age = biased_age()
            
            # Build person record
            person = {
                "id": str(uuid.uuid4()),
                "created_at": random_timestamp(),
                "status": random.choice(STATUSES),
                "household_id": household_id,
                "person": {
                    "name": {
                        "first": first_name,
                        "last": shared_last_name
                    },
                    "age": age,
                    "gender": gender,
                    "email": generate_email(first_name, shared_last_name),
                    "phone": generate_phone(),
                    "location": {
                        "city": shared_city,
                        "state": shared_state,
                        "country": "USA"
                    },
                    "interests": random.sample(INTERESTS, k=random.randint(1, 4)),
                    "job": {
                        "title": random.choice(COMMON_JOBS),
                        "company_name": random.choice(company_names),
                        "salary": round(random.uniform(40000, 180000), -3)  # Round to nearest 1000
                    }
                }
            }
            
            people.append(person)
            count += 1
    
    return people


def write_jsonl(data: List[Dict[str, Any]], filepath: str) -> None:
    """Write a list of dictionaries to a JSONL file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')


# ─── Command Line Interface ──────────────────────────────────────────────────

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic datasets for jsonl-algebra testing and examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --num-people 1000 --num-companies 50
  %(prog)s --deterministic --output-dir examples/
  %(prog)s --random --num-people 500 --max-household-size 3
  %(prog)s --companies-file data/companies.jsonl --people-file data/people.jsonl
        """
    )
    
    # Data generation parameters
    parser.add_argument(
        "--num-companies", 
        type=int, 
        default=20,
        help="Number of companies to generate (default: 20)"
    )
    parser.add_argument(
        "--num-people", 
        type=int, 
        default=100,
        help="Number of people to generate (default: 100)"
    )
    parser.add_argument(
        "--max-household-size", 
        type=int, 
        default=5,
        help="Maximum people per household (default: 5)"
    )
    
    # Randomness control
    parser.add_argument(
        "--deterministic", 
        action="store_true",
        help="Use deterministic seed for reproducible output (default)"
    )
    parser.add_argument(
        "--random", 
        action="store_true",
        help="Use random seed for varied output"
    )
    parser.add_argument(
        "--seed", 
        type=int, 
        default=DEFAULT_SEED,
        help=f"Random seed when using --deterministic (default: {DEFAULT_SEED})"
    )
    
    # Output control
    parser.add_argument(
        "--output-dir", 
        type=str, 
        default=".",
        help="Output directory (default: current directory)"
    )
    parser.add_argument(
        "--companies-file", 
        type=str, 
        default=None,
        help="Companies output file (default: <output-dir>/companies.jsonl)"
    )
    parser.add_argument(
        "--people-file", 
        type=str, 
        default=None,
        help="People output file (default: <output-dir>/people.jsonl)"
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Handle conflicting randomness options
    if args.random and args.deterministic:
        parser.error("Cannot specify both --random and --deterministic")
    
    # Default to deterministic if neither specified
    use_deterministic = not args.random
    
    # Set up output paths
    if args.companies_file is None:
        args.companies_file = os.path.join(args.output_dir, "companies.jsonl")
    if args.people_file is None:
        args.people_file = os.path.join(args.output_dir, "people.jsonl")
    
    # Display configuration
    print("📊 JSONL Algebra Dataset Generator")
    print("=" * 40)
    print(f"Companies:        {args.num_companies}")
    print(f"People:           {args.num_people}")
    print(f"Max household:    {args.max_household_size}")
    print(f"Deterministic:    {use_deterministic}")
    print(f"Output directory: {args.output_dir}")
    print(f"Faker available:  {HAS_FAKER}")
    print()
    
    # Set up randomness
    setup_randomness(args.seed, use_deterministic)
    
    # Generate companies first
    print("🏢 Generating companies...")
    companies = generate_companies(args.num_companies)
    write_jsonl(companies, args.companies_file)
    print(f"   → Wrote {len(companies)} companies to {args.companies_file}")
    
    # Generate people
    print("👥 Generating people...")
    company_names = [company["name"] for company in companies]
    people = generate_people(args.num_people, args.max_household_size, company_names)
    write_jsonl(people, args.people_file)
    print(f"   → Wrote {len(people)} people to {args.people_file}")
    
    # Summary
    print()
    print("✅ Dataset generation complete!")
    print(f"   Companies: {args.companies_file}")
    print(f"   People:    {args.people_file}")
    print()
    print("💡 Try these ja commands:")
    print(f"   ja '{args.people_file}' --group-by person.job.company_name --count")
    print(f"   ja '{args.people_file}' --select person.name,person.age --where 'person.age > 30'")
    print(f"   ja '{args.people_file}' --join '{args.companies_file}' --on 'person.job.company_name = name'")


if __name__ == "__main__":
    main()
