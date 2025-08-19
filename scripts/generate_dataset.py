#!/usr/bin/env python3
import json
import random
import uuid
import argparse
from datetime import datetime, timedelta
from faker import Faker

# ─── Configurable pools ──────────────────────────────────────────────────────
fake = Faker(); Faker.seed(42); random.seed(42)

STATUSES     = ["active", "inactive", "pending"]
GENDERS      = ["male", "female", "nonbinary"]
INTERESTS    = ["reading","hiking","cycling","cooking","traveling","gaming",
                "photography","yoga","sports","music","gardening","drawing"]
LAST_NAMES   = [fake.last_name() for _ in range(25)]
STATES       = ["CA","NY","TX","FL","IL","PA","OH","GA","NC","MI"]
CITIES_BY_ST  = {st: [fake.city() for _ in range(3)] for st in STATES}
COMMON_JOBS  = ["Software Engineer","Nurse","Teacher","Accountant",
                "Sales Manager","Product Manager","Electrician","Graphic Designer"]
INDUSTRIES   = ["Technology","Healthcare","Finance","Education","Retail","Manufacturing"]

# ─── Helpers ─────────────────────────────────────────────────────────────────
def random_timestamp(days_back=5*365):
    dt = datetime.utcnow() - timedelta(days=random.randint(0, days_back))
    return dt.isoformat() + "Z"

def biased_age():
    # clusters: 0–17, 18–35, 36–60, 61–85
    choices = [
        random.randint(0,17),
        random.randint(18,35),
        random.randint(36,60),
        random.randint(61,85),
    ]
    return random.choices(choices, weights=[0.15,0.4,0.3,0.15])[0]

# ─── Generators ──────────────────────────────────────────────────────────────
def generate_companies(n):
    companies = []
    for _ in range(n):
        cid = str(uuid.uuid4())
        st = random.choice(STATES)
        companies.append({
            "id": cid,
            "name": fake.company(),
            "industry": random.choice(INDUSTRIES),
            "headquarters": {
                "city": random.choice(CITIES_BY_ST[st]),
                "state": st,
                "country": "USA"
            },
            "size": random.randint(50, 20000)
        })
    return companies

def generate_people(total_people, max_per_household, company_names):
    people = []
    count = 0
    while count < total_people:
        hh_id = str(uuid.uuid4())
        hh_size = random.randint(1, max_per_household)
        # trim if overshoot
        hh_size = min(hh_size, total_people - count)
        shared_last = random.choice(LAST_NAMES)
        shared_state = random.choice(STATES)

        for _ in range(hh_size):
            gender = random.choices(GENDERS, weights=[0.45,0.45,0.1])[0]
            first = (fake.first_name_male() if gender=="male"
                     else fake.first_name_female())
            last = shared_last
            age = biased_age()
            st = shared_state
            person = {
                "id": str(uuid.uuid4()),
                "created_at": random_timestamp(),
                "status": random.choice(STATUSES),
                "household_id": hh_id,
                "person": {
                    "name": {"first": first, "last": last},
                    "age": age,
                    "gender": gender,
                    "email": f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}",
                    "phone": fake.phone_number(),
                    "location": {
                        "city": random.choice(CITIES_BY_ST[st]),
                        "state": st,
                        "country": "USA"
                    },
                    "interests": random.sample(INTERESTS, k=random.randint(1,3)),
                    "job": {
                        "title": random.choice(COMMON_JOBS),
                        "company_name": random.choice(company_names),
                        "salary": round(random.uniform(40000,180000), -3)
                    }
                }
            }
            people.append(person)
            count += 1
        # end household
    return people

# ─── Command-line interface ──────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(
        description="Generate people.jsonl and companies.jsonl for nested-JSON relational tests"
    )
    p.add_argument("--num-companies",      type=int, default=20)
    p.add_argument("--num-people",         type=int, default=100)
    p.add_argument("--max-people-per-household", type=int, default=5)
    p.add_argument("--companies-file",     type=str, default="companies.jsonl")
    p.add_argument("--people-file",        type=str, default="people.jsonl")
    args = p.parse_args()

    # 1) Companies
    companies = generate_companies(args.num_companies)
    with open(args.companies_file, "w") as f:
        for c in companies:
            f.write(json.dumps(c) + "\n")

    # 2) People
    company_names = [c["name"] for c in companies]
    people = generate_people(args.num_people,
                             args.max_people_per_household,
                             company_names)
    with open(args.people_file, "w") as f:
        for p in people:
            f.write(json.dumps(p) + "\n")

    print(f"→ Wrote {len(companies)} companies to {args.companies_file}")
    print(f"→ Wrote {len(people)} people to   {args.people_file}")

if __name__=="__main__":
    main()
