#!/usr/bin/env python3
"""
Demo script showcasing the new fluid API in ja (JSONL Algebra) v0.2

This script demonstrates various features of the fluid API including:
- Method chaining
- Complex aggregations
- Data transformations
- Query explanation
"""

import ja
import json
from datetime import datetime

def create_sample_data():
    """Create sample data for demonstration."""
    users = [
        {"id": 1, "name": "Alice", "age": 30, "city": "NYC", "joined": "2020-01-15"},
        {"id": 2, "name": "Bob", "age": 25, "city": "LA", "joined": "2021-03-20"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "NYC", "joined": "2019-07-10"},
        {"id": 4, "name": "Diana", "age": 28, "city": "Chicago", "joined": "2020-11-05"},
        {"id": 5, "name": "Eve", "age": 32, "city": "LA", "joined": "2021-01-30"},
    ]
    
    transactions = [
        {"user_id": 1, "amount": 100, "product": "Book", "date": "2024-01-10"},
        {"user_id": 1, "amount": 50, "product": "Coffee", "date": "2024-01-15"},
        {"user_id": 2, "amount": 200, "product": "Electronics", "date": "2024-01-12"},
        {"user_id": 3, "amount": 75, "product": "Book", "date": "2024-01-14"},
        {"user_id": 2, "amount": 30, "product": "Coffee", "date": "2024-01-16"},
        {"user_id": 4, "amount": 150, "product": "Clothing", "date": "2024-01-11"},
        {"user_id": 5, "amount": 90, "product": "Book", "date": "2024-01-13"},
    ]
    
    return users, transactions


def demo_basic_chaining():
    """Demonstrate basic method chaining."""
    print("\n" + "="*60)
    print("DEMO 1: Basic Method Chaining")
    print("="*60)
    
    users, _ = create_sample_data()
    
    result = (ja.query(users)
        .select(lambda r: r["age"] >= 30)
        .project(["name", "age", "city"])
        .sort("age")
        .collect())
    
    print("\nUsers aged 30 or older, sorted by age:")
    for user in result:
        print(f"  - {user['name']}, {user['age']} years old, from {user['city']}")


def demo_aggregations():
    """Demonstrate advanced aggregations."""
    print("\n" + "="*60)
    print("DEMO 2: Advanced Aggregations")
    print("="*60)
    
    users, _ = create_sample_data()
    
    city_stats = (ja.query(users)
        .groupby("city")
        .agg(
            count="count",
            avg_age="avg:age",
            median_age="median:age",
            names="list:name"
        )
        .sort("count", desc=True)
        .collect())
    
    print("\nCity statistics:")
    for city in city_stats:
        print(f"\n{city['city']}:")
        print(f"  - Population: {city['count']}")
        print(f"  - Average age: {city['avg_age']:.1f}")
        print(f"  - Median age: {city['median_age']:.1f}")
        print(f"  - Residents: {', '.join(city['names'])}")


def demo_join_and_analysis():
    """Demonstrate joins and complex analysis."""
    print("\n" + "="*60)
    print("DEMO 3: Joins and Complex Analysis")
    print("="*60)
    
    users, transactions = create_sample_data()
    
    customer_analysis = (ja.query(users)
        .join(transactions, on=[("id", "user_id")])
        .groupby("name")
        .agg(
            total_spent="sum:amount",
            transaction_count="count",
            avg_purchase="avg:amount",
            products="unique:product"
        )
        .sort("total_spent", desc=True)
        .collect())
    
    print("\nCustomer spending analysis:")
    for customer in customer_analysis:
        print(f"\n{customer['name']}:")
        print(f"  - Total spent: ${customer['total_spent']}")
        print(f"  - Transactions: {customer['transaction_count']}")
        print(f"  - Average purchase: ${customer['avg_purchase']:.2f}")
        print(f"  - Products bought: {', '.join(customer['products'])}")


def demo_transformations():
    """Demonstrate data transformations with map."""
    print("\n" + "="*60)
    print("DEMO 4: Data Transformations")
    print("="*60)
    
    users, _ = create_sample_data()
    
    def calculate_membership_years(user):
        join_year = int(user["joined"].split("-")[0])
        current_year = 2024
        return {**user, "membership_years": current_year - join_year}
    
    def categorize_member(user):
        years = user["membership_years"]
        if years >= 4:
            category = "Veteran"
        elif years >= 2:
            category = "Regular"
        else:
            category = "New"
        return {**user, "category": category}
    
    member_analysis = (ja.query(users)
        .map(calculate_membership_years)
        .map(categorize_member)
        .groupby("category")
        .agg(
            count="count",
            avg_age="avg:age",
            cities="unique:city"
        )
        .collect())
    
    print("\nMembership category analysis:")
    for category in member_analysis:
        print(f"\n{category['category']} Members:")
        print(f"  - Count: {category['count']}")
        print(f"  - Average age: {category['avg_age']:.1f}")
        print(f"  - Cities: {', '.join(category['cities'])}")


def demo_query_explanation():
    """Demonstrate query explanation feature."""
    print("\n" + "="*60)
    print("DEMO 5: Query Explanation")
    print("="*60)
    
    users, transactions = create_sample_data()
    
    complex_query = (ja.query(users)
        .select(lambda r: r["age"] > 25)
        .join(transactions, on=[("id", "user_id")])
        .map(lambda r: {**r, "year": int(r["joined"][:4])})
        .groupby("city")
        .agg(total="sum:amount", users="unique:name")
        .sort("total", desc=True)
        .limit(3))
    
    print("\nQuery execution plan:")
    print(complex_query.explain())
    
    print("\nQuery results:")
    results = complex_query.collect()
    for i, result in enumerate(results, 1):
        print(f"{i}. {result['city']}: ${result['total']} total from {len(result['users'])} users")


def demo_comparison():
    """Compare fluid API with traditional API."""
    print("\n" + "="*60)
    print("DEMO 6: Fluid API vs Traditional API")
    print("="*60)
    
    users, _ = create_sample_data()
    
    print("\nTraditional functional API:")
    print("filtered = ja.select(users, lambda r: r['age'] > 30)")
    print("projected = ja.project(filtered, ['name', 'city'])")
    print("sorted_data = ja.sort_by(projected, ['name'])")
    
    traditional = ja.sort_by(
        ja.project(
            ja.select(users, lambda r: r['age'] > 30),
            ['name', 'city']
        ),
        ['name']
    )
    
    print("\nFluid API (equivalent):")
    print("result = (ja.query(users)")
    print("    .select(lambda r: r['age'] > 30)")
    print("    .project(['name', 'city'])")
    print("    .sort('name')")
    print("    .collect())")
    
    fluid = (ja.query(users)
        .select(lambda r: r['age'] > 30)
        .project(['name', 'city'])
        .sort('name')
        .collect())
    
    print(f"\nBoth produce same result: {traditional == fluid}")
    print("Result:", fluid)


def main():
    """Run all demonstrations."""
    print("\n" + "="*60)
    print(" "*15 + "ja (JSONL Algebra) Fluid API Demo")
    print(" "*20 + "Version 0.2.0")
    print("="*60)
    
    demo_basic_chaining()
    demo_aggregations()
    demo_join_and_analysis()
    demo_transformations()
    demo_query_explanation()
    demo_comparison()
    
    print("\n" + "="*60)
    print("Demo completed! The fluid API provides:")
    print("  ✓ Intuitive method chaining")
    print("  ✓ Advanced aggregations (median, mode, std, unique, concat)")
    print("  ✓ Lazy evaluation with .stream() or eager with .collect()")
    print("  ✓ Query explanation with .explain()")
    print("  ✓ Full backward compatibility with traditional API")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()