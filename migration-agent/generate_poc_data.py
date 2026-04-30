#!/usr/bin/env python3
"""
Generate comprehensive 10k record CSV file with edge cases for POC testing
"""

import csv
import random
from datetime import datetime, timedelta
import string

def generate_random_string(length, include_special=False):
    """Generate random string with optional special characters"""
    chars = string.ascii_letters + string.digits
    if include_special:
        chars += "!@#$%^&*()_+-=[]{}|;:,.<>?"
    return ''.join(random.choice(chars) for _ in range(length))

def generate_phone_number():
    """Generate realistic phone number with various formats"""
    formats = [
        lambda: f"({random.randint(200,999)})-{random.randint(200,999)}-{random.randint(1000,9999)}",
        lambda: f"{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}",
        lambda: f"+1-{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}",
        lambda: f"{random.randint(1000000000,9999999999)}",
        lambda: f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}",
    ]
    return random.choice(formats)()

def generate_email(first_name, last_name):
    """Generate realistic email addresses"""
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'company.com', 'test.org']
    formats = [
        f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}",
        f"{first_name.lower()}{last_name.lower()}@{random.choice(domains)}",
        f"{first_name[0].lower()}{last_name.lower()}@{random.choice(domains)}",
        f"{first_name.lower()}{random.randint(10,99)}@{random.choice(domains)}",
    ]
    return random.choice(formats)

def generate_registration_date():
    """Generate registration dates within last 5 years"""
    start_date = datetime.now() - timedelta(days=5*365)
    random_days = random.randint(0, 5*365)
    return (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')

def main():
    # Define data characteristics
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emma', 'Chris', 'Lisa', 'Robert', 'Maria',
                   'James', 'Anna', 'William', 'Jennifer', 'Richard', 'Linda', 'Charles', 'Patricia', 'Daniel', 'Susan',
                   'Joseph', 'Margaret', 'Thomas', 'Dorothy', 'Christopher', 'Barbara', 'Matthew', 'Elizabeth', 'Anthony', 'Jessica',
                   'Mark', 'Sandra', 'Donald', 'Helen', 'Steven', 'Nancy', 'Paul', 'Betty', 'Andrew', 'Carol',
                   'Joshua', 'Ruth', 'Kenneth', 'Sharon', 'Kevin', 'Michelle', 'Brian', 'Laura', 'George', 'Sarah']

    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
                  'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
                  'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
                  'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
                  'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell', 'Carter', 'Roberts']

    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
              'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte', 'San Francisco', 'Indianapolis', 'Seattle', 'Denver', 'Boston',
              'El Paso', 'Detroit', 'Nashville', 'Portland', 'Memphis', 'Oklahoma City', 'Las Vegas', 'Louisville', 'Baltimore', 'Milwaukee',
              'Albuquerque', 'Tucson', 'Fresno', 'Sacramento', 'Mesa', 'Kansas City', 'Atlanta', 'Long Beach', 'Colorado Springs', 'Raleigh',
              'Miami', 'Virginia Beach', 'Omaha', 'Oakland', 'Minneapolis', 'Tulsa', 'Arlington', 'New Orleans', 'Wichita', 'Cleveland']

    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'Japan', 'China', 'India', 'Brazil']

    statuses = ['ACTIVE', 'INACTIVE', 'SUSPENDED', 'PENDING', 'CLOSED']

    # Open CSV file for writing
    with open('/Users/sagarshingare/agent_pythonCode/migration-agent/data/comprehensive_poc_10k.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL', 'PHONE', 'REGISTRATION_DATE', 'ACCOUNT_BALANCE', 'STATUS', 'CITY', 'COUNTRY']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Generate 10,000 records
        for customer_id in range(1, 10001):
            # Edge cases distribution
            is_edge_case = random.random() < 0.1  # 10% edge cases

            if is_edge_case:
                # Generate edge case data
                edge_case_type = random.choice(['null_values', 'special_chars', 'extreme_values', 'invalid_formats'])

                if edge_case_type == 'null_values':
                    first_name = random.choice([random.choice(first_names), None])
                    last_name = random.choice([random.choice(last_names), None])
                    email = random.choice([generate_email(first_name or 'Test', last_name or 'User'), None])
                    phone = random.choice([generate_phone_number(), None])
                    city = random.choice([random.choice(cities), None])
                    country = random.choice([random.choice(countries), None])
                elif edge_case_type == 'special_chars':
                    first_name = generate_random_string(random.randint(5,15), True)
                    last_name = generate_random_string(random.randint(5,15), True)
                    email = f"{generate_random_string(10, True)}@{generate_random_string(5)}.com"
                    phone = generate_random_string(15, True)
                    city = generate_random_string(random.randint(5,20), True)
                    country = generate_random_string(random.randint(5,15), True)
                elif edge_case_type == 'extreme_values':
                    first_name = 'A' * 50  # Max length
                    last_name = 'Z' * 50   # Max length
                    email = 'a' * 95 + '@test.com'  # Near max length
                    phone = '9' * 20      # Max length
                    city = 'X' * 50       # Max length
                    country = 'Y' * 50    # Max length
                else:  # invalid_formats
                    first_name = str(random.randint(1000,9999))  # Numbers as names
                    last_name = str(random.random())  # Float as name
                    email = 'invalid-email-format'
                    phone = 'not-a-phone-number'
                    city = str(random.randint(-1000,1000))  # Numbers as city
                    country = str(datetime.now())  # Date as country
            else:
                # Generate normal data
                first_name = random.choice(first_names)
                last_name = random.choice(last_names)
                email = generate_email(first_name, last_name)
                phone = generate_phone_number()
                city = random.choice(cities)
                country = random.choice(countries)

            # Common fields for all records
            registration_date = generate_registration_date()
            account_balance = round(random.uniform(-10000, 100000), 2)  # Include negative balances
            status = random.choice(statuses)

            # Write record
            writer.writerow({
                'CUSTOMER_ID': customer_id,
                'FIRST_NAME': first_name,
                'LAST_NAME': last_name,
                'EMAIL': email,
                'PHONE': phone,
                'REGISTRATION_DATE': registration_date,
                'ACCOUNT_BALANCE': account_balance,
                'STATUS': status,
                'CITY': city,
                'COUNTRY': country
            })

    print("Generated comprehensive_poc_10k.csv with 10,000 records including edge cases")

if __name__ == '__main__':
    main()