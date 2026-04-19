"""
Data Generation Script for Informatica ETL Migration Testing

Generates realistic test data for:
1. Customer360 ETL (M_BFSI_CUSTOMER_360) - 10,000 customer transactions
2. CompTime ETL (CompTime mappings) - 10,000 comp time records
"""

import csv
from datetime import datetime, timedelta
import random
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def generate_customer_360_data(num_records=10000):
    """
    Generate customer transaction data for Customer360 ETL.
    
    Informatica Source: SRC_CUSTOMER_TXN
    Fields: CUSTOMER_ID, ACCOUNT_ID, TXN_AMOUNT, TXN_DATE, LAST_UPDATED_TS
    """
    print(f"Generating {num_records} customer transaction records...")
    
    output_file = BASE_DIR / 'customer360' / 'sample_src_customer_txn_10k.csv'
    
    # Ensure directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header (no header in actual flat file, added for clarity)
        # writer.writerow(['CUSTOMER_ID', 'ACCOUNT_ID', 'TXN_AMOUNT', 'TXN_DATE', 'LAST_UPDATED_TS'])
        
        # Generate records
        base_date = datetime(2023, 1, 1)
        
        for i in range(num_records):
            customer_id = (i % 500) + 1  # 500 unique customers
            account_id = (i % 1000) + 1000  # 1000 unique accounts
            
            # Mix of valid and invalid transaction amounts
            # 95% valid transactions, 5% null amounts (will be rejected)
            if random.random() < 0.95:
                txn_amount = round(random.uniform(10, 500000), 2)
            else:
                txn_amount = None  # Null amount - will be rejected by validation
            
            # Spread transactions across 12 months
            txn_date = base_date + timedelta(days=random.randint(0, 365))
            last_updated_ts = txn_date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
            
            row = [
                customer_id,
                account_id,
                txn_amount if txn_amount is not None else '',
                txn_date.strftime('%Y-%m-%d'),
                last_updated_ts.strftime('%Y-%m-%d %H:%M:%S')
            ]
            writer.writerow(row)
    
    print(f"✓ Generated {num_records} records to: {output_file}")
    return output_file


def generate_comptime_data(num_records=10000):
    """
    Generate compensatory time data for CompTime ETL.
    
    Informatica Source: U0287D01 (Flat File)
    Fields: SSN, NAME, CURRENT_ACCT, CURRENT_ORG, FLSA_STATUS, 
            COMP_TIME_CUR_BAL, COMP_TIME_YEAR_EARNED, PP_END_DATE,
            DAILY_DATE_EARNED, COMP_TIME_RATE, COMP_TIME_HOURS, COMP_TIME_UNDEF
    """
    print(f"Generating {num_records} comp time records...")
    
    output_file = BASE_DIR / 'comptime' / 'sample_comptime_data_10k.csv'
    
    # Ensure directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'James', 'Emma', 'Robert', 'Olivia', 
                   'William', 'Sophia', 'David', 'Ava', 'Richard', 'Isabella', 'Joseph']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                  'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson']
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Generate records
        base_date = datetime(2023, 1, 1)
        
        for i in range(num_records):
            # Generate unique SSN-like identifiers (9 digits)
            ssn = str(random.randint(100000000, 999999999))
            
            # Generate names
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            name = f"{first_name} {last_name}"
            
            # Account and organization codes
            current_acct = f"ACC{str(i % 500).zfill(6)}"
            current_org = f"ORG{str(random.randint(1, 50)).zfill(3)}"
            
            # FLSA Status (Exempt or Non-Exempt)
            flsa_status = random.choice(['E', 'N'])
            
            # Comp time balances
            comp_time_cur_bal = round(random.uniform(0, 240), 2)  # Current balance in hours
            comp_time_year_earned = random.randint(0, 120)  # Hours earned this year
            
            # Pay period end date (yyyyMMdd format)
            pp_end_date = '20231231'
            
            # Daily date earned (yyyyMMdd format)
            daily_date_earned = (base_date + timedelta(days=random.randint(0, 365))).strftime('%Y%m%d')
            
            # Comp time rates and hours
            comp_time_rate = round(random.uniform(15, 50), 2)  # Hourly rate
            comp_time_hours = round(random.uniform(0, 40), 2)  # Hours earned
            comp_time_undef = round(random.uniform(0, 10), 2)  # Undefined category
            
            row = [
                ssn,
                name,
                current_acct,
                current_org,
                flsa_status,
                comp_time_cur_bal,
                comp_time_year_earned,
                pp_end_date,
                daily_date_earned,
                comp_time_rate,
                comp_time_hours,
                comp_time_undef
            ]
            writer.writerow(row)
    
    print(f"✓ Generated {num_records} records to: {output_file}")
    return output_file


def generate_pay_period_reference():
    """
    Generate PAY_PERIOD reference table.
    
    Informatica Source: PAY_PERIOD
    """
    print("Generating PAY_PERIOD reference data...")
    
    output_file = BASE_DIR / 'comptime' / 'sample_pay_period_10k.csv'
    
    # Ensure directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Generate 26 pay periods for 2 years (bi-weekly)
        for year in [2023, 2024]:
            start_date = datetime(year, 1, 1)
            for pp_num in range(1, 27):
                pp_end_date = start_date + timedelta(days=pp_num * 14)
                pp_start_date = pp_end_date - timedelta(days=13)
                
                pp_year_num = year
                lv_num = (pp_num - 1) // 2 + 1  # Leave accounting every 2 pay periods
                lv_year = year
                pay_date = pp_end_date + timedelta(days=2)
                
                # Current pay period flag (only latest)
                curr_pp_flag = 'Y' if (year == 2023 and pp_num == 26) else 'N'
                
                holiday_1 = None
                holiday_2 = None
                if year == 2023:
                    holiday_1 = '20230704'  # Independence Day
                    holiday_2 = '20231124'  # Thanksgiving
                
                row = [
                    pp_num,
                    pp_year_num,
                    pp_start_date.strftime('%Y-%m-%d %H:%M:%S'),
                    pp_end_date.strftime('%Y-%m-%d %H:%M:%S'),
                    lv_num,
                    lv_year,
                    pay_date.strftime('%Y-%m-%d %H:%M:%S'),
                    curr_pp_flag,
                    holiday_1 if holiday_1 else '',
                    holiday_2 if holiday_2 else ''
                ]
                writer.writerow(row)
    
    print(f"✓ Generated PAY_PERIOD reference data to: {output_file}")
    return output_file


if __name__ == "__main__":
    print("=" * 80)
    print("INFORMATICA ETL MIGRATION - TEST DATA GENERATION")
    print("=" * 80)
    print()
    
    # Generate all test data
    generate_customer_360_data(10000)
    print()
    
    generate_comptime_data(10000)
    print()
    
    generate_pay_period_reference()
    print()
    
    print("=" * 80)
    print("✓ All test data generated successfully!")
    print("=" * 80)
