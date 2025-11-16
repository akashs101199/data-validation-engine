"""
Generate sample e-commerce dataset with intentional quality issues
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

def generate_sample_data():
    """Generate 1000 sample e-commerce transactions with quality issues"""
    
    print("🎲 Generating sample e-commerce data...")
    
    np.random.seed(42)
    n_records = 1000
    
    # Generate base data
    data = {
        'customer_id': [f'CUST_{str(i).zfill(5)}' for i in range(1, n_records + 1)],
        'order_id': [f'ORD_{str(i).zfill(6)}' for i in range(1, n_records + 1)],
        'order_date': [(datetime.now() - timedelta(days=np.random.randint(0, 365))).strftime('%Y-%m-%d') 
                       for _ in range(n_records)],
        'product_category': np.random.choice(
            ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports', 'Beauty'], 
            n_records
        ),
        'product_name': np.random.choice(
            ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'], 
            n_records
        ),
        'quantity': np.random.randint(1, 10, n_records),
        'unit_price': np.round(np.random.uniform(10, 500, n_records), 2),
        'total_amount': 0,
        'discount_percent': np.random.choice([0, 5, 10, 15, 20, 25], n_records),
        'payment_method': np.random.choice(
            ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash'], 
            n_records
        ),
        'shipping_country': np.random.choice(
            ['USA', 'UK', 'Canada', 'Germany', 'France', 'Australia'], 
            n_records
        ),
        'shipping_cost': np.round(np.random.uniform(5, 30, n_records), 2),
        'customer_age': np.random.randint(18, 80, n_records),
        'customer_email': [f'customer{i}@email.com' for i in range(1, n_records + 1)],
        'customer_segment': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], n_records),
        'is_returned': np.random.choice([True, False], n_records, p=[0.1, 0.9]),
        'satisfaction_score': np.random.randint(1, 11, n_records),
    }
    
    df = pd.DataFrame(data)
    
    # Calculate total_amount correctly
    df['total_amount'] = (
        df['quantity'] * df['unit_price'] * (1 - df['discount_percent']/100) + df['shipping_cost']
    )
    df['total_amount'] = df['total_amount'].round(2)
    
    # Introduce data quality issues (10% of data)
    quality_issues_idx = np.random.choice(df.index, size=100, replace=False)
    
    # Issue 1: Missing values (20 records)
    df.loc[quality_issues_idx[:20], 'customer_email'] = None
    df.loc[quality_issues_idx[20:30], 'shipping_country'] = None
    
    # Issue 2: Negative values (10 records)
    df.loc[quality_issues_idx[30:35], 'quantity'] = -1
    df.loc[quality_issues_idx[35:40], 'unit_price'] = -50
    
    # Issue 3: Outliers (10 records)
    df.loc[quality_issues_idx[40:45], 'unit_price'] = 10000
    df.loc[quality_issues_idx[45:50], 'customer_age'] = 150
    
    # Issue 4: Duplicates (5 records)
    df.loc[quality_issues_idx[50:55], 'order_id'] = 'ORD_000999'
    
    # Issue 5: Invalid values (10 records)
    df.loc[quality_issues_idx[55:60], 'satisfaction_score'] = 15
    df.loc[quality_issues_idx[60:65], 'discount_percent'] = 150
    
    # Issue 6: Future dates (5 records)
    df.loc[quality_issues_idx[65:70], 'order_date'] = (
        datetime.now() + timedelta(days=30)
    ).strftime('%Y-%m-%d')
    
    # Issue 7: Inconsistent data (10 records)
    df.loc[quality_issues_idx[70:80], 'total_amount'] = 9999.99
    
    # Issue 8: Extra whitespace (10 records)
    df.loc[quality_issues_idx[80:90], 'product_name'] = (
        '  ' + df.loc[quality_issues_idx[80:90], 'product_name'] + '  '
    )
    
    # Issue 9: Mixed case in IDs (5 records)
    df.loc[quality_issues_idx[90:95], 'customer_id'] = (
        df.loc[quality_issues_idx[90:95], 'customer_id'].str.lower()
    )
    
    # Issue 10: Invalid email format (5 records)
    df.loc[quality_issues_idx[95:100], 'customer_email'] = 'invalid_email'
    
    # Create directory if it doesn't exist
    Path('data/raw').mkdir(parents=True, exist_ok=True)
    
    # Save to CSV
    output_path = 'data/raw/ecommerce_transactions.csv'
    df.to_csv(output_path, index=False)
    
    print(f"✅ Generated {len(df)} records")
    print(f"📁 Saved to: {output_path}")
    print("\n📊 Data Quality Issues Summary:")
    print("   - Missing values: 30 records")
    print("   - Negative values: 10 records")
    print("   - Outliers: 10 records")
    print("   - Duplicates: 5 records")
    print("   - Invalid ranges: 10 records")
    print("   - Future dates: 5 records")
    print("   - Calculation errors: 10 records")
    print("   - Formatting issues: 20 records")
    
    return df

if __name__ == "__main__":
    generate_sample_data()