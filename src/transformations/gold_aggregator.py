"""
Gold layer aggregation functions
"""

import polars as pl
from typing import Dict, List
from loguru import logger


class GoldAggregator:
    """
    Business aggregations for Gold layer
    """
    
    def __init__(self):
        pass
    
    def create_daily_sales_summary(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create daily sales summary"""
        logger.info("Creating daily sales summary...")
        
        summary = df.group_by('order_date').agg([
            pl.count('order_id').alias('order_count'),
            pl.sum('total_amount').alias('total_revenue'),
            pl.mean('total_amount').alias('avg_order_value'),
            pl.n_unique('customer_id').alias('unique_customers'),
            pl.sum('quantity').alias('total_items_sold')
        ]).sort('order_date', descending=True)
        
        return summary
    
    def create_customer_ltv(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate customer lifetime value"""
        logger.info("Calculating customer LTV...")
        
        ltv = df.group_by('customer_id').agg([
            pl.count('order_id').alias('total_orders'),
            pl.sum('total_amount').alias('lifetime_value'),
            pl.mean('total_amount').alias('avg_order_value'),
            pl.min('order_date').alias('first_order_date'),
            pl.max('order_date').alias('last_order_date'),
            pl.first('customer_segment').alias('segment'),
            pl.first('customer_age').alias('age')
        ]).sort('lifetime_value', descending=True)
        
        return ltv
    
    def create_product_performance(self, df: pl.DataFrame) -> pl.DataFrame:
        """Analyze product performance"""
        logger.info("Analyzing product performance...")
        
        performance = df.group_by(['product_category', 'product_name']).agg([
            pl.count('order_id').alias('units_sold'),
            pl.sum('quantity').alias('total_quantity'),
            pl.sum('total_amount').alias('revenue'),
            pl.mean('unit_price').alias('avg_price'),
            pl.mean('discount_percent').alias('avg_discount')
        ]).sort('revenue', descending=True)
        
        return performance
    
    def create_regional_analytics(self, df: pl.DataFrame) -> pl.DataFrame:
        """Regional sales analytics"""
        logger.info("Creating regional analytics...")
        
        regional = df.group_by('shipping_country').agg([
            pl.count('order_id').alias('order_count'),
            pl.sum('total_amount').alias('total_revenue'),
            pl.mean('total_amount').alias('avg_order_value'),
            pl.n_unique('customer_id').alias('unique_customers'),
            pl.mean('shipping_cost').alias('avg_shipping_cost')
        ]).sort('total_revenue', descending=True)
        
        return regional


if __name__ == "__main__":
    # Test with sample data
    df = pl.DataFrame({
        'order_date': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'order_id': ['O1', 'O2', 'O3'],
        'customer_id': ['C1', 'C2', 'C1'],
        'total_amount': [100, 200, 150],
        'quantity': [1, 2, 1],
        'product_category': ['Electronics', 'Clothing', 'Electronics'],
        'product_name': ['Phone', 'Shirt', 'Laptop'],
        'unit_price': [100, 100, 150],
        'discount_percent': [0, 10, 0],
        'shipping_country': ['USA', 'UK', 'USA'],
        'shipping_cost': [10, 15, 10],
        'customer_segment': ['Gold', 'Silver', 'Gold'],
        'customer_age': [30, 25, 30]
    })
    
    aggregator = GoldAggregator()
    
    print("\n=== Daily Sales ===")
    print(aggregator.create_daily_sales_summary(df))
    
    print("\n=== Customer LTV ===")
    print(aggregator.create_customer_ltv(df))
    
    print("\n=== Product Performance ===")
    print(aggregator.create_product_performance(df))