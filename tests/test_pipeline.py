"""
Unit tests for the pipeline
"""

import pytest
import polars as pl
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.agents.agentic_agents import DataProfilerAgent, QualityAgent, RemediationAgent
from src.transformations.silver_transformer import SilverTransformer
from src.transformations.gold_aggregator import GoldAggregator


def test_data_profiler():
    """Test data profiler agent"""
    df = pl.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    profiler = DataProfilerAgent()
    profile = profiler.profile_dataset(df, 'test')
    
    assert profile['row_count'] == 3
    assert profile['column_count'] == 3
    assert 'columns' in profile


def test_quality_agent():
    """Test quality scoring"""
    df = pl.DataFrame({
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    })
    
    quality_agent = QualityAgent()
    report = quality_agent.generate_quality_report(df, 'test')
    
    assert report.quality_score >= 0
    assert report.quality_score <= 100


def test_silver_transformer():
    """Test silver transformations"""
    df = pl.DataFrame({
        'Name': ['  Alice  ', 'Bob'],
        'Age': [25, 30]
    })
    
    transformer = SilverTransformer()
    df_clean = transformer.clean_whitespace(df)
    df_clean = transformer.standardize_column_names(df_clean)
    
    assert df_clean.columns == ['name', 'age']
    assert df_clean['name'][0] == 'Alice'


def test_gold_aggregator():
    """Test gold aggregations"""
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
    
    # Test daily sales
    daily_sales = aggregator.create_daily_sales_summary(df)
    assert len(daily_sales) == 2
    
    # Test customer LTV
    ltv = aggregator.create_customer_ltv(df)
    assert len(ltv) == 2


if __name__ == "__main__":
    pytest.main([__file__, '-v'])