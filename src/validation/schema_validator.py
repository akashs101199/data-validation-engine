"""
Schema validation using Pandera
"""

import polars as pl
import pandera.polars as pa
from pandera import Column, Check
from loguru import logger


class SchemaValidator:
    """
    Schema validation for e-commerce data
    """
    
    def __init__(self):
        self.schema = self._define_schema()
    
    def _define_schema(self):
        """Define expected schema"""
        schema = pa.DataFrameSchema({
            "customer_id": Column(
                str,
                checks=[
                    Check.str_matches(r'^[A-Z]+_\d+$')
                ],
                nullable=False
            ),
            "order_id": Column(
                str,
                checks=[
                    Check.str_matches(r'^ORD_\d+$')
                ],
                nullable=False
            ),
            "order_date": Column(
                str,
                nullable=False
            ),
            "product_category": Column(
                str,
                checks=[
                    Check.isin(['Electronics', 'Clothing', 'Home & Garden', 
                               'Books', 'Sports', 'Beauty'])
                ]
            ),
            "quantity": Column(
                int,
                checks=[Check.greater_than(0)]
            ),
            "unit_price": Column(
                float,
                checks=[Check.greater_than(0)]
            ),
            "total_amount": Column(
                float,
                checks=[Check.greater_than_or_equal_to(0)]
            ),
            "customer_age": Column(
                int,
                checks=[
                    Check.greater_than_or_equal_to(18),
                    Check.less_than_or_equal_to(100)
                ]
            ),
            "satisfaction_score": Column(
                int,
                checks=[
                    Check.greater_than_or_equal_to(1),
                    Check.less_than_or_equal_to(10)
                ]
            )
        })
        
        return schema
    
    def validate(self, df: pl.DataFrame, lazy: bool = False) -> pl.DataFrame:
        """
        Validate DataFrame against schema
        
        Args:
            df: DataFrame to validate
            lazy: If True, collect all errors before failing
        
        Returns:
            Validated DataFrame
        """
        logger.info("Validating schema...")
        
        try:
            validated_df = self.schema.validate(df, lazy=lazy)
            logger.info("✅ Schema validation passed")
            return validated_df
        
        except pa.errors.SchemaErrors as err:
            logger.error(f"Schema validation failed: {err}")
            raise


if __name__ == "__main__":
    # Test
    validator = SchemaValidator()
    
    # Valid data
    df_valid = pl.DataFrame({
        'customer_id': ['CUST_001', 'CUST_002'],
        'order_id': ['ORD_001', 'ORD_002'],
        'order_date': ['2024-01-01', '2024-01-02'],
        'product_category': ['Electronics', 'Clothing'],
        'quantity': [1, 2],
        'unit_price': [100.0, 50.0],
        'total_amount': [100.0, 100.0],
        'customer_age': [30, 25],
        'satisfaction_score': [8, 9]
    })
    
    result = validator.validate(df_valid)
    print("Validation successful!")
    print(result)