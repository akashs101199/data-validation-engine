"""
Silver layer transformation functions
"""

import polars as pl
from typing import List
from loguru import logger


class SilverTransformer:
    """
    Transformations for Silver layer data cleaning
    """
    
    def __init__(self):
        pass
    
    def clean_whitespace(self, df: pl.DataFrame) -> pl.DataFrame:
        """Remove leading/trailing whitespace from string columns"""
        logger.info("Cleaning whitespace...")
        
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col(col).str.strip_chars().alias(col)
                )
        
        return df
    
    def standardize_column_names(self, df: pl.DataFrame) -> pl.DataFrame:
        """Standardize column names to lowercase with underscores"""
        logger.info("Standardizing column names...")
        
        df = df.rename({
            col: col.lower().replace(' ', '_').replace('-', '_')
            for col in df.columns
        })
        
        return df
    
    def remove_duplicates(
        self, 
        df: pl.DataFrame, 
        subset: List[str] = None
    ) -> pl.DataFrame:
        """Remove duplicate rows"""
        logger.info("Removing duplicates...")
        
        original_count = len(df)
        
        if subset:
            df = df.unique(subset=subset)
        else:
            df = df.unique()
        
        removed = original_count - len(df)
        logger.info(f"Removed {removed} duplicate rows")
        
        return df
    
    def handle_missing_values(
        self, 
        df: pl.DataFrame,
        strategy: str = 'drop',
        columns: List[str] = None
    ) -> pl.DataFrame:
        """Handle missing values"""
        logger.info(f"Handling missing values with strategy: {strategy}")
        
        if columns is None:
            columns = df.columns
        
        if strategy == 'drop':
            for col in columns:
                if col in df.columns:
                    df = df.filter(pl.col(col).is_not_null())
        
        elif strategy == 'forward_fill':
            for col in columns:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).forward_fill().alias(col)
                    )
        
        elif strategy == 'backward_fill':
            for col in columns:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).backward_fill().alias(col)
                    )
        
        return df
    
    def apply_business_rules(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply business-specific validation rules"""
        logger.info("Applying business rules...")
        
        # Example: Ensure positive values for certain columns
        if 'quantity' in df.columns:
            df = df.filter(pl.col('quantity') > 0)
        
        if 'unit_price' in df.columns:
            df = df.filter(pl.col('unit_price') > 0)
        
        if 'total_amount' in df.columns:
            df = df.filter(pl.col('total_amount') >= 0)
        
        # Age validation
        if 'customer_age' in df.columns:
            df = df.filter(
                (pl.col('customer_age') >= 18) & 
                (pl.col('customer_age') <= 120)
            )
        
        return df


if __name__ == "__main__":
    # Test
    df = pl.DataFrame({
        'Name': ['  Alice  ', 'Bob', 'Charlie'],
        'Age': [25, 30, 35],
        'Salary': [50000, None, 70000]
    })
    
    transformer = SilverTransformer()
    df = transformer.clean_whitespace(df)
    df = transformer.standardize_column_names(df)
    print(df)