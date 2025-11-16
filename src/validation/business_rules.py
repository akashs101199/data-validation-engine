"""
Business rule validation
"""

import polars as pl
from typing import List, Dict, Tuple
from loguru import logger


class BusinessRuleValidator:
    """
    Validate business-specific rules
    """
    
    def __init__(self):
        self.rules = []
    
    def validate_all(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, List[Dict]]:
        """
        Apply all business rules
        
        Returns:
            (valid_df, failed_records)
        """
        logger.info("Validating business rules...")
        
        failed_records = []
        valid_mask = pl.lit(True)
        
        # Rule 1: No null in critical columns
        critical_cols = ['customer_id', 'order_id', 'order_date']
        for col in critical_cols:
            if col in df.columns:
                col_mask = df[col].is_not_null()
                failed_idx = (~col_mask).to_list()
                if any(failed_idx):
                    failed_records.append({
                        'rule': f'NOT_NULL_{col}',
                        'column': col,
                        'count': sum(failed_idx)
                    })
                valid_mask = valid_mask & col_mask
        
        # Rule 2: Positive values
        positive_cols = ['quantity', 'unit_price', 'total_amount']
        for col in positive_cols:
            if col in df.columns:
                col_mask = df[col] > 0
                failed_idx = (~col_mask).to_list()
                if any(failed_idx):
                    failed_records.append({
                        'rule': f'POSITIVE_{col}',
                        'column': col,
                        'count': sum(failed_idx)
                    })
                valid_mask = valid_mask & col_mask
        
        # Rule 3: Valid date range (not in future)
        if 'order_date' in df.columns:
            from datetime import datetime
            today = datetime.now().date()
            
            # Convert to date if string
            if df['order_date'].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col('order_date').str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                )
            
            col_mask = df['order_date'] <= today
            failed_idx = (~col_mask).to_list()
            if any(failed_idx):
                failed_records.append({
                    'rule': 'VALID_DATE_RANGE',
                    'column': 'order_date',
                    'count': sum(failed_idx)
                })
            valid_mask = valid_mask & col_mask
        
        # Filter valid records
        valid_df = df.filter(valid_mask)
        
        logger.info(f"Business rules: {len(valid_df)}/{len(df)} records passed")
        
        return valid_df, failed_records


if __name__ == "__main__":
    # Test
    df = pl.DataFrame({
        'customer_id': ['C1', 'C2', None, 'C4'],
        'order_id': ['O1', 'O2', 'O3', 'O4'],
        'order_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2025-12-31'],
        'quantity': [1, -1, 2, 3],
        'unit_price': [100.0, 50.0, 75.0, 200.0],
        'total_amount': [100.0, 50.0, 150.0, 600.0]
    })
    
    validator = BusinessRuleValidator()
    valid_df, failed = validator.validate_all(df)
    
    print(f"\nValid records: {len(valid_df)}")
    print(f"Failed rules: {failed}")
    print(valid_df)