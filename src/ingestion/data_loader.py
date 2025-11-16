"""
Data Loader module for various data sources
"""

import polars as pl
from pathlib import Path
from typing import Optional
from loguru import logger


class DataLoader:
    """
    Universal data loader supporting multiple formats
    """
    
    def __init__(self):
        self.supported_formats = ['csv', 'parquet', 'json', 'excel']
    
    def load(
        self, 
        file_path: str, 
        file_type: Optional[str] = None,
        **kwargs
    ) -> pl.DataFrame:
        """
        Load data from file
        
        Args:
            file_path: Path to the data file
            file_type: Type of file (csv, parquet, json, excel)
            **kwargs: Additional arguments for the loader
        
        Returns:
            Polars DataFrame
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Auto-detect file type if not provided
        if file_type is None:
            file_type = path.suffix[1:].lower()
        
        logger.info(f"Loading {file_type} file: {file_path}")
        
        if file_type == 'csv':
            df = pl.read_csv(file_path, **kwargs)
        elif file_type == 'parquet':
            df = pl.read_parquet(file_path, **kwargs)
        elif file_type == 'json':
            df = pl.read_json(file_path, **kwargs)
        elif file_type in ['xlsx', 'xls', 'excel']:
            # For Excel, convert pandas to polars
            import pandas as pd
            df_pd = pd.read_excel(file_path, **kwargs)
            df = pl.from_pandas(df_pd)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        logger.info(f"✅ Loaded {len(df)} rows, {len(df.columns)} columns")
        
        return df


if __name__ == "__main__":
    loader = DataLoader()
    df = loader.load("data/raw/ecommerce_transactions.csv")
    print(df.head())