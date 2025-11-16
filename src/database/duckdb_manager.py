"""
DuckDB Manager - Core database component for Medallion Architecture
Handles Bronze, Silver, and Gold layer operations
"""

import duckdb
import polars as pl
from pathlib import Path
from typing import Optional, List, Dict, Any
from loguru import logger
import yaml


class MedallionDuckDB:
    """
    Manages DuckDB database with Medallion Architecture layers
    """
    
    def __init__(self, db_path: str = "data/analytics.duckdb", 
                 config_path: str = "config/medallion_config.yaml"):
        self.db_path = db_path
        self.conn = None
        self.config = self._load_config(config_path)
        self._initialize_database()
    
    def _load_config(self, config_path: str) -> Dict:
        """Load medallion configuration"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return self._default_config()
    
    def _default_config(self) -> Dict:
        """Default configuration if config file not found"""
        return {
            'medallion': {
                'bronze': {'path': 'data/bronze/', 'format': 'parquet'},
                'silver': {'path': 'data/silver/', 'format': 'parquet'},
                'gold': {'path': 'data/gold/', 'format': 'parquet'}
            },
            'database': {
                'duckdb': {
                    'memory_limit': '2GB',
                    'threads': 4
                }
            }
        }
    
    def _initialize_database(self):
        """Initialize DuckDB connection and create schemas"""
        logger.info(f"Initializing DuckDB at {self.db_path}")
        self.conn = duckdb.connect(self.db_path)
        
        # Configure DuckDB
        db_config = self.config.get('database', {}).get('duckdb', {})
        if 'memory_limit' in db_config:
            self.conn.execute(f"SET memory_limit='{db_config['memory_limit']}'")
        if 'threads' in db_config:
            self.conn.execute(f"SET threads={db_config['threads']}")
        
        # Create schemas for each layer
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS quarantine")
        
        logger.info("✅ DuckDB initialized with medallion schemas")
    
    # ==================== BRONZE LAYER ====================
    
    def load_to_bronze(self, df: pl.DataFrame, table_name: str, mode: str = "append") -> int:
        """
        Load raw data to Bronze layer (no transformations)
        
        Args:
            df: Polars DataFrame with raw data
            table_name: Name for the bronze table
            mode: 'append' or 'replace'
        
        Returns:
            Number of rows loaded
        """
        logger.info(f"Loading {len(df)} rows to Bronze layer: {table_name}")
        
        bronze_path = Path(self.config['medallion']['bronze']['path'])
        bronze_path.mkdir(parents=True, exist_ok=True)
        
        # Save as parquet (raw format)
        parquet_path = bronze_path / f"{table_name}.parquet"
        df.write_parquet(parquet_path)
        
        # Register in DuckDB
        if mode == "replace":
            self.conn.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")
        
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze.{table_name} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        
        logger.info(f"✅ Loaded {len(df)} rows to bronze.{table_name}")
        return len(df)
    
    # ==================== SILVER LAYER ====================
    
    def promote_to_silver(
        self, 
        bronze_table: str, 
        silver_table: str,
        transformations: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Promote data from Bronze to Silver with cleaning and validation
        
        Args:
            bronze_table: Source table in bronze schema
            silver_table: Target table in silver schema
            transformations: List of transformation functions to apply
        
        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Promoting bronze.{bronze_table} → silver.{silver_table}")
        
        # Load from bronze
        df = pl.from_arrow(
            self.conn.execute(f"SELECT * FROM bronze.{bronze_table}").fetch_arrow_table()
        )
        
        original_count = len(df)
        
        # Apply transformations
        df = self._apply_silver_transformations(df)
        
        # Save to silver
        silver_path = Path(self.config['medallion']['silver']['path'])
        silver_path.mkdir(parents=True, exist_ok=True)
        
        parquet_path = silver_path / f"{silver_table}.parquet"
        df.write_parquet(parquet_path)
        
        # Register in DuckDB
        self.conn.execute(f"DROP TABLE IF EXISTS silver.{silver_table}")
        self.conn.execute(f"""
            CREATE TABLE silver.{silver_table} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        
        cleaned_count = len(df)
        logger.info(f"✅ Promoted {cleaned_count}/{original_count} rows to silver.{silver_table}")
        
        return df
    
    def _apply_silver_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply standard silver layer transformations"""
        
        # 1. Remove duplicates
        df = df.unique()
        
        # 2. Trim whitespace from string columns
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                df = df.with_columns(pl.col(col).str.strip_chars().alias(col))
        
        # 3. Standardize column names
        df = df.rename({col: col.lower() for col in df.columns})
        
        # 4. Convert date strings to proper dates
        if 'order_date' in df.columns:
            df = df.with_columns(
                pl.col('order_date').str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            )
        
        # 5. Remove rows with critical nulls
        critical_cols = ['customer_id', 'order_id', 'order_date']
        for col in critical_cols:
            if col in df.columns:
                df = df.filter(pl.col(col).is_not_null())
        
        # 6. Filter out invalid values
        if 'quantity' in df.columns:
            df = df.filter(pl.col('quantity') > 0)
        
        if 'unit_price' in df.columns:
            df = df.filter(pl.col('unit_price') > 0)
        
        if 'customer_age' in df.columns:
            df = df.filter(
                (pl.col('customer_age') >= 18) & (pl.col('customer_age') <= 100)
            )
        
        if 'satisfaction_score' in df.columns:
            df = df.filter(
                (pl.col('satisfaction_score') >= 1) & (pl.col('satisfaction_score') <= 10)
            )
        
        return df
    
    # ==================== GOLD LAYER ====================
    
    def create_gold_aggregate(
        self, 
        query: str, 
        table_name: str
    ) -> pl.DataFrame:
        """
        Create aggregated Gold layer table from Silver data
        
        Args:
            query: SQL query to create aggregation
            table_name: Name for the gold table
        
        Returns:
            Aggregated DataFrame
        """
        logger.info(f"Creating Gold layer aggregate: {table_name}")
        
        # Execute aggregation query
        result = self.conn.execute(query).fetch_arrow_table()
        df = pl.from_arrow(result)
        
        # Save to gold
        gold_path = Path(self.config['medallion']['gold']['path'])
        gold_path.mkdir(parents=True, exist_ok=True)
        
        parquet_path = gold_path / f"{table_name}.parquet"
        df.write_parquet(parquet_path)
        
        # Register in DuckDB
        self.conn.execute(f"DROP TABLE IF EXISTS gold.{table_name}")
        self.conn.execute(f"""
            CREATE TABLE gold.{table_name} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        
        logger.info(f"✅ Created gold.{table_name} with {len(df)} rows")
        
        return df
    
    # ==================== UTILITY METHODS ====================
    
    def query(self, sql: str) -> pl.DataFrame:
        """Execute SQL query and return Polars DataFrame"""
        result = self.conn.execute(sql).fetch_arrow_table()
        return pl.from_arrow(result)
    
    def get_table_stats(self, schema: str, table: str) -> Dict[str, Any]:
        """Get statistics for a table"""
        stats = {}
        
        # Row count
        result = self.conn.execute(
            f"SELECT COUNT(*) as count FROM {schema}.{table}"
        ).fetchone()
        stats['row_count'] = result[0]
        
        # Column info
        result = self.conn.execute(
            f"SELECT * FROM {schema}.{table} LIMIT 1"
        ).description
        stats['column_count'] = len(result)
        stats['columns'] = [col[0] for col in result]
        
        return stats
    
    def list_tables(self, schema: str) -> List[str]:
        """List all tables in a schema"""
        result = self.conn.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}'
        """).fetchall()
        
        return [row[0] for row in result]
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    # Example usage
    db = MedallionDuckDB()
    
    # List tables
    print("Bronze tables:", db.list_tables('bronze'))
    print("Silver tables:", db.list_tables('silver'))
    print("Gold tables:", db.list_tables('gold'))
    
    db.close()