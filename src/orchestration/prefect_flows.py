"""
Prefect Orchestration Flow for Agentic Data Engineering Pipeline
Orchestrates the complete ETL process across Medallion layers
"""

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import polars as pl
from pathlib import Path
from loguru import logger
from datetime import datetime
from typing import Dict, Any, Optional
import yaml
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.database.duckdb_manager import MedallionDuckDB
from src.agents.agentic_agents import DataProfilerAgent, QualityAgent, RemediationAgent


@task(name="Load Configuration", retries=2)
def load_config(config_path: str = "config/pipeline_config.yaml") -> Dict:
    """Load pipeline configuration"""
    logger.info(f"Loading configuration from {config_path}")
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        logger.warning(f"Config not found: {config_path}, using defaults")
        return {
            'pipeline': {
                'sources': [{
                    'name': 'ecommerce_transactions',
                    'type': 'csv',
                    'path': 'data/raw/ecommerce_transactions.csv'
                }]
            }
        }


@task(name="Extract Raw Data", retries=3, retry_delay_seconds=30)
def extract_raw_data(source_config: Dict) -> pl.DataFrame:
    """
    Extract data from source
    Supports: CSV, Parquet, JSON
    """
    logger.info(f"Extracting data from {source_config['path']}")
    
    file_type = source_config['type'].lower()
    file_path = source_config['path']
    
    if not Path(file_path).exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")
    
    if file_type == 'csv':
        df = pl.read_csv(file_path)
    elif file_type == 'parquet':
        df = pl.read_parquet(file_path)
    elif file_type == 'json':
        df = pl.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    logger.info(f"✅ Extracted {len(df)} rows, {len(df.columns)} columns")
    return df


@task(name="Load to Bronze Layer")
def load_to_bronze(
    df: pl.DataFrame, 
    table_name: str,
    db_path: str = "data/analytics.duckdb"
) -> Dict[str, Any]:
    """Load raw data to Bronze layer"""
    logger.info(f"Loading {len(df)} rows to Bronze layer")
    
    db = MedallionDuckDB(db_path=db_path)
    rows_loaded = db.load_to_bronze(df, table_name, mode='replace')
    db.close()
    
    return {
        'layer': 'bronze',
        'table': table_name,
        'rows_loaded': rows_loaded,
        'timestamp': datetime.now().isoformat()
    }


@task(name="Profile Data")
def profile_data(df: pl.DataFrame, dataset_name: str) -> Dict[str, Any]:
    """Profile dataset and detect quality issues"""
    logger.info(f"Profiling dataset: {dataset_name}")
    
    profiler = DataProfilerAgent()
    profile = profiler.profile_dataset(df, dataset_name)
    
    logger.info(f"Profile complete: {len(profile['issues_detected'])} issues detected")
    
    return profile


@task(name="Calculate Quality Score")
def calculate_quality_score(profile: Dict) -> float:
    """Calculate data quality score"""
    quality_agent = QualityAgent()
    score = quality_agent.calculate_quality_score(profile)
    
    logger.info(f"📊 Quality Score: {score}/100")
    
    return score


@task(name="Auto-Remediate Issues")
def auto_remediate(
    df: pl.DataFrame, 
    profile: Dict,
    enabled: bool = True
) -> tuple:
    """Automatically fix data quality issues"""
    if not enabled:
        logger.info("Auto-remediation disabled")
        return df, []
    
    logger.info("Starting auto-remediation")
    
    remediation_agent = RemediationAgent()
    df_fixed, actions = remediation_agent.auto_remediate(
        df, 
        profile['issues_detected']
    )
    
    logger.info(f"✅ Remediation complete: {len(actions)} actions taken")
    
    return df_fixed, actions


@task(name="Promote to Silver Layer")
def promote_to_silver(
    bronze_table: str,
    silver_table: str,
    db_path: str = "data/analytics.duckdb"
) -> Dict[str, Any]:
    """Promote data from Bronze to Silver with cleaning"""
    logger.info(f"Promoting {bronze_table} to Silver layer")
    
    db = MedallionDuckDB(db_path=db_path)
    df_silver = db.promote_to_silver(bronze_table, silver_table)
    db.close()
    
    return {
        'layer': 'silver',
        'table': silver_table,
        'rows': len(df_silver),
        'timestamp': datetime.now().isoformat()
    }


@task(name="Create Gold Aggregations")
def create_gold_aggregations(
    silver_table: str,
    aggregations: list,
    db_path: str = "data/analytics.duckdb"
) -> Dict[str, Any]:
    """Create Gold layer business aggregations"""
    logger.info(f"Creating Gold layer aggregations from {silver_table}")
    
    db = MedallionDuckDB(db_path=db_path)
    results = {}
    
    # Daily Sales Summary
    if 'daily_sales' in aggregations:
        try:
            query = f"""
                SELECT 
                    order_date,
                    COUNT(*) as order_count,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value,
                    COUNT(DISTINCT customer_id) as unique_customers
                FROM silver.{silver_table}
                GROUP BY order_date
                ORDER BY order_date DESC
            """
            df = db.create_gold_aggregate(query, 'daily_sales')
            results['daily_sales'] = len(df)
        except Exception as e:
            logger.error(f"Failed to create daily_sales: {e}")
    
    # Customer Lifetime Value
    if 'customer_lifetime_value' in aggregations:
        try:
            query = f"""
                SELECT 
                    customer_id,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as lifetime_value,
                    AVG(total_amount) as avg_order_value,
                    MIN(order_date) as first_order_date,
                    MAX(order_date) as last_order_date,
                    MAX(customer_segment) as segment
                FROM silver.{silver_table}
                GROUP BY customer_id
                ORDER BY lifetime_value DESC
            """
            df = db.create_gold_aggregate(query, 'customer_ltv')
            results['customer_ltv'] = len(df)
        except Exception as e:
            logger.error(f"Failed to create customer_ltv: {e}")
    
    # Product Performance
    if 'product_performance' in aggregations:
        try:
            query = f"""
                SELECT 
                    product_category,
                    product_name,
                    COUNT(*) as units_sold,
                    SUM(quantity) as total_quantity,
                    SUM(total_amount) as revenue,
                    AVG(unit_price) as avg_price
                FROM silver.{silver_table}
                GROUP BY product_category, product_name
                ORDER BY revenue DESC
            """
            df = db.create_gold_aggregate(query, 'product_performance')
            results['product_performance'] = len(df)
        except Exception as e:
            logger.error(f"Failed to create product_performance: {e}")
    
    # Regional Analytics
    if 'regional_analytics' in aggregations:
        try:
            query = f"""
                SELECT 
                    shipping_country,
                    COUNT(*) as order_count,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value,
                    COUNT(DISTINCT customer_id) as unique_customers
                FROM silver.{silver_table}
                GROUP BY shipping_country
                ORDER BY total_revenue DESC
            """
            df = db.create_gold_aggregate(query, 'regional_analytics')
            results['regional_analytics'] = len(df)
        except Exception as e:
            logger.error(f"Failed to create regional_analytics: {e}")
    
    db.close()
    
    logger.info(f"✅ Created {len(results)} Gold tables")
    
    return {
        'layer': 'gold',
        'tables': results,
        'timestamp': datetime.now().isoformat()
    }


@task(name="Send Quality Alert")
def send_quality_alert(quality_score: float, threshold: float = 80.0):
    """Send alert if quality score is below threshold"""
    if quality_score < threshold:
        logger.warning(f"⚠️ ALERT: Quality score {quality_score} is below threshold {threshold}")
        # In production: send email, Slack notification, etc.
    else:
        logger.info(f"✅ Quality score {quality_score} meets threshold {threshold}")


# ==================== MAIN FLOW ====================

@flow(
    name="Agentic Data Engineering Pipeline",
    description="End-to-end ETL with Medallion Architecture and Agentic Quality Control",
    task_runner=ConcurrentTaskRunner()
)
def agentic_etl_pipeline(
    config_path: str = "config/pipeline_config.yaml",
    enable_auto_remediation: bool = True
):
    """
    Main orchestration flow for the complete ETL pipeline
    
    Flow:
    1. Extract raw data
    2. Load to Bronze (raw)
    3. Profile & assess quality
    4. Auto-remediate issues
    5. Promote to Silver (clean)
    6. Create Gold aggregations (business-ready)
    7. Alert on quality issues
    """
    
    logger.info("🚀 Starting Agentic ETL Pipeline")
    
    try:
        # 1. Load configuration
        config = load_config(config_path)
        pipeline_config = config.get('pipeline', {})
        source_config = pipeline_config.get('sources', [{}])[0]
        
        # 2. Extract raw data
        df_raw = extract_raw_data(source_config)
        
        # 3. Load to Bronze layer
        bronze_result = load_to_bronze(
            df_raw, 
            table_name=source_config['name']
        )
        logger.info(f"Bronze: {bronze_result}")
        
        # 4. Profile data and assess quality
        profile = profile_data(df_raw, source_config['name'])
        quality_score = calculate_quality_score(profile)
        
        # 5. Auto-remediate issues (if enabled)
        df_remediated, actions = auto_remediate(
            df_raw, 
            profile,
            enabled=enable_auto_remediation
        )
        
        # 6. Promote to Silver layer
        silver_result = promote_to_silver(
            bronze_table=source_config['name'],
            silver_table=source_config['name']
        )
        logger.info(f"Silver: {silver_result}")
        
        # 7. Create Gold layer aggregations
        gold_result = create_gold_aggregations(
            silver_table=source_config['name'],
            aggregations=[
                'daily_sales',
                'customer_lifetime_value',
                'product_performance',
                'regional_analytics'
            ]
        )
        logger.info(f"Gold: {gold_result}")
        
        # 8. Send quality alerts if needed
        send_quality_alert(quality_score, threshold=80.0)
        
        # Pipeline summary
        summary = {
            'pipeline': 'agentic-data-engineering',
            'status': 'SUCCESS',
            'bronze': bronze_result,
            'silver': silver_result,
            'gold': gold_result,
            'quality_score': quality_score,
            'issues_found': len(profile['issues_detected']),
            'remediation_actions': len(actions),
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"✅ Pipeline completed successfully")
        logger.info(f"Summary: {summary}")
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # Configure logging
    logger.add("logs/pipeline.log", rotation="1 day")
    
    # Run the pipeline
    result = agentic_etl_pipeline(enable_auto_remediation=True)
    
    print("\n" + "="*70)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*70)
    print(f"Status: {result['status']}")
    print(f"Quality Score: {result['quality_score']}/100")
    print(f"Issues Found: {result['issues_found']}")
    print(f"Remediation Actions: {result['remediation_actions']}")
    print("="*70 + "\n")