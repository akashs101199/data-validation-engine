import pandas as pd
import pandera as pa
from ingestion.loader import load_csv
from validation.schema_validation import validate_schema
from validation.business_rules import validate_rules
from validation.drift_detector import detect_drift
from export.writer import export_validated_data
from validation.dashboard_streamlit_data import export_dashboard_data
from ml.anomaly_model import detect_anomalies

# Load raw data
try:
    df = load_csv("data/digital_wallet_ltv_dataset.csv")
    print("✅ Data loaded successfully.")
except Exception as e:
    print(f"❌ Failed to load data: {e}")
    exit()

# Validate schema
try:
    print("⏳ Running schema validation...")
    df = validate_schema(df, lazy=True)
    print("✅ Schema validation passed.")
    
except pa.errors.SchemaErrors as err:
    print(f"❌ Schema validation failed with {len(err.failure_cases)} errors.")
    
    # Isolate valid rows by dropping the indexes of the failed rows
    valid_indexes = df.index.difference(err.failure_cases.index)
    df_valid = df.loc[valid_indexes].copy()

    # Isolate and save the failed rows for review
    df_failed_schema = err.failure_cases
    export_validated_data(df_failed_schema, "data/failed_schema_validation.csv")
    print(f"📄 Saved {len(df_failed_schema)} schema-failing rows to 'data/failed_schema_validation.csv'")
    
    # Continue the pipeline with only the valid data
    df = df_valid
    print(f"➡️ Continuing pipeline with {len(df)} valid rows.")


# Apply business rules
try:
    validation_results, failed_df = validate_rules(df)
    if failed_df.empty:
        print("✅ Business rule validation passed.")
    else:
        print(f"⚠️ Found {len(failed_df)} rows failing business rules.")

except Exception as e:
    print(f"❌ Business rule validation error: {e}")
    exit()

# Export dashboard summary for Streamlit
export_dashboard_data(validation_results, failed_df)

# Anomaly Detection
try:
    df_with_anomalies = detect_anomalies(df)
    anomalous_df = df_with_anomalies[df_with_anomalies['anomaly'] == 1]
    if not anomalous_df.empty:
        print(f"⚠️ Detected {len(anomalous_df)} anomalous records.")
        export_validated_data(anomalous_df, "data/anomalous_records.csv")
    else:
        print("✅ No anomalies detected.")
except Exception as e:
    print(f"❌ Anomaly detection error: {e}")

# Export validated (clean) data
export_validated_data(df, "data/validated_output.csv")

# Drift detection (for demo, we compare the same data)
detect_drift(df, df)

print("✅ Data pipeline executed successfully.")