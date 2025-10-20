from pyod.models.knn import KNN
from sklearn.preprocessing import LabelEncoder
import pandas as pd

def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    # Check if 'email' column exists before trying to encode it
    if "email" in df_clean.columns:
        df_clean["email"] = LabelEncoder().fit_transform(df_clean["email"].fillna("missing"))
    
    # Select only numeric columns for the model
    numeric_df = df_clean.select_dtypes(include="number")
    
    # Ensure there's data to fit
    if not numeric_df.empty:
        model = KNN()
        model.fit(numeric_df)
        df_clean["anomaly"] = model.labels_
    else:
        # If no numeric data, return an empty anomaly column
        df_clean["anomaly"] = 0
        
    return df_clean