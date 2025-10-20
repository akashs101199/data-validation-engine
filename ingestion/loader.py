import pandas as pd

def load_csv(path):
    df = pd.read_csv(path)
    
    # Safely parse 'signup_date' if it exists
    if "signup_date" in df.columns:
        df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
        
    # Clean string columns by stripping whitespace
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # Convert Customer_ID to uppercase to ensure consistency
    if "Customer_ID" in df.columns:
        df["Customer_ID"] = df["Customer_ID"].str.upper()
        
    return df