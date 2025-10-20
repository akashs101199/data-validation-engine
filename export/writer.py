def export_validated_data(df, output_path):
    # Simple implementation to write the DataFrame to CSV
    df.to_csv(output_path, index=False)
    print(f"✅ Data exported to {output_path}")
