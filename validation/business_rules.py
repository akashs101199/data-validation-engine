import pandas as pd
import great_expectations as ge

def validate_rules(df):
    dataset = ge.dataset.PandasDataset(df.copy())
    
    # Define expectations
    dataset.expect_column_values_to_not_be_null("Customer_ID")
    dataset.expect_column_values_to_be_between("LTV", min_value=0)
    if "Age" in df.columns:
        dataset.expect_column_values_to_be_between("Age", min_value=18, max_value=100)
    if "Country" in df.columns:
        dataset.expect_column_values_to_not_be_null("Country")

    # Validate and get results
    validation_result = dataset.validate()

    failed_expectations = []
    failed_rows_indices = []

    for result in validation_result["results"]:
        if not result["success"]:
            failed_expectations.append({
                "expectation": result["expectation_config"]["expectation_type"],
                "column": result["expectation_config"]["kwargs"]["column"],
                "message": f"Failed: {result['expectation_config']['expectation_type']} on column {result['expectation_config']['kwargs']['column']}"
            })
            # Get indices of unexpected values if they exist
            if "unexpected_index_list" in result["result"]:
                failed_rows_indices.extend(result["result"]["unexpected_index_list"])

    # Get unique indices and retrieve failed rows
    unique_indices = sorted(list(set(failed_rows_indices)))
    failed_rows = df.iloc[unique_indices].copy()
    
    return failed_expectations, failed_rows