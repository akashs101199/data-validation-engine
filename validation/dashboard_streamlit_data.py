import pandas as pd
import matplotlib.pyplot as plt

def export_dashboard_data(validation_results, failed_df):
    # --- Export Failed Rules Summary ---
    # CORRECTED LOGIC: 'validation_results' is now the list of failed expectations directly.
    if validation_results:
        failed_rules_df = pd.DataFrame(validation_results)
    else:
        # Create an empty DataFrame with expected columns if no rules failed
        failed_rules_df = pd.DataFrame(columns=['expectation', 'column', 'message'])

    failed_rules_df.to_csv("reports/failed_rules.csv", index=False)

    # --- Export Failed Rows ---
    failed_df.to_csv("reports/failed_rows.csv", index=False)

    # --- Generate and Save a Failure Chart ---
    if not failed_rules_df.empty and 'column' in failed_rules_df.columns:
        failure_counts = failed_rules_df['column'].value_counts()
        plt.figure(figsize=(10, 6))
        failure_counts.plot(kind='bar', color='salmon')
        plt.title('Number of Failures by Column')
        plt.ylabel('Number of Failures')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("reports/failure_chart.png")
        plt.close()