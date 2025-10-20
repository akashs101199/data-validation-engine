from evidently.report import Report
from evidently.metrics import DataDriftTable

def detect_drift(current_df, ref_df):
    report = Report(metrics=[DataDriftTable()])
    report.run(reference_data=ref_df, current_data=current_df)
    report.save_html("dashboards/drift_report.html")
