# 🧪 Data Validation Engine for Machine Learning Pipelines

A modular and scalable system for validating datasets in machine learning workflows. It supports schema validation, business rule enforcement, data drift detection, and generates detailed interactive dashboards using **Streamlit**.

---

## 🚀 Features

- ✅ Schema validation using column data types and structure  
- 📋 Custom business rules for domain-specific logic  
- 🔁 Data drift detection (between reference and production data)  
- 📊 Streamlit dashboard for summary and visualizations  
- 📂 Modular codebase (easy to extend and customize)  
- 📝 Detailed failure reports (rows, rules, charts)

---

## 📁 Project Structure

```

data-validation-engine/
├── data/
│   └── digital\_wallet\_ltv\_dataset.csv
│   └── validated\_output.csv
├── dashboards/
│   └── streamlit\_app.py
├── ingestion/
│   └── loader.py
├── validation/
│   ├── schema\_validation.py
│   ├── business\_rules.py
│   ├── drift\_detector.py
│   └── dashboard\_streamlit\_data.py
├── export/
│   └── writer.py
├── reports/
├── main.py
├── requirements.txt
└── README.md

````

---

## ⚙️ Installation

### 1. Clone the repository

```bash
git clone https://github.com/akashs101199/data-validation-engine.git
cd data-validation-engine
````

### 2. Set up virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## 📦 Usage Guide

### ▶️ Run the Data Validation Pipeline

```bash
python main.py
```

This will:

1. Load the raw dataset
2. Validate schema
3. Apply business rules
4. Export clean data
5. Generate reports
6. Prepare dashboard data

### 📊 Launch the Dashboard

```bash
streamlit run dashboards/streamlit_app.py
```

---

## 🧪 Example Business Rules

Defined in `business_rules.py`:

* `customer_id` must not be null
* `ltv` must be greater than 0
* `score` should be between 0 and 1
* `ltv` positive if churned
* `signup_date` must not be null or in the future

---

## 📊 Output Artifacts

After running the pipeline, the following are generated:

* `reports/validation_summary.csv`
* `reports/failed_rules.csv`
* `reports/failed_rows.csv`
* `reports/failure_chart.png`
* `data/validated_output.csv`

---

## 📉 Drift Detection

Use:

```python
detect_drift(reference_df, new_df)
```

Generates:

* `dashboards/drift_report.html`

---

## 📷 Dashboard Preview

| Validation Summary           | Failed Rules               | Drift Report               |
| ---------------------------- | -------------------------- | -------------------------- |
| ![](screenshots/summary.png) | ![](screenshots/rules.png) | ![](screenshots/drift.png) |

---

## 🤖 Use Cases

* Data intake for ML pipelines
* Feature validation
* QA for incoming datasets
* Continuous monitoring

---

## 🧩 Future Improvements

* YAML-configured rule sets
* Slack/email alerting
* Airflow integration
* Cloud-based validation support

---

## 🧑‍💻 Contributing

Pull requests welcome! Let's build better data pipelines together.

````

---

You can save this as `README.md` and push it:

```bash
git add README.md
git commit -m "📝 Updated README: removed license section"
git push origin main
````
# data-validation-enginer
