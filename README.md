# 🧪 Data Validation Engine for Machine Learning Pipelines

A modular and scalable system for validating datasets in machine learning workflows. It supports schema validation, business rule enforcement, data drift detection, and generates detailed interactive dashboards using Streamlit.

## 🆕 **GenAI-Powered Upgrade (In Progress)** 

> 🚧 Currently enhancing this project with **AWS Bedrock** and **LangChain** to make data validation intelligent, conversational, and self-healing.

### 🤖 **What's Coming:**

#### **Phase 1: Intelligent Rule Generation** ⏳ *In Progress*
- **Natural Language Rule Creation**: Describe validation requirements in plain English, LLM generates Python validation code
- **Auto-Rule Discovery**: LLM analyzes dataset and suggests domain-specific validation rules
- **Example**: "All customers must have valid email addresses and be over 18 years old" → Automated rule generation

#### **Phase 2: Conversational Data Quality Assistant** 🔜 *Next*
- **Ask Questions About Your Data**: "What percentage of records failed validation today?"
- **Interactive Exploration**: "Show me all records where LTV is negative"
- **RAG-Powered Insights**: Query historical validation reports using natural language

#### **Phase 3: Anomaly Explanation & Auto-Fix** 🔮 *Planned*
- **Root Cause Analysis**: LLM explains WHY specific records failed validation
- **Intelligent Suggestions**: "This customer's signup_date is in the future. Consider using registration_date instead."
- **Auto-Remediation**: Propose fixes for common data quality issues

#### **Phase 4: Smart Drift Detection** 🔮 *Planned*
- **Business-Context Drift Analysis**: "Customer LTV decreased 15% - likely due to recent policy changes"
- **Predictive Alerts**: Detect drift patterns before they impact production models
- **Executive Summaries**: Auto-generate stakeholder reports in natural language

### 🛠️ **GenAI Tech Stack:**
- **LLM**: AWS Bedrock (Claude Sonnet)
- **Framework**: LangChain for agent orchestration
- **Vector DB**: FAISS for validation history RAG
- **Prompt Engineering**: Custom prompts for rule generation and anomaly explanation
- **Integration**: FastAPI endpoints for conversational interface

---

## 🚀 Features (Current)

* ✅ Schema validation using column data types and structure
* 📋 Custom business rules for domain-specific logic
* 🔁 Data drift detection (between reference and production data)
* 📊 Streamlit dashboard for summary and visualizations
* 📂 Modular codebase (easy to extend and customize)
* 📝 Detailed failure reports (rows, rules, charts)

## 📁 Project Structure
```
data-validation-engine/
├── data/
│   └── digital_wallet_ltv_dataset.csv
│   └── validated_output.csv
├── dashboards/
│   └── streamlit_app.py
├── ingestion/
│   └── loader.py
├── validation/
│   ├── schema_validation.py
│   ├── business_rules.py
│   ├── drift_detector.py
│   └── dashboard_streamlit_data.py
├── genai/                          # 🆕 NEW: GenAI Module
│   ├── rule_generator.py          # LLM-powered rule generation
│   ├── conversational_agent.py    # Natural language query interface
│   ├── anomaly_explainer.py       # Root cause analysis
│   └── drift_analyzer.py          # Intelligent drift detection
├── export/
│   └── writer.py
├── reports/
├── main.py
├── requirements.txt
└── README.md
```

## ⚙️ Installation

1. Clone the repository
```bash
git clone https://github.com/akashs101199/data-validation-engine.git
cd data-validation-engine
```

2. Set up virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. **🆕 Set up GenAI credentials** (for enhanced features)
```bash
# Add to .env file:
# AWS_ACCESS_KEY_ID=your_key
# AWS_SECRET_ACCESS_KEY=your_secret
# AWS_REGION=us-east-1
```

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

### 🆕 **Try GenAI-Powered Rule Generation** (Coming Soon)
```python
from genai.rule_generator import generate_validation_rules

# Describe your requirements in plain English
requirements = """
- All customers must have valid email addresses
- Customer age must be between 18 and 100
- LTV must be positive for active customers
- Signup date cannot be in the future
"""

# LLM generates Python validation code automatically
rules = generate_validation_rules(requirements, dataset_schema)
```

### 🆕 **Ask Questions About Your Data** (Coming Soon)
```python
from genai.conversational_agent import DataQualityAgent

agent = DataQualityAgent()

# Natural language queries
agent.ask("What percentage of records failed validation today?")
agent.ask("Show me trends in LTV validation failures over the last 30 days")
agent.ask("Which business rules have the highest failure rate?")
```

## 🧪 Example Business Rules

Defined in `business_rules.py`:
* `customer_id` must not be null
* `ltv` must be greater than 0
* `score` should be between 0 and 1
* `ltv` positive if churned
* `signup_date` must not be null or in the future

## 📊 Output Artifacts

After running the pipeline, the following are generated:
* `reports/validation_summary.csv`
* `reports/failed_rules.csv`
* `reports/failed_rows.csv`
* `reports/failure_chart.png`
* `data/validated_output.csv`
* 🆕 `reports/genai_insights.md` *(Coming soon - LLM-generated insights)*

## 📉 Drift Detection

Use:
```python
detect_drift(reference_df, new_df)
```

Generates:
* `dashboards/drift_report.html`
* 🆕 `reports/drift_analysis_genai.md` *(Coming soon - AI-powered drift explanation)*

## 🤖 Use Cases

**Current:**
* Data intake for ML pipelines
* Feature validation
* QA for incoming datasets
* Continuous monitoring

**🆕 With GenAI:**
* **Self-documenting data quality**: Auto-generate validation rule documentation
* **Conversational QA**: Non-technical stakeholders can query data quality
* **Intelligent alerting**: Context-aware notifications about critical issues
* **Root cause analysis**: Understand WHY data quality degraded

## 🧩 Roadmap

### ✅ **Phase 1 (Current)**
- [x] Schema validation
- [x] Business rules engine
- [x] Drift detection
- [x] Streamlit dashboard

### 🔄 **Phase 2 (In Progress) - GenAI Integration**
- [ ] LLM-powered rule generation from natural language
- [ ] Conversational data quality assistant
- [ ] Anomaly explanation and root cause analysis
- [ ] Auto-fix suggestions for common issues

### 🔮 **Phase 3 (Planned)**
- [ ] YAML-configured rule sets
- [ ] Slack/email alerting with GenAI summaries
- [ ] Airflow integration
- [ ] Cloud-based validation support (AWS Lambda)
- [ ] Multi-dataset drift analysis with business context

## 🛠️ Tech Stack

**Current:**
- Python, Pandas, NumPy
- Streamlit for dashboards
- Matplotlib for visualizations

**🆕 GenAI Stack:**
- AWS Bedrock (Claude Sonnet 4)
- LangChain (agent orchestration)
- FAISS (vector storage for RAG)
- FastAPI (conversational endpoints)

## 🧑‍💻 Contributing

Pull requests welcome! Let's build better data pipelines together.

**Especially interested in:**
- GenAI prompt engineering for data validation
- Novel use cases for LLMs in data quality
- Integration patterns with existing MLOps tools

## 📧 Contact

**Akash Shanmuganathan**
- LinkedIn: [linkedin.com/in/akash101199](https://linkedin.com/in/akash101199/)
- Email: akashs101199@gmail.com
- GitHub: [@akashs101199](https://github.com/akashs101199)

---

<div align="center">

**⭐ Star this repo if you're excited about GenAI-powered data validation!**

*Transforming data quality from reactive to intelligent* 🚀

</div>
