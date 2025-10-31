# 🤖 GenAI Data Validator

> An intelligent, conversational data validation engine powered by open-source LLMs. Transform data quality from reactive rules to proactive AI-driven insights.

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Mistral](https://img.shields.io/badge/Mistral-7B-orange.svg)](https://mistral.ai/)
[![LangChain](https://img.shields.io/badge/LangChain-🦜-green.svg)](https://www.langchain.com/)

## 🚀 What Makes This Different?

Traditional data validation = static rules + manual analysis.  
**GenAI Data Validator** = intelligent rules + conversational insights + self-healing data.

### 🧠 **Core Philosophy**
Data validation should be:
- **Conversational** - Ask questions in plain English
- **Intelligent** - LLM generates rules from business requirements
- **Explanatory** - Understand WHY data failed, not just that it did
- **Self-Improving** - Learn from validation patterns

---

## 🆕 **GenAI-Powered Features** 

### ✅ **Phase 1: Intelligent Rule Generation** 🔥 *In Progress*

**Before (Traditional):**
```python
# Manual rule writing
def validate_customer(row):
    if row['age'] < 18:
        return False
    if not is_valid_email(row['email']):
        return False
    return True
```

**After (GenAI-Powered):**
```python
# Natural language → Auto-generated rules
requirements = """
- Customers must be 18+ years old
- Email addresses must be valid
- LTV should be positive for active customers
"""

rules = generate_validation_rules(requirements, dataset_schema)
# LLM automatically generates optimized validation code
```

**Tech:** Mistral 7B (Ollama), LangChain, Code Generation Prompts

---

### 🔄 **Phase 2: Conversational Data Quality Assistant** 🔜 *Next*

**Ask questions about your data quality:**
```python
agent = DataQualityAgent()

# Natural language queries
agent.ask("What percentage of records failed validation today?")
# → "32% of records failed, primarily due to missing email addresses in the retail segment"

agent.ask("Show me customers with negative LTV")
# → Returns filtered DataFrame + explanation

agent.ask("Why did validation failures spike yesterday?")
# → "Spike correlates with bulk import from legacy CRM system - 450 records missing signup_date"
```

**Tech:** LangChain Agents, Pandas DataFrame Tools, RAG over validation history

---

### 🔍 **Phase 3: Anomaly Explanation & Auto-Fix** 🔮 *Planned*

**LLM explains validation failures in business terms:**
```
❌ Record Failed: customer_id=12345

Traditional Error:
"ltv validation failed: value -150 does not satisfy ltv > 0"

GenAI Explanation:
"This customer's LTV is negative (-150), which violates business rules. 
Likely causes:
1. Refund exceeded original purchase (check transaction history)
2. Data entry error during migration
3. Currency conversion issue

Suggested Fix:
→ Review transactions for customer_id=12345
→ If refund legitimate, update rule to allow negative LTV for refunded customers
→ If data error, check source system for correct value"
```

**Tech:** Mistral/Llama 3 for reasoning, Context-aware prompting

---

### 📊 **Phase 4: Smart Drift Detection with Business Context** 🔮 *Planned*

**Traditional Drift Alert:**
```
⚠️ Statistical drift detected: LTV mean shifted from 450 to 380 (p-value < 0.05)
```

**GenAI-Enhanced Analysis:**
```
⚠️ Significant LTV Decline Detected

What Changed:
- Average customer LTV dropped 15.6% (450 → 380)
- Shift began on March 15, 2025
- Primarily affects retail segment (-22%), e-commerce stable

Likely Business Causes:
1. New discount policy launched March 15 (affects retail)
2. Seasonal shopping patterns (post-holiday decline)
3. Competitor promotion detected via market analysis

Impact on ML Models:
- Customer churn model accuracy may degrade
- Recommend retraining with last 30 days data
- Feature importance: discount_rate now top predictor

Action Items:
→ Review pricing strategy with product team
→ Retrain models by March 31
→ Monitor competitor activity
```

**Tech:** Llama 3 for reasoning, Time-series analysis, External data integration

---

## 🛠️ **Tech Stack**

### **Current (Traditional Validation):**
- Python 3.9+, Pandas, NumPy
- Streamlit (dashboards)
- Matplotlib (visualizations)

### **🆕 GenAI Stack (Open Source):**

| Component | Technology | Why This Choice |
|-----------|-----------|-----------------|
| **LLM** | Mistral 7B / Llama 3 (via Ollama) | Privacy-first, runs locally, no API costs |
| **Framework** | LangChain | Agent orchestration, tool integration |
| **Vector DB** | ChromaDB / FAISS | Lightweight, open-source RAG |
| **Code Generation** | StarCoder / CodeLlama | Specialized for code generation |
| **Embeddings** | sentence-transformers | Free, runs locally |
| **API Layer** | FastAPI | Production-ready REST endpoints |
| **Frontend** | Streamlit + Gradio | Interactive UI for chat interface |

**Why Open Source?**
- ✅ **Privacy**: Data never leaves your infrastructure
- ✅ **Cost**: Zero API fees
- ✅ **Customization**: Fine-tune models on your data
- ✅ **Transparency**: Understand exactly what the LLM is doing

---

## 📁 Project Structure
```
genai-data-validator/
├── data/
│   └── digital_wallet_ltv_dataset.csv
│   └── validated_output.csv
├── dashboards/
│   └── streamlit_app.py
│   └── chat_interface.py              # 🆕 Conversational UI
├── ingestion/
│   └── loader.py
├── validation/
│   ├── schema_validation.py
│   ├── business_rules.py
│   ├── drift_detector.py
│   └── dashboard_streamlit_data.py
├── genai/                              # 🆕 GenAI Module
│   ├── __init__.py
│   ├── rule_generator.py              # Natural language → Python rules
│   ├── conversational_agent.py        # Q&A over validation data
│   ├── anomaly_explainer.py           # Root cause analysis
│   ├── drift_analyzer.py              # Business-context drift detection
│   ├── prompts/
│   │   ├── rule_generation.txt
│   │   ├── anomaly_explanation.txt
│   │   └── drift_analysis.txt
│   └── models/
│       └── model_manager.py           # Ollama integration
├── export/
│   └── writer.py
├── reports/
│   └── genai_insights.md              # 🆕 LLM-generated insights
├── tests/
│   └── test_genai_features.py
├── main.py
├── requirements.txt
├── ollama_setup.sh                     # 🆕 Quick Ollama setup
└── README.md
```

---

## ⚙️ Installation

### 1. Clone the repository
```bash
git clone https://github.com/akashs101199/genai-data-validator.git
cd genai-data-validator
```

### 2. Set up virtual environment
```bash
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. 🆕 **Install Ollama & Pull Models**

**Install Ollama:**
```bash
# macOS/Linux
curl -fsSL https://ollama.ai/install.sh | sh

# Or visit: https://ollama.ai/download
```

**Pull Open-Source Models:**
```bash
# For rule generation & conversation (lightweight)
ollama pull mistral:7b

# For advanced reasoning (optional, larger model)
ollama pull llama3:8b

# For code generation (optional)
ollama pull codellama:7b
```

**Quick Setup Script:**
```bash
chmod +x ollama_setup.sh
./ollama_setup.sh
```

### 5. Configure Models (optional)
```bash
# Create .env file
cp .env.example .env

# Edit .env:
# DEFAULT_LLM=mistral:7b
# EMBEDDING_MODEL=all-MiniLM-L6-v2
# OLLAMA_HOST=http://localhost:11434
```

---

## 📦 Usage Guide

### ▶️ **Traditional Validation (Current)**
```bash
python main.py
```

This runs:
1. Load dataset
2. Schema validation
3. Business rules
4. Export clean data
5. Generate reports

### 🆕 **GenAI-Enhanced Validation**

#### **Generate Rules from Natural Language:**
```python
from genai.rule_generator import generate_validation_rules

# Describe requirements in plain English
requirements = """
Business Requirements:
- All customers must have valid email addresses
- Customer age between 18-100
- LTV must be positive for active customers
- Signup date cannot be in the future
- Credit score between 300-850
"""

# LLM generates Python validation code
generated_rules = generate_validation_rules(
    requirements=requirements,
    dataset_schema=df.dtypes.to_dict(),
    model="mistral:7b"
)

print(generated_rules)  # Ready-to-use Python functions
```

#### **Ask Questions About Your Data:**
```python
from genai.conversational_agent import DataQualityAgent

agent = DataQualityAgent(validation_results_df)

# Natural language queries
response = agent.ask("What are the top 3 validation failures?")
# → "1. Missing email (245 records), 2. Invalid LTV (123 records), 3. Future signup_date (45 records)"

response = agent.ask("Show me customers with LTV > 1000 who failed validation")
# → Returns filtered DataFrame + explanation

response = agent.ask("Why did failures increase 40% this week?")
# → Analyzes patterns and provides business insights
```

#### **Get Explanations for Anomalies:**
```python
from genai.anomaly_explainer import explain_failure

failed_record = df[df['validation_failed'] == True].iloc[0]

explanation = explain_failure(
    record=failed_record,
    rule="ltv > 0",
    context={"segment": "retail", "source": "legacy_crm"}
)

print(explanation)
# → Detailed root cause analysis + suggested fixes
```

### 📊 **Launch Dashboards**

**Traditional Dashboard:**
```bash
streamlit run dashboards/streamlit_app.py
```

**🆕 GenAI Chat Interface:**
```bash
streamlit run dashboards/chat_interface.py
```

---

## 🧪 Example: Rule Generation Demo

**Input (Natural Language):**
```
Validate digital wallet customer data with these rules:
- Email must be valid format
- Age 18-100
- LTV positive
- Wallet balance non-negative
- Transaction count ≥ 0
- Last_active_date within last 2 years
```

**Output (Generated Python Code):**
```python
def validate_customer_record(row):
    """Auto-generated validation function"""
    errors = []
    
    # Email validation
    if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', str(row['email'])):
        errors.append("Invalid email format")
    
    # Age validation
    if not (18 <= row['age'] <= 100):
        errors.append(f"Age {row['age']} outside valid range [18-100]")
    
    # LTV validation
    if row['ltv'] <= 0:
        errors.append(f"LTV must be positive, got {row['ltv']}")
    
    # Wallet balance validation
    if row['wallet_balance'] < 0:
        errors.append("Wallet balance cannot be negative")
    
    # Transaction count validation
    if row['transaction_count'] < 0:
        errors.append("Transaction count must be non-negative")
    
    # Last active date validation
    two_years_ago = datetime.now() - timedelta(days=730)
    if pd.to_datetime(row['last_active_date']) < two_years_ago:
        errors.append("Customer inactive for over 2 years")
    
    return len(errors) == 0, errors
```

---

## 🚀 Features

### **Current (Traditional Validation):**
* ✅ Schema validation (data types, structure)
* ✅ Custom business rules
* ✅ Statistical drift detection
* ✅ Streamlit dashboard with visualizations
* ✅ Detailed failure reports

### **🆕 GenAI-Enhanced:**
* 🤖 **Natural language rule generation** - No more manual coding
* 💬 **Conversational data exploration** - Ask questions, get insights
* 🔍 **Intelligent anomaly explanation** - Understand root causes
* 📊 **Business-context drift analysis** - Why drift happened, not just that it did
* 🔧 **Auto-fix suggestions** - Remediation recommendations
* 📝 **Executive summaries** - LLM-generated reports for stakeholders

---

## 📊 Output Artifacts

**Traditional Reports:**
* `reports/validation_summary.csv`
* `reports/failed_rules.csv`
* `reports/failed_rows.csv`
* `reports/failure_chart.png`
* `data/validated_output.csv`

**🆕 GenAI-Enhanced Reports:**
* `reports/genai_insights.md` - Natural language analysis
* `reports/anomaly_explanations.json` - Root cause for each failure
* `reports/drift_business_impact.md` - Business-context drift report
* `reports/suggested_fixes.csv` - Auto-generated remediation steps

---

## 🤖 Use Cases

### **Traditional Data Validation:**
- Data intake for ML pipelines
- Feature validation
- QA for incoming datasets
- Continuous monitoring

### **🆕 With GenAI:**
- **Self-documenting pipelines** - Auto-generate validation documentation
- **Citizen data scientists** - Non-technical users validate data via chat
- **Intelligent alerting** - Context-aware alerts ("LTV dropped due to pricing change")
- **Automated root cause analysis** - Reduce debugging time by 80%
- **Compliance reporting** - Generate audit-ready reports automatically

---

## 🧩 Roadmap

### ✅ **Phase 1 (Completed)**
- [x] Schema validation
- [x] Business rules engine
- [x] Statistical drift detection
- [x] Streamlit dashboard

### 🔄 **Phase 2 (In Progress) - GenAI Integration**
- [x] Ollama setup & model integration
- [ ] Natural language rule generation (70% complete)
- [ ] Conversational Q&A agent (in development)
- [ ] Anomaly explanation engine (prototyping)

### 🔜 **Phase 3 (Next)**
- [ ] Auto-fix suggestions
- [ ] Business-context drift analysis
- [ ] Fine-tuned model on validation patterns
- [ ] Multi-dataset validation orchestration

### 🔮 **Phase 4 (Future)**
- [ ] YAML-based rule configuration
- [ ] Slack/email integration with GenAI summaries
- [ ] Airflow/Prefect DAG generation
- [ ] Cloud deployment (Docker + Kubernetes)
- [ ] Real-time streaming validation

---

## 🛠️ Development

### **Running Tests:**
```bash
pytest tests/ -v
```

### **Testing GenAI Features:**
```bash
# Test rule generation
python -m genai.rule_generator --test

# Test conversational agent
python -m genai.conversational_agent --demo

# Benchmark LLM performance
python scripts/benchmark_models.py
```

### **Model Comparison:**
```bash
# Compare Mistral vs Llama3 for your use case
python scripts/compare_models.py --task rule_generation
```

---

## 📚 Documentation

- [GenAI Architecture](docs/genai_architecture.md)
- [Prompt Engineering Guide](docs/prompt_engineering.md)
- [Model Selection Guide](docs/model_selection.md)
- [API Reference](docs/api_reference.md)

---

## 🧑‍💻 Contributing

Contributions welcome! Especially interested in:
- 🎯 Novel prompt engineering techniques
- 🔧 New GenAI use cases for data validation
- 🚀 Performance optimizations
- 📚 Documentation improvements

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📧 Contact

**Akash Shanmuganathan**
- LinkedIn: [linkedin.com/in/akash101199](https://linkedin.com/in/akash101199/)
- Email: akashs101199@gmail.com
- GitHub: [@akashs101199](https://github.com/akashs101199)

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

---

<div align="center">

**⭐ Star this repo if you're excited about GenAI-powered data validation!**

*Making data quality intelligent, conversational, and self-healing* 🤖

[![Star History](https://api.star-history.com/svg?repos=akashs101199/genai-data-validator&type=Date)](https://star-history.com/#akashs101199/genai-data-validator&Date)

</div>

