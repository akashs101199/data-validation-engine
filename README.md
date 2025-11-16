# 🏗️ Agentic Data Engineering Platform

> **Open-source, end-to-end data engineering solution with AI-powered quality control and Medallion Architecture**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 Overview

A modern, production-ready data engineering platform featuring:
- **🥉🥈🥇 Medallion Architecture** (Bronze, Silver, Gold layers)
- **🤖 Agentic AI** for autonomous data quality management
- **⚡ High-Performance** processing with Polars and DuckDB
- **🔄 Orchestration** with Prefect
- **📊 Interactive Dashboards** with Streamlit

## 🚀 Quick Start

### 1. Setup
```bash
# Clone and navigate
git clone <your-repo>
cd agentic-data-engineer

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run initial setup
python scripts/setup_initial.py
```

### 2. Generate Sample Data
```bash
python scripts/generate_sample_data.py
```

### 3. Run the Pipeline
```bash
python src/orchestration/prefect_flows.py
```

### 4. Launch Dashboard
```bash
streamlit run dashboards/streamlit_medallion_app.py
```

Visit: **http://localhost:8501**

## 📊 Architecture
```
Raw Data → 🥉 Bronze → 🥈 Silver → 🥇 Gold → Analytics
           (Raw)     (Clean)    (Business)
```

## ✨ Features

- ✅ Automated ETL Pipeline
- 🔍 Intelligent Data Profiling
- 🔧 Auto-Remediation
- 📈 Quality Monitoring
- 🚨 Drift Detection
- 🎯 Business Aggregations

## 📦 Project Structure
```
agentic-data-engineer/
├── config/              # Configuration files
├── data/               # Data layers (bronze/silver/gold)
├── src/
│   ├── agents/         # AI agents
│   ├── database/       # DuckDB manager
│   ├── orchestration/  # Prefect flows
│   └── transformations/# Data transformations
├── dashboards/         # Streamlit app
└── scripts/            # Utility scripts
```

## 🤝 Contributing

Contributions welcome! Please open an issue or submit a PR.

## 📝 License

MIT License - see LICENSE file

---

**Built with ❤️ using DuckDB, Polars, Prefect & Streamlit**