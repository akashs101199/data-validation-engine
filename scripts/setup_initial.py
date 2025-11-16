#!/usr/bin/env python3
"""
Initial setup script for Agentic Data Engineering Platform
Creates directory structure and initializes the project
"""

import os
import sys
from pathlib import Path
import subprocess

def print_banner():
    """Print welcome banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║   🏗️  AGENTIC DATA ENGINEERING PLATFORM                      ║
    ║                                                              ║
    ║   Open-source ETL with Medallion Architecture               ║
    ║   & AI-Powered Quality Control                              ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def check_python_version():
    """Verify Python version"""
    print("🔍 Checking Python version...")
    if sys.version_info < (3, 10):
        print(f"❌ Python 3.10+ required. Current: {sys.version}")
        sys.exit(1)
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")

def create_directory_structure():
    """Create all required directories"""
    print("\n📁 Creating directory structure...")
    
    directories = [
        "config",
        "data/raw",
        "data/bronze",
        "data/silver",
        "data/gold",
        "data/quarantine",
        "src/agents",
        "src/ingestion",
        "src/transformations",
        "src/validation",
        "src/database",
        "src/orchestration",
        "dashboards",
        "scripts",
        "reports",
        "logs",
        "tests",
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"  ✓ Created: {directory}")
    
    # Create __init__.py files
    init_dirs = [
        "src", "src/agents", "src/ingestion", "src/transformations",
        "src/validation", "src/database", "src/orchestration", "tests"
    ]
    
    for directory in init_dirs:
        init_file = Path(directory) / "__init__.py"
        init_file.touch()
    
    print("✅ Directory structure created")

def install_dependencies():
    """Install Python dependencies"""
    print("\n📦 Installing dependencies...")
    print("  This may take a few minutes...")
    
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "-r", "requirements.txt"],
            check=True
        )
        print("✅ Dependencies installed successfully")
    except subprocess.CalledProcessError:
        print("⚠️  Some packages failed. Run: pip install -r requirements.txt")

def print_next_steps():
    """Print next steps"""
    print("\n" + "="*70)
    print("✅ SETUP COMPLETE!")
    print("="*70)
    
    print("\n📋 Next Steps:\n")
    print("1️⃣  Generate sample data:")
    print("   python scripts/generate_sample_data.py")
    
    print("\n2️⃣  Run the ETL pipeline:")
    print("   python src/orchestration/prefect_flows.py")
    
    print("\n3️⃣  Launch the dashboard:")
    print("   streamlit run dashboards/streamlit_medallion_app.py")
    
    print("\n4️⃣  Access the dashboard:")
    print("   🌐 http://localhost:8501")
    
    print("\n" + "="*70)
    print("📚 Check README.md for full documentation")
    print("="*70 + "\n")

def main():
    """Main setup function"""
    try:
        print_banner()
        check_python_version()
        create_directory_structure()
        
        # Ask if user wants to install dependencies
        response = input("\n📦 Install dependencies now? (y/n): ").lower()
        if response == 'y':
            install_dependencies()
        else:
            print("⏭️  Skipped dependency installation")
        
        print_next_steps()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()