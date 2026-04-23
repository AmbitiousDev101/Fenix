"""
Fenix - Master Controller
Automates infrastructure, pipeline execution, and dashboard launching.
"""

import subprocess
import sys
import os
import time
import argparse
from pathlib import Path

# --- Configuration ---
PROJECT_ROOT = Path(__file__).resolve().parent
LAKEHOUSE_DIR = PROJECT_ROOT / "lakehouse"
ENV_FILE = PROJECT_ROOT / ".env"

def log(msg, symbol="🚀"):
    print(f"\n{symbol} {msg}")

def run_command(cmd, name, shell=True):
    log(f"Running: {name}...", symbol="⏳")
    try:
        result = subprocess.run(cmd, shell=shell, check=True)
        return True
    except subprocess.CalledProcessError:
        log(f"Failed: {name}", symbol="❌")
        return False

def check_requirements():
    """Verify system requirements."""
    if not ENV_FILE.exists():
        log("Missing .env file! Please copy .env.example to .env and add your API key.", symbol="⚠️")
        sys.exit(1)
    
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True, shell=True)
    except:
        log("Docker is not running! Please start Docker Desktop.", symbol="⚠️")
        sys.exit(1)

def reset_everything():
    """Total nuke and rebuild."""
    log("Resetting Infrastructure and Data...", symbol="💥")
    run_command("docker-compose --profile airflow down -v", "Docker Down")
    
    # Wipe data layers
    for folder in ["silver", "gold"]:
        path = LAKEHOUSE_DIR / folder
        if path.exists():
            import shutil
            shutil.rmtree(path)
            log(f"Wiped {folder} layer")
    
    db_file = LAKEHOUSE_DIR / "fenix_analytics.duckdb"
    if db_file.exists():
        os.remove(db_file)
        log("Wiped Analytics Database")

def start_infra():
    """Start Docker containers."""
    log("Starting Infrastructure (Redpanda & Airflow)...", symbol="🐳")
    run_command("docker-compose --profile airflow up -d", "Docker Up")
    log("Waiting for services to stabilize (10s)...")
    time.sleep(10)

def run_pipeline(historical=False):
    """Execute the data pipeline."""
    log("Executing Fenix Data Pipeline...", symbol="🦅")
    cmd = "python orchestrate.py --full"
    if historical:
        cmd += " --historical"
    run_command(cmd, "Pipeline Execution")

def start_dashboard():
    """Launch Streamlit UI."""
    log("Launching Dashboard...", symbol="📊")
    subprocess.Popen(["python", "-m", "streamlit", "run", "dashboard.py"], shell=True)

def main():
    parser = argparse.ArgumentParser(description="Fenix Master Controller")
    parser.add_argument("--full", action="store_true", help="Start infra and run pipeline")
    parser.add_argument("--reset", action="store_true", help="Nuke everything and start fresh")
    parser.add_argument("--dashboard", action="store_true", help="Just launch the dashboard")
    parser.add_argument("--historical", action="store_true", help="Include historical data")
    
    args = parser.parse_args()

    check_requirements()

    if args.reset:
        reset_everything()
        start_infra()
        run_pipeline(args.historical)
        start_dashboard()
    elif args.full:
        start_infra()
        run_pipeline(args.historical)
        start_dashboard()
    elif args.dashboard:
        start_dashboard()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
