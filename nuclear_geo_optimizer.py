import os
import sys
import argparse
import uuid
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import nltk
from rich.console import Console

console = Console()

def bootstrap_environment():
    """Create necessary directories and download resources."""
    console.print("[bold cyan]Nuclear AI GEO Optimizer v3.4[/bold cyan] - Bootstrapping environment...", style="cyan")
    
    # Create nodes/ folder if missing
    os.makedirs("nodes", exist_ok=True)
    
    # Create empty nodes/__init__.py if missing
    init_file = os.path.join("nodes", "__init__.py")
    if not os.path.exists(init_file):
        with open(init_file, "w") as f:
            pass

    # Load .env
    load_dotenv()
    
    # Download NLTK data
    try:
        nltk.download(['stopwords', 'punkt'], quiet=True, download_dir=None)
    except Exception as e:
        console.print(f"[yellow]Warning: Could not download NLTK data ([/yellow]{e}[yellow]). Functionality may be limited.[/yellow]")

def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="NUCLEAR AI GEO Optimizer v3.4 MVP")
    parser.add_argument("--url", required=True, help="Target website URL")
    parser.add_argument("--locale", required=True, choices=["en", "it"], help="Locale (en or it)")
    parser.add_argument("--typo", "--type", choices=["tech", "food", "freelancer"], default="tech", help="Business type for geo-targeting")
    args = parser.parse_args()
    return args

def initialize_state(url: str, locale: str, business_type: str) -> dict:
    """Initialize the exact state dictionary per the contract."""
    return {
        "run_id": str(uuid.uuid4()),
        "started_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "url": url,
        "locale": locale,
        "business_type": business_type,
        "status": "initialized"
    }

def run_pipeline(state: dict) -> dict:
    """Run the 6-node DAG strictly synchronously in exact order."""
    
    try:
        from nodes.orchestrator_node import process as orchestrator_process
        from nodes.content_fetcher_node import process as content_fetcher_process
        from nodes.prospector_node import process as prospector_process
        from nodes.researcher_node import process as researcher_process
        from nodes.validator_node import process as validator_process
        from nodes.finalizer_node import process as finalizer_process
    except ImportError as e:
        console.print(f"[bold red]Import Error: Make sure all node files exist. Details: {e}[/bold red]")
        sys.exit(1)

    nodes = [
        ("Orchestrator Node", orchestrator_process),
        ("Content Fetcher Node", content_fetcher_process),
        ("Prospector Node", prospector_process),
        ("Researcher Node", researcher_process),
        ("Validator Node", validator_process),
        ("Finalizer Node", finalizer_process),
    ]

    current_state = state

    for node_name, node_func in nodes:
        console.print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting [bold green]{node_name}[/bold green]...")
        try:
            current_state = node_func(current_state)
        except Exception as e:
            console.print(f"[bold red]CRITICAL NODE FAILURE ({node_name}): {e}[/bold red]")
            # Downstream nodes must gracefully handle missing data.
            # Continue the pipeline.
            pass
        console.print(f"[{datetime.now().strftime('%H:%M:%S')}] Completed [bold green]{node_name}[/bold green].")

    return current_state

def main():
    bootstrap_environment()
    args = parse_args()
    state = initialize_state(args.url, args.locale, args.typo)
    
    console.print(f"Starting pipeline for URL: [bold yellow]{args.url}[/bold yellow] | Locale: [bold yellow]{args.locale}[/bold yellow]")
    
    final_state = run_pipeline(state)
    
    console.print(f"[bold cyan]Pipeline execution finished. Status: {final_state.get('status', 'Completed')}[/bold cyan]")

if __name__ == "__main__":
    main()
