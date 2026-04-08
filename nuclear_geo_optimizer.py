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
    console.print("[bold cyan]Nuclear AI GEO Optimizer v4.5 Agency-Grade[/bold cyan] - Bootstrapping environment...", style="cyan")
    
    if getattr(sys, 'frozen', False):
        exports_dir = os.path.abspath(os.path.join(os.path.dirname(sys.executable), "exports"))
        console.print(f"[bold magenta][System][/bold magenta] Packaged runtime active. Executable: {sys.executable}")
        console.print(f"[bold magenta][System][/bold magenta] Exports routing to: {exports_dir}")

    
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
    parser = argparse.ArgumentParser(description="NUCLEAR AI GEO Optimizer v4.5 Agency-Grade")
    parser.add_argument("--url", required=True, help="Target website URL")
    parser.add_argument("--locale", required=True, choices=["en", "it"], help="Locale (en or it)")
    parser.add_argument("--runner", choices=["legacy", "hybrid"], default="legacy", help="Execution runner")
    parser.add_argument("--run-mode", choices=["lite", "standard", "agency"], default="standard", help="Hybrid runner mode")
    args = parser.parse_args()
    return args

def initialize_state(url: str, locale: str) -> dict:
    """Initialize the exact state dictionary per the contract."""
    return {
        "run_id": str(uuid.uuid4()),
        "started_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "url": url,
        "locale": locale,
        "audit_integrity_status": "valid",
        "audit_integrity_reasons": [],
        "status": "initialized"
    }

def run_pipeline(state: dict) -> dict:
    """Run the 6-node DAG strictly synchronously in exact order."""
    
    try:
        from nodes.orchestrator_node import process as orchestrator_process
        from nodes.content_fetcher_node import process as content_fetcher_process
        from nodes.prospector_node import process as prospector_process
        from nodes.business_profile_selector_node import process as business_profile_selector_process
        from nodes.content_strategist_node import process as content_strategist_process
        from nodes.content_engineering_node import process as content_engineering_process
        from nodes.schema_generation_node import process as schema_generation_process
        from nodes.crawler_policy_node import process as crawler_policy_process
        from nodes.earned_media_node import process as earned_media_process
        from nodes.source_quality_node import process as source_quality_process
        from nodes.researcher_node import process as researcher_process
        from nodes.model_analytics_node import process as model_analytics_process
        from nodes.implementation_blueprint_node import process as implementation_blueprint_process
        from nodes.agentic_readiness_node import process as agentic_readiness_process
        from nodes.validator_node import process as validator_process
        from nodes.finalizer_node import process as finalizer_process
    except ImportError as e:
        console.print(f"[bold red]Import Error: Make sure all node files exist. Details: {e}[/bold red]")
        sys.exit(1)

    nodes = [
        ("Content Fetcher Node", content_fetcher_process),
        ("Orchestrator Node", orchestrator_process),
        ("Prospector Node", prospector_process),
        ("Business Profile Selector Node", business_profile_selector_process),
        ("Content Strategist Node", content_strategist_process),
        ("Content Engineering Node", content_engineering_process),
        ("Schema Generation Node", schema_generation_process),
        ("Crawler Policy Node", crawler_policy_process),
        ("Earned Media Node", earned_media_process),
        ("Source Quality Node", source_quality_process),
        ("Researcher Node", researcher_process),
        ("Model Analytics Node", model_analytics_process),
        ("Implementation Blueprint Node", implementation_blueprint_process),
        ("Agentic Readiness Node", agentic_readiness_process),
        ("Validator Node", validator_process),
        ("Finalizer Node", finalizer_process),
    ]

    current_state = state
    executed_nodes = set()

    for node_name, node_func in nodes:
        if node_name in executed_nodes:
            continue
            
        console.print(f"[{datetime.now().strftime('%H:%M:%S')}] Executing [bold green]{node_name}[/bold green]...")
        try:
            current_state = node_func(current_state)
            executed_nodes.add(node_name)
        except Exception as e:
            console.print(f"[bold red]CRITICAL NODE FAILURE ({node_name}): {e}[/bold red]")
            current_state["status"] = "degraded"
            # Downstream nodes must gracefully handle missing data.
            # Continue the pipeline.
            pass

    return current_state

def main():
    bootstrap_environment()
    args = parse_args()
    state = initialize_state(args.url, args.locale)

    console.print(
        f"Starting pipeline for URL: [bold yellow]{args.url}[/bold yellow] | "
        f"Locale: [bold yellow]{args.locale}[/bold yellow] | "
        f"Runner: [bold yellow]{args.runner}[/bold yellow]"
    )

    if args.runner == "legacy":
        final_state = run_pipeline(state)
    else:
        from nodes.execution_manager import run_hybrid_pipeline
        final_state = run_hybrid_pipeline(state, run_mode=args.run_mode)

    console.print(f"[bold cyan]Pipeline execution finished. Status: {final_state.get('status', 'Completed')}[/bold cyan]")

if __name__ == "__main__":
    main()
