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
    console.print("[bold cyan]Nuclear AI GEO Optimizer v5.0 Agency-Grade[/bold cyan] - Bootstrapping environment...", style="cyan")
    
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

    # ── Strict Pre-flight Checks ──
    from nodes.bootstrap_checks import validate_profile_registries
    validate_profile_registries()

def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="NUCLEAR AI GEO Optimizer v5.0 Agency-Grade")
    parser.add_argument("--url", required=True, help="Target website URL")
    parser.add_argument("--locale", required=True, choices=["en", "it"], help="Locale (en or it)")
    parser.add_argument("--runner", choices=["legacy", "hybrid"], default="hybrid",
                        help="Execution runner (legacy is now a compatibility alias for hybrid/standard)")
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
    """
    DEPRECATED: Thin compatibility wrapper.
    Delegates to run_hybrid_pipeline(mode='standard') which executes
    all 16 nodes sequentially with full state-reducer protection.
    """
    from nodes.execution_manager import run_hybrid_pipeline
    return run_hybrid_pipeline(state, run_mode="standard")

def main():
    bootstrap_environment()
    args = parse_args()
    state = initialize_state(args.url, args.locale)

    # Map legacy runner to hybrid/standard for backward compat
    run_mode = args.run_mode if args.runner == "hybrid" else "standard"

    console.print(
        f"Starting pipeline for URL: [bold yellow]{args.url}[/bold yellow] | "
        f"Locale: [bold yellow]{args.locale}[/bold yellow] | "
        f"Mode: [bold yellow]{run_mode}[/bold yellow]"
    )

    from nodes.execution_manager import run_hybrid_pipeline
    final_state = run_hybrid_pipeline(state, run_mode=run_mode)

    console.print(f"[bold cyan]Pipeline execution finished. Status: {final_state.get('status', 'Completed')}[/bold cyan]")

if __name__ == "__main__":
    main()
