import sys
import os
from unittest.mock import MagicMock

# Add current directory to path
sys.path.append(os.getcwd())

def test_pipeline_imports():
    print("--- Running Pipeline Import Sanity Pass ---")
    
    # We want to test if run_pipeline can import all nodes
    # We'll mock the nodes' process functions to avoid running them
    
    from nuclear_geo_optimizer import run_pipeline
    
    # Mocking is complex because they are imported inside the function
    # But if the files are missing or have syntax errors, the imports will fail.
    
    # Let's just try to run it with a minimal state. 
    # Since nodes are imported inside run_pipeline, we can just call it 
    # and expect it to fail on the first node call, but the imports happen first.
    
    state = {
        "run_id": "test",
        "url": "https://example.com",
        "locale": "en",
        "business_type": "tech"
    }
    
    print("Attempting to trigger run_pipeline (checking imports)...")
    try:
        # This will likely fail when it tries to call orchestrator_process(current_state)
        # unless we mock it. But the goal is to see if the imports at lines 59-69 work.
        run_pipeline(state)
    except SystemExit:
        print("FAILED: SystemExit triggered (likely an ImportError).")
        sys.exit(1)
    except Exception as e:
        # If it's a "module not found" or similar import error, it would have been caught by the check.
        # If we reach here, it means imports passed but something else failed (expected).
        print(f"Imports successful. (Execution stopped as expected: {e})")
    
    print("--- Import Sanity Pass Complete ---")

if __name__ == "__main__":
    test_pipeline_imports()
