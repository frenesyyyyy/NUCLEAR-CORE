import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from content_engineering_node import process

def test_thin_but_semantic_rescue():
    print("--- Test 1: Thin-but-Semantic Rescue Logic ---")
    state = {
        "audit_integrity_status": "degraded",
        "client_content_depth": {
            "semantic_signals": {
                "schema_signal_count": 2, # Structural signal
                "cta_count": 0
            }
        },
        "client_content_clean": "<h1>Welcome</h1><p>Our Pricing</p><button>Order Now</button>",
        "json_ld_blocks": [{"@type": "WebSite"}],
        "meta_description": "A very semantic meta description exists."
    }
    # Note: Node calculates its own internal 'word_count' via content.split()
    # word_count in state is used for the condition check too
    result = process(state)
    eng = result["content_engineering"]
    
    assert eng["thin_but_semantic"] == True
    assert eng["is_extreme_degraded"] == False # Rescued by structure
    print("  PASSED\n")

def test_extreme_degraded_fallback():
    print("--- Test 2: Extreme Degraded Fallback ---")
    state = {
        "word_count": 40,
        "client_content_clean": "Just some random words and no structure.",
        "json_ld_blocks": []
    }
    result = process(state)
    eng = result["content_engineering"]
    assert eng["is_extreme_degraded"] == True
    assert eng["thin_but_semantic"] == False
    print("  PASSED\n")

if __name__ == "__main__":
    test_thin_but_semantic_rescue()
    test_extreme_degraded_fallback()
    print("=== ALL CONTENT ENGINEERING TESTS PASSED ===")
