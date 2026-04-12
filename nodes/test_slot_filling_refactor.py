
import sys
import os
import json
from unittest.mock import MagicMock

# Mock Heavy Dependencies BEFORE importing researcher_node
sys.modules["sentence_transformers"] = MagicMock()
sys.modules["chromadb"] = MagicMock()
sys.modules["google.generativeai"] = MagicMock()

# Add project root to path
sys.path.append(os.getcwd())

from nodes.researcher_node import _assemble_tier_queries, TierSlotBuilder

def test_slot_filling_recovery():
    print("\n--- Testing Slot-Filling Recovery (Wave 1 -> Wave 3) ---")
    
    # Setup mock data where Wave 1 (Model) provides only 2 valid queries
    model_queries = [
        {"query": "best lawyer in Rome", "source": "model"}, # Family: best-of
        {"query": "attorney near me", "source": "model"},    # Family: local
        {"query": "bad query", "source": "model"},           # Will be rejected by score
    ]
    
    # State with fallback templates
    state = {
        "business_profile_key": "legal_service",
        "business_profile": {
            "blind_fallback_templates": {
                "en": ["legal advice {services}", "top law firm {location}"]
            }
        },
        "target_business": "Law Firm",
        "target_industry": "Legal",
        "og_tags": {"og:title": "Expert Lawyers in Rome"},
        "client_content_clean": "We provide legal services in Rome."
    }
    
    anchors = {
        "primary_anchors": ["lawyer", "legal", "attorney"],
        "secondary_anchors": ["rome", "advice"],
        "brand_anchors": ["parenti"]
    }

    # Mock dependencies
    # _score_query_candidate mock to return high for valid, low for bad
    from nodes import researcher_node
    researcher_node._score_query_candidate = MagicMock(side_effect=lambda q, *args: 85 if "bad" not in q else 40)
    
    # Execute assembly for T1 (Target 5)
    accepted, reliability = _assemble_tier_queries(
        "blind_discovery",
        model_queries,
        state,
        brand_tokens={"parenti"},
        is_local=True,
        location="Rome, Italy",
        loc_conf="high",
        locale="en",
        anchors_raw=anchors,
        gemini_client=None, # Skip Wave 4 micro-gen
        regen_context=None  # Skip Wave 2 regen
    )
    
    print(f"Accepted Count: {len(accepted)} / 5")
    print(f"Waves Used: {reliability.get('waves_used')}")
    print(f"Exact Target Achieved: {reliability.get('exact_target_achieved')}")
    
    for i, q in enumerate(accepted):
        print(f" Slot {i+1}: [{q.get('intent_family')}] {q['query']} ({q.get('source')})")

    # Assertions
    assert len(accepted) >= 3, "Should have recovered at least some slots via fallback."
    print("--- Test Passed ---")

if __name__ == "__main__":
    try:
        test_slot_filling_recovery()
    except Exception as e:
        print(f"Test Failed: {e}")
        sys.exit(1)
