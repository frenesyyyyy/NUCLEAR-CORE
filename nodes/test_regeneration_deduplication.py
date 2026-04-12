import json
import os
import re
from nodes.researcher_node import _assemble_tier_queries, _normalize_query_text_for_dedupe
from unittest.mock import MagicMock

def test_dedupe_normalization():
    # Test normalization edge cases
    q1 = "Miglior avvocato a Roma"
    q2 = " miglior avvocato a roma. "
    q3 = "MIGLIOR  AVVOCATO A ROMA"
    
    n1 = _normalize_query_text_for_dedupe(q1)
    n2 = _normalize_query_text_for_dedupe(q2)
    n3 = _normalize_query_text_for_dedupe(q3)
    
    assert n1 == n2 == n3
    assert n1 == "miglior avvocato a roma"

def test_assemble_tier_queries_deduplication():
    # Mock state and inputs
    state = {
        "business_profile_key": "legal_lawyer",
        "business_profile": {
            "contextual_fallback_templates": {
                "it": ["consulenza legale {services}"]
            }
        },
        "target_business": "Studio Legale",
        "target_industry": "Legal",
        "discovered_location": "Roma",
        "service_zones": ["Roma"]
    }
    anchors = {
        "primary_anchors": ["divorzio", "penale"],
        "secondary_anchors": ["civile"]
    }
    brand_tokens = {"Parenti"}
    
    # Phase 1 queries (Model)
    # Includes two near-duplicates
    model_queries = [
        {"query": "avvocato divorzio Roma", "source": "model"},
        {"query": "Avvocato divorzio Roma.", "source": "model"}, # Should be deduped
        {"query": "penale roma", "source": "model"}
    ]
    
    # Mocking _score_query_candidate to always accept
    import nodes.researcher_node
    nodes.researcher_node._score_query_candidate = MagicMock(return_value=100.0)
    
    # Run assembly
    accepted, reliability = _assemble_tier_queries(
        "contextual_discovery", model_queries, state, brand_tokens,
        True, "Roma", "high", "it", anchors
    )
    
    # Assertions
    accepted_texts = [q["query"] for q in accepted]
    assert "avvocato divorzio Roma" in accepted_texts
    assert "Avvocato divorzio Roma." not in accepted_texts
    assert reliability["duplicate_rejections_count"] >= 1
    
    # Check that regeneration also dedupes
    # We pass a regen_context that will trigger regeneration if min not met
    # Let's set min=4
    import nodes.researcher_node
    nodes.researcher_node.TIER_POLICY["contextual_discovery"] = {"min": 4, "target": 5, "max": 8}
    
    # Mock regeneration to return a duplicate
    nodes.researcher_node._regenerate_tier_candidates = MagicMock(return_value=[
        {"query": "avvocato divorzio Roma", "source": "model"}, # Duplicate
        {"query": "nuova query unica", "source": "model"} # Unique
    ])
    
    # Reload assembly with regen context
    regen_ctx = {"gemini_client": MagicMock()}
    accepted, reliability = _assemble_tier_queries(
        "contextual_discovery", model_queries, state, brand_tokens,
        True, "Roma", "high", "it", anchors, regen_context=regen_ctx
    )
    
    accepted_texts = [q["query"] for q in accepted]
    # Check for uniqueness
    assert len(accepted_texts) == len(set(_normalize_query_text_for_dedupe(t) for t in accepted_texts))
    assert reliability["duplicate_rejections_count"] >= 2
    assert "nuova query unica" in accepted_texts

if __name__ == "__main__":
    try:
        test_dedupe_normalization()
        test_assemble_tier_queries_deduplication()
        print("Regeneration deduplication tests passed!")
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
