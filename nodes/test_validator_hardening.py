import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from validator_node import process

def test_full_premium_ready():
    print("--- Test 1: Full Premium Readiness ---")
    state = {
        "business_profile_key": "b2b_saas_tech",
        "business_profile_summary": {"label": "B2B SaaS", "must_have_signals": ["A"]},
        "content_engineering": {"rewrite_tasks": ["B"]},
        "schema_recommendations": {"recommended_blocks": ["C"]},
        "crawler_policy": {"bot_matrix": ["D"]},
        "earned_media": {"strength_score": 50},
        "source_taxonomy": {"trust_mix_summary": "Good"},
        "model_analytics": {"share_of_model": {"P": 50}},
        "implementation_blueprint": {"copy_blocks": ["E"]},
        "agentic_readiness": {"issues": []},
        "geo_recommendation_pack": "Some recommendations",
        "metrics": {"Information Gain": 20, "Entity Consensus": 50},
        "confidence_score": 80,
        "citation_status": "Verified"
    }
    result = process(state)
    assert result["overall_pipeline_readiness"] == "full_premium"
    assert all(result["validation_summary"].values())
    assert result["roi_verified"] == True
    print("  PASSED\n")

def test_core_only_ready():
    print("--- Test 2: Core Only Readiness ---")
    state = {
        "geo_recommendation_pack": "Some recommendations",
        "metrics": {"Information Gain": 5, "Entity Consensus": 90},
        "confidence_score": 80,
        "citation_status": "Verified"
    }
    result = process(state)
    assert result["overall_pipeline_readiness"] == "core_only"
    assert result["validation_summary"]["profile_selected"] == False
    assert result["roi_verified"] == False # Visibility Plateau
    print("  PASSED\n")

def test_missing_pack():
    print("--- Test 3: Missing Recommendation Pack ---")
    state = {
        "geo_recommendation_pack": "Unavailable"
    }
    result = process(state)
    assert result["validation"] == "Incomplete"
    print("  PASSED\n")

if __name__ == "__main__":
    test_full_premium_ready()
    test_core_only_ready()
    test_missing_pack()
    print("=== ALL VALIDATOR HARDENING TESTS PASSED ===")
