import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from model_analytics_node import process

def test_perplexity_visibility_diagnostics():
    print("--- Test 1: Perplexity Visibility & Diagnostics ---")
    state = {
        "stress_test_log": [
            {"tier": "blind_discovery", "max_pts": 10, "points": 10, "matched": True},
            {"tier": "contextual_discovery", "max_pts": 10, "points": 0, "matched": False},
            {"tier": "branded_validation", "max_pts": 10, "points": 10, "matched": True},
        ],
        "stress_test_diagnostics": {
            "query_count": 3,
            "fallback_count": 0,
            "bucket_diversity": 2,
            "point_conversion": 6.67
        },
        "source_taxonomy": {
            "owned_count": 2,
            "earned_count": 2,
        },
        "earned_media": {
            "reputation_risk_score": 10
        },
        "business_profile_key": "b2b_saas_tech"
    }
    result = process(state)
    ma = result["model_analytics"]
    
    # Check Diagnostics propagation
    assert ma["stress_test_diagnostics"]["query_count"] == 3
    assert ma["stress_test_diagnostics"]["fallback_count"] == 0
    
    # 20/30 = 66.67%
    assert ma["share_of_model"]["Live AI Search"] == 66.67
    # 2 owned, 2 earned = 50% earned
    assert ma["citation_share"]["earned"] == 50.0
    print("  PASSED\n")

def test_high_fallback_warning():
    print("--- Test 2: High Fallback Warning Detection ---")
    state = {
        "stress_test_log": [{"tier": "blind_discovery", "max_pts": 10, "points": 0, "matched": False}],
        "stress_test_diagnostics": {
            "query_count": 10,
            "fallback_count": 8, # > 50%
            "bucket_diversity": 1,
            "point_conversion": 0.0
        },
        "source_taxonomy": {},
        "earned_media": {},
        "business_profile_key": "local_dentist"
    }
    result = process(state)
    ma = result["model_analytics"]
    # We check if fallback_count is passed; Finalizer handles the banner, but Analytics must preserve the count
    assert ma["stress_test_diagnostics"]["fallback_count"] == 8
    print("  PASSED\n")

def test_reputation_risk_poisoning():
    print("--- Test 3: Reputation Risk Poisoning ---")
    state = {
        "stress_test_log": [{"tier": "blind_discovery", "max_pts": 10, "points": 10, "matched": True}],
        "source_taxonomy": {"earned_count": 1},
        "earned_media": {
            "reputation_risk_score": 85 # Extreme risk
        },
        "business_profile_key": "b2b_saas_tech"
    }
    result = process(state)
    ma = result["model_analytics"]
    risks = ma["engine_specific_risks"]["Live AI Search"]
    assert any("Reputation Poisoning" in r for r in risks)
    print("  PASSED\n")

if __name__ == "__main__":
    test_perplexity_visibility_diagnostics()
    test_high_fallback_warning()
    test_reputation_risk_poisoning()
    print("=== ALL MODEL ANALYTICS TESTS PASSED ===")
