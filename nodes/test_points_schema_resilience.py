import json
from nodes.researcher_node import _normalize_query_candidate, _run_stress_test, _normalize_stress_test_result
from nodes.model_analytics_node import process as analytics_process
from unittest.mock import MagicMock

def test_query_candidate_normalization():
    # T1 normalisation
    c1 = {"query": "test query"}
    norm1 = _normalize_query_candidate(c1, "blind_discovery")
    assert norm1["points"] == 25
    assert norm1["tier"] == "blind_discovery"
    assert norm1["source"] == "model"
    
    # T2 normalisation
    c2 = {"query": "context query", "source": "entity_fallback"}
    norm2 = _normalize_query_candidate(c2, "contextual_discovery")
    assert norm2["points"] == 15
    assert norm2["tier"] == "contextual_discovery"
    assert norm2["source"] == "entity_fallback"

    # T3 normalisation
    c3 = {"query": "brand query"}
    norm3 = _normalize_query_candidate(c3, "branded_validation")
    assert norm3["points"] == 20
    assert norm3["tier"] == "branded_validation"

def test_stress_test_executor_resilience():
    # Simulate a mix of valid and malformed candidates
    all_queries = [
        {"query": "Valid T1", "tier": "blind_discovery", "points": 25, "source": "model"},
        {"query": "Malformed T1", "tier": "blind_discovery"}, # Missing points
        {"query": "Broken Tier", "points": 15}, # Missing tier
    ]
    
    # Mock dependencies
    import nodes.researcher_node
    nodes.researcher_node.execute_with_backoff = MagicMock(return_value="Mocked Answer")
    nodes.researcher_node._brand_mentioned = MagicMock(return_value=True)
    
    serper_key = "test_key"
    brand_name = "TestBrand"
    url = "https://test.com"
    og_title = "Test Brand Title"
    
    visibility_points, total_possible, log, tier_stats, diagnostics = _run_stress_test(
        all_queries, serper_key, brand_name, url, og_title
    )
    
    assert total_possible == 40 # 25 + 0 (missing) + 15 (recovered)
    assert len(log) == 3
    assert diagnostics["schema_repairs"] >= 1
    
    # Verify that every log entry has a stable schema
    for entry in log:
        assert "points" in entry
        assert "max_pts" in entry
        assert "tier" in entry
        assert "matched" in entry

def test_analytics_defensive_normalization():
    # Simulate a state with inconsistent stress_test_log
    state = {
        "business_profile_key": "tech_saas",
        "stress_test_log": [
            {"query": "Q1", "tier": "blind_discovery", "matched": True}, # Missing points/max_pts
            {"query": "Q2", "points": 10, "max_pts": 25, "tier": "contextual_discovery", "matched": False},
        ],
        "stress_test_tier_stats": {
            "blind_discovery": {"queries": 1, "matches": 1, "pts": 25, "max": 25},
            "contextual_discovery": {"queries": 1, "matches": 0, "pts": 0, "max": 15}
        },
        "metrics": {"Defensible Evidence Depth": 50},
        "confidence_score": 80
    }
    
    # Call analytics process
    final_state = analytics_process(state)
    
    analytics = final_state.get("model_analytics", {})
    assert analytics.get("schema_integrity") == "repaired"
    assert "schema_reliability_note" in analytics
    
    # Regression check: visibility should not be 0
    # Q1: repaired max_pts=25, points=25 (matched=True)
    # Q2: max_pts=25, points=10
    # Total possible = 50, Earned = 35. Vis = 70%
    assert analytics["raw_visibility_score"] > 0

if __name__ == "__main__":
    test_query_candidate_normalization()
    test_stress_test_executor_resilience()
    test_analytics_defensive_normalization()
    print("All points schema resilience tests passed!")
