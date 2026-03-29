import os
import sys

# Add parent to path so we can import nodes
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nodes.validator_node import _compute_verdict, _compute_data_confidence
from nodes.implementation_blueprint_node import _determine_primary_problem, _validate_actions, _generate_specific_actions

def test_0_discovery():
    state = {
        "source_of_truth_mode": "hybrid",
        "audit_integrity_status": "valid",
        "stress_test_tier_stats": {
            "blind_discovery": {"queries": 10, "matches": 0},
            "contextual_discovery": {"queries": 10, "matches": 0}
        },
        "metrics": {
            "Defensible Evidence Depth": 50,
            "Entity Consensus": 50
        }
    }
    verdict, reason = _compute_verdict(state)
    print(f"Test 0 Discovery: Verdict={verdict}")
    assert verdict in ["NOT CLIENT READY", "REQUIRES ANALYST REVIEW"], "Failed: Should not be CLIENT READY"

def test_confidence_scoring():
    state = {
        "client_content_depth": {
            "extraction_quality": "high",
            "word_count": 500
        },
        "source_taxonomy": {
            "owned_count": 0,
            "earned_count": 0,
            "review_count": 0,
            "directory_count": 0
        },
        "schema_type_counts": {"WebSite": 1, "Organization": 1}
    }
    # Quality=high (+40), citations<=1 (cap at 45) -> score should be capped at 45.
    score = _compute_data_confidence(state)
    print(f"Test Confidence Scoring (Thin Evidence): Score={score}")
    assert score <= 45, f"Failed: Score too high ({score})"

def test_action_priority():
    state = {
        "stress_test_tier_stats": {
             "blind_discovery": {"queries": 10, "matches": 0},
             "contextual_discovery": {"queries": 10, "matches": 0}
        },
        "source_taxonomy": {"owned_count": 10}
    }
    prob = _determine_primary_problem(state)
    print(f"Test Action Priority (0 discovery): Primary Problem={prob}")
    assert prob == "DISCOVERY_FAILURE", "Failed: Priority should be DISCOVERY_FAILURE"

def test_generic_actions():
    actions = [
        {"action_title": "Improve SEO", "action_type": "Technical SEO"},
        {"action_title": "Add FAQ", "action_type": "Content Strategy"},
        {"action_title": "Deploy New Page: Dental Implants", "action_type": "Content Expansion"}
    ]
    valid = _validate_actions(actions)
    print(f"Test Generic Actions: Valid count={len(valid)}")
    assert len(valid) == 1, "Failed: Did not filter generic actions"
    assert valid[0]["action_title"] == "Deploy New Page: Dental Implants"

if __name__ == "__main__":
    test_0_discovery()
    test_confidence_scoring()
    test_action_priority()
    test_generic_actions()
    print("ALL TESTS PASSED")
