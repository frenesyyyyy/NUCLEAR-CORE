import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from implementation_blueprint_node import process

def test_blueprint_generation():
    print("--- Test 1: Full Agency Blueprint Generation ---")
    state = {
        "target_industry": "CRM SaaS",
        "missing_page_types": [["Pricing Table", "Critical for comparison retrieval"]],
        "trust_signal_gaps": [["P.IVA", "Legal requirement for Italian entities"]],
        "discovery_intent_gaps": [["Instructional", "Missing 'How-to' setup guide"]],
        "crawler_policy": {
            "recommended_robots_txt": "User-agent: *\nAllow: /semantic-data/"
        },
        "audit_integrity_status": "valid"
    }
    result = process(state)
    ib = result["implementation_blueprint"]
    
    # Validate Phased Action Mapping
    assert len(ib["page_priorities"]) == 1
    assert ib["page_priorities"][0]["action_title"] == "Deploy New Page: Pricing Table"
    assert ib["page_priorities"][0]["evidence_basis"] == "Market discovery gap / profile-inferred"
    
    assert len(ib["trust_actions"]) == 1
    assert "P.IVA" in ib["trust_actions"][0]["action_title"]
    assert ib["trust_actions"][0]["priority"] == "Critical"
    
    assert len(ib["discovery_gap_actions"]) == 1
    assert "Instructional" in ib["discovery_gap_actions"][0]["action_title"]
    
    assert "Disallowing" not in ib["robots_patch"]
    print("  PASSED\n")

def test_degraded_integrity_tagging():
    print("--- Test 2: Degraded Integrity Tagging (Evidence Status) ---")
    state = {
        "missing_page_types": [["Service Page", "Gap"]],
        "audit_integrity_status": "degraded"
    }
    result = process(state)
    ib = result["implementation_blueprint"]
    
    assert "partial-evidence" in ib["page_priorities"][0]["evidence_basis"]
    print("  PASSED\n")

def test_empty_state_fallback():
    print("--- Test 3: Empty state fallback ---")
    state = {}
    result = process(state)
    ib = result["implementation_blueprint"]
    assert ib["page_priorities"] == []
    assert ib["trust_actions"] == []
    assert "# robots.txt update skipped" in ib["robots_patch"]
    print("  PASSED\n")

if __name__ == "__main__":
    test_blueprint_generation()
    test_degraded_integrity_tagging()
    test_empty_state_fallback()
    print("=== ALL IMPLEMENTATION BLUEPRINT TESTS PASSED ===")
