import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from implementation_blueprint_node import process

def test_blueprint_generation():
    print("--- Test 1: Full Agency Blueprint Generation ---")
    state = {
        "target_industry": "CRM SaaS",
        "missing_page_types": [["Pricing Table", "Critical for comparison retrieval", "query_gap", "medium", ["Market gap detected"]]],
        "trust_signal_gaps": [["P.IVA", "Legal requirement", "on_site", "high", ["Missing on footer"]]],
        "discovery_intent_gaps": [["Instructional", "Missing setup guide", "query_gap", "medium", ["Query analysis"]]],
        "crawler_policy": {
            "recommended_robots_txt": "User-agent: *\nAllow: /semantic-data/",
            "bot_matrix": [
                {"bot": "ChatGPT-User", "reason": "Visibility", "evidence_origin": "on_site", "evidence_confidence": "high", "supporting_signals": ["Signal1"]}
            ]
        },
        "audit_integrity_status": "valid"
    }
    result = process(state)
    ib = result["implementation_blueprint"]
    
    # Validate Phased Action Mapping
    assert len(ib["page_priorities"]) == 1
    assert ib["page_priorities"][0]["action_title"] == "Deploy New Page: Pricing Table"
    assert ib["page_priorities"][0]["evidence_origin"] == "query_gap"
    assert ib["page_priorities"][0]["evidence_confidence"] == "medium"
    
    assert len(ib["trust_actions"]) == 1
    assert "P.IVA" in ib["trust_actions"][0]["action_title"]
    assert ib["trust_actions"][0]["priority"] == "Critical"
    assert ib["trust_actions"][0]["evidence_origin"] == "on_site"
    
    assert len(ib["discovery_gap_actions"]) == 1
    assert "Instructional" in ib["discovery_gap_actions"][0]["action_title"]
    
    assert "Allow: /" in ib["robots_patch"]
    print("  PASSED\n")

def test_degraded_integrity_tagging():
    print("--- Test 2: Degraded Integrity Tagging (Evidence Status) ---")
    state = {
        "trust_signal_gaps": [["Legal Note", "Req", "on_site", "high", ["Extracted"]]],
        "audit_integrity_status": "degraded"
    }
    result = process(state)
    ib = result["implementation_blueprint"]
    
    # on_site should be downgraded to mixed on degraded crawl
    action = ib["trust_actions"][0]
    assert action["evidence_origin"] == "mixed"
    assert action["evidence_confidence"] == "low"
    assert any("Crawl Status" in s for s in action["supporting_signals"])
    print("  PASSED\n")

def test_empty_state_fallback():
    print("--- Test 3: Empty state fallback ---")
    state = {
        "business_profile_key": "b2b_saas_tech",
        "target_industry": "Software",
        "audit_integrity_status": "valid"
    }
    result = process(state)
    ib = result["implementation_blueprint"]
    # Should have at least the 2 specific fallback actions
    assert len(ib["all_strategic_actions"]) >= 2
    assert any("Entity Trust Anchors" in a["action_title"] for a in ib["all_strategic_actions"])
    print("  PASSED\n")

if __name__ == "__main__":
    test_blueprint_generation()
    test_degraded_integrity_tagging()
    test_empty_state_fallback()
    print("=== ALL IMPLEMENTATION BLUEPRINT TESTS PASSED ===")

