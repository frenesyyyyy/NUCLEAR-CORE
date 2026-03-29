import sys, os, json
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from finalizer_node import process

def test_full_report_generation():
    print("--- Test: Full Agency-Grade Report Generation ---")
    state = {
        "run_id": "TEST_ACTION_PLAN",
        "url": "https://example.com",
        "brand_name": "TestBrand",
        "target_industry": "SaaS",
        "locale": "en",
        "business_profile_summary": {
            "label": "B2B SaaS",
            "macro_industry": "Software",
            "geo_behavior": "authority_driven"
        },
        "content_engineering": {
            "answer_first_score": 90,
            "evidence_density_score": 85,
            "chunkability_score": 80
        },
        "model_analytics": {
            "stress_test_diagnostics": {"query_count": 10, "fallback_count": 2, "bucket_diversity": 3, "point_conversion": 1.5},
            "tier_metrics": {
                "blind_discovery": {"queries": 10, "matches": 1}
            }
        },
        "implementation_blueprint": {
            "page_priorities": [{"action_title": "New Landing Page", "why_it_matters": "X", "evidence_basis": "Y", "priority": "High"}],
            "trust_actions": [{"action_title": "P.IVA Display", "why_it_matters": "Italian Legal", "evidence_basis": "direct-evidence", "priority": "Critical"}],
            "discovery_gap_actions": [],
            "schema_actions": [],
            "crawler_actions": [],
             "robots_patch": "User-agent: *\nAllow: /"
        },
        "metrics": {"Entity Consensus": 70, "Defensible Evidence Depth": 60, "Hallucination Risk": 10},
        "confidence_score": 85
    }
    
    result = process(state)
    report_path = result["markdown_report_path"]
    
    assert os.path.exists(report_path)
    with open(report_path, "r", encoding="utf-8") as f:
        content = f.read()
        assert "B2B SaaS" in content
        assert "Tiered Visibility Intelligence" in content
        assert "Agency Strategic Action Plan" in content
        assert "30-Day Quick Wins" in content
        assert "P.IVA Display" in content
        assert "Why this matters for visibility" in content
        assert "Implementation Specs (Dev-Ready)" in content
        assert "**Confidence level:** High" in content
    
    print(f"  PASSED: Report generated with Strategic Action Plan at {report_path}")

if __name__ == "__main__":
    test_full_report_generation()
    print("=== FINALIZER VERIFICATION COMPLETE ===")
