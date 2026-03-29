import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from earned_media_node import process

def test_no_sources():
    print("--- Test 1: No external sources ---")
    state = {
        "brand_name": "Acme CRM",
        "target_industry": "SaaS",
        "discovered_location": "Worldwide",
        "business_profile": {},
        "raw_data_complete": {},
        "external_sources": [],
        "url": "https://acme-crm.com",
    }
    result = process(state)
    em = result["earned_media"]
    assert em["strength_score"] == 0
    assert em["reputation_risk_score"] == 0
    assert em["warning_effect_risk"] == False
    assert em["source_breakdown"]["review"] == 0
    assert "No external sources" in em["notes"]
    print(f"  Score: {em['strength_score']}  Risk: {em['reputation_risk_score']}  PASSED\n")

def test_strong_brand():
    print("--- Test 2: Strong brand — multiple editorial and review sources ---")
    state = {
        "brand_name": "Acme CRM",
        "target_industry": "SaaS",
        "discovered_location": "Global",
        "business_profile": {},
        "raw_data_complete": {
            "source_urls": [
                "https://techcrunch.com/2024/01/acme",
                "https://forbes.com/acme-review",
                "https://g2.com/products/acme",
                "https://trustpilot.com/review/acme-crm.com",
                "https://reddit.com/r/saas/acme",
            ]
        },
        "external_sources": [
            "https://capterra.com/software/acme",
            "https://crunchbase.com/organization/acme",
        ],
        "url": "https://acme-crm.com",
    }
    result = process(state)
    em = result["earned_media"]
    assert em["strength_score"] > 0
    assert em["source_breakdown"]["review"] == 3    # g2 + trustpilot + capterra
    assert em["source_breakdown"]["editorial"] == 2  # techcrunch + forbes
    assert em["source_breakdown"]["forum"] == 1      # reddit
    assert em["source_breakdown"]["directory"] == 1  # crunchbase only
    assert em["warning_effect_risk"] == False
    print(f"  Score: {em['strength_score']}  Risk: {em['reputation_risk_score']}  Breakdown: {em['source_breakdown']}  PASSED\n")

def test_negative_signals():
    print("--- Test 3: Negative/scam signals detected ---")
    state = {
        "brand_name": "Shady Corp",
        "target_industry": "Finance",
        "discovered_location": "Unknown",
        "business_profile": {},
        "raw_data_complete": {},
        "external_sources": [
            "https://scam-alert.com/shady-corp",
            "https://trustpilot.com/review/shadycorp",
            "https://ripoff-report.com/shady",
        ],
        "url": "https://shadycorp.com",
    }
    result = process(state)
    em = result["earned_media"]
    assert em["reputation_risk_score"] > 0
    assert em["warning_effect_risk"] == True
    assert any(m["negative_signal"] for m in em["mentions"])
    print(f"  Rep Risk: {em['reputation_risk_score']}  Warning: {em['warning_effect_risk']}  PASSED\n")

def test_owned_sources_filtered():
    print("--- Test 4: Owned sources get low weight ---")
    state = {
        "brand_name": "Acme",
        "target_industry": "Tech",
        "discovered_location": "Worldwide",
        "business_profile": {},
        "raw_data_complete": {},
        "external_sources": [
            "https://acme.com/blog/post",
            "https://acme.com/about",
        ],
        "url": "https://acme.com",
    }
    result = process(state)
    em = result["earned_media"]
    assert em["source_breakdown"]["owned"] == 2
    assert em["strength_score"] < 10  # Owned sources contribute very little
    print(f"  Score: {em['strength_score']}  Breakdown: {em['source_breakdown']}  PASSED\n")

if __name__ == "__main__":
    test_no_sources()
    test_strong_brand()
    test_negative_signals()
    test_owned_sources_filtered()
    print("=== ALL EARNED MEDIA TESTS PASSED ===")
