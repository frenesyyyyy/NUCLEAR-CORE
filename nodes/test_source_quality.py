import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from source_quality_node import process

def test_no_sources():
    print("--- Test 1: No sources ---")
    state = {
        "earned_media": {},
        "business_profile_key": "b2b_saas",
    }
    result = process(state)
    sq = result["source_taxonomy"]
    assert sq["owned_count"] == 0
    assert sq["unknown_count"] == 0
    assert "CRITICAL: Zero external footprint" in sq["citation_source_risk"][0]
    assert "severely lacking" in sq["trust_mix_summary"]
    print("  PASSED\n")

def test_b2b_saas_missing_reviews():
    print("--- Test 2: B2B SaaS missing reviews ---")
    state = {
        "earned_media": {
            "source_breakdown": {
                "editorial": 2,
                "directory": 1,
            }
        },
        "business_profile_key": "b2b_saas",
    }
    result = process(state)
    sq = result["source_taxonomy"]
    assert sq["earned_count"] == 2
    assert sq["review_count"] == 0
    assert any("Missing validation" in risk for risk in sq["citation_source_risk"])
    assert "Strong independent validation" in sq["trust_mix_summary"]  # 2/3 = 66% high trust (editorial)
    print("  PASSED\n")

def test_local_ghost():
    print("--- Test 3: Local Ghost (freelancer with only owned/forum) ---")
    state = {
        "earned_media": {
            "source_breakdown": {
                "owned": 1,
                "forum": 1,
            }
        },
        "business_profile_key": "freelancer_consultant",
    }
    result = process(state)
    sq = result["source_taxonomy"]
    assert sq["owned_count"] == 1
    assert any("Local ghost" in risk for risk in sq["citation_source_risk"])
    assert "Weak independent" in sq["trust_mix_summary"]  # 0 high trust
    print("  PASSED\n")

def test_healthy_mix():
    print("--- Test 4: Healthy mix (media_blog) ---")
    state = {
        "earned_media": {
            "source_breakdown": {
                "editorial": 4,
                "forum": 1,
                "owned": 1,
            }
        },
        "business_profile_key": "media_blog",
    }
    result = process(state)
    sq = result["source_taxonomy"]
    assert sq["earned_count"] == 4
    assert len(sq["citation_source_risk"]) == 0
    assert "Strong independent validation" in sq["trust_mix_summary"]
    print("  PASSED\n")

if __name__ == "__main__":
    test_no_sources()
    test_b2b_saas_missing_reviews()
    test_local_ghost()
    test_healthy_mix()
    print("=== ALL SOURCE QUALITY TESTS PASSED ===")
