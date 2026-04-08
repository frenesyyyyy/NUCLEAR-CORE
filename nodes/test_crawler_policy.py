import sys, os, json
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from crawler_policy_node import process

def test_b2b_saas_no_robots():
    print("--- Test 1: B2B SaaS — no robots.txt ---")
    state = {
        "robots_txt_status": "not_found",
        "url": "https://acme-crm.com",
        "business_profile": {"geo_behavior": "authority_driven"},
        "business_profile_summary": {"label": "B2B SaaS", "geo_behavior": "authority_driven"},
        "business_profile_key": "b2b_saas_tech",
        "target_industry": "CRM Software",
    }
    result = process(state)
    cp = result["crawler_policy"]
    assert cp["robots_status"] == "not_found"
    assert len(cp["bot_matrix"]) == 5
    # B2B SaaS: all bots should be allowed (citation-critical)
    for bot in cp["bot_matrix"]:
        assert bot["recommended"] == "Allow", f"{bot['bot']} should be Allow for b2b_saas_tech"
    assert "not_found" in cp["crawl_risk_notes"][0].lower() or "no robots" in cp["crawl_risk_notes"][0].lower()
    print(f"  Bot matrix: {len(cp['bot_matrix'])} bots")
    print(f"  Risk notes: {cp['crawl_risk_notes'][0][:60]}...")
    print("  PASSED\n")

def test_local_dentist_allowed():
    print("--- Test 2: Local Dentist — robots allowed ---")
    state = {
        "robots_txt_status": "allowed",
        "url": "https://dentist-rome.it",
        "business_profile": {"geo_behavior": "proximity_trust"},
        "business_profile_summary": {"label": "Local Dentist", "geo_behavior": "proximity_trust"},
        "business_profile_key": "local_dentist",
        "target_industry": "Dentistry",
    }
    result = process(state)
    cp = result["crawler_policy"]
    # Local: citation bots allowed, training bots blocked
    for bot in cp["bot_matrix"]:
        if bot["purpose"] == "citation":
            assert bot["recommended"] == "Allow", f"{bot['bot']} (citation) should be Allow"
        elif bot["purpose"] == "training":
            assert bot["recommended"] == "Disallow", f"{bot['bot']} (training) should be Disallow"
    print(f"  Notes: {cp['notes']}")
    print("  PASSED\n")

def test_media_blog_restricted():
    print("--- Test 3: Media Blog — robots restricted ---")
    state = {
        "robots_txt_status": "restricted",
        "url": "https://tech-blog.com",
        "business_profile": {"geo_behavior": "discovery_driven"},
        "business_profile_summary": {"label": "Media / Blog", "geo_behavior": "discovery_driven"},
        "business_profile_key": "media_blog",
        "target_industry": "Tech Publishing",
    }
    result = process(state)
    cp = result["crawler_policy"]
    # Media blog: training bots blocked, citation allowed
    for bot in cp["bot_matrix"]:
        if bot["purpose"] == "training":
            assert bot["recommended"] == "Disallow", f"{bot['bot']} should be Disallow for media_blog"
        else:
            assert bot["recommended"] == "Allow", f"{bot['bot']} should be Allow for media_blog"
    # Should have risk note about citation bots being blocked (since restricted)
    assert any("citation" in n.lower() or "critical" in n.lower() for n in cp["crawl_risk_notes"])
    print(f"  Risk: {cp['crawl_risk_notes'][0][:80]}...")
    print("  PASSED\n")

def test_robots_txt_output():
    print("--- Test 4: Verify robots.txt generation ---")
    state = {
        "robots_txt_status": "not_found",
        "url": "https://example.com",
        "business_profile": {},
        "business_profile_summary": {"label": "B2B SaaS", "geo_behavior": "authority_driven"},
        "business_profile_key": "b2b_saas_tech",
        "target_industry": "SaaS",
    }
    result = process(state)
    robots = result["crawler_policy"]["recommended_robots_txt"]
    assert "User-agent: GPTBot" in robots
    assert "User-agent: PerplexityBot" in robots
    assert "User-agent: Googlebot" in robots
    print(f"  robots.txt length: {len(robots)} chars")
    print("  PASSED\n")

if __name__ == "__main__":
    test_b2b_saas_no_robots()
    test_local_dentist_allowed()
    test_media_blog_restricted()
    test_robots_txt_output()
    print("=== ALL CRAWLER POLICY TESTS PASSED ===")
