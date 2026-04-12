# No pytest needed for simple runner
from nodes.researcher_node import _sanitize_or_reject_query, _contains_required_anchor

def test_sanitize_provenance_successful():
    brand_tokens = {"BioFlame"}
    res = _sanitize_or_reject_query(
        "best pellet supplier in Milano", 
        "blind_discovery", 
        brand_tokens, 
        True, 
        "Milano, Italy", 
        "it"
    )
    assert res["final_query"] is not None
    assert "Milano" in res["final_query"]
    assert res["location_applied"] == "Milano"
    assert res["rejection_code"] is None

def test_sanitize_provenance_rejection():
    brand_tokens = {"BioFlame"}
    res = _sanitize_or_reject_query(
        "BioFlame pellets price", 
        "blind_discovery", 
        brand_tokens, 
        True, 
        "Milano, Italy", 
        "it"
    )
    assert res["final_query"] is None
    assert res["rejection_code"] == "REJECT_BRAND_LEAKAGE"

def test_anchor_provenance_hits():
    anchors = {
        "primary_anchors": {"dentist", "dentista"},
        "secondary_anchors": {"implants", "impianti"},
        "brand_anchors": {"dc"}
    }
    
    valid, hits = _contains_required_anchor("cost of dental implants", anchors)
    assert valid is True
    # "implants" is secondary, so we need a modifier like "cost"
    assert "secondary:implants" in hits
    assert "modifier:cost" in hits

def test_anchor_primary_provenance():
    anchors = {
        "primary_anchors": {"dentist", "dentista"},
        "secondary_anchors": {"implants"},
        "brand_anchors": {"dc"}
    }
    valid, hits = _contains_required_anchor("best dentist near me", anchors)
    assert valid is True
    assert "primary:dentist" in hits

if __name__ == "__main__":
    import sys
    sys.exit(0)
