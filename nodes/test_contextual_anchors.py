# No pytest needed for simple runner
from nodes.researcher_node import _extract_required_query_anchors, _contains_required_anchor

def test_pellet_ancor_extraction():
    state = {
        "brand_name": "BioFlame",
        "target_industry": "Pellet and Biomass Supplier",
        "target_business": "Industrial Heating Supplies",
        "og_tags": {"og:title": "BioFlame - High Quality Wood Pellets"},
        "raw_data_complete": {"orchestrator_entities": ["enplus a1", "biomassa"]}
    }
    anchors = _extract_required_query_anchors(state)
    assert "pellet" in anchors["primary_anchors"]
    assert "biomass" in anchors["primary_anchors"]
    assert "enplus a1" in anchors["secondary_anchors"]
    assert "wood" in anchors["secondary_anchors"]
    assert "bioflame" in anchors["brand_anchors"]
    assert "bioflame" not in anchors["primary_anchors"]

def test_dental_clinic_validation():
    anchors = {
        "primary_anchors": {"dentist", "dentista", "clinic", "implants", "impianti"},
        "secondary_anchors": {"cleaning", "pulizia", "whitening", "sbiancamento"},
        "brand_anchors": {"dentalcenter", "dc"}
    }
    
    # Rejection: Generic
    assert _contains_required_anchor("how to save money online", anchors) is False
    assert _contains_required_anchor("best places to visit", anchors) is False
    
    # Acceptance: Primary anchor
    assert _contains_required_anchor("best dentist near me", anchors) is True
    assert _contains_required_anchor("cost of dental implants", anchors) is True
    
    # Acceptance: Secondary + Modifier
    assert _contains_required_anchor("price for teeth whitening", anchors) is True
    assert _contains_required_anchor("how to choose dental cleaning", anchors) is True
    
    # Rejection: Brand only
    assert _contains_required_anchor("is dentalcenter good", anchors) is False
    assert _contains_required_anchor("dc clinics reviews", anchors) is False

def test_saas_crm_validation():
    anchors = {
        "primary_anchors": {"crm", "software", "sales pipeline", "lead management"},
        "secondary_anchors": {"automation", "integration", "reporting"},
        "brand_anchors": {"salestitan", "st"}
    }
    
    # Rejection: Generic
    assert _contains_required_anchor("digital transformation tips", anchors) is False
    
    # Acceptance: Primary
    assert _contains_required_anchor("best crm for sales team", anchors) is True
    assert _contains_required_anchor("sales pipeline management software", anchors) is True
    
    # Acceptance: Secondary + Modifier
    assert _contains_required_anchor("automation in lead management", anchors) is True
    assert _contains_required_anchor("compare software integrations", anchors) is True

def test_legal_service_validation():
    anchors = {
        "primary_anchors": {"lawyer", "attorney", "visa", "immigration law"},
        "secondary_anchors": {"citizenship", "green card", "asylum"},
        "brand_anchors": {"lexglobal", "lg"}
    }
    
    # Rejection: Generic
    assert _contains_required_anchor("how to move to another country", anchors) is False
    
    # Acceptance: Primary
    assert _contains_required_anchor("best immigration lawyer in Rome", anchors) is True
    assert _contains_required_anchor("cost of visa application", anchors) is True
    
    # Acceptance: Secondary + Modifier
    assert _contains_required_anchor("citizenship requirements comparison", anchors) is True
    assert _contains_required_anchor("asylum application process", anchors) is True

if __name__ == "__main__":
    # Simple manual run
    import sys
    sys.exit(0)
