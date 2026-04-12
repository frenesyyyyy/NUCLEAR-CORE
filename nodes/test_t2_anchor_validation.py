import os
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nodes.researcher_node import _extract_required_query_anchors, _contains_required_anchor

def test_t2_anchor_validation():
    print("Running test_t2_anchor_validation...")
    state = {
        "discovered_location": "Roma, Italy",
        "brand_name": "Centro Terapia Del Dolore",
        "target_business": "Centro Medico",
        "target_industry": "Terapia del dolore",
        "primary_industry": "Sanità",
        "raw_data_complete": {
            "orchestrator_entities": [
                "Ernia del disco", 
                "Sindrome tunnel carpale",
                "Dolore cronico",
                "Stimolatore midollare"
            ]
        },
        "og_tags": {
            "og:title": "Centro Terapia del Dolore a Roma - Dott. Rossi"
        }
    }

    anchors = _extract_required_query_anchors(state)
    print("Extracted Anchors:")
    print(f" Primary: {anchors.get('primary_anchors')}")
    print(f" Secondary: {anchors.get('secondary_anchors')}")
    print(f" Brand: {anchors.get('brand_anchors')}")

    # Locations and prepositions should NOT be in primary or secondary
    assert "roma" not in anchors["primary_anchors"], "Location 'roma' should not be a primary anchor"
    assert "roma" not in anchors["secondary_anchors"], "Location 'roma' should not be a secondary anchor"
    assert "per" not in anchors["primary_anchors"], "Modifier 'per' should not be a primary anchor"

    # Test ACCEPT cases
    accept_queries = [
        "migliori terapie per ernia disco lombare Roma",
        "sindrome tunnel carpale mani Roma", 
        "stimolatore midollare per dolore cronico opinioni Roma"
    ]
    
    for q in accept_queries:
        is_valid, hits = _contains_required_anchor(q, anchors)
        assert is_valid is True, f"Expected ACCEPT for: '{q}', but was REJECTED."
        print(f"[PASS] ACCEPT query recognized: '{q}' -> Hits: {hits}")

    # Test REJECT cases
    reject_queries = [
        "miglior ristorante per cena a Roma",  # has "per", "Roma" but no domain anchors
        "sito ufficiale centro terapia del dolore roma" # has brand and location, but brand alone doesn't count
    ]

    for q in reject_queries:
        is_valid, hits = _contains_required_anchor(q, anchors)
        assert is_valid is False, f"Expected REJECT for: '{q}', but was ACCEPTED with hits {hits}."
        print(f"[PASS] REJECT query correctly rejected: '{q}'")

if __name__ == "__main__":
    test_t2_anchor_validation()
