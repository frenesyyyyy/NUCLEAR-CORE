"""Test for business_profile_selector_node."""
import json
from nodes.business_profile_selector_node import process
from nodes.business_profiles import DEFAULT_PROFILE_KEY

def test_node():
    mock_state = {
        "business_type": "software",
        "target_industry": "B2B SaaS Automation",
        "scale_level": "Global",
        "schema_type_counts": {"SoftwareApplication": 1},
        "discovered_location": "",
        "classification_notes": "Initial analysis"
    }
    
    print("Testing B2B SaaS / Tech state...")
    result = process(mock_state)
    
    assert result["business_profile_key"] == DEFAULT_PROFILE_KEY
    assert "label" in result["business_profile_summary"]
    assert result["business_profile_summary"]["label"] == "B2B SaaS / Tech"
    
    print("Test passed: B2B SaaS / Tech correctly classified and summary loaded.")

    mock_local_state = {
        "business_type": "unknown",
        "target_industry": "Dentist in Rome",
        "scale_level": "Local",
        "schema_type_counts": {"Dentist": 1},
        "discovered_location": "Rome"
    }
    
    print("\nTesting Local Service YMYL state...")
    result_local = process(mock_local_state)
    # Keyword scoring directly returns the Canonical Agency Profile
    assert result_local["business_profile_key"] == "local_healthcare_ymyl"
    # because of the alias router in business_profiles.py, the returned dictionary label will be "Local Healthcare / Medical"
    assert result_local["business_profile_summary"]["label"] == "Local Healthcare / Medical"
    
    print("Test passed: Local Service YMYL correctly classified and summary loaded.")

if __name__ == "__main__":
    try:
        test_node()
        print("\nAll node tests passed!")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\nTest failed: {str(e)}")
        exit(1)
