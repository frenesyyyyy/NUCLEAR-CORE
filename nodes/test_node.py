"""Test for business_profile_selector_node."""
import json
from nodes.business_profile_selector_node import process

def test_node():
    mock_state = {
        "business_type": "software",
        "target_industry": "B2B SaaS Automation",
        "scale_level": "Global",
        "schema_type_counts": {"SoftwareApplication": 1},
        "discovered_location": "",
        "classification_notes": "Initial analysis"
    }
    
    print("Testing B2B SaaS state...")
    result = process(mock_state)
    
    assert result["business_profile_key"] == "b2b_saas"
    assert "label" in result["business_profile_summary"]
    assert result["business_profile_summary"]["label"] == "B2B SaaS"
    assert "query_distribution" in result["business_profile_summary"]
    assert "blind" in result["business_profile_summary"]["query_distribution"]
    
    print("Test passed: B2B SaaS correctly classified and summary loaded.")

    mock_local_state = {
        "business_type": "unknown",
        "target_industry": "Dentist in Rome",
        "scale_level": "Local",
        "schema_type_counts": {"Dentist": 1},
        "discovered_location": "Rome"
    }
    
    print("\nTesting Local Dentist state...")
    result_local = process(mock_local_state)
    assert result_local["business_profile_key"] == "local_dentist"
    assert result_local["business_profile_summary"]["label"] == "Local Dentist"
    
    print("Test passed: Local Dentist correctly classified and summary loaded.")

if __name__ == "__main__":
    try:
        test_node()
        print("\nAll node tests passed!")
    except Exception as e:
        print(f"\nTest failed: {str(e)}")
        exit(1)
