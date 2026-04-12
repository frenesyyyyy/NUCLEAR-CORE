import os
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nodes.business_profile_selector_node import process

def test_healthcare_misclassification_veto():
    """
    Test that a site with strong healthcare anchors is correctly vetoed 
    if initially misclassified as b2b_saas_tech.
    """
    print("Running test_healthcare_misclassification_veto...")
    
    state = {
        "business_type": "unknown",
        "target_industry": "Centro Medico e Terapia del Dolore",
        "scale_level": "Local",
        "discovered_location": "Roma, Italy",
        "schema_type_counts": {"Organization": 1, "SoftwareApplication": 2}, # Tricks the initial selector into SaaS
        "page_title": "Centro Terapia del Dolore a Roma - Dott. Rossi",
        "brand_name": "Centro Terapia del Dolore",
        "client_content_clean": "Benvenuti nel nostro centro medico specializzato nella terapia del dolore. Curiamo dolore cronico, sciatalgia e forniamo infiltrazioni. Oltre al trattamento del paziente, offriamo una piattaforma software b2b per la gestione dei dati in cloud tramite api.",
        "og_tags": {
            "og:title": "Centro Terapia del Dolore a Roma",
            "og:description": "Trattamenti e visite per la terapia del dolore."
        }
    }
    
    new_state = process(state)
    
    profile_key = new_state.get("business_profile_key")
    print(f"Resulting Profile Key: {profile_key}")
    print(f"Classification Evidence: {new_state.get('classification_evidence')}")
    
    assert profile_key == "local_healthcare_ymyl", f"Expected local_healthcare_ymyl but got {profile_key}"
    print("[PASS] Test passed successfully.")

if __name__ == "__main__":
    test_healthcare_misclassification_veto()
