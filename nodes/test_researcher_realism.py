import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from researcher_node import _is_realistic_query, _sanitize_or_reject_query

def test_realism_gate():
    print("--- Test 1: Realism Gate (Taxonomy & AI-Goo) ---")
    
    # Bad queries (Taxonomical / Awkward)
    assert _is_realistic_query("Migliori Food Delivery Service") == False
    assert _is_realistic_query("Piattaforme Marketplace affidabili") == False
    assert _is_realistic_query("Solution for B2B SaaS") == False
    assert _is_realistic_query("Best [Industry] in [Location]") == False
    
    # Good queries (Natural)
    assert _is_realistic_query("ordina cena a domicilio milano") == True
    assert _is_realistic_query("software gestione vendite b2b") == True
    assert _is_realistic_query("avvocato penalista esperto roma") == True
    print("  PASSED\n")

def test_brand_neutrality_enforcement():
    print("--- Test 2: Brand Neutrality Enforcement ---")
    
    brand_context = {
        "brand_name": "Just Eat",
        "domain_token": "justeat",
        "aliases": ["JustEat", "Just Eat Italy"]
    }
    
    # T1/T2 should REJECT brand presence
    assert _sanitize_or_reject_query("Just Eat promo code", "blind_discovery", brand_context) is None
    assert _sanitize_or_reject_query("miglior alternativa a Just Eat", "contextual_discovery", brand_context) is None
    
    # T3 SHOULD ALLOW brand presence
    query_t3 = "Just Eat flotta rider milano"
    res_t3 = _sanitize_or_reject_query(query_t3, "branded_validation", brand_context)
    assert res_t3 == query_t3
    
    # Natural sanitization (if possible)
    # If the LLM generates "Deliveroo vs Just Eat" and we just wanted "Deliveroo alternatives"
    # Our current logic is strict drop for T1/T2 if brand token exists.
    assert _sanitize_or_reject_query("compra pizza su justeat", "blind_discovery", brand_context) is None
    
    print("  PASSED\n")

if __name__ == "__main__":
    test_realism_gate()
    test_brand_neutrality_enforcement()
    print("=== RESEARCHER REALISM TESTS PASSED ===")
