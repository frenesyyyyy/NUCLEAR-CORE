# No pytest needed for simple runner
from nodes.profile_selector import select_business_profile
from nodes.business_profile_selector_node import process as process_selector
from nodes.researcher_node import _fill_profile_fallback_template

def test_pellet_supplier_classification():
    # Test 1: Pellet / Biomass Supplier
    state = {
        "target_industry": "Pellets and Biomass Supplier",
        "business_type": "ecommerce",
        "client_content_clean": "Vendita pellet di abete certificato ENplus A1 in sacchi da 15kg. Consegna a domicilio in tutta Italia. Scheda tecnica disponibile.",
        "schema_type_counts": {"Product": 1, "Offer": 1}
    }
    # Run through the node to test tie-breaker
    result = process_selector(state)
    assert result["business_profile_key"] == "specialty_goods_supplier"
    # It might win naturally or via tie-breaker, both are fine

def test_specialty_ingredients_classification():
    # Test 2: Specialty Ingredients
    state = {
        "target_industry": "Food Ingredients for Professionals",
        "business_type": "retailer",
        "client_content_clean": "Fornitura di ingredienti speciali per pasticceria e industria alimentare. Packaging industriale e certificazione di qualità.",
        "schema_type_counts": {"Product": 5}
    }
    result = process_selector(state)
    assert result["business_profile_key"] == "specialty_goods_supplier"

def test_agricultural_inputs_classification():
    # Test 3: Agricultural Inputs
    state = {
        "target_industry": "Agricultural Products",
        "business_type": "supplier",
        "client_content_clean": "Rivenditore prodotti per agricoltura: sementi, concimi e substrati professionali. Formati sfuso e sacconi.",
        "schema_type_counts": {"Product": 1, "LocalBusiness": 1}
    }
    result = process_selector(state)
    assert result["business_profile_key"] == "specialty_goods_supplier"

def test_industrial_material_classification():
    # Test 4: Industrial Material
    state = {
        "target_industry": "Industrial Steel Components",
        "business_type": "ecommerce",
        "client_content_clean": "Components for industrial machinery. Technical specifications and ISO certification. Wholesale supply.",
        "schema_type_counts": {"Product": 10}
    }
    result = process_selector(state)
    assert result["business_profile_key"] == "specialty_goods_supplier"

def test_mainstream_consumer_shop_classification():
    # Test 5: Mainstream Consumer Shop
    state = {
        "target_industry": "Fashion Clothing",
        "business_type": "ecommerce",
        "client_content_clean": "Buy the latest fashion trends online. Free shipping on orders over 50. New collection available in our shop.",
        "schema_type_counts": {"Product": 20, "Offer": 20}
    }
    result = process_selector(state)
    assert result["business_profile_key"] == "ecommerce_retail"

def test_placeholder_filling_success():
    # Test placeholder filling success
    template = "best {core_noun} {quality_modifier} in {location}"
    entity_terms = {
        "core_noun": "pellet",
        "quality_modifier": "certified",
        "location": "Rome"
    }
    result = _fill_profile_fallback_template(template, entity_terms)
    assert result == "best pellet certified in Rome"

def test_placeholder_filling_missing_noun():
    # Test failure when core_noun is missing
    template = "best {core_noun} {quality_modifier}"
    entity_terms = {"quality_modifier": "certified"}
    result = _fill_profile_fallback_template(template, entity_terms)
    assert result is None

def test_placeholder_filling_unresolved_placeholders():
    # Test failure when placeholders remain
    template = "best {core_noun} {quality_modifier} for {use_case}"
    entity_terms = {"core_noun": "pellet", "quality_modifier": "certified"}
    result = _fill_profile_fallback_template(template, entity_terms)
    assert result is None
