"""
Business Intelligence Layer — 8 Canonical Agency Profiles.

Defines the authoritative profile registry for the GEO pipeline.
Every downstream node MUST import DEFAULT_PROFILE_KEY from this module
for its .get() fallbacks instead of hardcoding strings.

Profile Architecture:
  - 8 canonical keys (the ONLY keys a pipeline should actively use)
  - Legacy Alias Router (maps retired/transitional keys to canonical ones)
"""

# ─────────────────────────────────────────────────────────────────────────────
# Global Fallback Constant — import this everywhere, never hardcode the string
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_PROFILE_KEY = "b2b_saas_tech"

# ─────────────────────────────────────────────────────────────────────────────
# 8 Canonical Agency Profiles
# ─────────────────────────────────────────────────────────────────────────────

BUSINESS_INTELLIGENCE_PROFILES = {

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 1. B2B SaaS / Tech
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "b2b_saas_tech": {
        "label": "B2B SaaS / Tech",
        "macro_industry": "Software & Technology",
        "geo_behavior": "authority_driven",
        "query_style": "technical_authority",
        "scale_default": "Global",
        "location_enforce": False,
        "stress_test_budget": {"blind": 14, "contextual": 12, "branded": 8},
        "serper_mode": "global_leaders",
        "allowed_schema_types": [
            "SoftwareApplication", "Organization", "WebSite",
            "FAQPage", "HowTo", "Product",
        ],
        "must_have_signals": [
            "use-case pages", "pricing page", "integration pages",
            "API documentation", "comparison pages",
        ],
        "scoring_weights": {
            "technical": 0.50,
            "content_depth": 0.40,
            "eeat_trust": 0.10,
        },
        "persona_templates": [
            {"persona": "Founder / Operator", "intent": "problem-solving"},
            {"persona": "Head of Ops / Buyer", "intent": "comparison"},
            {"persona": "End User / Evaluator", "intent": "validation"},
        ],
        "blind_fallback_templates": {
            "en": ["best b2b software", "saas platforms for enterprise", "business automation tools"],
            "it": ["migliori software b2b", "piattaforme saas per aziende", "strumenti automazione business"],
        },
        "contextual_fallback_templates": {
            "en": ["how to choose b2b software", "best saas integrations for office"],
            "it": ["come scegliere un software b2b", "migliori integrazioni saas per ufficio"],
        },
    },

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 2a. Local Healthcare / YMYL
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "local_healthcare_ymyl": {
        "label": "Local Healthcare / Medical",
        "macro_industry": "Healthcare & Medical Services",
        "geo_behavior": "proximity_trust",
        "query_style": "medical_trust",
        "scale_default": "Local",
        "location_enforce": True,
        "stress_test_budget": {"blind": 5, "contextual": 7, "branded": 8},
        "serper_mode": "local_leaders",
        "allowed_schema_types": [
            "LocalBusiness", "MedicalBusiness",
            "PostalAddress", "Dentist", "Physician", "FAQPage"
        ],
        "must_have_signals": [
            "city/location mentions", "doctor/practitioner bios",
            "reviews", "NAP consistency", "appointment booking",
        ],
        "scoring_weights": {
            "eeat_trust": 0.50,
            "local_entities": 0.30,
            "technical": 0.20,
        },
        "persona_templates": [
            {"persona": "Emergency Patient", "intent": "urgent"},
            {"persona": "Family Patient", "intent": "trust"},
            {"persona": "Insurance Researcher", "intent": "comparison"},
        ],
        "blind_fallback_templates": {
            "en": ["best local specialist near me", "trusted clinic nearby", "urgent medical care"],
            "it": ["miglior specialista in zona", "clinica affidabile vicino a me", "guardia medica urgente"],
        },
        "contextual_fallback_templates": {
            "en": ["cost of medical consultation", "how to choose the best clinic"],
            "it": ["costo della visita medica", "come scegliere la migliore clinica"],
        },
    },

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 2b. Local Legal / YMYL
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "local_legal_ymyl": {
        "label": "Local Legal Services",
        "macro_industry": "Legal & Official Services",
        "geo_behavior": "proximity_authority",
        "query_style": "legal_authority",
        "scale_default": "Local",
        "location_enforce": True,
        "stress_test_budget": {"blind": 5, "contextual": 7, "branded": 8},
        "serper_mode": "local_leaders",
        "allowed_schema_types": [
            "LegalService", "Organization", "LocalBusiness",
            "PostalAddress", "Person",
        ],
        "must_have_signals": [
            "city/location mentions", "attorney bios",
            "bar association registry", "NAP consistency", "consultation forms",
        ],
        "scoring_weights": {
            "eeat_trust": 0.50,
            "local_entities": 0.30,
            "technical": 0.20,
        },
        "persona_templates": [
            {"persona": "Defendant / Client", "intent": "defense"},
            {"persona": "Family Legal Seeker", "intent": "planning"},
            {"persona": "Business Owner", "intent": "compliance"},
        ],
        "blind_fallback_templates": {
            "en": ["best local lawyer near me", "trusted law firm nearby", "urgent legal counsel"],
            "it": ["miglior avvocato in zona", "studio legale affidabile vicino a me", "consulenza legale urgente"],
        },
        "contextual_fallback_templates": {
            "en": ["cost of legal consultation", "how to choose a good lawyer"],
            "it": ["costo della consulenza legale", "come scegliere un buon avvocato"],
        },
    },

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 3. E-commerce / Retail
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "ecommerce_retail": {
        "label": "E-commerce / Retail",
        "macro_industry": "Retail & Consumer Goods",
        "geo_behavior": "comparison_driven",
        "query_style": "commercial_comparison",
        "scale_default": "National",
        "location_enforce": False,
        "stress_test_budget": {"blind": 10, "contextual": 12, "branded": 8},
        "serper_mode": "category_leaders",
        "allowed_schema_types": [
            "Product", "Offer", "AggregateOffer",
            "Review", "Organization", "BreadcrumbList",
        ],
        "must_have_signals": [
            "Product schema", "collection/category pages",
            "reviews", "cart/checkout flow", "shipping info",
        ],
        "scoring_weights": {
            "technical": 0.40,
            "eeat_trust": 0.30,
            "content_depth": 0.30,
        },
        "persona_templates": [
            {"persona": "Explorer", "intent": "discovery"},
            {"persona": "Comparer", "intent": "comparison"},
            {"persona": "Buyer", "intent": "validation"},
        ],
        "blind_fallback_templates": {
            "en": ["best products online", "where to buy quality items", "top rated online store"],
            "it": ["migliori prodotti online", "dove comprare articoli di qualita", "miglior negozio online"],
        },
        "contextual_fallback_templates": {
            "en": ["brand comparison online", "best budget online store"],
            "it": ["confronto brand online", "miglior negozio online economico"],
        },
    },

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 4. Hospitality / Travel
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "hospitality_travel": {
        "label": "Hospitality / Travel",
        "macro_industry": "Hospitality & Travel",
        "geo_behavior": "experience_proximity",
        "query_style": "sensory_experiential",
        "scale_default": "Local",
        "location_enforce": True,
        "stress_test_budget": {"blind": 8, "contextual": 10, "branded": 6},
        "serper_mode": "local_leaders",
        "allowed_schema_types": [
            "Hotel", "LodgingBusiness", "Restaurant", "LocalBusiness",
            "Menu", "OpeningHoursSpecification", "TouristAttraction",
        ],
        "must_have_signals": [
            "menu text", "reservation/booking info", "photos",
            "reviews", "room types", "location/address",
        ],
        "scoring_weights": {
            "local_entities": 0.50,
            "eeat_trust": 0.25,
            "technical": 0.25,
        },
        "persona_templates": [
            {"persona": "Traveler / Guest", "intent": "discovery"},
            {"persona": "Event Planner", "intent": "comparison"},
            {"persona": "Review Checker", "intent": "validation"},
        ],
        "blind_fallback_templates": {
            "en": ["best places to stay", "top rated local dining", "authentic local cuisine"],
            "it": ["i posti migliori dove stare", "migliori ristoranti locali recensiti", "cucina locale autentica"],
        },
        "contextual_fallback_templates": {
            "en": ["menu and prices", "unique experiences nearby"],
            "it": ["menu e prezzi", "esperienze uniche in zona"],
        },
    },

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 5. Publisher / Media
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "publisher_media": {
        "label": "Publisher / Media",
        "macro_industry": "Publishing & Media",
        "geo_behavior": "discovery_driven",
        "query_style": "thought_leadership",
        "scale_default": "Global",
        "location_enforce": False,
        "stress_test_budget": {"blind": 12, "contextual": 10, "branded": 6},
        "serper_mode": "topic_authorities",
        "allowed_schema_types": [
            "Article", "NewsArticle", "BlogPosting",
            "Person", "Organization", "WebPage",
        ],
        "must_have_signals": [
            "author pages", "content clusters", "freshness signals",
            "publication dates", "editorial guidelines",
        ],
        "scoring_weights": {
            "eeat_trust": 0.40,
            "content_depth": 0.40,
            "technical": 0.20,
        },
        "persona_templates": [
            {"persona": "Information Searcher", "intent": "information seeking"},
            {"persona": "Loyal Reader", "intent": "validation"},
            {"persona": "Content Curator", "intent": "aggregation"},
        ],
        "blind_fallback_templates": {
            "en": ["how to guides", "latest news on industry", "expert opinions"],
            "it": ["guide approfondite", "ultime notizie di settore", "opinioni degli esperti"],
        },
        "contextual_fallback_templates": {
            "en": ["detailed facts and figures", "trend analysis"],
            "it": ["dati e cifre dettagliate", "analisi dei trend"],
        },
    },

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 6. Professional Services
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "professional_services": {
        "label": "Professional Services",
        "macro_industry": "Professional & Business Services",
        "geo_behavior": "authority_case_study",
        "query_style": "case_study_authority",
        "scale_default": "National",
        "location_enforce": False,
        "stress_test_budget": {"blind": 8, "contextual": 8, "branded": 8},
        "serper_mode": "category_leaders",
        "allowed_schema_types": [
            "Organization", "Service", "Person",
            "ProfessionalService", "LocalBusiness",
        ],
        "must_have_signals": [
            "case studies", "service pages", "team page",
            "client testimonials", "industry certifications",
        ],
        "scoring_weights": {
            "eeat_trust": 0.50,
            "content_depth": 0.30,
            "technical": 0.20,
        },
        "persona_templates": [
            {"persona": "Decision Maker", "intent": "provider discovery"},
            {"persona": "Evaluator / Procurement", "intent": "comparison"},
            {"persona": "Referral Seeker", "intent": "validation"},
        ],
        "blind_fallback_templates": {
            "en": ["best consulting firms", "expert professional services", "industry leading agency"],
            "it": ["migliori societa di consulenza", "servizi professionali esperti", "agenzia leader di settore"],
        },
        "contextual_fallback_templates": {
            "en": ["case studies for consulting", "roi for professional services"],
            "it": ["case study di consulenza", "roi per i servizi professionali"],
        },
    },

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 7. Marketplace / Aggregator
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "marketplace_aggregator": {
        "label": "Marketplace / Aggregator",
        "macro_industry": "Aggregators & Directories",
        "geo_behavior": "authority_driven",
        "query_style": "informational_comparison",
        "scale_default": "Global",
        "location_enforce": False,
        "stress_test_budget": {"blind": 14, "contextual": 12, "branded": 8},
        "serper_mode": "global_leaders",
        "allowed_schema_types": [
            "CollectionPage", "ItemList", "Organization", "WebSite"
        ],
        "must_have_signals": [
            "Search/Filter functionality", "category grouping", "provider listings", "user reviews",
        ],
        "scoring_weights": {
            "technical": 0.50,
            "content_depth": 0.40,
            "eeat_trust": 0.10,
        },
        "persona_templates": [
            {"persona": "Shopper / User", "intent": "comparison"},
            {"persona": "Service Provider", "intent": "visibility"},
        ],
        "blind_fallback_templates": {
            "en": ["best platforms to find services", "compare online vendors", "directory for reviews"],
            "it": ["migliori piattaforme per trovare servizi", "confronta venditori online", "directory per recensioni"],
        },
        "contextual_fallback_templates": {
            "en": ["how to compare providers online", "best aggregators for comparison"],
            "it": ["come confrontare fornitori online", "migliori aggregatori per confronto"],
        },
    },

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 8. Education / Institution
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "education_institution": {
        "label": "Education & Institution",
        "macro_industry": "Education & Training",
        "geo_behavior": "proximity_or_authority",
        "query_style": "authoritative_informational",
        "scale_default": "National",
        "location_enforce": True,
        "stress_test_budget": {"blind": 14, "contextual": 12, "branded": 8},
        "serper_mode": "local_pack",
        "allowed_schema_types": [
            "EducationalOrganization", "Course", "ItemPage", "FAQPage"
        ],
        "must_have_signals": [
            "Course syllabuses", "accreditation details", "faculty bios", "admissions info",
        ],
        "scoring_weights": {
            "eeat_trust": 0.50,
            "content_depth": 0.30,
            "technical": 0.20,
        },
        "persona_templates": [
            {"persona": "Prospective Student", "intent": "education"},
            {"persona": "Parent", "intent": "validation"},
        ],
        "blind_fallback_templates": {
            "en": ["best universities for", "top courses on", "where to study"],
            "it": ["migliori università per", "migliori corsi di", "dove studiare"],
        },
        "contextual_fallback_templates": {
            "en": ["how to apply for colleges", "requirements for studies"],
            "it": ["come fare domanda per università", "requisiti per studi"],
        },
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Legacy Alias Router — prevents KeyError crashes during transition.
# Maps every retired/transitional key to its canonical successor.
# DO NOT add new canonical profiles here; add them above.
# ─────────────────────────────────────────────────────────────────────────────

_LEGACY_ALIASES = {
    # B2B / SaaS variants
    "b2b_saas":             "b2b_saas_tech",
    "consumer_saas":        "b2b_saas_tech",
    "tech":                 "b2b_saas_tech",
    "software":             "b2b_saas_tech",

    # Healthcare / YMYL variants
    "local_dentist":        "local_healthcare_ymyl",
    "dentist":              "local_healthcare_ymyl",
    "healthcare":           "local_healthcare_ymyl",
    
    # Legal / YMYL variants
    "local_law_firm":       "local_legal_ymyl",
    "legal":                "local_legal_ymyl",
    "lawyer":               "local_legal_ymyl",
    "law_firm":             "local_legal_ymyl",

    # DEPRECATED / Legacy blending variants
    "local_service":        "local_healthcare_ymyl",
    "general_local_business": "hospitality_travel", # Rerouted general fallback

    # E-commerce variants
    "ecommerce_brand":      "ecommerce_retail",
    "ecommerce":            "ecommerce_retail",
    "retail":               "ecommerce_retail",

    # Hospitality variants
    "restaurant_hospitality": "hospitality_travel",
    "food":                 "hospitality_travel",
    "hotel":                "hospitality_travel",
    "coliving":             "hospitality_travel",

    # Publisher variants
    "media_blog":           "publisher_media",
    "blog":                 "publisher_media",
    "news":                 "publisher_media",

    # Professional Services variants
    "freelancer_consultant":     "professional_services",
    "agency_marketing":          "professional_services",
    "education_course_provider": "professional_services",
    "local_tech_provider":       "professional_services",
    "freelancer":                "professional_services",
    "consulting":                "professional_services",

    # Marketplace / Aggregator variants
    "marketplace":          "marketplace_aggregator",
    "aggregator":           "marketplace_aggregator",
    "platform":             "marketplace_aggregator",

    # Education variants
    "university":           "education_institution",
    "school":               "education_institution",
    "course":               "education_institution",
}


def normalize_profile_key(key: str) -> str:
    """
    Normalizes a given profile key to its canonical form from BUSINESS_INTELLIGENCE_PROFILES.
    If the key is legacy, it routes via _LEGACY_ALIASES.
    If the key is entirely unknown, it falls back to DEFAULT_PROFILE_KEY.
    """
    if not key or not isinstance(key, str):
        return DEFAULT_PROFILE_KEY
        
    k = key.lower().strip()
    if k in BUSINESS_INTELLIGENCE_PROFILES:
        return k
        
    if k in _LEGACY_ALIASES:
        return _LEGACY_ALIASES[k]
        
    return DEFAULT_PROFILE_KEY


def assert_valid_profile_key(key: str) -> None:
    """
    Validates that a provided key belongs to the canonical registry.
    This does NOT check legacy aliases - the key should be normalized FIRST before calling this.
    """
    if key not in BUSINESS_INTELLIGENCE_PROFILES:
        raise ValueError(f"Invalid canonical profile key '{key}'. Valid keys: {list(BUSINESS_INTELLIGENCE_PROFILES.keys())}")


def get_platform_like_profiles() -> set[str]:
    """Returns canonical profiles that behave like platforms, marketplaces, or large online catalogs."""
    return {"marketplace_aggregator", "ecommerce_retail", "b2b_saas_tech"}


def get_local_trust_profiles() -> set[str]:
    """Returns canonical profiles tied to physical local footprints or local subjective trust/ratings."""
    return {"local_healthcare_ymyl", "local_legal_ymyl", "hospitality_travel", "professional_services"}
