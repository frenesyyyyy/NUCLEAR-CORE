"""
source_matrix.py — Profile-Aware External Source Authority Framework
GEO Optimizer Pipeline | v4.5

This module defines:
A. Source family taxonomy (12 canonical families)
B. Domain → source family classification patterns
C. Per-profile source packs (relevant families, weights, trust anchors)
D. Helper functions for profile-aware presence detection and weighting

All data is deterministic and stateless. No external API calls.
"""

from __future__ import annotations
from nodes.business_profiles import normalize_profile_key

# ─────────────────────────────────────────────────────────────────────────────
# A. SOURCE FAMILY TAXONOMY
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_FAMILIES = [
    "app_ecosystems",
    "review_ecosystems",
    "software_comparison_ecosystems",
    "professional_directories",
    "local_directories_maps",
    "editorial_news_pr",
    "marketplace_partner_ecosystems",
    "docs_integrations_developer",
    "forums_communities",
    "official_registries_legal",
    "employer_workforce_reputation",
    "social_proof_platforms",
    "ignored_noise",
    "unclassified_candidate",
]

# ─────────────────────────────────────────────────────────────────────────────
# B. DOMAIN → SOURCE FAMILY PATTERNS
# Each entry: (hostname_pattern, source_family, base_weight)
# More specific patterns should appear first.
# ─────────────────────────────────────────────────────────────────────────────

DOMAIN_FAMILY_MAP: list[tuple[str, str, float]] = [

    # ── App ecosystems ────────────────────────────────────────────────────────
    ("apps.apple.com",           "app_ecosystems", 3.0),
    ("play.google.com",          "app_ecosystems", 3.0),
    ("appstore.",                "app_ecosystems", 3.0),
    ("microsoft.com/store",      "app_ecosystems", 2.5),

    # ── Software comparison / review ecosystems ───────────────────────────────
    ("g2.com",                   "software_comparison_ecosystems", 3.5),
    ("capterra.com",             "software_comparison_ecosystems", 3.0),
    ("getapp.com",               "software_comparison_ecosystems", 2.5),
    ("softwareadvice.com",       "software_comparison_ecosystems", 2.5),
    ("alternatives.to",          "software_comparison_ecosystems", 2.0),
    ("slashdot.org",             "software_comparison_ecosystems", 2.0),
    ("sourceforge.net",          "software_comparison_ecosystems", 2.0),
    ("producthunt.com",          "software_comparison_ecosystems", 2.5),
    ("crozdesk.com",             "software_comparison_ecosystems", 2.0),
    ("appvizer.",                "software_comparison_ecosystems", 2.0),

    # ── General review ecosystems ─────────────────────────────────────────────
    ("trustpilot.com",           "review_ecosystems", 3.5),
    ("reviews.io",               "review_ecosystems", 3.0),
    ("sitejabber.com",           "review_ecosystems", 2.5),
    ("tripadvisor.",             "review_ecosystems", 3.0),
    ("yelp.com",                 "review_ecosystems", 3.0),
    ("g.page",                   "review_ecosystems", 3.0),
    ("maps.google.",             "review_ecosystems", 3.5),
    ("miodottore.it",            "review_ecosystems", 3.0),
    ("idoctors.it",              "review_ecosystems", 3.0),
    ("prontopro.it",             "review_ecosystems", 2.5),
    ("trovadentisti.it",         "review_ecosystems", 3.0),
    ("doctolib.",                "review_ecosystems", 3.0),
    ("zocdoc.com",               "review_ecosystems", 3.0),
    ("healthgrades.com",         "review_ecosystems", 3.0),

    # ── Professional directories ──────────────────────────────────────────────
    ("clutch.co",                "professional_directories", 3.0),
    ("crunchbase.com",           "professional_directories", 2.5),
    ("linkedin.com",             "professional_directories", 2.0),
    ("avvocati-italia.it",       "professional_directories", 3.5),
    ("studiolegale.",            "professional_directories", 2.5),
    ("avvocato.",                "professional_directories", 2.5),
    ("lawyer.",                  "professional_directories", 2.5),
    ("leggio.it",                "professional_directories", 3.0),
    ("orarioavvocati.it",        "professional_directories", 3.0),
    ("avvocati.it",              "professional_directories", 3.5),
    ("legal500.com",             "professional_directories", 3.5),
    ("chambers.com",             "professional_directories", 3.5),
    ("martindale.com",           "professional_directories", 3.5),
    ("findlaw.com",              "professional_directories", 3.0),
    ("justia.com",               "professional_directories", 3.0),
    ("dentisti-italia.it",       "professional_directories", 3.5),
    ("dnb.com",                  "professional_directories", 2.5),
    ("kompass.com",              "professional_directories", 2.0),
    ("manta.com",                "professional_directories", 2.0),

    # ── Local directories / maps ──────────────────────────────────────────────
    ("paginegialle.it",          "local_directories_maps",   3.0),
    ("paginebianche.it",         "local_directories_maps",   2.5),
    ("virgilio.it",              "local_directories_maps",   2.5),
    ("tuttocitta.it",            "local_directories_maps",   2.5),
    ("google.com/maps",          "local_directories_maps",   3.5),
    ("maps.apple.com",           "local_directories_maps",   3.0),
    ("bing.com/maps",            "local_directories_maps",   2.5),
    ("cylex.it",                 "local_directories_maps",   2.5),
    ("foursquare.com",           "local_directories_maps",   2.5),
    ("yell.com",                 "local_directories_maps",   2.5),
    ("businessprofile.",         "local_directories_maps",   2.5),

    # ── Editorial / news / PR ─────────────────────────────────────────────────
    ("techcrunch.com",           "editorial_news_pr", 3.5),
    ("wired.com",                "editorial_news_pr", 3.5),
    ("forbes.com",               "editorial_news_pr", 3.5),
    ("bloomberg.com",            "editorial_news_pr", 3.5),
    ("guardian.com",             "editorial_news_pr", 3.5),
    ("corriere.it",              "editorial_news_pr", 3.5),
    ("ilsole24ore.com",          "editorial_news_pr", 3.5),
    ("repubblica.it",            "editorial_news_pr", 3.5),
    ("medium.com",               "editorial_news_pr", 2.5),
    ("substack.com",             "editorial_news_pr", 2.5),
    ("venturebeat.com",          "editorial_news_pr", 3.0),
    ("theregister.com",          "editorial_news_pr", 3.0),
    ("eurogamer.net",            "editorial_news_pr", 3.0),

    # ── Marketplace / partner ecosystems ─────────────────────────────────────
    ("deliveroo.",               "marketplace_partner_ecosystems", 2.5),
    ("ubereats.com",             "marketplace_partner_ecosystems", 2.5),
    ("justeat.",                 "marketplace_partner_ecosystems", 2.5),
    ("glovo.",                   "marketplace_partner_ecosystems", 2.5),
    ("amazon.",                  "marketplace_partner_ecosystems", 2.5),
    ("ebay.",                    "marketplace_partner_ecosystems", 2.5),
    ("etsy.com",                 "marketplace_partner_ecosystems", 2.5),
    ("shopify.com/app",          "marketplace_partner_ecosystems", 3.0),
    ("partner.",                 "marketplace_partner_ecosystems", 2.0),
    ("integrations.",            "marketplace_partner_ecosystems", 2.0),

    # ── Docs / integrations / developer ecosystems ───────────────────────────
    ("github.com",               "docs_integrations_developer", 3.0),
    ("npmjs.com",                "docs_integrations_developer", 2.5),
    ("pypi.org",                 "docs_integrations_developer", 2.5),
    ("developer.",               "docs_integrations_developer", 2.5),
    ("docs.",                    "docs_integrations_developer", 2.5),
    ("api.",                     "docs_integrations_developer", 2.0),
    ("zapier.com",               "docs_integrations_developer", 3.0),
    ("make.com",                 "docs_integrations_developer", 2.5),
    ("ifttt.com",                "docs_integrations_developer", 2.0),

    # ── Forums / communities ──────────────────────────────────────────────────
    ("reddit.com",               "forums_communities", 2.0),
    ("news.ycombinator.com",     "forums_communities", 2.5),
    ("quora.com",                "forums_communities", 1.5),
    ("stackoverflow.com",        "forums_communities", 2.5),
    ("discord.com",              "forums_communities", 1.5),
    ("denuncia.it",              "forums_communities", 1.0),

    # ── Official registries / legal ───────────────────────────────────────────
    ("registroimprese.it",       "official_registries_legal", 3.5),
    ("ordine-avvocati.",         "official_registries_legal", 4.0),
    ("consiglionazionaleforense.", "official_registries_legal", 4.0),
    ("ordineavvocati.",          "official_registries_legal", 4.0),
    ("giustizia.it",             "official_registries_legal", 3.5),
    ("camera.it",                "official_registries_legal", 3.0),
    ("gov.uk",                   "official_registries_legal", 3.5),
    ("companieshouse.gov.uk",    "official_registries_legal", 3.5),
    ("sec.gov",                  "official_registries_legal", 3.5),

    # ── Employer / workforce reputation ──────────────────────────────────────
    ("glassdoor.com",            "employer_workforce_reputation", 2.5),
    ("indeed.com",               "employer_workforce_reputation", 2.0),
    ("linkedin.com/jobs",        "employer_workforce_reputation", 2.0),
    ("ambitionbox.com",          "employer_workforce_reputation", 2.0),

    # ── Social proof platforms ────────────────────────────────────────────────
    ("twitter.com",              "social_proof_platforms", 1.0),
    ("x.com",                    "social_proof_platforms", 1.0),
    ("instagram.com",            "social_proof_platforms", 1.0),
    ("facebook.com",             "social_proof_platforms", 1.0),
    ("tiktok.com",               "social_proof_platforms", 1.0),
    ("youtube.com",              "social_proof_platforms", 1.5),
]

# ─────────────────────────────────────────────────────────────────────────────
# C. BUSINESS-TYPE-SPECIFIC SOURCE PACKS
# Keys must match BUSINESS_INTELLIGENCE_PROFILES keys exactly.
# Each pack defines:
# - relevant_families: ordered list, highest priority first
# - weights_override: {family: multiplier} — boost/reduce vs base weight
# - trust_anchors: specific domain patterns that are "gold standard"
# - irrelevant_families: families that should NOT count as missing gaps
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_PACKS: dict[str, dict] = {

    "local_healthcare_ymyl": {
        "label": "Healthcare Provider / Clinic / Medical Center",
        "relevant_families": [
            "local_directories_maps",
            "review_ecosystems",
            "professional_directories",
            "editorial_news_pr",
            "forums_communities",
            "employer_workforce_reputation",
        ],
        "weights_override": {
            # Primary trust signals for multi-location clinics
            "local_directories_maps": 2.5,          # Google Maps / local directories are king for 'near me' queries
            "review_ecosystems": 2.2,               # Patient reviews (MioDottore, Doctolib, Healthgrades) are critical
            "professional_directories": 1.8,        # Practitioner/clinic registries (dentisti-italia, ordini medici)
            "editorial_news_pr": 1.4,               # Healthcare editorial / local press coverage
            "forums_communities": 0.8,              # Patient forums, low authority but real signal
            "employer_workforce_reputation": 0.6,   # Glassdoor/Indeed — relevant for large clinic chains
            # Suppressed / irrelevant
            "app_ecosystems": 0.0,
            "software_comparison_ecosystems": 0.0,
            "marketplace_partner_ecosystems": 0.0,
            "docs_integrations_developer": 0.0,
            "official_registries_legal": 0.3,       # Corporate trust only, not primary healthcare anchor
        },
        "trust_anchors": [
            # Patient review ecosystems
            "miodottore.it", "doctolib.", "healthgrades.com", "zocdoc.com",
            "idoctors.it", "trovadentisti.it", "topdoctors.", "dottori.it",
            "medicitalia.it", "pazienti.it",
            # Local maps & directories
            "maps.google.", "google.com/maps", "paginegialle.it", "paginebianche.it",
            "cylex.it", "virgilio.it",
            # Practitioner / clinic registries
            "dentisti-italia.it", "medici-italia.it",
            "fnomceo.it", "ordinemedici.",
            # Healthcare editorial
            "corriere.it/salute", "repubblica.it/salute", "humanitas.it",
        ],
        "irrelevant_families": [
            "app_ecosystems",
            "software_comparison_ecosystems",
            "marketplace_partner_ecosystems",
            "docs_integrations_developer",
        ],
        "missing_gap_message": {
            "local_directories_maps": (
                "No local directory or Google Maps presence detected. "
                "Critical gap for geographic healthcare queries ('clinic near me', 'doctor [city]'). "
                "Multi-location providers must have verified listings per location."
            ),
            "review_ecosystems": (
                "No patient review platform presence (e.g. MioDottore, Doctolib, Healthgrades, Zocdoc). "
                "Patient trust credibility is severely limited without verified review coverage."
            ),
            "professional_directories": (
                "No practitioner or clinic directory presence detected (e.g. dentisti-italia.it, ordini medici). "
                "Professional registry validation gap weakens institutional credibility."
            ),
            "editorial_news_pr": (
                "No healthcare editorial or local press coverage detected. "
                "Lack of editorial validation limits authority for competitive medical queries."
            ),
        },
        "scoring_weights": {
            "visibility": 0.30,         # Multi-location discovery depends heavily on visibility
            "evidence_depth": 0.25,
            "authority": 0.30,
            "confidence": 0.15,
        }
    },

    "local_legal_ymyl": {
        "label": "Local Legal Services",
        "relevant_families": [
            "official_registries_legal",
            "professional_directories",
            "local_directories_maps",
            "editorial_news_pr",
            "forums_communities",
        ],
        "weights_override": {
            "official_registries_legal": 2.5,   # Bar association is paramount
            "professional_directories": 1.8,
            "local_directories_maps": 1.6,
            "editorial_news_pr": 1.2,
            "review_ecosystems": 0.8,
            "app_ecosystems": 0.0,
            "software_comparison_ecosystems": 0.0,
        },
        "trust_anchors": [
            "ordine-avvocati.", "consiglionazionaleforense.", "ordineavvocati.",
            "avvocati.it", "avvocati-italia.it", "legal500.com", "chambers.com",
            "martindale.com", "findlaw.com", "leggio.it"
        ],
        "irrelevant_families": [
            "app_ecosystems",
            "software_comparison_ecosystems",
            "marketplace_partner_ecosystems",
            "docs_integrations_developer",
            "employer_workforce_reputation",
        ],
        "missing_gap_message": {
            "official_registries_legal": "No bar association / ordine avvocati listing detected. Critical trust gap for lawyers.",
            "professional_directories": "No legal directory presence (e.g. avvocati.it, Legal500, Chambers). Huge visibility risk.",
            "local_directories_maps": "No local mapping presence found. Risk for 'lawyer near me' queries.",
        },
        "scoring_weights": {
            "visibility": 0.25,
            "evidence_depth": 0.30,
            "authority": 0.30,
            "confidence": 0.15,
        }
    },

    "local_dentist": {
        "label": "Local Dentist / Clinic",
        "relevant_families": [
            "local_directories_maps",
            "review_ecosystems",
            "professional_directories",
            "editorial_news_pr",
            "forums_communities",
        ],
        "weights_override": {
            "local_directories_maps": 2.0,
            "review_ecosystems": 1.8,
            "professional_directories": 1.6,
            "editorial_news_pr": 1.0,
            "software_comparison_ecosystems": 0.0,
            "app_ecosystems": 0.3,
            "marketplace_partner_ecosystems": 0.0,
            "docs_integrations_developer": 0.0,
            "official_registries_legal": 1.5,
        },
        "trust_anchors": [
            "miodottore.it", "idoctors.it", "trovadentisti.it",
            "doctolib.", "zocdoc.com", "healthgrades.com",
            "maps.google.", "paginegialle.it", "dentisti-italia.it"
        ],
        "irrelevant_families": [
            "app_ecosystems",
            "software_comparison_ecosystems",
            "marketplace_partner_ecosystems",
            "docs_integrations_developer",
        ],
        "missing_gap_message": {
            "local_directories_maps": "No Google Maps / local listing detected. Primary discovery mechanism for dental clinics.",
            "review_ecosystems": "No healthcare review platform presence (e.g. MioDottore, Doctolib). High risk for patient intent queries.",
            "professional_directories": "No dental/medical directory presence detected.",
        },
        "scoring_weights": {
            "visibility": 0.25,
            "evidence_depth": 0.30,
            "authority": 0.30,
            "confidence": 0.15,
        }
    },

    "freelancer_consultant": {
        "label": "Freelancer / Independent Consultant",
        "relevant_families": [
            "professional_directories",
            "review_ecosystems",
            "editorial_news_pr",
            "forums_communities",
            "local_directories_maps",
            "social_proof_platforms",
        ],
        "weights_override": {
            "professional_directories": 1.8,
            "review_ecosystems": 1.5,
            "editorial_news_pr": 1.5,
            "forums_communities": 1.2,
            "app_ecosystems": 0.0,
            "official_registries_legal": 0.8,  # optional if small business
            "software_comparison_ecosystems": 0.3,
            "marketplace_partner_ecosystems": 0.5,
        },
        "trust_anchors": [
            "clutch.co", "prontopro.it", "linkedin.com",
            "upwork.com", "toptal.com", "trustpilot.com",
        ],
        "irrelevant_families": [
            "app_ecosystems",
            "marketplace_partner_ecosystems",
            "docs_integrations_developer",
            "employer_workforce_reputation",
        ],
        "missing_gap_message": {
            "professional_directories": "No professional directory presence (e.g. Clutch, ProntoPro, LinkedIn). Critical for client sourcing.",
            "review_ecosystems": "No testimonial or review platform presence detected.",
        },
        "scoring_weights": {
            "visibility": 0.30,
            "evidence_depth": 0.25,
            "authority": 0.25,
            "confidence": 0.20,
        }
    },

    "b2b_saas_tech": {
        "label": "B2B SaaS",
        "relevant_families": [
            "software_comparison_ecosystems",
            "editorial_news_pr",
            "docs_integrations_developer",
            "marketplace_partner_ecosystems",
            "review_ecosystems",
            "forums_communities",
        ],
        "weights_override": {
            "software_comparison_ecosystems": 2.0,
            "docs_integrations_developer": 1.8,
            "editorial_news_pr": 1.5,
            "marketplace_partner_ecosystems": 1.3,
            "review_ecosystems": 1.3,
            "local_directories_maps": 0.3,
            "official_registries_legal": 0.3,
            "app_ecosystems": 0.8,
        },
        "trust_anchors": [
            "g2.com", "capterra.com", "producthunt.com",
            "getapp.com", "github.com", "zapier.com",
            "techcrunch.com", "venturebeat.com"
        ],
        "irrelevant_families": [
            "local_directories_maps",
            "official_registries_legal",
            "employer_workforce_reputation",
        ],
        "missing_gap_message": {
            "software_comparison_ecosystems": "No software review/comparison platform presence (G2, Capterra, ProductHunt). Critical gap for SaaS buyer discovery.",
            "docs_integrations_developer": "No developer ecosystem presence (GitHub, Zapier, docs). Risk for technical buyer evaluation.",
            "editorial_news_pr": "No press/editorial mentions detected.",
        },
        "scoring_weights": {
            "visibility": 0.35,
            "evidence_depth": 0.25,
            "authority": 0.30,
            "confidence": 0.10,
        }
    },

    "ecommerce_retail": {
        "label": "Ecommerce Brand",
        "relevant_families": [
            "review_ecosystems",
            "editorial_news_pr",
            "marketplace_partner_ecosystems",
            "forums_communities",
            "social_proof_platforms",
        ],
        "weights_override": {
            "review_ecosystems": 2.0,
            "editorial_news_pr": 1.5,
            "marketplace_partner_ecosystems": 1.3,
            "forums_communities": 1.2,
            "social_proof_platforms": 1.0,
            "software_comparison_ecosystems": 0.2,
            "official_registries_legal": 0.3,
            "docs_integrations_developer": 0.3,
        },
        "trust_anchors": [
            "trustpilot.com", "reviews.io", "google.com/maps",
            "reddit.com", "amazon.", "sitejabber.com"
        ],
        "irrelevant_families": [
            "official_registries_legal",
            "software_comparison_ecosystems",
            "docs_integrations_developer",
        ],
        "missing_gap_message": {
            "review_ecosystems": "No consumer review platform presence (Trustpilot, Reviews.io). High risk for purchase-intent queries.",
            "marketplace_partner_ecosystems": "No marketplace or reseller presence detected.",
            "forums_communities": "No community/forum mention detected. Risk for social proof queries.",
        },
        "scoring_weights": {
            "visibility": 0.35,
            "evidence_depth": 0.25,
            "authority": 0.30,
            "confidence": 0.10,
        }
    },

    "marketplace_aggregator": {
        "label": "Marketplace / Platform / Delivery",
        "relevant_families": [
            "app_ecosystems",
            "review_ecosystems",
            "editorial_news_pr",
            "marketplace_partner_ecosystems",
            "forums_communities",
            "employer_workforce_reputation",
        ],
        "weights_override": {
            "app_ecosystems": 2.0,
            "review_ecosystems": 1.6,
            "editorial_news_pr": 1.5,
            "marketplace_partner_ecosystems": 1.8,
            "forums_communities": 1.2,
            "employer_workforce_reputation": 1.2,
            "software_comparison_ecosystems": 0.3,
            "official_registries_legal": 0.3,
            "docs_integrations_developer": 0.5,
        },
        "trust_anchors": [
            "apps.apple.com", "play.google.com", "trustpilot.com",
            "glassdoor.com", "techcrunch.com", "bloomberg.com",
            "reddit.com"
        ],
        "irrelevant_families": [
            "official_registries_legal",
            "software_comparison_ecosystems",
            "professional_directories",
        ],
        "missing_gap_message": {
            "app_ecosystems": "No App Store / Google Play presence detected. Critical for platform-centric discovery.",
            "marketplace_partner_ecosystems": "No vendor/partner/seller ecosystem presence detected.",
            "review_ecosystems": "No consumer trust platform presence detected.",
        },
        "scoring_weights": {
            "visibility": 0.40,
            "evidence_depth": 0.20,
            "authority": 0.30,
            "confidence": 0.10,
        }
    },

    "hospitality_travel": {
        "label": "Restaurant / Hospitality",
        "relevant_families": [
            "review_ecosystems",
            "local_directories_maps",
            "editorial_news_pr",
            "marketplace_partner_ecosystems",
            "forums_communities",
        ],
        "weights_override": {
            "review_ecosystems": 2.0,
            "local_directories_maps": 1.8,
            "editorial_news_pr": 1.3,
            "marketplace_partner_ecosystems": 1.2,
            "forums_communities": 1.0,
            "software_comparison_ecosystems": 0.0,
            "app_ecosystems": 0.5,
            "official_registries_legal": 0.2,
        },
        "trust_anchors": [
            "tripadvisor.", "google.com/maps", "maps.google.",
            "yelp.com", "thefork.", "deliveroo.", "justeat.",
            "corriere.it", "paginegialle.it"
        ],
        "irrelevant_families": [
            "software_comparison_ecosystems",
            "docs_integrations_developer",
            "official_registries_legal",
            "employer_workforce_reputation",
        ],
        "missing_gap_message": {
            "review_ecosystems": "No restaurant/hospitality review platform presence (e.g. TripAdvisor, Google Maps, Yelp). Critical for discovery.",
            "local_directories_maps": "No local directory / Google Maps presence. High risk for 'near me' queries.",
            "marketplace_partner_ecosystems": "No food delivery platform listing (e.g. Deliveroo, JustEat, Glovo).",
        },
        "scoring_weights": {
            "visibility": 0.30,
            "evidence_depth": 0.25,
            "authority": 0.35,
            "confidence": 0.10,
        }
    },

    "professional_services": {
        "label": "Marketing / Creative Agency",
        "relevant_families": [
            "professional_directories",
            "editorial_news_pr",
            "review_ecosystems",
            "forums_communities",
            "social_proof_platforms",
        ],
        "weights_override": {
            "professional_directories": 1.8,
            "editorial_news_pr": 1.6,
            "review_ecosystems": 1.4,
            "forums_communities": 1.1,
            "app_ecosystems": 0.3,
            "official_registries_legal": 0.4,
            "software_comparison_ecosystems": 0.8,
        },
        "trust_anchors": [
            "clutch.co", "trustpilot.com", "linkedin.com",
            "forbes.com", "medium.com"
        ],
        "irrelevant_families": [
            "app_ecosystems",
            "official_registries_legal",
            "employer_workforce_reputation",
        ],
        "missing_gap_message": {
            "professional_directories": "No agency directory presence (e.g. Clutch). Client sourcing risk.",
            "editorial_news_pr": "No press/editorial mentions. Thought leadership gap.",
        },
        "scoring_weights": {
            "visibility": 0.30,
            "evidence_depth": 0.25,
            "authority": 0.25,
            "confidence": 0.20,
        }
    },

    "education_institution": {
        "label": "Education / Course Provider",
        "relevant_families": [
            "review_ecosystems",
            "editorial_news_pr",
            "forums_communities",
            "professional_directories",
            "social_proof_platforms",
        ],
        "weights_override": {
            "review_ecosystems": 1.8,
            "editorial_news_pr": 1.5,
            "forums_communities": 1.5,
            "social_proof_platforms": 1.2,
            "app_ecosystems": 1.0,
            "software_comparison_ecosystems": 0.5,
            "official_registries_legal": 0.3,
        },
        "trust_anchors": [
            "trustpilot.com", "coursereport.com", "switchup.org",
            "reddit.com", "producthunt.com"
        ],
        "irrelevant_families": [
            "official_registries_legal",
            "marketplace_partner_ecosystems",
        ],
        "missing_gap_message": {
            "review_ecosystems": "No course/education review platform presence.",
            "forums_communities": "No community discussion presence (e.g. Reddit). High risk for peer recommendations.",
        },
        "scoring_weights": {
            "visibility": 0.35,
            "evidence_depth": 0.25,
            "authority": 0.30,
            "confidence": 0.10,
        }
    },

    "publisher_media": {
        "label": "Media / Blog / Publisher",
        "relevant_families": [
            "editorial_news_pr",
            "forums_communities",
            "social_proof_platforms",
            "review_ecosystems",
        ],
        "weights_override": {
            "editorial_news_pr": 2.0,
            "forums_communities": 1.5,
            "social_proof_platforms": 1.2,
            "review_ecosystems": 0.8,
            "app_ecosystems": 0.5,
            "official_registries_legal": 0.2,
            "software_comparison_ecosystems": 0.2,
        },
        "trust_anchors": [
            "techcrunch.com", "wired.com", "reddit.com",
            "news.ycombinator.com", "medium.com"
        ],
        "irrelevant_families": [
            "official_registries_legal",
            "software_comparison_ecosystems",
            "professional_directories",
            "marketplace_partner_ecosystems",
        ],
        "missing_gap_message": {
            "editorial_news_pr": "No press/editorial mentions found outside owned properties.",
        },
        "scoring_weights": {
            "visibility": 0.35,
            "evidence_depth": 0.25,
            "authority": 0.30,
            "confidence": 0.10,
        }
    },
}


# Safe fallbacks for profiles not explicitly mapped
SOURCE_PACKS["general_local_business"] = {
    "label": "General Local Business",
    "relevant_families": [
        "local_directories_maps",
        "review_ecosystems",
        "editorial_news_pr",
        "forums_communities",
    ],
    "weights_override": {
        "local_directories_maps": 1.8,
        "review_ecosystems": 1.6,
        "editorial_news_pr": 1.2,
        "software_comparison_ecosystems": 0.2,
        "app_ecosystems": 0.3,
        "official_registries_legal": 0.8,
    },
    "trust_anchors": [
        "google.com/maps", "maps.google.", "paginegialle.it",
        "yelp.com", "trustpilot.com"
    ],
    "irrelevant_families": [
        "software_comparison_ecosystems",
        "docs_integrations_developer",
    ],
    "missing_gap_message": {
        "local_directories_maps": "No local listing / Google Maps presence detected.",
        "review_ecosystems": "No review platform presence detected.",
    },
    "scoring_weights": {
        "visibility": 0.30,
        "evidence_depth": 0.30,
        "authority": 0.25,
        "confidence": 0.15,
    }
}


SOURCE_PACKS["unknown"] = dict(SOURCE_PACKS["general_local_business"])
# Safely assign specific unknown weights overriding general local
SOURCE_PACKS["unknown"]["scoring_weights"] = {
    "visibility": 0.30,
    "evidence_depth": 0.25,
    "authority": 0.25,
    "confidence": 0.20,
}


# ─────────────────────────────────────────────────────────────────────────────
# D. HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def get_source_pack(profile_key: str) -> dict:
    """Return the source pack for a given business profile key, normalized to canonical."""
    norm_key = normalize_profile_key(profile_key)
    if norm_key not in SOURCE_PACKS:
        return SOURCE_PACKS["general_local_business"]
    return SOURCE_PACKS[norm_key]

def get_profile_scoring_weights(profile_key: str) -> dict[str, float]:
    """Return exactly 4 deterministic pillar weights for the profile."""
    pack = get_source_pack(profile_key)
    weights = pack.get("scoring_weights", {
        "visibility": 0.30,
        "evidence_depth": 0.25,
        "authority": 0.25,
        "confidence": 0.20
    })
    return weights


def classify_url_to_family(url: str, brand_domain: str, brand_name: str = None) -> tuple[str, float]:
    """
    Classify a URL into a source family and return (family, base_weight).
    Returns ("owned", 0.5) for brand-owned URLs.
    Unclassified URLs are split into ("ignored_noise", 0.0) or ("unclassified_candidate", 0.5).
    """
    from urllib.parse import urlparse
    import re
    
    if not url:
        return "ignored_noise", 0.0

    try:
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
    except Exception:
        return "ignored_noise", 0.0

    hostname = hostname.lower().strip()
    if hostname.startswith("www."):
        hostname = hostname[4:]

    if not hostname:
        return "ignored_noise", 0.0

    # Owned detection
    if brand_domain:
        bd = brand_domain.lower().strip()
        if bd.startswith("www."):
            bd = bd[4:]
        if bd and bd in hostname:
            return "owned", 0.5

    url_lower = url.lower()
    
    # Noise Heuristics (Must run BEFORE canonical patterns to override generic domains)
    noise_exact_domains = {
        "youtube.com", "youtu.be", "vimeo.com", "tiktok.com", "instagram.com", "facebook.com",
        "twitter.com", "x.com", "pinterest.com", "wa.me", "api.whatsapp.com", "bit.ly", "goo.gl", "t.co",
        "duckduckgo.com", "baidu.com", "yandex.com", "reddit.com/search"
    }
    
    noise_paths = [
        "google.com/search", "google.it/search", "search.yahoo.com", "bing.com/search",
        "maps.google.com/search"
    ]
    
    # 1. Exact or subdomain match for known pure-noise aggregators/social sites
    is_noise_domain = any(hostname == nd or hostname.endswith("." + nd) for nd in noise_exact_domains)
    if is_noise_domain:
        return "ignored_noise", 0.0
        
    # 2. Path-based check for utility URLs
    if any(np in url_lower for np in noise_paths):
        return "ignored_noise", 0.0

    # Check canonical patterns
    for pattern, family, weight in DOMAIN_FAMILY_MAP:
        if pattern in hostname or pattern in url_lower:
            return family, weight

    # Brand Relevance Tie-breaker
    if brand_name:
        brand_slug = re.sub(r'[^a-z0-9]', '', brand_name.lower())
        if brand_slug and len(brand_slug) > 3:
            if brand_slug in url_lower:
                return "unclassified_candidate", 0.5

    # If it survived noise filters, it's an unresolved candidate
    return "unclassified_candidate", 0.5


def get_canonical_source_urls(state: dict) -> list[str]:
    """
    Extracts, deduplicates, and unifies all external source URLs found across 
    different pipeline stages (structured Serper arrays + legacy raw arrays)
    so nodes operate on exactly one canonical external dataset.
    """
    external_sources_raw = state.get("external_sources_raw", [])
    raw_data = state.get("raw_data_complete", {})
    
    all_source_urls: list[str] = []
    
    # 1. Prioritize structured URLs from SERPER discovery
    for source_dict in external_sources_raw:
        s_url = source_dict.get("url")
        if s_url and s_url not in all_source_urls:
            all_source_urls.append(s_url)
            
    # 2. Add scraped or implicitly gathered URLs
    raw_source_urls = raw_data.get("source_urls", [])
    for s_url in raw_source_urls:
        if s_url and s_url not in all_source_urls:
            all_source_urls.append(s_url)
            
    return all_source_urls


def compute_profile_aware_strength(
    family_breakdown: dict[str, int],
    family_weights: dict[str, float],
    total_sources: int,
) -> int:
    """
    Compute 0–100 brand strength with profile-aware family weights.
    """
    if total_sources == 0:
        return 0

    raw = sum(
        count * family_weights.get(family, 0.5)
        for family, count in family_breakdown.items()
    )
    # Soft ceiling: 30 weighted points = 100
    return min(100, int((raw / 30.0) * 100))


def get_missing_relevant_sources(
    family_breakdown: dict[str, int],
    pack: dict,
) -> list[str]:
    """
    Return a list of relevant family gap messages where zero sources were detected.
    Skips families that appear in 'irrelevant_families'.
    """
    gaps = []
    irrelevant = set(pack.get("irrelevant_families", []))
    gap_messages = pack.get("missing_gap_message", {})

    for family in pack.get("relevant_families", []):
        if family in irrelevant:
            continue
        if family_breakdown.get(family, 0) == 0 and family in gap_messages:
            gaps.append(gap_messages[family])

    return gaps


def get_irrelevant_ignored(pack: dict) -> list[str]:
    """Return human-readable list of ignored irrelevant families."""
    return pack.get("irrelevant_families", [])


def check_trust_anchor_presence(
    all_source_urls: list[str],
    pack: dict,
) -> list[str]:
    """Return which trust anchor patterns were found in the source list."""
    found = []
    anchors = pack.get("trust_anchors", [])
    for anchor in anchors:
        for url in all_source_urls:
            if anchor in url.lower():
                found.append(anchor)
                break
    return found


# ─────────────────────────────────────────────────────────────────────────────
# E. FIRST-PARTY SITE EVIDENCE INFERENCE
# Detects ecosystem presence from on-site content/metadata when no external
# source URL confirms it. Returns inferred families with confidence levels.
# ─────────────────────────────────────────────────────────────────────────────

# Signals per source family: each entry is (regex_pattern, confidence_weight)
# confidence_weight: "high" = explicit markup, "medium" = CTA/text, "low" = generic
_ONSITE_INFERENCE_RULES: dict[str, list[tuple[str, str]]] = {

    "app_ecosystems": [
        # Strong: explicit app store open-graph / meta tags
        (r'al:ios:app_store_id',                    "high"),
        (r'al:android:package',                     "high"),
        (r'al:android:app_name',                    "high"),
        (r'content="itms-apps',                     "high"),
        (r'play\.google\.com/store/apps',            "high"),
        (r'apps\.apple\.com',                        "high"),
        # Medium: download CTAs in page text
        (r'(download|scarica).{0,25}(app|application)', "medium"),
        (r'(get|available)\s+(it\s+)?on\s+(the\s+)?(app\s+store|google\s+play)', "medium"),
        (r'app\s+store',                             "medium"),
        (r'google\s+play',                           "medium"),
        # Low: mentions of mobile app generally
        (r'mobile\s+app',                            "low"),
        (r'ios\s+app',                                "low"),
        (r'android\s+app',                            "low"),
    ],

    "marketplace_partner_ecosystems": [
        # Strong: explicit partner/vendor/rider signup sections
        (r'(become|diventa|register\s+as)\s+a?\s*(partner|vendor|seller|rider|restaurant|merchant)', "high"),
        (r'(partner|merchant|restaurant|vendor)\s+(portal|hub|onboarding|signup|dashboard)', "high"),
        (r'list\s+your\s+(restaurant|business|shop|property)',  "high"),
        (r'(iscrivere|registra)\s+(il\s+)?tuo\s+(ristorante|esercizio|locale)', "high"),
        (r'rider\s+(signup|onboarding|registration|app)',       "high"),
        (r'courier\s+(signup|onboarding|registration)',          "high"),
        # Medium: partner/seller interest language
        (r'partner\s+with\s+us',                     "medium"),
        (r'grow\s+your\s+business\s+with',            "medium"),
        (r'(join|unisciti\s+a)\s+(our|the)?\s*(network|platform|marketplace)', "medium"),
        (r'add\s+your\s+(restaurant|business|shop)',  "medium"),
        (r'sell\s+on\s+our\s+platform',               "medium"),
        (r'aggiungi\s+la\s+tua\s+attività',           "medium"),
        # Low: generic partner/vendor mentions
        (r'\bpartner\b',                              "low"),
        (r'\bvendor\b',                               "low"),
        (r'\bmerchant\b',                             "low"),
    ],

    "docs_integrations_developer": [
        (r'api\..*\.(com|io|co)',                     "high"),
        (r'github\.com',                              "high"),
        (r'developer\s+documentation',                "medium"),
        (r'api\s+reference',                          "medium"),
        (r'sdk\s+download',                           "medium"),
        (r'open\s+source',                            "medium"),
        (r'\bwebhook\b',                              "low"),
        (r'\bapi\s+key\b',                            "low"),
    ],

    "review_ecosystems": [
        # Strong: explicit embedded review widget schema
        (r'"@type":\s*"Review"',                     "high"),
        (r'"@type":\s*"AggregateRating"',            "high"),
        (r'data-review',                              "medium"),
        (r'\btrustscore\b',                           "medium"),
        # Medium: explicit review platform mentions
        (r'trustpilot',                              "medium"),
        (r'google\s+reviews',                        "medium"),
        (r'lascia\s+una\s+recensione',               "medium"),
        (r'leave\s+a\s+review',                      "medium"),
    ],
}


def infer_families_from_site_evidence(
    client_content_clean: str,
    og_tags: dict,
    json_ld_blocks: list,
    client_content_raw,
) -> dict[str, dict]:
    """
    Scan on-site content/metadata for ecosystem presence signals.

    Returns a dict of {family: {"confidence": str, "signals": [str]}}
    only for families where at least one signal was detected.

    - "high" confidence: explicit markup (app store meta tags, partner signup forms)
    - "medium" confidence: CTA text or platform name mentions
    - "low" confidence: generic term presence

    This does NOT fabricate external citations. The result is used solely
    to avoid penalizing brands for missing external sources when first-party
    evidence clearly demonstrates the ecosystem exists.
    """
    import re

    # Build search corpus from all available on-site signals
    corpus_parts = []

    if client_content_clean:
        corpus_parts.append(client_content_clean[:20000])

    # og_tags may contain app meta tags (al:ios:app_store_id etc.)
    if og_tags and isinstance(og_tags, dict):
        corpus_parts.append(" ".join(f"{k} {v}" for k, v in og_tags.items()))

    # json_ld_blocks for AggregateRating etc.
    if json_ld_blocks and isinstance(json_ld_blocks, list):
        for block in json_ld_blocks:
            if isinstance(block, dict):
                import json as _json
                try:
                    corpus_parts.append(_json.dumps(block))
                except Exception:
                    pass
            elif isinstance(block, str):
                corpus_parts.append(block)

    # client_content_raw may be a list of {url, html} dicts from the fetcher
    if client_content_raw:
        if isinstance(client_content_raw, list):
            for page in client_content_raw:
                if isinstance(page, dict):
                    corpus_parts.append(page.get("html", "")[:5000])
        elif isinstance(client_content_raw, str):
            corpus_parts.append(client_content_raw[:10000])

    corpus = " ".join(corpus_parts).lower()

    if not corpus.strip():
        return {}

    inferred: dict[str, dict] = {}

    for family, rules in _ONSITE_INFERENCE_RULES.items():
        matched_signals = []
        best_confidence = None

        confidence_rank = {"high": 3, "medium": 2, "low": 1}

        for pattern, confidence in rules:
            if re.search(pattern, corpus, re.IGNORECASE):
                matched_signals.append(pattern)
                if best_confidence is None or confidence_rank[confidence] > confidence_rank[best_confidence]:
                    best_confidence = confidence

        if matched_signals:
            inferred[family] = {
                "confidence": best_confidence,
                "signals": matched_signals[:5],  # cap to avoid noise in output
            }

    return inferred
