"""
Schema Generation Node — GEO Optimizer Pipeline.

Turns schema detection results into implementation-ready JSON-LD
recommendations, driven by the selected business profile.
All logic is deterministic; no external API calls.
"""

import json
import re
from typing import Any
from rich.console import Console
from nodes.business_profiles import DEFAULT_PROFILE_KEY, normalize_profile_key

console = Console()

# ─────────────────────────────────────────────────────────────────────────────
# Profile → Required Schema Types Mapping
# ─────────────────────────────────────────────────────────────────────────────

PROFILE_SCHEMA_MAP: dict[str, list[dict[str, str]]] = {
    "b2b_saas_tech": [
        {"schema_type": "Organization",        "page_type": "Homepage",     "rationale": "Establishes core brand entity for AI Knowledge Graph inclusion."},
        {"schema_type": "SoftwareApplication",  "page_type": "Product Pages","rationale": "Critical for appearing in AI 'Best Tools' and feature-comparison answers."},
        {"schema_type": "FAQPage",              "page_type": "FAQ / Support","rationale": "Optimizes for direct-answer extraction on technical and pricing queries."},
        {"schema_type": "WebSite",              "page_type": "Homepage",     "rationale": "Enables sitelinks and brand search actions in AI search snapshots."},
    ],
    "local_healthcare_ymyl": [
        {"schema_type": "MedicalBusiness",      "page_type": "Location Pages","rationale": "Geo-anchors the practice for neighborhood-specific local visibility."},
        {"schema_type": "Physician",            "page_type": "Bio/Team",      "rationale": "Maps practitioners to build topical authority and personal brand trust."},
        {"schema_type": "LocalBusiness",        "page_type": "Location Pages","rationale": "Strengthens baseline visibility for geographic areas."},
        {"schema_type": "FAQPage",              "page_type": "Service Pages", "rationale": "Tied to emergency and treatment-specific direct-answer queries."},
    ],
    "local_legal_ymyl": [
        {"schema_type": "LegalService",         "page_type": "Location Pages","rationale": "Geo-anchors the legal practice for local visibility."},
        {"schema_type": "Person",               "page_type": "Bio/Team",      "rationale": "Builds trust and establishes individual attorney credentials."},
        {"schema_type": "Organization",         "page_type": "Homepage",      "rationale": "Establishes law firm entity for knowledge graph and brand authority."},
    ],
    "ecommerce_retail": [
        {"schema_type": "Product",              "page_type": "Product Pages","rationale": "Base requirement for AI visibility in product-specific shopping intent queries."},
        {"schema_type": "AggregateOffer",       "page_type": "Product Pages","rationale": "Powers price-range and multi-item comparison visibility in AI answers."},
        {"schema_type": "Review",               "page_type": "Product Pages","rationale": "Builds trust points for product conversion validation."},
        {"schema_type": "Organization",         "page_type": "Homepage",     "rationale": "Ensures brand recognition during direct comparisons with competitors."},
    ],
    "hospitality_travel": [
        {"schema_type": "Restaurant",           "page_type": "Homepage",     "rationale": "Core entity for dining intent; allows direct menu and booking visibility."},
        {"schema_type": "Menu",                 "page_type": "Menu Pages",   "rationale": "Directly enables AI answers about specific dishes, prices, and dietary info."},
        {"schema_type": "LocalBusiness",        "page_type": "Homepage",     "rationale": "Important for nearby/discovery local queries without specific cuisine."},
        {"schema_type": "OpeningHoursSpecification", "page_type": "Contact/Footer", "rationale": "Addresses practical planning intent instantly."},
    ],
    "publisher_media": [
        {"schema_type": "Article",               "page_type": "Article Pages","rationale": "Enables quote-level citation and reference listing in AI search answers."},
        {"schema_type": "NewsArticle",           "page_type": "Article Pages","rationale": "Prioritizes fresh event-driven knowledge components."},
        {"schema_type": "Person",                "page_type": "Author Bio",   "rationale": "Builds author-level E-E-A-T and topical authority for deep-dive queries."},
        {"schema_type": "BlogPosting",           "page_type": "Article Pages","rationale": "Semantic indicator for long form content grouping."},
    ],
    "professional_services": [
        {"schema_type": "Organization",         "page_type": "Homepage",     "rationale": "Brand identity anchor for agency-level authority and reputation."},
        {"schema_type": "Service",              "page_type": "Solution Pages","rationale": "Powers visibility for specialized services like 'SaaS SEO' or 'B2B Ads'."},
        {"schema_type": "Person",               "page_type": "Team Page",    "rationale": "Authoritative signal linking expertise to specific senior team members."},
    ],
    "marketplace_aggregator": [
        {"schema_type": "Organization",         "page_type": "Homepage",     "rationale": "Brand identity anchor for platform authority."},
        {"schema_type": "CollectionPage",       "page_type": "Category Pages","rationale": "Structures comparison arrays for category searches."},
        {"schema_type": "ItemList",             "page_type": "Search Results", "rationale": "Itemizes listings for informational discovery options."},
        {"schema_type": "WebSite",              "page_type": "Homepage",     "rationale": "Enables sitelinks and direct internal search hooks."},
    ],
    "education_institution": [
        {"schema_type": "EducationalOrganization","page_type": "Homepage",   "rationale": "Establishes core institutional entity for AI Knowledge Graph."},
        {"schema_type": "Course",               "page_type": "Course Pages", "rationale": "Enables direct surfacing in 'best courses for [subject]' queries."},
        {"schema_type": "FAQPage",              "page_type": "Admissions",   "rationale": "Directly answers pre-enrollment informational queries."},
    ],
    "specialty_goods_supplier": [
        {"schema_type": "Product",              "page_type": "Product Pages","rationale": "Enables visibility for specific technical/industrial product queries."},
        {"schema_type": "Offer",                "page_type": "Product Pages","rationale": "Powers transactional intent for unit-based or bulk pricing."},
        {"schema_type": "AggregateOffer",       "page_type": "Product Pages","rationale": "Crucial for industrial suppliers with volume-based discount tiers."},
        {"schema_type": "LocalBusiness",        "page_type": "Homepage",     "rationale": "Geo-anchors the supplier for regional distribution trust."},
        {"schema_type": "Organization",         "page_type": "Homepage",     "rationale": "Establishes corporate authority and certification validation."},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_schema_keys(schema_counts: dict) -> set[str]:
    """Return a lowercased set of all detected schema type names."""
    return {k.lower() for k in schema_counts.keys()}


def _generate_jsonld_stub(
    schema_type: str,
    url: str,
    page_title: str,
    meta_description: str,
    target_industry: str,
) -> dict[str, Any]:
    """
    Generate a safe starter JSON-LD block for a given schema type.
    Uses inferred values from state where possible, placeholders otherwise.
    """
    base: dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": schema_type,
    }

    if schema_type == "Organization":
        base["name"] = page_title or "[Your Company Name]"
        base["url"] = url
        base["description"] = meta_description or "[Brief description of your organization]"
        base["logo"] = f"{url.rstrip('/')}/logo.png"
        base["sameAs"] = ["[LinkedIn URL]", "[Twitter URL]"]

    elif schema_type == "SoftwareApplication":
        base["name"] = page_title or "[Product Name]"
        base["applicationCategory"] = target_industry or "BusinessApplication"
        base["operatingSystem"] = "Web"
        base["offers"] = {"@type": "Offer", "price": "0", "priceCurrency": "EUR"}

    elif schema_type in ("Product",):
        base["name"] = page_title or "[Product Name]"
        base["description"] = meta_description or "[Product description]"
        base["brand"] = {"@type": "Brand", "name": "[Brand Name]"}
        base["offers"] = {"@type": "Offer", "price": "[Price]", "priceCurrency": "EUR", "availability": "https://schema.org/InStock"}

    elif schema_type == "AggregateOffer":
        base["@type"] = "AggregateOffer"
        base["lowPrice"] = "[Lowest Price]"
        base["highPrice"] = "[Highest Price]"
        base["priceCurrency"] = "EUR"
        base["offerCount"] = "[Number of Offers]"

    elif schema_type == "FAQPage":
        base["mainEntity"] = [
            {
                "@type": "Question",
                "name": "[Frequently Asked Question]",
                "acceptedAnswer": {"@type": "Answer", "text": "[Direct, concise answer]"}
            }
        ]

    elif schema_type in ("Dentist", "MedicalOrganization"):
        base["name"] = page_title or "[Clinic Name]"
        base["url"] = url
        base["address"] = {"@type": "PostalAddress", "streetAddress": "[Address]", "addressLocality": "[City]", "addressCountry": "IT"}
        base["telephone"] = "[+39 XXX XXX XXXX]"
        if schema_type == "Dentist":
            base["medicalSpecialty"] = "Dentistry"

    elif schema_type in ("LocalBusiness", "Restaurant", "LegalService"):
        base["name"] = page_title or "[Business Name]"
        base["url"] = url
        base["address"] = {"@type": "PostalAddress", "streetAddress": "[Address]", "addressLocality": "[City]", "addressCountry": "IT"}
        base["telephone"] = "[Phone]"
        if schema_type == "Restaurant":
            base["servesCuisine"] = "[Cuisine Type]"
            base["hasMenu"] = f"{url.rstrip('/')}/menu"

    elif schema_type == "Person":
        base["name"] = "[Full Name]"
        base["jobTitle"] = "[Job Title]"
        base["url"] = url
        base["sameAs"] = ["[LinkedIn URL]"]

    elif schema_type == "Course":
        base["name"] = "[Course Title]"
        base["description"] = meta_description or "[Course description]"
        base["provider"] = {"@type": "Organization", "name": page_title or "[Provider Name]"}

    elif schema_type == "Article":
        base["headline"] = page_title or "[Article Headline]"
        base["author"] = {"@type": "Person", "name": "[Author Name]"}
        base["datePublished"] = "[YYYY-MM-DD]"
        base["publisher"] = {"@type": "Organization", "name": "[Publication Name]"}

    elif schema_type == "Service":
        base["name"] = "[Service Name]"
        base["description"] = "[Service description]"
        base["provider"] = {"@type": "Organization", "name": page_title or "[Company Name]"}
        base["areaServed"] = "[City / Region]"

    elif schema_type == "Menu":
        base["name"] = "Menu"
        base["url"] = f"{url.rstrip('/')}/menu"
        base["hasMenuSection"] = [
            {"@type": "MenuSection", "name": "[Section]", "hasMenuItem": [
                {"@type": "MenuItem", "name": "[Dish]", "offers": {"@type": "Offer", "price": "[Price]", "priceCurrency": "EUR"}}
            ]}
        ]

    elif schema_type == "WebSite":
        base["name"] = page_title or "[Site Name]"
        base["url"] = url
        base["potentialAction"] = {
            "@type": "SearchAction",
            "target": f"{url.rstrip('/')}/search?q={{search_term_string}}",
            "query-input": "required name=search_term_string"
        }

    elif schema_type == "BreadcrumbList":
        base["itemListElement"] = [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": url},
            {"@type": "ListItem", "position": 2, "name": "[Category]", "item": f"{url.rstrip('/')}/[category]"},
        ]

    else:
        base["name"] = page_title or "[Name]"
        base["url"] = url

    return base


def _compute_completeness(
    detected_types: set[str],
    required_specs: list[dict[str, str]],
) -> int:
    """
    Score 0-100 measuring how many required schema types are already present.
    """
    if not required_specs:
        return 100
    covered = sum(1 for spec in required_specs if spec["schema_type"].lower() in detected_types)
    return int((covered / len(required_specs)) * 100)


# ─────────────────────────────────────────────────────────────────────────────
# Main Process
# ─────────────────────────────────────────────────────────────────────────────

def process(state: dict) -> dict:
    """
    Generate profile-aware schema recommendations with starter JSON-LD.
    Gated by profile-specific allowlists to prevent misclassification leakage.
    """
    console.print("[bold blue]Node: Schema Generation[/bold blue] | Building profile-aware schema recommendations...")

    schema_counts: dict   = state.get("schema_type_counts", {})
    json_ld_blocks: list  = state.get("json_ld_blocks", [])
    target_industry: str  = state.get("target_industry", "Unknown")
    profile: dict         = state.get("business_profile", {})
    profile_summary: dict = state.get("business_profile_summary", {})
    url: str              = state.get("url", "https://example.com")
    page_title: str       = state.get("page_title", "")
    meta_description: str = state.get("meta_description", "")
    profile_key: str      = state.get("business_profile_key", DEFAULT_PROFILE_KEY)
    
    # Normalize profile key
    profile_key = normalize_profile_key(profile_key)
    
    # Audit Integrity Context
    integrity_status = state.get("audit_integrity_status", "valid")
    allowed_types = profile.get("allowed_schema_types", []) # Safety guard from business_profiles.py

    detected_types = _normalize_schema_keys(schema_counts)
    
    # Get blueprint/specs for this profile
    required_specs = PROFILE_SCHEMA_MAP.get(profile_key, PROFILE_SCHEMA_MAP[DEFAULT_PROFILE_KEY])

    # Determine what is missing, with SAFETY GATING
    missing_types: list[str] = []
    recommended_blocks: list[dict[str, Any]] = []
    blocked_count = 0

    for spec in required_specs:
        s_type = spec["schema_type"]
        
        # 1. Check if already present
        if s_type.lower() in detected_types:
            continue
            
        # 2. SAFETY GUARD: Is this type allowed for this business profile?
        if allowed_types and s_type not in allowed_types:
            console.print(f"   [yellow]Safety Guard[/yellow]: Blocked '{s_type}' as irrelevant for profile '{profile_key}'")
            blocked_count += 1
            continue

        # 3. Generate recommendation
        missing_types.append(s_type)
        jsonld = _generate_jsonld_stub(s_type, url, page_title, meta_description, target_industry)
        
        # Provisonal Tagging
        reco_label = s_type
        if integrity_status != "valid":
            reco_label = f"{s_type} [PROVISIONAL]"

        recommended_blocks.append({
            "page_type": spec["page_type"],
            "schema_type": reco_label,
            "rationale": spec["rationale"],
            "jsonld": json.dumps(jsonld, indent=2, ensure_ascii=False),
            "evidence_origin": "on_site" if integrity_status == "valid" else "profile_inference",
            "evidence_confidence": "high" if integrity_status == "valid" else "medium",
            "supporting_signals": [f"Industry standard for {profile_key}", f"Detected as missing on {spec['page_type']}"]
        })

    completeness = _compute_completeness(detected_types, required_specs)

    notes_parts: list[str] = []
    if integrity_status == "invalid":
        notes_parts.append("[AUDIT INTEGRITY ALERT] Recommendations are profile-inferred due to failed crawl.")
    
    if completeness == 100:
        notes_parts.append("All profile-required schema types are already present.")
    else:
        notes_parts.append(f"{len(missing_types)} schema type(s) missing for the '{profile_summary.get('label', profile_key)}' profile.")
    
    if blocked_count > 0:
        notes_parts.append(f"Note: {blocked_count} irrelevant types were suppressed by safety filters.")
        
    if json_ld_blocks:
        notes_parts.append(f"{len(json_ld_blocks)} existing JSON-LD block(s) detected on page.")
        
    notes = " ".join(notes_parts)

    state["schema_recommendations"] = {
        "schema_completeness_score": completeness,
        "missing_types": missing_types,
        "recommended_blocks": recommended_blocks,
        "notes": notes,
        "safety_blocked_count": blocked_count
    }

    console.print(
        f"[bold green]Schema Generation Complete[/bold green] | "
        f"Completeness: [cyan]{completeness}%[/cyan] | "
        f"Missing: [yellow]{len(missing_types)}[/yellow] | "
        f"Safety Blocked: [yellow]{blocked_count}[/yellow]"
    )

    return state
