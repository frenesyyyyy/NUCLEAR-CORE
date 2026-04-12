"""
Profile Selector — Deterministic Keyword Scoring Matrix.

Replaces static if/else chains with a vectorized keyword frequency
approach. Each of the 6 canonical profiles has 20+ terms (EN + IT).
The profile with the highest aggregate hit count wins.

Tie-break / zero-score fallback: professional_services.
"""

import re
from rich.console import Console

console = Console()

# ─────────────────────────────────────────────────────────────────────────────
# Keyword Scoring Matrix — 20+ terms per profile (English + Italian)
# ─────────────────────────────────────────────────────────────────────────────

PROFILE_KEYWORDS: dict[str, list[str]] = {

    "b2b_saas_tech": [
        # EN
        "software", "saas", "cloud", "dashboard", "api",
        "b2b", "workflow", "datacenter", "cybersecurity", "devops",
        "crm", "erp", "microservices", "infrastructure", "middleware",
        # IT
        "piattaforma b2b", "infrastruttura it", "gestionale", "sviluppo software",
    ],

    "local_healthcare_ymyl": [
        # EN
        "dentist", "clinic", "doctor", "healthcare", "medical",
        "therapy", "emergency", "consultation", "patient", "wellness",
        "appointment", "diagnosis", "treatment", "surgery", "insurance",
        # IT / Auto Repair (YMYL Safety)
        "pazienti", "dentista", "sanità", "cura", "benessere",
        "medico", "clinica", "ospedale", "ambulatorio", "visita",
        "officina", "riparazione", "tagliando", "manutenzione",
        "ricambi", "carrozzeria", "meccanico", "patologia", "diagnostica"
    ],

    "local_legal_ymyl": [
        # EN
        "lawyer", "attorney", "legal", "court", "law",
        "litigation", "defense", "consultation", "settlement", "lawsuit",
        # IT
        "avvocato", "studio legale", "legale", "tribunale", "causa",
        "difesa", "consulenza legale", "diritto", "risarcimento",
    ],

    "ecommerce_retail": [
        # EN
        "shop", "cart", "checkout", "shipping", "ecommerce",
        "retail", "store", "buy", "product", "price",
        "discount", "collection", "order", "delivery", "warranty",
        "brand", "catalog", "marketplace", "dealership", "test drive", "showroom",
        "grocery", "supermarket", "food", "organic", "bio", "supplements",
        # IT / Auto Retail
        "compra", "carrello", "spedizione", "prodotti", "negozio online",
        "acquista", "offerta", "sconto", "prezzo", "auto", "concessionaria",
        "veicoli", "usato", "nuovo", "pronta consegna",
        "supermercato", "biologico", "naturale", "alimentare", "alimentari", "cibo", "spesa", "erboristeria"
    ],

    "hospitality_travel": [
        # EN
        "hotel", "rooms", "booking", "coliving", "co-living",
        "restaurant", "menu", "travel", "vacation", "resort",
        "accommodation", "guest", "check-in", "check-out", "suite",
        "hostel", "reception", "breakfast", "dining", "campus",
        "rental", "short-term", "airbnb", "flat", "bnb",
        # IT
        "prenotazione", "alloggi", "ospiti", "ristorante", "camere",
        "soggiorno", "colazione", "pranzo", "cena", "albergo",
        "struttura ricettiva", "pensione", "affitto breve",
    ],

    "publisher_media": [
        # EN
        "article", "news", "blog", "magazine", "publish",
        "author", "editorial", "newsletter", "podcast", "journal",
        "read", "content", "press", "column", "opinion",
        "interview", "report",
        # IT
        "giornale", "notizia", "articolo", "media", "redazione",
        "rivista", "editore", "pubblicazione",
    ],

    "professional_services": [
        # EN
        "agency", "consulting", "services", "strategy", "management",
        "financial", "advisor", "outsource", "consultant", "audit",
        "accounting", "tax", "advisory", "planning", "coaching",
        "freelance", "firm",
        # IT
        "agenzia", "consulenza", "servizi", "professionisti", "commercialista",
        "studio professionale", "gestione", "strategia",
    ],

    "marketplace_aggregator": [
        "annunci", "directory", "cerca", "confronta", "offerte", "listing",
        "portale", "marketplace", "trova", "comparatore",
    ],

    "education_institution": [
        "corsi", "università", "laurea", "studenti", "academy", "master",
        "formazione", "docenti", "campus", "lezioni", "iscrizione",
    ],

    "specialty_goods_supplier": [
        # EN
        "pellets", "biomass", "heating fuel", "industrial supply", "specialty chemicals",
        "agricultural inputs", "technical goods", "bulk", "wholesale", "distributor",
        "raw materials", "components", "packaging", "certification", "iso", "standard",
        "specifications", "datasheet", "msds", "bulk pricing", "freight",
        # IT
        "pellet", "biomasse", "combustibile", "fornitura industriale", "prodotti tecnici",
        "sfuso", "sacchi", "kg", "litri", "formato", "certificato", "scheda tecnica",
        "qualità", "consegna", "distributore", "confezionamento", "ingrosso", "compatibilità",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Negative Keywords Matrix — Subtracts points to prevent misclassification
# ─────────────────────────────────────────────────────────────────────────────

NEGATIVE_KEYWORDS: dict[str, list[str]] = {
    "local_healthcare_ymyl": [
        "bio", "biologico", "supermercato", "alimentare", "alimentari", "spesa", "grocery", 
        "carrello", "checkout", "spedizione", "ecommerce", "negozio online", "naturasi", "naturale"
    ],
    "ecommerce_retail": [
        "avvocato", "studio legale", "pazienti", "ospedale", "clinica", "terapia"
    ]
}
# ─────────────────────────────────────────────────────────────────────────────
# Evidence Blob Builder — collects all available text signals
# ─────────────────────────────────────────────────────────────────────────────

def _build_evidence_blob(state_context: dict) -> str:
    """
    Aggregate every available text signal into a single lowercase blob
    for keyword frequency analysis.
    """
    parts: list[str] = []

    # Core orchestrator outputs
    parts.append(state_context.get("page_title", ""))
    parts.append(state_context.get("brand_name", ""))
    parts.append(state_context.get("target_industry", ""))

    # Entity decomposition fields (from refactored orchestrator)
    parts.append(state_context.get("primary_industry", ""))
    secondary = state_context.get("secondary_revenue_streams", [])
    if isinstance(secondary, list):
        parts.extend(secondary)

    # Scraped site content (truncated to 8 KB for speed)
    parts.append(state_context.get("client_content_clean", "")[:8000])

    # OG tags (often contain industry signals)
    og = state_context.get("og_tags", {})
    if isinstance(og, dict):
        parts.append(og.get("og:description", ""))
        parts.append(og.get("og:title", ""))

    # Structured data from prospector
    raw_data = state_context.get("raw_data_complete", {})
    if isinstance(raw_data, dict):
        parts.extend(raw_data.get("faq_patterns", []))
        parts.extend(raw_data.get("topic_gaps", []))

    return " ".join(str(p) for p in parts if p).lower()


# ─────────────────────────────────────────────────────────────────────────────
# Main Selection Function
# ─────────────────────────────────────────────────────────────────────────────

def select_business_profile(
    business_type: str = "",
    target_industry: str = "",
    scale_level: str = "",
    schema_type_counts: dict = None,
    discovered_location: str = "",
    extra_context: dict = None,
) -> tuple[str, dict]:
    """
    Deterministically select a canonical business profile using keyword
    frequency scoring.

    Returns:
        (profile_key, metadata_dict) where metadata contains reliability
        and evidence traces.
    """
    schema_type_counts = schema_type_counts or {}

    # ── 1. BUILD EVIDENCE BLOB ──
    ctx = extra_context.copy() if extra_context else {}
    ctx.update({
        "target_industry": target_industry,
        "scale_level": scale_level,
        "discovered_location": discovered_location,
    })
    evidence_blob = _build_evidence_blob(ctx)

    # ── 2. VECTORIZED KEYWORD SCORING ──
    scores: dict[str, int] = {}
    score_details: dict[str, list[str]] = {}

    for profile_key, keywords in PROFILE_KEYWORDS.items():
        profile_score = 0
        matched_terms: list[str] = []

        for kw in keywords:
            count = evidence_blob.count(kw.lower())
            if count > 0:
                profile_score += count
                matched_terms.append(f"{kw}({count})")

        # Apply negative weighting
        if profile_key in NEGATIVE_KEYWORDS:
            for n_kw in NEGATIVE_KEYWORDS[profile_key]:
                count = evidence_blob.count(n_kw.lower())
                if count > 0:
                    penalty = count * 3  # Apply strong penalty
                    profile_score -= penalty
                    matched_terms.append(f"NEG:{n_kw}(-{penalty})")

        scores[profile_key] = profile_score
        score_details[profile_key] = matched_terms

    # ── 3. SCHEMA TYPE BONUS (structural signal boost) ──
    _SCHEMA_BONUSES = {
        "SoftwareApplication": ("b2b_saas_tech", 3),
        "Product":             ("ecommerce_retail", 3),
        "Offer":               ("ecommerce_retail", 2),
        "AggregateOffer":      ("ecommerce_retail", 2),
        "Hotel":               ("hospitality_travel", 4),
        "LodgingBusiness":     ("hospitality_travel", 4),
        "Restaurant":          ("hospitality_travel", 3),
        "Menu":                ("hospitality_travel", 2),
        "LocalBusiness":       ("local_healthcare_ymyl", 1), # Splitting local weight
        "MedicalBusiness":     ("local_healthcare_ymyl", 4),
        "Dentist":             ("local_healthcare_ymyl", 4),
        "Physician":           ("local_healthcare_ymyl", 4),
        "LegalService":        ("local_legal_ymyl", 4),
        "Attorney":            ("local_legal_ymyl", 4),
        "Article":             ("publisher_media", 2),
        "NewsArticle":         ("publisher_media", 3),
        "BlogPosting":         ("publisher_media", 2),
        "ProfessionalService": ("professional_services", 3),
        # Distributed bonuses for specialty suppliers
        "LocalBusiness":       ("specialty_goods_supplier", 1),
        "Organization":        ("specialty_goods_supplier", 1),
    }

    for schema_type, count in schema_type_counts.items():
        if count > 0 and schema_type in _SCHEMA_BONUSES:
            target_profile, bonus_pts = _SCHEMA_BONUSES[schema_type]
            scores[target_profile] = scores.get(target_profile, 0) + bonus_pts
            score_details.setdefault(target_profile, []).append(f"schema:{schema_type}(+{bonus_pts})")

    # ── 4. DETERMINE WINNER ──
    winner = "professional_services"  # safe default on tie / zero
    highest_score = 0

    for profile_key, score in scores.items():
        if score > highest_score:
            highest_score = score
            winner = profile_key

    # ── 5. BUILD METADATA ──
    if highest_score == 0:
        evidence = ["Score 0 across all profiles: fallback to professional_services."]
        reliability = "low"
    else:
        top_terms = score_details.get(winner, [])[:10]
        evidence = [
            f"Keyword matrix matched {highest_score} total hits for '{winner}'.",
            f"Top terms: {', '.join(top_terms)}" if top_terms else "",
        ]
        # Runner-up gap for confidence
        sorted_scores = sorted(scores.values(), reverse=True)
        gap = sorted_scores[0] - sorted_scores[1] if len(sorted_scores) > 1 else sorted_scores[0]
        if highest_score > 8 and gap > 3:
            reliability = "high"
        elif highest_score > 3:
            reliability = "medium"
        else:
            reliability = "low"

        evidence.append(f"Score gap to runner-up: {gap} pts (confidence: {reliability}).")

    console.print(f"   [dim]Keyword Scores: { {k: v for k, v in sorted(scores.items(), key=lambda x: -x[1])} }[/dim]")

    return winner, {"reliability": reliability, "evidence": [e for e in evidence if e]}
