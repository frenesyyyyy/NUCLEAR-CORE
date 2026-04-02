"""Node for deterministic, scalable business profile selection using structural signals."""
import re

def _norm(text: str) -> str:
    """Lowercase, strip, and normalize whitespace of a string."""
    if not text: return ""
    return re.sub(r'\s+', ' ', str(text)).strip().lower()

def _has_multi_provider_inventory_signals(text: str) -> bool:
    """Detect keywords indicating a marketplace inventory (listings, browsing many vendors)."""
    patterns = [
        r"\bbrowse\b", r"\bdiscover\b", r"\bcompare\b", r"\blistings\b", 
        r"\bavailable in\b", r"\bchoose from\b", r"\bsearch for\b",
        r"\bnearby options\b", r"\bfind a\b", r"\btop rated\b",
        r"ristoranti a\b", r"consegniamo a\b", r"offerte a\b",
        r"view all categories", r"see all restaurants", r"browse by cuisine",
        r"consegna a domicilio", r"delivery in\b"
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)

def _has_partner_onboarding_signals(text: str) -> bool:
    """Detect keywords indicating partner/merchant onboarding (supply side)."""
    patterns = [
        r"become a partner", r"list your (business|property|restaurant|hotel|service|shop)",
        r"join (our|the) network", r"for (restaurants|hotels|merchants|partners|vendors)",
        r"partner with us", r"grow your business with", r"merchant (operations|onboarding|portal|hub)",
        r"onboard your", r"registra la tua", r"diventa partner", r"iscrivere il mio ristorante",
        r"lavora con noi", r"aggiungi la tua attività", r"business portal"
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)

def _has_platform_mediation_signals(text: str) -> bool:
    """Detect keywords indicating platform mediation between supply and demand."""
    patterns = [
        r"order (through|on|via)", r"book (through|on|via)", r"platform", 
        r"marketplace", r"two-sided", r"aggregator", r"delivery service",
        r"mediation", r"booking engine", r"powered by", r"piattaforma",
        r"commissioni per ordine", r"service charges apply", r"network availability",
        r"fees", r"terms of service"
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)

def _has_coverage_network_signals(text: str) -> bool:
    """Detect signals of a network/coverage area rather than single location."""
    patterns = [
        r"cities we serve", r"coverage area", r"serviceable zones",
        r"available in", r"le nostre città", r"zone coperte",
        r"national presence", r"network availability"
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)

def _has_local_venue_signals(text: str, schema_counts: dict) -> bool:
    """Detect signals of a single physical venue or office."""
    local_schemas = {"restaurant", "localbusiness", "dentist", "medicalbusiness", "store", "lodging"}
    if any(s.lower() in local_schemas for s in schema_counts):
        # We check schema but don't strictly return True to allow marketplace override
        pass
    
    singular_patterns = [
        r"visit us at\b", r"our location\b", r"la nostra sede\b",
        r"book (a|your) table\b", r"prenota un tavolo\b", 
        r"make an appointment\b", r"chiama per prenotare\b",
        r"our (staff|doctors|team|chef|menu)\b",
        r"vienici a trovare\b"
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in singular_patterns)

def _build_evidence_blob(state_context: dict) -> str:
    """Collect all available text signals for classification."""
    parts = []
    parts.append(state_context.get("page_title", ""))
    parts.append(state_context.get("brand_name", ""))
    parts.append(state_context.get("target_industry", ""))
    parts.append(state_context.get("business_type", ""))
    parts.append(state_context.get("client_content_clean", "")[:8000]) # truncated
    
    # Add FAQ and Gaps if available
    raw_data = state_context.get("raw_data_complete", {})
    if isinstance(raw_data, dict):
        parts.extend(raw_data.get("faq_patterns", []))
        parts.extend(raw_data.get("topic_gaps", []))
    
    return " ".join([str(p) for p in parts if p]).lower()

def select_business_profile(
    business_type: str, 
    target_industry: str, 
    scale_level: str, 
    schema_type_counts: dict, 
    discovered_location: str,
    extra_context: dict = None,
    ignore_legal: bool = False
) -> tuple[str, dict]:
    """
    Deterministically select a business profile using structural and semantic signals.
    v4.5 Hotfix: Prioritizes Marketplace over Venue; strengthened structural checks.
    """
    # ── 1. PREPARE CONTEXT ──
    ctx_state = extra_context or {}
    # Ensure mandatory fields are present in ctx_state for _build_evidence_blob
    ctx_state.update({
        "business_type": business_type,
        "target_industry": target_industry,
        "scale_level": scale_level,
        "discovered_location": discovered_location
    })
    
    context = _build_evidence_blob(ctx_state)
    schema_counts = {k.lower(): v for k, v in (schema_type_counts or {}).items()}
    
    norm_business = _norm(business_type)
    is_local_scaling = _norm(scale_level) == "local"
    
    evidence = []
    reliability = "low"
    
    # ── 1.b STRONG OVERRIDES (Lawyers) ──
    if not ignore_legal:
        if any(kw in context for kw in ["studio legale", "avvocato", "legal firm", "lawyer"]):
            evidence.append("Signal: Strong legal keywords override generic platform/freelance hints.")
            return "local_law_firm", {"reliability": "high", "evidence": evidence}

    if any(kw in context for kw in ["dentist", "dentista", "dentale", "dental clinic"]):
        evidence.append("Signal: Dental clinic keywords override generic markers.")
        return "local_dentist", {"reliability": "high", "evidence": evidence}
    
    # ── 2. MARKETPLACE / PLATFORM DETECTION (Structural Priority) ──
    mkt_score = 0
    if _has_platform_mediation_signals(context):
        mkt_score += 1
        evidence.append("Signal: Platform/Mediation language ('marketplace', 'delivery service')")
    if _has_partner_onboarding_signals(context):
        mkt_score += 1
        evidence.append("Signal: Partner onboarding ('diventa partner', 'merchant hub')")
    if _has_multi_provider_inventory_signals(context):
        mkt_score += 1
        evidence.append("Signal: Multi-provider inventory ('browse restaurants', 'listings')")
    if _has_coverage_network_signals(context):
        mkt_score += 1
        evidence.append("Signal: Network coverage ('cities we serve', 'available in')")
    
    # Direct model/brand hints
    if any(k in context for k in ["marketplace", "aggregator", "ordering platform", "piattaforma", "just eat", "deliveroo", "glovo"]):
        mkt_score += 1

    # Logic: Marketplaces are aggregators of multiple providers.
    # If we see 2+ structural signals, or 1 signal + platform context, classify as marketplace.
    is_platform_profile = (mkt_score >= 2) or (mkt_score >= 1 and any(k in context for k in ["aggregator", "platform", "platformu"]))
    
    if is_platform_profile:
        reliability = "high" if mkt_score >= 3 else "medium"
        return "marketplace", {"reliability": reliability, "evidence": evidence}

    # ── 3. PREVIOUS HIGH PRIORITY LOCAL ENTITIES (Moved up) ──
    # Logic extracted to 1.b to prevent marketplace leakage for lawyers/dentists.

    # ── 4. VENUE-SPECIFIC CLASSIFICATION (Restaurant/Hospitality) ──
    # Check for local venue signals (singular focus)
    has_venue = _has_local_venue_signals(context, schema_counts)
    
    # Hotfix: require strong SINGULAR venue signals to avoid "food" -> restaurant trap
    if has_venue and any(k in context for k in ["restaurant", "ristorante", "trattoria", "hotel"]):
        evidence.append("Signal: Local venue identifiers (reservation, address, singular menu)")
        return "restaurant_hospitality", {"reliability": "high", "evidence": evidence}

    # ── 5. INDUSTRY-SPECIFIC MAPPING ──
    # B2B SaaS
    if any(kw in context for kw in ["saas", "software", "product", "automation", "cloud"]) and not is_local_scaling:
        evidence.append("Signal: B2B software indicators")
        return "b2b_saas", {"reliability": "medium", "evidence": evidence}

    # Ecommerce
    if any(kw in context for kw in ["ecommerce", "shop", "store", "product", "cart"]):
        evidence.append("Signal: Retail/Ecommerce indicators")
        return "ecommerce_brand", {"reliability": "medium", "evidence": evidence}

    # Agency
    if any(kw in context for kw in ["agency", "consulting", "services", "agenzia"]):
        evidence.append("Signal: Service agency indicators")
        return "agency_marketing", {"reliability": "medium", "evidence": evidence}

    # ── 6. CONSERVATIVE FALLBACKS ──
    # If the business type was 'food' but we failed to find strong platform or venue signals
    if "food" in context:
        evidence.append("Signal: General food-related context (Conservative marketplace fallback)")
        # If it's food and we are here, it's likely a profile that didn't provide address/reservation
        # but is categorized as food. Aggregator is safer than freelancer.
        return "marketplace", {"reliability": "low", "evidence": evidence}

    if is_local_scaling or bool(discovered_location) and str(discovered_location).lower() not in ["worldwide", "national"]:
        # Only fallback to freelancer if we have a specific location and no other flags
        evidence.append("Fallback: Individual consultant (local scale detected)")
        return "freelancer_consultant", {"reliability": "low", "evidence": evidence}

    evidence.append("Default fallback: B2B SaaS")
    return "b2b_saas", {"reliability": "low", "evidence": evidence}
