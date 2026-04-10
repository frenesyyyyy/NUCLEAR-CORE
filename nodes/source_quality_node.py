"""
Source Quality Node — GEO Optimizer Pipeline.

Aggregates earned media buckets into a definitive source taxonomy,
computes a profile-aware trust mix summary, and evaluates citation
source risk based on the business profile context.

v4.5 Authority Upgrade: Now uses profile-aware source family breakdown
from source_matrix.py to generate contextual gap analysis, eliminating
false universal penalties for missing irrelevant source types.

All logic is deterministic; no external API calls.
"""

from typing import Any
from rich.console import Console
from nodes.source_matrix import (
    get_source_pack,
    get_missing_relevant_sources,
    get_irrelevant_ignored,
    check_trust_anchor_presence,
    classify_url_to_family,
    get_canonical_source_urls
)
from nodes.earned_media_node import _FAMILY_TO_LEGACY_BUCKET
from nodes.business_profiles import DEFAULT_PROFILE_KEY, normalize_profile_key, get_local_trust_profiles

console = Console()


def _evaluate_trust_mix(
    earned_count: int,
    owned_count: int,
    forum_count: int,
    review_count: int,
    directory_count: int,
    total_sources: int,
    profile_key: str = DEFAULT_PROFILE_KEY,
    family_breakdown: dict = None,
    source_pack: dict = None,
) -> str:
    """
    Generate a profile-aware text summary of the brand's off-site trust mix.
    """
    if total_sources == 0:
        return "No external sources discovered. Trust mix is severely lacking."

    if owned_count == total_sources:
        return "Echo chamber detected. All discovered sources are owned properties. No independent validation."

    # Profile-aware: check relevant family coverage
    if family_breakdown and source_pack:
        relevant_families = source_pack.get("relevant_families", [])
        irrelevant = set(source_pack.get("irrelevant_families", []))
        relevant_count = sum(
            family_breakdown.get(f, 0)
            for f in relevant_families
            if f not in irrelevant
        )

        ratio = (relevant_count / total_sources) * 100 if total_sources > 0 else 0
        profile_label = source_pack.get("label", profile_key)

        if ratio >= 50:
            return f"Strong independent validation for '{profile_label}'. Majority of sources are from relevant authority families for this vertical."
        elif ratio >= 20:
            return f"Moderate independent validation for '{profile_label}'. Some relevant source family presence, but coverage gaps remain."
        else:
            return f"Weak independent validation for '{profile_label}'. Detected sources are mostly low-priority for this vertical."

    # Legacy fallback (no family data)
    high_trust = earned_count + review_count
    ratio = (high_trust / total_sources) * 100

    if ratio >= 50:
        return "Strong independent validation. Majority of sources are high-trust editorial or review platforms."
    elif ratio >= 20:
        return "Moderate independent validation. Some editorial/review presence, but relies heavily on directories or forums."
    else:
        return "Weak independent validation. Predominantly low-authority directory listings or owned properties."


def _evaluate_citation_risk(
    earned_count: int,
    review_count: int,
    forum_count: int,
    directory_count: int,
    total_sources: int,
    profile_key: str,
    missing_relevant: list = None,
) -> list[str]:
    """
    Evaluate specific citation risks based on the business profile.
    v4.5: Uses profile-aware missing source gaps instead of universal platform checks.
    """
    risks = []

    if total_sources == 0:
        risks.append("CRITICAL: Zero external footprint. AI engines have no independent context for this brand.")
        return risks

    # Profile-aware risk from missing relevant sources
    if missing_relevant:
        for gap in missing_relevant:
            risks.append(f"Source Gap: {gap}")
        return risks

    # Legacy fallback risk logic (if source_matrix data not available)
    profile_key = normalize_profile_key(profile_key)
    
    if profile_key in ("b2b_saas_tech", "ecommerce_retail", "publisher_media", "marketplace_aggregator"):
        if review_count == 0:
            risks.append("Missing validation: No review platform mentions (G2, Trustpilot, App Store). High risk of exclusion from comparison queries.")

    elif profile_key in ("education_institution", "publisher_media", "professional_services"):
        if earned_count == 0:
            risks.append("Authority gap: No editorial/press mentions. High risk for 'best in class' or thought-leadership queries.")

    elif profile_key in get_local_trust_profiles().union({"local_healthcare_ymyl", "local_legal_ymyl", "hospitality_travel"}):
        if review_count == 0 and directory_count == 0:
            risks.append("Local ghost: No local directory or review citations detected. High risk for 'near me' discovery.")
        elif review_count == 0:
            risks.append("Incomplete footprint: Discovered on directories but missing on high-trust review platforms relevant to this vertical.")

    # General risks
    if (earned_count + review_count) == 0 and total_sources > 0:
        if forum_count > 0:
            risks.append("Sentiment vulnerability: Brand relies entirely on unverified forum/community chatter for independent validation.")

    return risks


def process(state: dict) -> dict:
    """
    Create a source taxonomy and trust-mix summary.

    v4.5: Extended with profile-aware family gap analysis, trust anchor detection,
    and irrelevant-source filtering. All existing keys preserved.
    """
    console.print("[bold blue]Node: Source Quality[/bold blue] | Building profile-aware source taxonomy and trust mix...")

    earned_media: dict  = state.get("earned_media", {})
    profile: dict       = state.get("business_profile", {})
    # ── Profile key normalization (mandatory first step) ────────────────────
    profile_key = normalize_profile_key(state.get("business_profile_key", DEFAULT_PROFILE_KEY))

    # ── Source pack selection (BEFORE any gap/scoring computation) ─────────
    source_pack = get_source_pack(profile_key)
    console.print(f"   Pack selected: [cyan]{source_pack.get('label', profile_key)}[/cyan] for profile [cyan]{profile_key}[/cyan]")
    
    # ── FIXED: Parse structured URLs directly to guarantee LLM-free metrics ──
    owned_count = 0
    earned_count = 0
    forum_count = 0
    review_count = 0
    directory_count = 0
    unclassified_count = 0
    noise_count = 0

    brand_domain = state.get("url", "").replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]
    brand_name = state.get("brand_name", "")
    
    family_breakdown: dict = {f: 0 for f, _ in _FAMILY_TO_LEGACY_BUCKET.items() if f != "unknown"}
    family_breakdown["ignored_noise"] = 0
    family_breakdown["unclassified_candidate"] = 0
    
    # Process canonically aggregated structured sources
    all_source_urls = get_canonical_source_urls(state)
    print(f"[DEBUG] Source Quality aggregated {len(all_source_urls)} canonical URLs")
    
    for url in all_source_urls:
        if not url:
            continue
            
        family, _ = classify_url_to_family(url, brand_domain, brand_name)
        family_breakdown[family] = family_breakdown.get(family, 0) + 1
        
        legacy_bucket = _FAMILY_TO_LEGACY_BUCKET.get(family, "unclassified")
        if legacy_bucket == "owned": owned_count += 1
        elif legacy_bucket == "editorial": earned_count += 1
        elif legacy_bucket == "forum": forum_count += 1
        elif legacy_bucket == "review": review_count += 1
        elif legacy_bucket == "directory": directory_count += 1
        elif legacy_bucket == "noise": noise_count += 1
        else: unclassified_count += 1

    total_sources = sum([
        owned_count, earned_count, forum_count,
        review_count, directory_count, unclassified_count
    ]) # Exclude noise from total sources

    first_party_inferred: list = earned_media.get("first_party_inferred_families", [])

    # ── FIXED: First-Party App & Ecosystem Inference ──
    client_content = str(state.get("client_content_clean", state.get("client_content_raw", ""))).lower()
    og_tags = state.get("og_tags", {})
    og_text = str(og_tags).lower()
    full_text = client_content + " " + og_text

    first_party_app_detected = any(k in full_text for k in ["al:ios:app_store_id", "al:android:package", "app store", "google play"])
    first_party_partner_detected = any(k in full_text for k in ["diventa partner", "rider", "lavora con noi", "diventa nostro partner", "consegna con noi", "become a partner", "delivery partner"])

    if normalize_profile_key(profile_key) == "marketplace_aggregator":
        if first_party_app_detected:
            first_party_inferred.append({"family": "app_ecosystems", "tag": "first_party"})
            
        if first_party_partner_detected:
            first_party_inferred.append({"family": "marketplace_partner_ecosystems", "tag": "first_party"})

    # Derive set of families confirmed only by first-party inference (not external)
    inferred_family_names: set = {i["family"] for i in first_party_inferred if isinstance(i, dict)}

    # Missing relevant sources (profile-contextual gaps only)
    gap_check_breakdown = family_breakdown.copy()
    for f in inferred_family_names:
        gap_check_breakdown[f] = 1

    missing_relevant = get_missing_relevant_sources(gap_check_breakdown, source_pack)

    # Irrelevant families that were intentionally skipped
    irrelevant_ignored = get_irrelevant_ignored(source_pack)

    # Trust anchor presence check
    trust_anchors_found = check_trust_anchor_presence(all_source_urls, source_pack)

    # ── Trust mix summary (profile-aware) ─────────────────────────────────────
    trust_mix_summary = _evaluate_trust_mix(
        earned_count=earned_count,
        owned_count=owned_count,
        forum_count=forum_count,
        review_count=review_count,
        directory_count=directory_count,
        total_sources=total_sources,
        profile_key=profile_key,
        family_breakdown=family_breakdown,
        source_pack=source_pack,
    )

    citation_risks = _evaluate_citation_risk(
        earned_count=earned_count,
        review_count=review_count,
        forum_count=forum_count,
        directory_count=directory_count,
        total_sources=total_sources,
        profile_key=profile_key,
        missing_relevant=missing_relevant if missing_relevant else None,
    )



    # ── Source detection notes ─────────────────────────────────────────────────
    notes_parts = []
    if not earned_media:
        notes_parts.append("Warning: No earned_media data found in state. Taxonomy will be zeroed out.")

    if trust_anchors_found:
        notes_parts.append(f"Trust anchors detected: {', '.join(trust_anchors_found[:5])}.")
    else:
        notes_parts.append(f"No trust anchors found for '{source_pack.get('label', profile_key)}' profile.")

    if inferred_family_names:
        inferred_labels = ", ".join(sorted(inferred_family_names))
        notes_parts.append(f"First-party ecosystem evidence detected (not external citations): {inferred_labels}.")

    if citation_risks:
        notes_parts.append(f"Identified {len(citation_risks)} citation/trust risk factor(s).")
    else:
        notes_parts.append("Citation footprint appears healthy for this profile.")

    if irrelevant_ignored:
        notes_parts.append(f"Irrelevant source families intentionally excluded from gap analysis: {', '.join(irrelevant_ignored[:4])}.")

    # Determine trust_mix label for validator consumption
    trust_mix_label = "Unknown"
    if trust_mix_summary:
        tms_lower = trust_mix_summary.lower()
        if "strong" in tms_lower:
            trust_mix_label = "Strong"
        elif "moderate" in tms_lower:
            trust_mix_label = "Moderate"
        elif "weak" in tms_lower:
            trust_mix_label = "Weak"
        elif "no external" in tms_lower or "echo chamber" in tms_lower:
            trust_mix_label = "Critical"

    state["source_taxonomy"] = {
        "total_sources_detected": total_sources,
        "owned_count": owned_count,
        "earned_count": earned_count,
        "forum_count": forum_count,
        "review_count": review_count,
        "directory_count": directory_count,
        "unclassified_count": unclassified_count,
        "ignored_noise_count": noise_count,
        "trust_mix":            trust_mix_label,
        "trust_mix_summary":    trust_mix_summary,
        "citation_source_risk": citation_risks,
        "notes":                " ".join(notes_parts),
        # ── New outputs (additive) ──
        "source_pack_used":                 profile_key,
        "source_family_breakdown":          family_breakdown,
        "penalized_relevant_gaps":          missing_relevant if missing_relevant else [],
        "ignored_irrelevant_gaps":          irrelevant_ignored,
        "relevant_gap_count":               len(missing_relevant) if missing_relevant else 0,
        "trust_anchors_found":              trust_anchors_found,
        "source_detection_notes":           f"Pack: '{source_pack.get('label', profile_key)}'. Relevant families: {source_pack.get('relevant_families', [])}.",
    }

    console.print(
        f"[bold green]Source Quality Complete[/bold green] | "
        f"Total Sources: [cyan]{total_sources}[/cyan] | "
        f"Trust Mix: [yellow]{trust_mix_summary.split('.')[0]}[/yellow] | "
        f"Gaps: [{'red' if missing_relevant else 'green'}]{len(missing_relevant)}[/{'red' if missing_relevant else 'green'}]"
    )

    return state
