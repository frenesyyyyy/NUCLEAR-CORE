"""
Model Analytics Node — GEO Optimizer Pipeline.

Aggregates stress test results, earned media, and source taxonomy into 
a cross-model analytics summary. Focuses on Perplexity visibility
while removing placeholder engines by default.
"""

from typing import Any
from rich.console import Console
from nodes.source_matrix import get_profile_scoring_weights
from nodes.business_profiles import DEFAULT_PROFILE_KEY, normalize_profile_key, get_platform_like_profiles

console = Console()

def _calculate_share_of_model(
    stress_test_log: list[dict], 
    tier_stats: dict = None, 
    show_placeholders: bool = False,
    tier_reliability: dict = None,
    authority_composite: float = 0.0
) -> dict[str, Any]:
    """
    Calculate discovery visibility with partial-failure resilience.
    Distinguishes observed matches from inferred support.
    """
    # 1. Observed Visibility (Direct discovery evidence)
    total_pts = sum(q.get("max_pts", 0) for q in stress_test_log)
    earned_pts = sum(q.get("points", 0) for q in stress_test_log)
    observed_visibility = round((earned_pts / total_pts * 100) if total_pts > 0 else 0.0, 2)
    
    # 2. De-collapse Logic: Partial Data Support
    any_degraded = any(tr.get("generation_degraded", False) for tr in (tier_reliability or {}).values())
    partial_data = any_degraded or (total_pts == 0 and stress_test_log)
    
    # If discovery is zero but authority is strong AND construction was degraded,
    # we provide a 'support score' to prevent total report collapse.
    support_score = 0.0
    recalc_reason = None
    
    if observed_visibility == 0 and any_degraded:
        # Inferred support based on off-site signals when discovery construction failed
        support_score = round(authority_composite * 0.15, 2) # Max 15% inferred
        recalc_reason = "Inferred from partial tier yield (Construction Degraded)"
    elif any_degraded:
        recalc_reason = "Tiers partially degraded; visibility carries lower confidence."

    final_visibility = max(observed_visibility, support_score)
    
    results = {
        "Live AI Search": final_visibility,
        "observed_visibility_score": observed_visibility,
        "partial_data_support_score": support_score,
        "final_visibility_score": final_visibility,
        "partial_data": partial_data,
        "recalc_reason": recalc_reason,
        "tier_metrics": tier_stats or {}
    }
    
    if show_placeholders:
        results.update({
            "GPT-4o (Search)": 0.0,
            "Claude-3.5 (Search)": 0.0,
            "Gemini 1.5 (Search)": 0.0
        })
        
    return results

def _calculate_citation_share(taxonomy: dict[str, int]) -> dict[str, float]:
    """
    Convert raw taxonomy counts into percentage share.
    """
    counts = [
        taxonomy.get("owned_count", 0),
        taxonomy.get("earned_count", 0),
        taxonomy.get("forum_count", 0),
        taxonomy.get("review_count", 0),
        taxonomy.get("directory_count", 0),
        taxonomy.get("unknown_count", 0)
    ]
    total = sum(counts)
    if total == 0:
        return {}

    return {
        "owned": round((taxonomy.get("owned_count", 0) / total * 100), 2),
        "earned": round((taxonomy.get("earned_count", 0) / total * 100), 2),
        "forum": round((taxonomy.get("forum_count", 0) / total * 100), 2),
        "review": round((taxonomy.get("review_count", 0) / total * 100), 2),
        "directory": round((taxonomy.get("directory_count", 0) / total * 100), 2),
    }

def _estimate_position_adjusted_metrics(stress_test_log: list[dict]) -> int:
    """
    Estimate 'real-estate' in AI answers using tiered multipliers.
    """
    estimates = {
        "blind_discovery": 150,
        "contextual_discovery": 100,
        "branded_validation": 50
    }
    
    total_estimated_words = 0
    for q in stress_test_log:
        if q.get("matched"):
            tier = q.get("tier", "branded_validation")
            total_estimated_words += estimates.get(tier, 50)
            
    return total_estimated_words


def _apply_provenance_weighting(stress_test_log: list[dict]) -> dict:
    """
    Weight discovery outcomes by query provenance quality.
    model queries weight 1.0, entity_fallback 0.7, profile_fallback 0.4.
    Returns dict with weighted_earned, weighted_total, weighted_visibility_pct.
    """
    PROVENANCE_WEIGHTS = {
        "model": 1.0,
        "entity_fallback": 0.7,
        "profile_fallback": 0.4,
    }
    
    weighted_earned = 0.0
    weighted_total = 0.0
    
    for q in stress_test_log:
        source = q.get("source", "model")
        w = PROVENANCE_WEIGHTS.get(source, 1.0)
        max_pts = q.get("max_pts", 0)
        earned_pts = q.get("points", 0)
        
        weighted_total += max_pts * w
        weighted_earned += earned_pts * w
    
    weighted_pct = round((weighted_earned / weighted_total * 100) if weighted_total > 0 else 0.0, 2)
    
    return {
        "weighted_earned": round(weighted_earned, 2),
        "weighted_total": round(weighted_total, 2),
        "weighted_visibility_pct": weighted_pct,
    }

def _generate_engine_risks(
    perplexity_share: float,
    tier_stats: dict,
    reputation_risk: int,
    profile_key: str
) -> dict[str, list[str]]:
    """
    Generate engine-specific risks based on visibility, tiered performance, and reputation.
    """
    risks = {"Live AI Search": []}

    if perplexity_share < 30:
        risks["Live AI Search"].append("Critical Visibility Gap: Brand is largely invisible to discovery queries.")
    
    # Tier-specific check
    blind_stats = tier_stats.get("blind_discovery", {})
    if blind_stats.get("max", 0) > 0:
        blind_hit_rate = (blind_stats.get("matches", 0) / blind_stats.get("queries", 1)) * 100
        if blind_hit_rate < 10:
            risks["Live AI Search"].append("Blind Discovery Failure: Brand fails to appear in generic category searches.")

    if reputation_risk > 40:
        risks["Live AI Search"].append("Reputation Poisoning: Negative off-site signals may suppress brand references.")

    return risks


def _normalize_log_entries(log: list[dict]) -> tuple[list[dict], int]:
    """
    Defensive normalization for analytics stability.
    Ensures every entry has points, max_pts, and tier.
    Returns (normalized_log, repair_count).
    """
    normalized = []
    repair_count = 0
    for entry in log:
        if not isinstance(entry, dict):
            repair_count += 1
            continue
            
        is_repaired = False
        tier = entry.get("tier", "unknown")
        pts = entry.get("points")
        max_pts = entry.get("max_pts")
        
        if pts is None:
            pts = (entry.get("max_pts") if entry.get("matched") else 0) or 0
            is_repaired = True
            
        if max_pts is None:
            # Fallback based on tier
            max_pts = 25 if tier == "blind_discovery" else (15 if tier == "contextual_discovery" else 20)
            is_repaired = True
            
        if tier == "unknown":
            is_repaired = True
            
        norm = entry.copy()
        norm["points"] = pts
        norm["max_pts"] = max_pts
        norm["tier"] = tier
        norm["source"] = entry.get("source", "unknown")
        
        if is_repaired: repair_count += 1
        normalized.append(norm)
        
    return normalized, repair_count

def process(state: dict) -> dict:
    """
    Generate model-aware GEO analytics summary.
    """
    console.print("[bold blue]Node: Model Analytics[/bold blue] | Synthesizing tiered visibility intelligence...")

    # Config flag for placeholders
    show_placeholder_engines = state.get("show_placeholder_engines", False)

    stress_test_log = state.get("stress_test_log", [])
    tier_stats      = state.get("stress_test_tier_stats", {})
    
    # -- DEFENSIVE SCHEMA NORMALIZATION --
    stress_test_log, repair_count = _normalize_log_entries(stress_test_log)
    if repair_count > 0:
        console.print(f"      [yellow]SCHEMA_REPAIR[/yellow] Successfully normalized {repair_count} inconsistent log entries.")
    
    earned_media    = state.get("earned_media", {})
    source_taxonomy = state.get("source_taxonomy", {})
    profile_key     = state.get("business_profile_key", DEFAULT_PROFILE_KEY)
    profile_key     = normalize_profile_key(profile_key)
    rep_risk        = earned_media.get("reputation_risk_score", 0)

    # Authority Composite (Conservative) - Calculate early for visibility resilience
    auth_match = state.get("authority_match_score", 0)
    brand_strength = earned_media.get("profile_aware_strength", earned_media.get("strength_score", 0))
    tier_reliability = state.get("tier_query_reliability", {})
    
    PLATFORM_LIKE_PROFILES = get_platform_like_profiles()
    inferred_families = state.get("earned_media", {}).get("first_party_inferred_families", [])
    if profile_key in PLATFORM_LIKE_PROFILES and inferred_families:
        rescue_pts = 0
        for f in inferred_families:
            conf = f.get("confidence", "low")
            if conf == "high": rescue_pts += 12
            elif conf == "medium": rescue_pts += 8
        brand_strength = min(100, brand_strength + min(24, rescue_pts))

    # 60% weight to off-site strength (trust), 40% to on-site authority matching (relevance)
    authority_composite = (auth_match * 0.4) + (brand_strength * 0.6)

    # 1. Tiered Share of Model (Raw Visibility) with Resilience
    share_of_model = _calculate_share_of_model(
        stress_test_log, tier_stats, show_placeholder_engines, 
        tier_reliability=tier_reliability, authority_composite=authority_composite
    )
    
    perp_share = share_of_model.get("Live AI Search", 0.0)
    raw_visibility_score = perp_share if isinstance(perp_share, (int, float)) else 0.0
    
    # 1b. Provenance-Weighted Visibility
    provenance_metrics = _apply_provenance_weighting(stress_test_log)
    provenance_weighted_visibility = provenance_metrics["weighted_visibility_pct"]

    # 1c. Degradation-Aware Confidence Reduction
    any_degraded = share_of_model.get("partial_data", False)
    degradation_factor = 0.8 if any_degraded else 1.0
    
    # Apply degradation factor to raw visibility
    raw_visibility_score = raw_visibility_score * degradation_factor
    
    # Compute aggregate query construction confidence
    reliability_values = [
        tr.get("query_construction_reliability", 1.0)
        for tr in (tier_reliability or {}).values()
    ]
    query_construction_confidence = round(
        sum(reliability_values) / max(len(reliability_values), 1) * 100, 1
    )

    # 3. Authority-Adjusted Visibility
    # Uses the prescribed scaling to prevent zero-crush where discovery works but authority is developing
    if profile_key in PLATFORM_LIKE_PROFILES:
        authority_adjusted_visibility_score = raw_visibility_score * (0.7 + 0.3 * (authority_composite / 100))
    else:
        authority_adjusted_visibility_score = raw_visibility_score * (0.5 + 0.5 * (authority_composite / 100))
        
    # 4. Global Recalibrated "GEO Target Score" utilizing Profile Weights
    weights = get_profile_scoring_weights(profile_key)
    ev_depth = state.get("metrics", {}).get("Defensible Evidence Depth", 0)
    conf = state.get("confidence_score", 0)
    
    geo_score = (
        (authority_adjusted_visibility_score * weights.get("visibility", 0)) +
        (ev_depth * weights.get("evidence_depth", 0)) +
        (authority_composite * weights.get("authority", 0)) +
        (conf * weights.get("confidence", 0))
    )

    # -- Dynamic EEAT Penalty Logic --
    from nodes.business_profiles import BUSINESS_INTELLIGENCE_PROFILES
    active_profile = BUSINESS_INTELLIGENCE_PROFILES.get(profile_key, BUSINESS_INTELLIGENCE_PROFILES.get(DEFAULT_PROFILE_KEY))
    profile_weights = active_profile.get("scoring_weights", {})
    eeat_weight = float(profile_weights.get("eeat_trust", 0.0))
    
    # Check for missing Author Bios (EEAT)
    schema_counts = state.get("schema_type_counts", {})
    has_person_schema = any("person" in str(k).lower() for k in schema_counts)
    raw_text = str(state.get("client_content_clean", "")).lower() + str(state.get("raw_data_complete", {})).lower()
    has_author_text = any(w in raw_text for w in ["author", "bio", "doctor", "practitioner", "team", "about us", "chi siamo"])
    
    if not (has_person_schema or has_author_text):
        eeat_penalty = 20.0 * eeat_weight
        geo_score = max(0.0, geo_score - eeat_penalty)

    # 5. Engine Breakdown (Live Only by default)
    engine_breakdown = [
        {
            "engine": "Live AI Search",
            "status": "Live",
            "visibility_score": authority_adjusted_visibility_score,  # Adjusted used for downstream perception
            "raw_visibility_score": raw_visibility_score,
            "confidence": "High (Direct Evidence)" if not any_degraded else "Medium (Partial Yield)",
            "recalc_reason": share_of_model.get("recalc_reason")
        }
    ]
    
    if show_placeholder_engines:
        placeholders = [
            {"engine": "GPT-4o (Search)", "status": "Placeholder", "visibility_score": 0.0, "confidence": "Estimative"},
            {"engine": "Claude-3.5 (Search)", "status": "Placeholder", "visibility_score": 0.0, "confidence": "Estimative"},
            {"engine": "Gemini 1.5 (Search)", "status": "Placeholder", "visibility_score": 0.0, "confidence": "Estimative"}
        ]
        engine_breakdown.extend(placeholders)

    # 6. Position Adjusted metrics
    estimated_word_count = _estimate_position_adjusted_metrics(stress_test_log)

    # 7. Citation Share
    citation_share = _calculate_citation_share(source_taxonomy)

    # 8. Risks
    engine_risks = _generate_engine_risks(authority_adjusted_visibility_score, tier_stats, rep_risk, profile_key)

    # 9. Notes
    notes = f"Profile-aware recalibration applied '{profile_key}'. Raw visibility {raw_visibility_score}% adjusted to {authority_adjusted_visibility_score:.1f}% based on auth composite {authority_composite:.1f}. "
    notes += f"Score dynamically adjusted using the [{profile_key}] weighting matrix to prevent structural bias. "
    
    recalc_reason = share_of_model.get("recalc_reason")
    if recalc_reason:
        notes += f"RECALC NOTE: {recalc_reason} "
    
    if any_degraded:
        notes += "DEGRADATION NOTE: Query construction was degraded for one or more tiers — visibility estimates carry reduced confidence. "
    if not show_placeholder_engines:
        notes += "Only Live AI Search evidence is currently integrated."

    # Reliability note for downstream nodes
    query_reliability_note = None
    if any_degraded:
        degraded_tiers = [
            k for k, v in tier_reliability.items()
            if v.get("generation_degraded", False)
        ]
        query_reliability_note = (
            f"Discovery estimates are based on reduced-confidence query construction "
            f"due to limited valid query yield in tier(s): {', '.join(degraded_tiers)}."
        )
    state["query_construction_reliability_note"] = query_reliability_note

    model_analytics = {
        "share_of_model": share_of_model,
        "tier_metrics": tier_stats,
        "engine_breakdown": engine_breakdown,
        "position_adjusted_word_count": estimated_word_count,
        "citation_share": citation_share,
        "engine_specific_risks": engine_risks,
        "stress_test_diagnostics": state.get("stress_test_diagnostics", {}),
        "raw_visibility_score": raw_visibility_score,
        "observed_visibility_score": share_of_model.get("observed_visibility_score", 0.0),
        "partial_data_support_score": share_of_model.get("partial_data_support_score", 0.0),
        "final_visibility_score": share_of_model.get("final_visibility_score", 0.0),
        "partial_data": any_degraded,
        "recalc_reason": share_of_model.get("recalc_reason"),
        "authority_adjusted_visibility_score": round(authority_adjusted_visibility_score, 2),
        "authority_composite": round(authority_composite, 2),
        "profile_weight_pack_used": profile_key,
        "geo_optimization_score": round(geo_score, 2),
        "provenance_weighted_visibility": provenance_metrics,
        "query_construction_confidence": query_construction_confidence,
        "tier_query_reliability": tier_reliability,
        "notes": notes,
        "schema_integrity": "repaired" if repair_count > 0 else "clean"
    }
    if repair_count > 0:
        model_analytics["schema_reliability_note"] = f"Analytics corrected {repair_count} malformed schema entries."
        
    state["model_analytics"] = model_analytics

    console.print(f"   [green]Model Analytics Complete[/green] | Adjusted Vis: {authority_adjusted_visibility_score:.1f}% | Auth Composite: {authority_composite:.1f}")
    return state
