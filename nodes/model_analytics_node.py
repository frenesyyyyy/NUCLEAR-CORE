"""
Model Analytics Node — GEO Optimizer Pipeline.

Aggregates stress test results, earned media, and source taxonomy into 
a cross-model analytics summary. Focuses on Perplexity visibility
while removing placeholder engines by default.
"""

from typing import Any
from rich.console import Console
from nodes.source_matrix import get_profile_scoring_weights

console = Console()

def _calculate_share_of_model(stress_test_log: list[dict], tier_stats: dict = None, show_placeholders: bool = False) -> dict[str, Any]:
    """
    Calculate the share of visibility percentage per model and include tiered metrics.
    """
    total_pts = sum(q.get("max_pts", 0) for q in stress_test_log)
    earned_pts = sum(q.get("points", 0) for q in stress_test_log)
    
    if total_pts == 0 and not stress_test_log:
        perplexity_share = "N/A (Extraction Failed)"
    else:
        perplexity_share = round((earned_pts / total_pts * 100) if total_pts > 0 else 0.0, 2)
    results = {
        "Live AI Search": perplexity_share,
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

def process(state: dict) -> dict:
    """
    Generate model-aware GEO analytics summary.
    """
    console.print("[bold blue]Node: Model Analytics[/bold blue] | Synthesizing tiered visibility intelligence...")

    # Config flag for placeholders
    show_placeholder_engines = state.get("show_placeholder_engines", False)

    stress_test_log = state.get("stress_test_log", [])
    tier_stats      = state.get("stress_test_tier_stats", {})
    earned_media    = state.get("earned_media", {})
    source_taxonomy = state.get("source_taxonomy", {})
    profile_key     = state.get("business_profile_key", "b2b_saas")
    rep_risk        = earned_media.get("reputation_risk_score", 0)

    # 1. Tiered Share of Model (Raw Visibility)
    share_of_model = _calculate_share_of_model(stress_test_log, tier_stats, show_placeholder_engines)
    perp_share = share_of_model.get("Live AI Search", 0.0)
    raw_visibility_score = perp_share

    # 2. Authority Composite (Conservative)
    # Blends on-site semantic consensus with off-site authority
    auth_match = state.get("authority_match_score", 0)
    brand_strength = earned_media.get("profile_aware_strength", earned_media.get("strength_score", 0))
    # 60% weight to off-site strength (trust), 40% to on-site authority matching (relevance)
    authority_composite = (auth_match * 0.4) + (brand_strength * 0.6)

    # 3. Authority-Adjusted Visibility
    # Uses the prescribed scaling to prevent zero-crush where discovery works but authority is developing
    PLATFORM_PROFILES = {"marketplace", "consumer_saas", "ecommerce_brand"}
    if profile_key in PLATFORM_PROFILES:
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

    # 5. Engine Breakdown (Live Only by default)
    engine_breakdown = [
        {
            "engine": "Live AI Search",
            "status": "Live",
            "visibility_score": authority_adjusted_visibility_score,  # Adjusted used for downstream perception
            "raw_visibility_score": raw_visibility_score,
            "confidence": "High (Direct Evidence)"
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
    if not show_placeholder_engines:
        notes += "Only Live AI Search evidence is currently integrated."

    state["model_analytics"] = {
        "share_of_model": share_of_model,
        "tier_metrics": tier_stats,
        "engine_breakdown": engine_breakdown,
        "position_adjusted_word_count": estimated_word_count,
        "citation_share": citation_share,
        "engine_specific_risks": engine_risks,
        "stress_test_diagnostics": state.get("stress_test_diagnostics", {}),
        "raw_visibility_score": raw_visibility_score,
        "authority_adjusted_visibility_score": round(authority_adjusted_visibility_score, 2),
        "authority_composite": round(authority_composite, 2),
        "profile_weight_pack_used": profile_key,
        "geo_optimization_score": round(geo_score, 2),
        "notes": notes
    }

    console.print(f"   [green]Model Analytics Complete[/green] | Adjusted Vis: {authority_adjusted_visibility_score:.1f}% | Auth Composite: {authority_composite:.1f}")
    return state
