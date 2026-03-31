"""
Model Analytics Node — GEO Optimizer Pipeline.

Aggregates stress test results, earned media, and source taxonomy into 
a cross-model analytics summary. Focuses on Perplexity visibility
while removing placeholder engines by default.
"""

from typing import Any
from rich.console import Console

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
        "Perplexity": perplexity_share,
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
    risks = {"Perplexity": []}

    if perplexity_share < 30:
        risks["Perplexity"].append("Critical Visibility Gap: Brand is largely invisible to discovery queries.")
    
    # Tier-specific check
    blind_stats = tier_stats.get("blind_discovery", {})
    if blind_stats.get("max", 0) > 0:
        blind_hit_rate = (blind_stats.get("matches", 0) / blind_stats.get("queries", 1)) * 100
        if blind_hit_rate < 10:
            risks["Perplexity"].append("Blind Discovery Failure: Brand fails to appear in generic category searches.")

    if reputation_risk > 40:
        risks["Perplexity"].append("Reputation Poisoning: Negative off-site signals may suppress brand references.")

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

    # 1. Tiered Share of Model
    share_of_model = _calculate_share_of_model(stress_test_log, tier_stats, show_placeholder_engines)
    perp_share = share_of_model.get("Perplexity", 0.0)

    # 2. Engine Breakdown (Live Only by default)
    engine_breakdown = [
        {
            "engine": "Perplexity",
            "status": "Live",
            "visibility_score": perp_share,
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

    # 3. Position Adjusted metrics
    estimated_word_count = _estimate_position_adjusted_metrics(stress_test_log)

    # 4. Citation Share
    citation_share = _calculate_citation_share(source_taxonomy)

    # 5. Risks
    engine_risks = _generate_engine_risks(perp_share, tier_stats, rep_risk, profile_key)

    # 6. Notes
    notes = f"Perplexity visibility established at {perp_share}%. "
    if not show_placeholder_engines:
        notes += "Only Perplexity live-search evidence is currently integrated in this environment."
    else:
        notes += "Other engines are in placeholder mode."

    state["model_analytics"] = {
        "share_of_model": share_of_model,
        "tier_metrics": tier_stats,
        "engine_breakdown": engine_breakdown,
        "position_adjusted_word_count": estimated_word_count,
        "citation_share": citation_share,
        "engine_specific_risks": engine_risks,
        "stress_test_diagnostics": state.get("stress_test_diagnostics", {}),
        "notes": notes
    }

    console.print(f"   [green]Model Analytics Complete[/green] | Perplexity Share: {perp_share}%")
    return state
