"""
Implementation Blueprint Node — GEO Optimizer Pipeline.

Transforms technical audit findings into implementation-ready strategic action blocks.
Organizes gaps into prioritized agency deliverables with evidence-based rationales.
"""

from typing import Any
from rich.console import Console

console = Console()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _map_evidence_status(is_provisional: bool, is_direct: bool = False) -> str:
    if is_direct: return "direct-evidence"
    return "partial-evidence" if is_provisional else "profile-inferred"

def _generate_page_actions(missing_pages: list[list[str]], is_provisional: bool) -> list[dict]:
    actions = []
    status = _map_evidence_status(is_provisional, is_direct=not is_provisional)
    for p in missing_pages[:5]:
        actions.append({
            "action_title": f"Deploy New Page: {p[0]}",
            "why_it_matters": p[1],
            "expected_impact": "Captures high-intent discovery queries and builds topical authority.",
            "evidence_basis": f"Market discovery gap / {status}",
            "priority": "High",
            "action_type": "Content Expansion"
        })
    return actions

def _generate_trust_actions(trust_gaps: list[list[str]], locale: str, is_provisional: bool) -> list[dict]:
    actions = []
    status = _map_evidence_status(is_provisional, is_direct=not is_provisional)
    for g in trust_gaps:
        actions.append({
            "action_title": f"Integrate Trust Anchor: {g[0]}",
            "why_it_matters": g[1],
            "expected_impact": "Reduces AI hallucination risk and hardens entity validation.",
            "evidence_basis": f"{status} signal requirement",
            "priority": "Critical" if any(k in str(g[0]).upper() for k in ["IVA", "PEC", "ADDRESS", "LEGAL"]) else "High",
            "action_type": "E-E-A-T Hardening"
        })
    return actions

def _generate_discovery_actions(intent_gaps: list[list[str]], is_provisional: bool) -> list[dict]:
    actions = []
    status = _map_evidence_status(is_provisional, is_direct=not is_provisional)
    for g in intent_gaps:
        actions.append({
            "action_title": f"Optimize for {g[0]} Intent",
            "why_it_matters": g[1],
            "expected_impact": "Aligns on-site semantics with actual user search behavior.",
            "evidence_basis": f"User search pattern / {status}",
            "priority": "Medium",
            "action_type": "Intent Alignment"
        })
    return actions

def _determine_primary_problem(state: dict) -> str:
    tier_stats = state.get("stress_test_tier_stats", {})
    taxonomy = state.get("source_taxonomy", {})
    
    blind_matches = tier_stats.get("blind_discovery", {}).get("matches", 0)
    contextual_matches = tier_stats.get("contextual_discovery", {}).get("matches", 0)
    
    citations = taxonomy.get("owned_count", 0) + taxonomy.get("earned_count", 0) + taxonomy.get("review_count", 0) + taxonomy.get("directory_count", 0)
    
    if blind_matches == 0 and contextual_matches == 0:
        return "DISCOVERY_FAILURE"
    elif citations <= 1:
        return "AUTHORITY_FAILURE"
    
    return "BALANCED"

def _generate_specific_actions(state: dict) -> list[dict]:
    """Generates agency-grade strategic actions grounded in explicit context when direct extraction is missing."""
    profile_key = state.get("business_profile_key", "")
    locale = state.get("locale", "en")
    location = state.get("discovered_location", "Unknown Location")
    industry = state.get("target_industry", "Service")
    
    label = profile_key.replace("_", " ").title()
    loc_phrase = f" in {location}" if location and location != "Unknown" and location != "Unknown Location" else ""
    
    actions = [
        {
            "action_title": f"Standardize {label} Service Identity{loc_phrase}",
            "why_it_matters": f"Missing clear {industry} classification causes AI engines to ignore the brand in category-level discovery.",
            "expected_impact": "Stabilizes brand recognition for broad intent queries.",
            "evidence_basis": "profile-inferred (low confidence)",
            "priority": "High",
            "action_type": "Structural Alignment"
        },
        {
            "action_title": f"Publish Conversational FAQ for {industry} Intent",
            "why_it_matters": "AI engines prioritize websites that directly answer user questions over brochure copy.",
            "expected_impact": "Increases citation rate in long-tail informational queries.",
            "evidence_basis": "profile-inferred (low confidence)",
            "priority": "Medium",
            "action_type": "Content Strategy"
        },
        {
            "action_title": "Hard-Code Entity Trust Anchors",
            "why_it_matters": "Connecting the brand to known industry entities and professional registries builds credibility.",
            "expected_impact": "Hardens the 'Knowledge Graph' presence to prevent AI hallucination.",
            "evidence_basis": "profile-inferred (low confidence)",
            "priority": "High",
            "action_type": "E-E-A-T Hardening"
        }
    ]
    return actions

def _validate_actions(actions: list[dict]) -> list[dict]:
    """Reject vague consultant-speak and generic filler, ensuring sellable actions."""
    valid_actions = []
    bad_keywords = ["improve seo", "create blog", "add faq", "optimize content"]
    seen_titles = set()
    
    for a in actions:
        title = a.get("action_title", "")
        if len(title) < 5: 
            continue
        
        title_lower = title.lower()
        # Reject specifically vague titles matched entirely
        if title_lower in bad_keywords:
            continue
            
        if title in seen_titles:
            continue
            
        valid_actions.append(a)
        seen_titles.add(title)
        
    return valid_actions

def process(state: dict) -> dict:
    """Transform findings into implementation-ready GEO action blocks."""
    console.print("[bold blue]Node: Implementation Blueprint[/bold blue] | Formatting agency action plan...")

    # Data safety
    profile             = state.get("business_profile", {})
    profile_summary     = state.get("business_profile_summary", {})
    content_eng         = state.get("content_engineering", {})
    schema_recs         = state.get("schema_recommendations", {})
    crawler_policy      = state.get("crawler_policy", {})
    industry            = state.get("target_industry", "Unknown")
    profile_key         = state.get("business_profile_key", "general")
    locale              = state.get("locale", "en")
    source_mode         = state.get("source_of_truth_mode", "hybrid")
    
    # New Strategic Gaps from Strategist
    missing_pages   = state.get("missing_page_types", [])
    trust_gaps      = state.get("trust_signal_gaps", [])
    intent_gaps     = state.get("discovery_intent_gaps", [])
    entity_gaps     = state.get("entity_trust_gaps", [])

    # Integrity Context
    integrity_status = state.get("audit_integrity_status", "valid")
    is_provisional = (integrity_status != "valid") or (source_mode == "offsite_only")

    # 1. Action Generation with Fail-safes
    page_actions = _generate_page_actions(missing_pages, is_provisional)
    trust_actions = _generate_trust_actions(trust_gaps, locale, is_provisional)
    discovery_actions = _generate_discovery_actions(intent_gaps, is_provisional)
    
    # Technical / Crawler
    robots_patch = crawler_policy.get("recommended_robots_txt", "# No policy change recommended.") if isinstance(crawler_policy, dict) else "# robots.txt update skipped."
    crawler_actions = [{
        "action_title": "Update robots.txt Policy",
        "why_it_matters": "Enables better crawler access to semantic content.",
        "expected_impact": "Allows AI search engines to properly index core brand signals.",
        "evidence_basis": "Robots.txt Analysis",
        "priority": "Medium",
        "action_type": "Technical SEO"
    }] if "Allow" in robots_patch else []

    # Schema actions
    schema_recs_list = schema_recs.get("recommended_blocks", []) if isinstance(schema_recs, dict) else []
    schema_actions = []
    for s in schema_recs_list[:3]:
         schema_actions.append({
            "action_title": f"Inject {s.get('schema_type')} Schema",
            "why_it_matters": s.get('rationale'),
            "expected_impact": "Direct machine-readable evidence for LLM training sets.",
            "evidence_basis": f"Schema Gap / {_map_evidence_status(is_provisional, is_direct=True)}",
            "priority": "High",
            "action_type": "Structured Data"
         })

    # MANDATORY MINIMUM CHECK & SPECIFIC FALLBACKS
    all_raw_actions = page_actions + trust_actions + discovery_actions + crawler_actions + schema_actions
    all_valid_actions = _validate_actions(all_raw_actions)
    
    if len(all_valid_actions) < 3:
        console.print("      [yellow]Blueprint Gap: Generating profile-aware specific actions.[/yellow]")
        fallback = _generate_specific_actions(state)
        valid_fallback = _validate_actions(fallback)
        all_valid_actions.extend(valid_fallback[:(3-len(all_valid_actions))])

    # PRIORITY ENGINE SORTING
    primary_problem = _determine_primary_problem(state)
    discovery_ops = [a for a in all_valid_actions if a["action_type"] in ["Content Expansion", "Intent Alignment", "Content Strategy"]]
    authority_ops = [a for a in all_valid_actions if a["action_type"] in ["E-E-A-T Hardening", "Structured Data", "Structural Alignment"]]
    technical_ops = [a for a in all_valid_actions if a["action_type"] in ["Technical SEO"]]

    final_sorted_actions = []
    if primary_problem == "DISCOVERY_FAILURE":
        final_sorted_actions = discovery_ops + authority_ops + technical_ops
    elif primary_problem == "AUTHORITY_FAILURE":
        final_sorted_actions = authority_ops + discovery_ops + technical_ops
    else:
        # Balanced, maintain generation order as heuristic
        final_sorted_actions = all_valid_actions

    # Separate out for categorized use (remapping for backwards compatibility in templates)
    page_priorities = [a for a in final_sorted_actions if a["action_type"] == "Content Expansion"]
    trust_priorities = [a for a in final_sorted_actions if a["action_type"] == "E-E-A-T Hardening"]
    discovery_gap_priorities = [a for a in final_sorted_actions if a["action_type"] == "Intent Alignment"]

    # Assemble Agency Blueprint
    state["implementation_blueprint"] = {
        "page_priorities": page_priorities,
        "trust_actions": trust_priorities,
        "discovery_gap_actions": discovery_gap_priorities,
        "schema_actions": schema_actions,
        "crawler_actions": crawler_actions,
        "all_strategic_actions": final_sorted_actions, # For Finalizer/Verdict
        "robots_patch": robots_patch,
        "is_provisional": is_provisional
    }

    console.print(f"      [green]Blueprint Complete: {len(final_sorted_actions)} ordered actions generated ([cyan]{primary_problem}[/cyan]).[/green]")
    return state
