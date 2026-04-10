"""
Implementation Blueprint Node — GEO Optimizer Pipeline.

Transforms technical audit findings into implementation-ready strategic action blocks.
Organizes gaps into prioritized agency deliverables with evidence-based rationales.
"""

import json
from typing import Any
from rich.console import Console

console = Console()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ground_action(action: dict, state: dict, integrity_status: str) -> dict | None:
    """
    Central validation and grounding engine for implementation actions.
    Enforces strict origin/confidence/signal mappings.
    Returns grounded action dict or None if action should be suppressed.
    """
    title = action.get("action_title", action.get("title", ""))
    origin = action.get("evidence_origin", "profile_inference")
    confidence = action.get("evidence_confidence", "low")
    signals = action.get("supporting_signals", [])
    
    # ── 1. Origin & Confidence Validation ──
    # on_site requires valid crawl
    if origin == "on_site" and integrity_status != "valid":
        origin = "mixed" if integrity_status == "degraded" else "profile_inference"
        confidence = "low"
        signals.append(f"Crawl Status: {integrity_status}")

    # profile_inference can never be high confidence
    if origin == "profile_inference":
        confidence = "low"
    
    # ── 2. Signal Normalization ──
    # Ensure signals are non-empty and useful
    signals = [s for s in signals if s and len(str(s)) > 3]
    if not signals:
        if origin == "query_gap": signals = ["Stress test discovery gap"]
        elif origin == "on_site": signals = ["Direct evidence extracted"]
        else: signals = ["Industry best practice for profile"]

    if len(signals) == 1 and len(signals[0]) < 10:
        return None

    # ── 3. Agency Mode Suppression ──
    strict_mode = state.get("agency_strict_mode", False)
    if strict_mode and confidence == "low" and action.get("priority") != "Critical":
        return None

    # Update the action object
    action["action_title"] = title
    action["evidence_origin"] = origin
    action["evidence_confidence"] = confidence
    action["supporting_signals"] = signals[:3] # Cap at top 3
    
    # Remap for report rendering (backwards compat)
    action["evidence_basis"] = f"{origin} ({confidence})"
    
    return action

def _generate_page_actions(missing_pages: list, state: dict, integrity_status: str) -> list[dict]:
    actions = []
    for p in missing_pages[:5]:
        # Support both [name, why] and [name, why, origin, conf, signals]
        raw = {
            "action_title": f"Deploy New Page: {p[0]}",
            "why_it_matters": p[1],
            "expected_impact": "Captures high-intent discovery queries.",
            "priority": "High",
            "action_type": "Content Expansion"
        }
        if len(p) >= 5:
            raw.update({
                "evidence_origin": p[2],
                "evidence_confidence": p[3],
                "supporting_signals": p[4]
            })
        else:
            raw.update({
                "evidence_origin": "query_gap" if integrity_status == "valid" else "profile_inference",
                "evidence_confidence": "medium" if integrity_status == "valid" else "low",
                "supporting_signals": [p[1]]
            })
            
        grounded = _ground_action(raw, state, integrity_status)
        if grounded: actions.append(grounded)
    return actions

def _generate_trust_actions(trust_gaps: list, state: dict, integrity_status: str) -> list[dict]:
    actions = []
    for g in trust_gaps:
        if isinstance(g, dict):
            status = g.get("status", "missing")
            signal = g.get("signal", "Unknown")
            origin = g.get("evidence_origin", "on_site")
            conf = g.get("evidence_confidence", "low")
            sigs = g.get("supporting_signals", [])
        elif isinstance(g, list) and len(g) >= 1:
            signal = g[0]
            status = "missing"
            origin = g[2] if len(g) >= 3 else "profile_inference"
            conf = g[3] if len(g) >= 4 else "low"
            sigs = g[4] if len(g) >= 5 else [f"Missing {signal}"]
        else:
            continue

        if status not in ["missing", "present_unstructured", "present_but_not_machine_readable"]:
            continue

        if status == "missing":
            priority = "Critical"
            action_title = f"Integrate Trust Anchor: {signal}"
            why_it_matters = f"Critical absence of {signal} breaks fundamental entity validation."
            impact = "Hardens entity validation and legal integrity."
        elif status == "present_unstructured":
            priority = "High"
            action_title = f"Expose {signal} into Semantic UI Elements"
            why_it_matters = f"{signal} found loosely in text but not exposed correctly for efficient bot crawling."
            impact = "Improves crawler accessibility for trust anchors."
        else: # present_but_not_machine_readable
            priority = "Medium"
            action_title = f"Inject JSON-LD Schema for {signal}"
            why_it_matters = f"{signal} exists securely on the UI but lacks semantic Organization/LocalBusiness markup."
            impact = "Guarantees deterministic entity extraction by LLM crawlers."

        raw = {
            "action_title": action_title,
            "why_it_matters": why_it_matters,
            "expected_impact": impact,
            "priority": priority,
            "action_type": "E-E-A-T Hardening",
            "evidence_origin": origin,
            "evidence_confidence": conf,
            "supporting_signals": sigs
        }
            
        grounded = _ground_action(raw, state, integrity_status)
        if grounded: actions.append(grounded)
    return actions

def _generate_discovery_actions(intent_gaps: list, state: dict, integrity_status: str) -> list[dict]:
    actions = []
    for g in intent_gaps:
        raw = {
            "action_title": f"Optimize for {g[0]} Intent",
            "why_it_matters": g[1],
            "expected_impact": "Aligns on-site semantics with search behavior.",
            "priority": "Medium",
            "action_type": "Intent Alignment"
        }
        if len(g) >= 5:
            raw.update({
                "evidence_origin": g[2],
                "evidence_confidence": g[3],
                "supporting_signals": g[4]
            })
        else:
            raw.update({
                "evidence_origin": "query_gap",
                "evidence_confidence": "medium",
                "supporting_signals": ["Stresstest Intent Gap"]
            })
            
        grounded = _ground_action(raw, state, integrity_status)
        if grounded: actions.append(grounded)
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

def _generate_specific_actions(state: dict, integrity_status: str) -> list[dict]:
    """Generates agency-grade strategic actions when direct extraction is missing."""
    profile_key = state.get("business_profile_key", "")
    location = state.get("discovered_location", "Unknown Location")
    industry = state.get("target_industry", "Service")
    
    label = profile_key.replace("_", " ").title()
    loc_phrase = f" in {location}" if location and location != "Unknown" else ""
    
    actions = [
        {
            "action_title": f"Standardize {label} Service Identity{loc_phrase}",
            "why_it_matters": f"Missing clear {industry} classification.",
            "expected_impact": "Stabilizes brand recognition.",
            "priority": "High",
            "action_type": "Structural Alignment",
            "evidence_origin": "profile_inference",
            "evidence_confidence": "low",
            "supporting_signals": [f"Standard for {profile_key} profile"]
        },
        {
            "action_title": "Hard-Code Entity Trust Anchors",
            "why_it_matters": "Builds credibility in knowledge graph.",
            "expected_impact": "Prevents AI hallucination.",
            "priority": "High",
            "action_type": "E-E-A-T Hardening",
            "evidence_origin": "profile_inference",
            "evidence_confidence": "low",
            "supporting_signals": ["Safety signal requirement"]
        }
    ]
    
    grounded_actions = []
    for a in actions:
        g = _ground_action(a, state, integrity_status)
        if g: grounded_actions.append(g)
    return grounded_actions

def _validate_actions(actions: list[dict]) -> list[dict]:
    """Reject vague consultant-speak and generic filler."""
    valid_actions = []
    bad_keywords = ["improve seo", "create blog", "add faq", "optimize content"]
    seen_titles = set()
    
    for a in actions:
        title = a.get("action_title", "")
        if len(title) < 5 or title.lower() in bad_keywords or title in seen_titles:
            continue
        valid_actions.append(a)
        seen_titles.add(title)
    return valid_actions


def process(state: dict) -> dict:
    """Transform findings into implementation-ready GEO action blocks."""
    console.print("[bold blue]Node: Implementation Blueprint[/bold blue] | Formatting agency action plan...")

    # Data safety
    crawler_policy      = state.get("crawler_policy", {})
    schema_recs         = state.get("schema_recommendations", {})
    profile_key         = state.get("business_profile_key", "general")
    source_mode         = state.get("source_of_truth_mode", "hybrid")
    
    # Strat Gaps
    missing_pages       = state.get("missing_page_types", [])
    trust_gaps          = state.get("trust_signal_gaps", [])
    intent_gaps         = state.get("discovery_intent_gaps", [])

    # Integrity Context
    integrity_status = state.get("audit_integrity_status", "valid")

    # 1. Generation with grounding
    page_actions = _generate_page_actions(missing_pages, state, integrity_status)
    trust_actions = _generate_trust_actions(trust_gaps, state, integrity_status)
    discovery_actions = _generate_discovery_actions(intent_gaps, state, integrity_status)
    
    # 2. Technical / Crawler
    robots_patch = crawler_policy.get("recommended_robots_txt", "# robots.txt update skipped.")
    crawler_actions = []
    if "Allow" in robots_patch:
        matrix = crawler_policy.get("bot_matrix", [])
        for entry in matrix[:2]: # Top 2 bot recs as actions
             raw = {
                "action_title": f"Update Policy for {entry['bot']}",
                "why_it_matters": entry["reason"],
                "expected_impact": "Enables better crawler access to semantic content.",
                "priority": "Medium",
                "action_type": "Technical SEO",
                "evidence_origin": entry.get("evidence_origin", "on_site"),
                "evidence_confidence": entry.get("evidence_confidence", "high"),
                "supporting_signals": entry.get("supporting_signals", [])
             }
             grounded = _ground_action(raw, state, integrity_status)
             if grounded: crawler_actions.append(grounded)

    # 3. Schema actions
    schema_recs_list = schema_recs.get("recommended_blocks", []) if isinstance(schema_recs, dict) else []
    schema_actions = []
    for s in schema_recs_list[:3]:
         raw = {
            "action_title": f"Inject {s.get('schema_type')} Schema",
            "why_it_matters": s.get('rationale'),
            "expected_impact": "Direct machine-readable evidence for LLM retrieval.",
            "priority": "High",
            "action_type": "Structured Data",
            "evidence_origin": s.get("evidence_origin", "on_site"),
            "evidence_confidence": s.get("evidence_confidence", "high"),
            "supporting_signals": s.get("supporting_signals", [])
         }
         grounded = _ground_action(raw, state, integrity_status)
         if grounded: schema_actions.append(grounded)

    # 4. Researcher Strategy Pack
    researcher_actions = []
    try:
        pack_json = state.get("geo_recommendation_pack", "[]")
        pack = json.loads(pack_json) if isinstance(pack_json, str) else pack_json
        for item in (pack if isinstance(pack, list) else []):
            raw = {
                "action_title": item.get("title", "Strategic Action"),
                "why_it_matters": item.get("rationale", ""),
                "expected_impact": "Improves GEO authority and visibility.",
                "priority": item.get("priority", "Medium"),
                "action_type": item.get("implementation_type", "Strategy"),
                "evidence_origin": item.get("evidence_origin", "mixed"),
                "evidence_confidence": item.get("evidence_confidence", "medium"),
                "supporting_signals": item.get("supporting_signals", [])
            }
            grounded = _ground_action(raw, state, integrity_status)
            if grounded: researcher_actions.append(grounded)
    except:
        pass

    # 5. Consolidation & Minimum Check
    all_raw_actions = researcher_actions + page_actions + trust_actions + discovery_actions + schema_actions + crawler_actions
    all_valid_actions = _validate_actions(all_raw_actions)
    
    if len(all_valid_actions) < 3:
        fallback = _generate_specific_actions(state, integrity_status)
        all_valid_actions.extend(fallback)

    # Priority Engine Sorting
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
        final_sorted_actions = all_valid_actions

    # Assemble Agency Blueprint
    state["implementation_blueprint"] = {
        "page_priorities": [a for a in final_sorted_actions if a["action_type"] == "Content Expansion"],
        "trust_actions": [a for a in final_sorted_actions if a["action_type"] == "E-E-A-T Hardening"],
        "discovery_gap_actions": [a for a in final_sorted_actions if a["action_type"] in ["Intent Alignment", "Content Strategy"]],
        "schema_actions": [a for a in final_sorted_actions if a["action_type"] == "Structured Data"],
        "crawler_actions": [a for a in final_sorted_actions if a["action_type"] == "Technical SEO"],
        "all_strategic_actions": final_sorted_actions,
        "primary_problem": primary_problem,
        "robots_patch": robots_patch,
        "integrity_status": integrity_status
    }

    console.print(f"      [green]Blueprint Complete: {len(final_sorted_actions)} grounded actions generated.[/green]")
    return state

