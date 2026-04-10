from typing import Any
from rich.console import Console
from nodes.business_profiles import DEFAULT_PROFILE_KEY, normalize_profile_key, get_platform_like_profiles

console = Console()

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_rate(matches: int, queries: int) -> float:
    """Return hit-rate percentage, safe against division by zero."""
    if queries <= 0:
        return 0.0
    return (matches / queries) * 100

# Local profiles that need combined T1+T2 discovery floors
_LOCAL_DISCOVERY_PROFILES = {"local_healthcare_ymyl", "local_legal_ymyl"}

# ─────────────────────────────────────────────────────────────────────────────
# Integrity Score
# ─────────────────────────────────────────────────────────────────────────────

def _compute_integrity_score(state: dict) -> int:
    """ Compute a deterministic 0-100 score based on extraction and classification quality. """
    score = 0
    status = state.get("audit_integrity_status", "invalid")
    if status == "invalid": return 10
    
    depth = state.get("client_content_depth", {})
    wc = depth.get("word_count", 0)
    signals = depth.get("semantic_signals", {})
    
    # 1. Extraction Depth (30 pts)
    quality = depth.get("extraction_quality", "Failed")
    if quality == "high": score += 30
    elif quality == "medium": score += 20
    elif quality == "low": score += 10
    
    # 2. Structural Density (30 pts)
    sig_score = 0
    if depth.get("schema_block_count", 0) >= 2: sig_score += 10
    if signals.get("cta_count", 0) >= 2: sig_score += 10
    if signals.get("heading_count", 0) >= 3: sig_score += 10
    score += sig_score
    
    # 3. Word Count (20 pts)
    if wc > 800: score += 20
    elif wc > 400: score += 15
    elif wc > 150: score += 5
    
    # 4. Classification Reliability (20 pts)
    rel = state.get("classification_reliability", "low")
    if rel == "high": score += 20
    elif rel == "medium": score += 10
    
    mode = state.get("source_of_truth_mode", "hybrid")
    if mode == "offsite_only":
        return min(score, 25)
    
    return min(score, 100)

# ─────────────────────────────────────────────────────────────────────────────
# Data Confidence
# ─────────────────────────────────────────────────────────────────────────────

def _compute_data_confidence(state: dict) -> int:
    """ Compute true evidence-based data confidence. """
    score = 0
    depth = state.get("client_content_depth", {})
    extraction_quality = depth.get("extraction_quality", "Failed")
    taxonomy = state.get("source_taxonomy", {})

    if extraction_quality == "high": score += 40
    elif extraction_quality == "medium": score += 20
    elif extraction_quality == "low": score += 10
    
    inferred_families = state.get("earned_media", {}).get("first_party_inferred_families", [])
    qualifying_inferred_count = sum(1 for f in inferred_families if f.get("confidence") in {"medium", "high"})
    inferred_credit = min(6, qualifying_inferred_count * 2)
    
    citations = (taxonomy.get("owned_count", 0) + taxonomy.get("earned_count", 0) + 
                 taxonomy.get("review_count", 0) + taxonomy.get("directory_count", 0) + inferred_credit)
    
    if citations > 15: score += 30
    elif citations > 5: score += 15
    elif citations > 0: score += 5
    
    schema_counts = state.get("schema_type_counts", {})
    if len(schema_counts) > 3: score += 30
    elif len(schema_counts) > 0: score += 15
    
    penalized_gaps = taxonomy.get("penalized_relevant_gaps", [])
    if len(penalized_gaps) >= 2:
        score = min(score, 50)
    elif citations <= 1 and len(penalized_gaps) > 0:
        score = min(score, 45)
    
    if depth.get("word_count", 0) < 200:
        score = min(score, 50)
        
    return min(score, 100)

# ─────────────────────────────────────────────────────────────────────────────
# Verdict Engine (v5.1 — Contradiction Aware)
# ─────────────────────────────────────────────────────────────────────────────

def _detect_contradictions(
    candidate_verdict: str,
    confidence: int,
    penalized_gaps: list,
    trust_mix: str,
    trust_conf: str,
    blind_rate: float,
    contextual_rate: float,
    auth_composite: float,
    schema_conf: str,
    integrity_status: str,
    consensus: float
) -> tuple[list[str], list[str]]:
    """ Detect discordant signals that force Analyst Review. """
    flags: list[str] = []
    reasons: list[str] = []

    # C1: Confidence dissonance
    if confidence < 70 and candidate_verdict == "CLIENT READY":
        flags.append("C1")
        reasons.append(f"Confidence ({confidence}%) contradicts readiness recommendation.")

    # C2: Trust/Gap Dissonance
    is_strong_trust = (isinstance(trust_mix, str) and "strong" in str(trust_mix).lower()) or trust_conf == "High"
    if len(penalized_gaps) >= 2 and is_strong_trust:
        flags.append("C2")
        reasons.append(f"Metric Dissonance: High trust rating contradicted by {len(penalized_gaps)} critical source gaps.")

    # C3: Discovery/Authority Drift
    combined_discovery = (blind_rate + contextual_rate) / 2
    if (0 < combined_discovery < 20 or (0 < blind_rate < 5)) and auth_composite > 75:
        flags.append("C3")
        reasons.append(f"Authority Drift: Strong institutional authority ({auth_composite:.0f}) but weak real-world discovery footprint.")

    # C4: Technical/Trust Conflict
    if schema_conf == "Low" and is_strong_trust:
        flags.append("C4")
        reasons.append("Technical Conflict: Strong external trust mix but absent on-site technical validation (Low Schema).")

    # C5: Integrity/Consensus Dissonance
    if integrity_status == "degraded" and consensus > 70:
        flags.append("C5")
        reasons.append(f"Signal Dissonance: High consensus ({consensus:.0f}) claimed on degraded site extraction.")

    # C6: Total Discovery Shadow
    if blind_rate == 0 and candidate_verdict == "CLIENT READY":
        flags.append("C6")
        reasons.append("Visibility Shadow: Candidate is ready but remains invisible to all non-branded queries.")

    return flags, reasons

def _compute_verdict(state: dict) -> tuple[str, str, list[str], list[str]]:
    """ Compute sellability verdict via 4-phase veto chain. """
    reasons: list[str] = []

    # Phase 0: Signal Gathering
    metrics = state.get("metrics", {})
    confidence = state.get("confidence_score", metrics.get("Data Confidence Score", 0))
    evidence_depth = metrics.get("Defensible Evidence Depth", 0)
    consensus = metrics.get("Entity Consensus", 0)
    taxonomy = state.get("source_taxonomy", {})
    trust_mix = taxonomy.get("trust_mix", "Unknown")
    penalized_gaps = taxonomy.get("penalized_relevant_gaps", [])
    integrity_status = state.get("audit_integrity_status", "invalid")
    source_mode = state.get("source_of_truth_mode", "hybrid")
    tier_stats = state.get("stress_test_tier_stats", {})
    blind_rate = _safe_rate(tier_stats.get("blind_discovery", {}).get("matches", 0), 
                            tier_stats.get("blind_discovery", {}).get("queries", 0))
    contextual_rate = _safe_rate(tier_stats.get("contextual_discovery", {}).get("matches", 0), 
                                 tier_stats.get("contextual_discovery", {}).get("queries", 0))
    profile_key = normalize_profile_key(state.get("business_profile_key", DEFAULT_PROFILE_KEY))
    PLATFORM_PROFILES = get_platform_like_profiles()
    analytics = state.get("model_analytics", {})
    auth_composite = analytics.get("authority_composite", 0)
    schema_conf = state.get("schema_confidence", "Low")
    trust_conf = state.get("trust_confidence", "Low")

    # Phase 1: Hard Vetoes
    if integrity_status == "invalid":
        return "NOT CLIENT READY", "V1: Extraction failed.", [], []
    if source_mode == "offsite_only":
        return "NOT CLIENT READY", "V2: No on-site evidence.", [], []
    if confidence < 40:
        return "NOT CLIENT READY", f"V3: Confidence low ({confidence}%).", [], []
    if blind_rate == 0 and contextual_rate == 0:
        return "NOT CLIENT READY", "V4: Zero discovery.", [], []

    # Phase 2: Soft Vetoes (Accumulation)
    if confidence < 70:
        reasons.append(f"S1: Confidence ({confidence}%) < 70%.")
    if len(penalized_gaps) >= 2:
        reasons.append(f"S2: {len(penalized_gaps)} critical gaps.")
    if str(trust_mix).lower() == "weak":
        reasons.append("S4: Trust mix Weak.")
    if integrity_status == "degraded":
        reasons.append("S5: Degraded extraction.")
    
    min_ev = 20 if profile_key in PLATFORM_PROFILES else 30
    if evidence_depth < min_ev:
        reasons.append(f"S6a: Evidence depth ({evidence_depth}) low.")
    if consensus < 30:
        reasons.append(f"S6b: Consensus ({consensus}) low.")

    if profile_key in _LOCAL_DISCOVERY_PROFILES:
        if (blind_rate + contextual_rate) / 2 < 20:
            reasons.append("S7: Local discovery rate low.")

    # Phase 4 Candidate (Pre-Contradiction)
    is_ready = (len(reasons) == 0 and confidence >= 70 and len(penalized_gaps) < 2)
    candidate_verdict = "CLIENT READY" if is_ready else "REQUIRES ANALYST REVIEW"

    # Phase 3: Contradictions
    flags, contra_reasons = _detect_contradictions(
        candidate_verdict, confidence, penalized_gaps, trust_mix, 
        trust_conf, blind_rate, contextual_rate, 
        auth_composite, schema_conf, integrity_status, consensus
    )

    if contra_reasons:
        all_reasons = ["CONTRADICTION DETECTED:"] + contra_reasons + reasons
    else:
        all_reasons = reasons
    if blind_rate == 0: all_reasons.append("Zero blind discovery.")

    # Final readiness check
    is_really_ready = is_ready and blind_rate > 0 and not flags
    
    if is_really_ready:
        return "CLIENT READY", f"Ready: Conf {confidence}%, Gaps {len(penalized_gaps)}.", [], []
    
    return "REQUIRES ANALYST REVIEW", " | ".join(all_reasons), flags, contra_reasons

# ─────────────────────────────────────────────────────────────────────────────
# Main Process
# ─────────────────────────────────────────────────────────────────────────────

def process(state: dict) -> dict:
    console.print("[cyan]Validator Node[/cyan]: Assessing audit integrity...")
    
    integ_status = state.get("audit_integrity_status", "invalid")
    integ_reasons = state.get("audit_integrity_reasons", [])
    
    # 1. Integrity Score
    val_score = _compute_integrity_score(state)
    state["audit_integrity_score"] = val_score
    
    if integ_status == "invalid": readiness = "invalid_audit"
    elif integ_status == "degraded": readiness = "degraded"
    elif val_score >= 75: readiness = "valid_premium"
    else: readiness = "valid_core"
    state["overall_pipeline_readiness"] = readiness

    # 2. Confidence/Trust Engine
    data_confidence = _compute_data_confidence(state)
    state["confidence_score"] = data_confidence
    
    depth = state.get("client_content_depth", {})
    state["evidence_confidence"] = "High" if depth.get("extraction_quality", "Failed") == "high" else "Low"
    taxonomy = state.get("source_taxonomy", {})
    has_critical_gaps = len(taxonomy.get("penalized_relevant_gaps", [])) > 0
    state["trust_confidence"] = "High" if (taxonomy.get("earned_count", 0) > 2 and not has_critical_gaps) else "Low"
    state["schema_confidence"] = "High" if len(state.get("schema_type_counts", {})) > 2 else "Low"
    
    # 3. Verdict
    verdict, reason, flags, c_reasons = _compute_verdict(state)
    state["agency_verdict"] = verdict
    state["agency_verdict_reason"] = reason
    state["contradiction_flags"] = flags
    state["contradiction_reasons"] = c_reasons

    # 4. Strategic Surface
    risk_list = []
    if integ_status != "valid": risk_list.append(f"Integrity {integ_status}")
    if len(taxonomy.get("penalized_relevant_gaps", [])) > 0: risk_list.append("Critical source gaps")
    if data_confidence < 70: risk_list.append("Low confidence")
    for cr in c_reasons: risk_list.append(f"CRITICAL DISSONANCE: {cr}")
    
    if not risk_list: risk_list.append("No critical risks identified.")
    
    state.update({
        "validation": "Complete" if readiness != "invalid_audit" else "Failed",
        "decision_summary": f"{verdict}: {reason}",
        "decision_risks": risk_list,
        "decision_next_step": "Proceed to client." if verdict == "CLIENT READY" else "Manual review required."
    })
    
    console.print(f"   [green]Validator Complete[/green] | Verdict: {verdict}")
    return state
