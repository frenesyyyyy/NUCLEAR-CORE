from rich.console import Console

console = Console()

def _compute_integrity_score(state: dict) -> int:
    """ Compute a deterministic 0-100 score based on extraction and classification quality. """
    score = 0
    status = state.get("audit_integrity_status", "invalid")
    if status == "invalid": return 10 # punitive baseline
    
    depth = state.get("client_content_depth", {})
    wc = depth.get("word_count", 0)
    signals = depth.get("semantic_signals", {})
    
    # 1. Extraction Depth (30 pts)
    quality = depth.get("extraction_quality", "Failed")
    if quality == "high": score += 30
    elif quality == "medium": score += 20
    elif quality == "low": score += 10
    
    # 2. Structural Density (30 pts) - Recalibration
    # Awards points for schema, CTAs and headings to prevent score collapse on thin site-rescue
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
    
    # ── v4.5 Integrity Cap ──
    mode = state.get("source_of_truth_mode", "hybrid")
    if mode == "offsite_only":
        return min(score, 25) # Hard cap for off-site only audits
    
    return min(score, 100)


def _compute_data_confidence(state: dict) -> int:
    """ Compute true evidence-based data confidence. """
    score = 0
    depth = state.get("client_content_depth", {})
    extraction_quality = depth.get("extraction_quality", "Failed")
    taxonomy = state.get("source_taxonomy", {})
    metrics = state.get("metrics", {})
    
    # Base from extraction
    if extraction_quality == "high": score += 40
    elif extraction_quality == "medium": score += 20
    elif extraction_quality == "low": score += 10
    
    # Citations
    citations = taxonomy.get("owned_count", 0) + taxonomy.get("earned_count", 0) + taxonomy.get("review_count", 0) + taxonomy.get("directory_count", 0)
    if citations > 15: score += 30
    elif citations > 5: score += 15
    elif citations > 0: score += 5
    
    # Schema
    schema_counts = state.get("schema_type_counts", {})
    if len(schema_counts) > 3: score += 30
    elif len(schema_counts) > 0: score += 15
    
    # Hard credibility cap
    if citations <= 1:
        score = min(score, 45)
    
    if depth.get("word_count", 0) < 200:
        score = min(score, 50)
        
    return min(score, 100)

def _compute_verdict(state: dict) -> tuple[str, str]:
    """ Compute exact agency sellability verdict. """
    tier_stats = state.get("stress_test_tier_stats", {})
    metrics = state.get("metrics", {})
    source_mode = state.get("source_of_truth_mode", "hybrid")
    integrity_status = state.get("audit_integrity_status", "invalid")
    
    blind = tier_stats.get("blind_discovery", {})
    contextual = tier_stats.get("contextual_discovery", {})
    
    blind_hit_rate = (blind.get("matches", 0) / blind.get("queries", 1)) * 100 if blind.get("queries", 0) > 0 else 0
    contextual_hit_rate = (contextual.get("matches", 0) / contextual.get("queries", 1)) * 100 if contextual.get("queries", 0) > 0 else 0
    
    ed = metrics.get("Defensible Evidence Depth", 0)
    vc = metrics.get("Entity Consensus", 0)

    # Hard Invalid/Degraded Rules
    if source_mode == "offsite_only" or integrity_status == "invalid":
        return "NOT CLIENT READY", "Site extraction failed/blocked. Findings are unverified off-site inferences only."
    
    if blind_hit_rate == 0 and contextual_hit_rate == 0:
        return "NOT CLIENT READY", "Brand has absolute zero discovery visibility. Fundamental positioning failure."

    if ed < 30 or vc < 30:
        return "REQUIRES ANALYST REVIEW", "Visibility or Evidence metrics are too weak to support a confident automated strategy."
    
    if integrity_status == "degraded":
        return "REQUIRES ANALYST REVIEW", "Partial site rescue limits recommendation reliability."

    # Contradiction guard is naturally handled by the zero-hit check above, but adding safety checking.
    if blind_hit_rate == 0:
        return "REQUIRES ANALYST REVIEW", "CLIENT READY downgraded: Brand has zero blind discovery visibility."

    return "CLIENT READY", "Strong extraction, positive validation, and evidence-grounded strategic roadmap."


def process(state: dict) -> dict:
    console.print("[cyan]Validator Node[/cyan]: Assessing audit integrity and pipeline readiness...")
    
    integ_status = state.get("audit_integrity_status", "invalid")
    integ_reasons = state.get("audit_integrity_reasons", [])
    class_rel = state.get("classification_reliability", "low")
    
    # 1. Audit Validity Data
    validity_score = _compute_integrity_score(state)
    state["audit_integrity_score"] = validity_score
    
    validity_summary = {
        "integrity_status": integ_status,
        "integrity_reasons": integ_reasons,
        "integrity_score": validity_score
    }
    
    # 2. Overall Pipeline Readiness
    if integ_status == "invalid":
        readiness = "invalid_audit"
    elif integ_status == "degraded":
        readiness = "degraded"
    elif validity_score >= 75:
        readiness = "valid_premium"
    else:
        readiness = "valid_core"
    
    state["audit_validity_summary"] = validity_summary
    state["overall_pipeline_readiness"] = readiness

    # 3. Decision Reliability Engine (v4.6)
    data_confidence = _compute_data_confidence(state)
    state["confidence_score"] = data_confidence
    
    # Also store diagnostic subcomponents if helpful
    depth = state.get("client_content_depth", {})
    state["evidence_confidence"] = "High" if depth.get("extraction_quality", "Failed") == "high" else "Low"
    taxonomy = state.get("source_taxonomy", {})
    state["trust_confidence"] = "High" if (taxonomy.get("owned_count", 0) + taxonomy.get("earned_count", 0)) > 5 else "Low"
    state["schema_confidence"] = "High" if len(state.get("schema_type_counts", {})) > 2 else "Low"
    
    verdict, reason = _compute_verdict(state)
    state["agency_verdict"] = verdict
    state["agency_verdict_reason"] = reason

    # 4. Component Completion
    state["component_completion_summary"] = {
        "profile_selected": bool(state.get("business_profile_key")),
        "fetch_complete": integ_status != "invalid",
        "schema_generated": bool(state.get("schema_recommendations")),
        "blueprint_ready": bool(state.get("implementation_blueprint")),
        "research_done": bool(state.get("metrics"))
    }

    # 5. ROI and Strategic Validation
    validation_status = "Failed" if readiness == "invalid_audit" else "Complete"
    roi_verified = False
    validator_notes = ""
    
    if readiness == "invalid_audit":
        validator_notes = f"Audit Invalid: {', '.join(integ_reasons)}"
    elif readiness == "degraded":
        validator_notes = "Audit Degraded: Partial extraction limits reliability of on-site recommendations."
    
    # ── Provisional Classification Warning ──
    if integ_status != "valid" and class_rel == "low":
        prov_msg = "Audit validity is limited by failed extraction and provisional business-model classification."
        if validator_notes:
            validator_notes = f"{validator_notes} | {prov_msg}"
        else:
            validator_notes = prov_msg
            
    if not validator_notes:
        # Business Logic Validation
        if data_confidence > 65 and verdict == "CLIENT READY":
            roi_verified = True
            validator_notes = f"High-Yield Opportunity: Verified by decision engine. ({data_confidence}% Data Confidence)"
        else:
            roi_verified = False
            validator_notes = f"Visibility Risk: {reason}"
            
    console.print(f"   [green]Validator Complete[/green] | Readiness: {readiness} | Verdict: {verdict} | Confidence: {data_confidence}%")
    
    state.update({
        "validation": validation_status,
        "roi_verified": roi_verified,
        "validator_notes": validator_notes
    })

    return state
