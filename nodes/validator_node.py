from rich.console import Console

console = Console()

def process(state: dict) -> dict:
    console.print("[cyan]Validator Node[/cyan]: Validating research and projecting ROI viability...")
    
    # Fallbacks and default returns
    validation = "Failed"
    roi_verified = False
    validator_notes = "Validation skipped or failed."
    
    metrics = state.get("metrics", {})
    citation_status = state.get("citation_status", "Low Verification")
    recommendation_pack = state.get("geo_recommendation_pack", "")
    
    # Validation Logic
    # An audit is 'Valid' if we have a generated recommendation pack
    if not recommendation_pack or recommendation_pack == "Unavailable":
        validation = "Incomplete"
        validator_notes = "Missing GEO Recommendation Pack. Unable to verify ROI."
    else:
        validation = "Complete"
        
        # Determine ROI Viability based on Information Gain, Entity Consensus, and Confidence
        info_gain = metrics.get("Information Gain", 0)
        consensus = metrics.get("Entity Consensus", 0)
        confidence = state.get("confidence_score", 0)
        
        if confidence < 50 or citation_status == "Low Verification":
            roi_verified = False
            validator_notes = "Intelligence Blocked: Site architecture (JS/Robots/Thin Content) prevents AI engines from extracting value. Unblocking this is Priority 1."
        elif info_gain > 10 and citation_status in ["Verified", "Partially Verified"]:
            roi_verified = True
            validator_notes = "High-Yield Opportunity: The market is asking questions this brand does not answer. Expanding Top-of-Funnel FAQ authority will capture AI intent."
        elif info_gain <= 10 and consensus > 80:
            roi_verified = False
            validator_notes = "Visibility Plateau: Market is heavily saturated with client entities. Limited conversational upside. Focus on technical entity consensus."
        else:
            roi_verified = False
            validator_notes = "Visibility Risk: Brand is absent from organic discovery and technical authority signals. Foundational E-E-A-T reset required."
            
    console.print(f"[green]Validator Node[/green]: Validation: {validation} | ROI Verified: {roi_verified}")
    
    # Assign safely to state
    state["validation"] = validation
    state["roi_verified"] = roi_verified
    state["validator_notes"] = validator_notes

    return state
