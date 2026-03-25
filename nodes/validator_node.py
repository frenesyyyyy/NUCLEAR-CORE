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
        
        # Determine ROI Viability based on Information Gain and Entity Consensus
        info_gain = metrics.get("Information Gain", 0)
        consensus = metrics.get("Entity Consensus", 0)
        
        # If there's an information gap and we mapped entities reasonably
        if info_gain >= 30 and citation_status in ["Verified", "Partially Verified"]:
            roi_verified = True
            validator_notes = "Strong opportunity identified. Content gap provides positive ROI validation."
        elif info_gain < 30 and consensus > 80:
            roi_verified = False
            validator_notes = "Market is heavily saturated with client entities. Limited low-hanging fruit. ROI viability lower."
        elif citation_status == "Low Verification":
            roi_verified = False
            validator_notes = "Data verification is too low to project confident ROI."
        else:
            roi_verified = True
            validator_notes = "Moderate opportunity. ROI verified based on incremental improvements."
            
    console.print(f"[green]Validator Node[/green]: Validation: {validation} | ROI Verified: {roi_verified}")
    
    # Assign safely to state
    state["validation"] = validation
    state["roi_verified"] = roi_verified
    state["validator_notes"] = validator_notes

    return state
