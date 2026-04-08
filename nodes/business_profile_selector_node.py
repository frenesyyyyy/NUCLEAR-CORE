import copy
from rich.console import Console
from nodes.business_profiles import BUSINESS_INTELLIGENCE_PROFILES, DEFAULT_PROFILE_KEY
from nodes.profile_selector import select_business_profile

console = Console()

def process(state: dict) -> dict:
    """
    Classifies the business into a GEO specialization profile.
    v4.5 Hotfix: Passes extra_context (content, FAQs, gaps) for structural detection.
    """
    new_state = state.copy()
    
    try:
        # 1. Extract primary signals
        business_type = state.get("business_type", "unknown")
        target_industry = state.get("target_industry", "unknown")
        scale_level = state.get("scale_level", "National")
        schema_type_counts = state.get("schema_type_counts", {})
        discovered_location = state.get("discovered_location", "")
        
        # 2. Package extra context for structural analysis
        extra_context = {
            "page_title": state.get("page_title", ""),
            "brand_name": state.get("brand_name", ""),
            "client_content_clean": state.get("client_content_clean", ""),
            "raw_data_complete": state.get("raw_data_complete", {}),
            "og_tags": state.get("og_tags", {})
        }

        console.log(f"[bold blue]Node: Business Profile Selector[/bold blue] | Analyzing: {target_industry} ({business_type})")

        # 3. Select the profile key using structural signals
        key, metadata = select_business_profile(
            business_type=business_type,
            target_industry=target_industry,
            scale_level=scale_level,
            schema_type_counts=schema_type_counts,
            discovered_location=discovered_location,
            extra_context=extra_context
        )

        # ── DEFENSIVE SEMANTIC GUARD (v4.5 Hotfix) ──
        if key == "local_law_firm":
            hazard_tokens = ["food", "delivery", "restaurant", "ristorante", "trattoria", "pizza", "burger", "meal", "menu"]
            evidence_str = str(target_industry).lower() + " " + str(business_type).lower() + " " + extra_context.get("client_content_clean", "")[:2000].lower()
            
            if any(t in evidence_str for t in hazard_tokens):
                console.log(f"   [yellow]Hazard Detected[/yellow] | Semantic conflict! Overriding strict legal check and re-running.")
                key, metadata = select_business_profile(
                    business_type=business_type,
                    target_industry=target_industry,
                    scale_level=scale_level,
                    schema_type_counts=schema_type_counts,
                    discovered_location=discovered_location,
                    extra_context=extra_context,
                    ignore_legal=True
                )
                metadata["reliability"] = "low"
                metadata["evidence"].append("Semantic boundary conflict detected (food/restaurant keywords found on putative legal entity). Legal override was forcefully BLOCKED.")

        # 4. Load the profile object safely preventing global template mutation
        profile = copy.deepcopy(BUSINESS_INTELLIGENCE_PROFILES.get(key, BUSINESS_INTELLIGENCE_PROFILES.get(DEFAULT_PROFILE_KEY)))
        
        # 5. Populate and report
        new_state.update({
            "business_profile_key": key,
            "business_profile": profile,
            "classification_reliability": metadata.get("reliability", "low"),
            "classification_evidence": metadata.get("evidence", []),
            "business_profile_summary": {
                "label": profile.get("label"),
                "macro_industry": profile.get("macro_industry"),
                "reliability_score": metadata.get("reliability", "low"),
                "evidence_signals": metadata.get("evidence", [])
            }
        })

        console.log(f"   [green]Classification Complete[/green] | Profile: [cyan]{key}[/cyan] [Reliability: {metadata.get('reliability')}]")

    except Exception as e:
        console.log(f"   [bold red]Classification Error[/bold red] | Using fallback: {str(e)}")
        fallback_profile = BUSINESS_INTELLIGENCE_PROFILES.get(DEFAULT_PROFILE_KEY)
        new_state.update({
            "business_profile_key": DEFAULT_PROFILE_KEY,
            "business_profile": fallback_profile,
            "classification_reliability": "low",
            "classification_evidence": [f"Node execution error: {str(e)}"]
        })

    return new_state
