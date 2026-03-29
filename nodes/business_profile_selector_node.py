"""workflow node for selecting and loading business intelligence profiles."""
from rich.console import Console
from nodes.business_profiles import BUSINESS_INTELLIGENCE_PROFILES
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

        # 4. Load the profile object
        profile = BUSINESS_INTELLIGENCE_PROFILES.get(key, BUSINESS_INTELLIGENCE_PROFILES.get("b2b_saas"))
        
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
        console.log(f"   [bold red]Classification Error[/bold red] | Using fallback B2B SaaS: {str(e)}")
        fallback_profile = BUSINESS_INTELLIGENCE_PROFILES.get("b2b_saas")
        new_state.update({
            "business_profile_key": "b2b_saas",
            "business_profile": fallback_profile,
            "classification_reliability": "low",
            "classification_evidence": [f"Node execution error: {str(e)}"]
        })

    return new_state
