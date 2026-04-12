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

        # ── MEDICAL SEMANTIC GUARD (B2B SAAS VETO) ──
        if key == "b2b_saas_tech":
            medical_anchors = [
                "terapia del dolore", "centro medico", "clinica", "medico", "visite", 
                "trattamenti", "paziente", "dolore cronico", "sciatalgia", "infiltrazioni", 
                "agopuntura", "ernia del disco", "terapia", "visita"
            ]
            
            # Build an aggregated evidence string for the medical check
            evidence_str = (
                str(target_industry).lower() + " " + 
                str(business_type).lower() + " " + 
                extra_context.get("page_title", "").lower() + " " + 
                extra_context.get("og_tags", {}).get("og:description", "").lower() + " " + 
                str(schema_type_counts).lower() + " " + 
                extra_context.get("client_content_clean", "")[:5000].lower()
            )
            
            # Count distinct medical anchors present
            anchors_found = [a for a in medical_anchors if a in evidence_str]
            if len(anchors_found) >= 2:
                console.log(f"   [yellow]Medical Guard[/yellow] | {len(anchors_found)} medical anchors found {anchors_found}. Vetoing b2b_saas_tech.")
                key = "local_healthcare_ymyl"
                metadata["reliability"] = "high"
                metadata["evidence"].append(f"Strong medical signals detected {anchors_found}. Overriding b2b_saas_tech classification to local_healthcare_ymyl.")


        # ── DEFENSIVE SEMANTIC GUARD (v4.5 Hotfix) ──
        if key in ("local_healthcare_ymyl", "local_legal_ymyl"):
            hazard_tokens = ["food", "delivery", "restaurant", "ristorante", "trattoria", "pizza", "burger", "meal", "menu"]
            evidence_str = str(target_industry).lower() + " " + str(business_type).lower() + " " + extra_context.get("client_content_clean", "")[:2000].lower()
            
            if any(t in evidence_str for t in hazard_tokens):
                console.log(f"   [yellow]Hazard Detected[/yellow] | Semantic conflict! Overriding strict YMYL check and falling back.")
                key = "hospitality_travel"
                metadata["reliability"] = "low"
                metadata["evidence"].append("Semantic boundary conflict detected (food/restaurant keywords found on putative YMYL entity). Hospitality fallback applied.")

        # ── SPECIALTY GOODS TIE-BREAKER ──
        if key == "ecommerce_retail":
            specialty_signals = [
                "pellet", "biomass", "fuel", "industrial", "wholesale", "bulk", 
                "sfuso", "sacchi", "scheda tecnica", "certificato", "packaging", 
                "formato", "agricultural", "supply", "distribuzione"
            ]
            evidence_str = str(target_industry).lower() + " " + str(business_type).lower() + " " + extra_context.get("client_content_clean", "")[:5000].lower()
            
            if any(s in evidence_str for s in specialty_signals):
                console.log(f"   [yellow]Tie-breaker[/yellow] | Specialty signals detected! Overriding ecommerce_retail.")
                key = "specialty_goods_supplier"
                metadata["reliability"] = "high"
                metadata["evidence"].append("Specialty/Industrial signals detected in site content. Overriding generic ecommerce classification.")

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
                "canonical_key": key,
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
