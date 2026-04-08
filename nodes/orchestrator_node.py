import os
import json
import re
import copy
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
from rich.console import Console
from google import genai
from google.genai import types

console = Console()

# ─────────────────────────────────────────────────────────────────────────────
# Legacy Taxonomy (retained for backward-compatible type_config defaults)
# ─────────────────────────────────────────────────────────────────────────────
BUSINESS_TYPE_MAP = {
    "tech":          {"scale_default": "Global", "query_style": "technical_authority", "location_enforce": False},
    "local_tech":    {"scale_default": "National", "query_style": "geo_technical", "location_enforce": True},
    "food":          {"scale_default": "Local", "query_style": "sensory_experiential", "location_enforce": True},
    "food_blog":     {"scale_default": "National", "query_style": "narrative_recipe", "location_enforce": False},
    "freelancer":    {"scale_default": "Local", "query_style": "personal_expertise", "location_enforce": True},
    "dentist":       {"scale_default": "Local", "query_style": "medical_trust", "location_enforce": True},
    "blog":          {"scale_default": "National", "query_style": "thought_leadership", "location_enforce": False},
}


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic Schema for Structured Output (Agnostic Entity Decomposition)
# ─────────────────────────────────────────────────────────────────────────────

class ScaleLevel(str, Enum):
    LOCAL = "Local"
    NATIONAL = "National"
    GLOBAL = "Global"

class IntentType(str, Enum):
    INFORMATIONAL = "Informational"
    TRANSACTIONAL = "Transactional"
    NAVIGATIONAL = "Navigational"

class PersonaEntry(BaseModel):
    persona_label: str = Field(description="Name of the target persona")
    intent: str = Field(description="Primary intent of this persona")
    revenue_stream: str = Field(description="Which revenue stream this persona maps to")

class OrchestratorOutput(BaseModel):
    primary_industry: str = Field(
        description="The single most dominant macro-category of the business (e.g., 'Hotel & Co-living', 'Law Firm', 'E-commerce Retail', 'SaaS Platform')."
    )
    secondary_revenue_streams: list[str] = Field(
        default_factory=list,
        description="Distinct secondary services, sub-niches, or practice areas. Examples: ['Restaurant', 'Event Space'] or ['Divorce Law', 'Corporate Law'] or ['B2B Wholesale']."
    )
    brand_name: str = Field(description="The commercial brand name")
    target_audience_summary: str = Field(
        description="A concise description of who the primary customers are"
    )
    persona_matrix: list[PersonaEntry] = Field(
        default_factory=list,
        description="A list of distinct target personas covering ALL identified revenue streams"
    )
    scale_level: ScaleLevel = Field(
        default=ScaleLevel.LOCAL,
        description="Geographic scale: Local if physical address detected, National for country-wide, Global for location-agnostic"
    )
    intent_type: IntentType = Field(
        default=IntentType.TRANSACTIONAL,
        description="Dominant user intent type"
    )
    discovered_location: str = Field(
        default="Worldwide",
        description="MUST be a single City and Country (e.g., 'Milano, Italy') OR 'National'. NEVER use parentheses. NEVER list multiple regions."
    )
    service_zones: list[str] = Field(
        default_factory=list,
        description="An array of single strings representing individual cities or neighborhoods (e.g., ['Milano', 'Roma', 'Bologna', 'Monza']). DO NOT group them in a single string."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Defensive JSON Fallback Parser (for edge cases where structured output fails)
# ─────────────────────────────────────────────────────────────────────────────

def _safe_parse_json_response(raw: str) -> dict:
    """Defensive JSON parser to strip markdown fences and handle malformed strings."""
    if not raw:
        return {}
    clean = raw.strip()
    if clean.startswith("```json"):
        clean = clean[7:]
    if clean.startswith("```"):
        clean = clean[3:]
    if clean.endswith("```"):
        clean = clean[:-3]
    clean = clean.strip()
    
    try:
        return json.loads(clean)
    except Exception as e:
        console.print(f"   [yellow]Warning[/yellow]: JSON parse failed: {e}. Attempting greedy regex match...")
        match = re.search(r'(\{.*\})', clean, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except Exception:
                pass
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# Main Process
# ─────────────────────────────────────────────────────────────────────────────

def process(state: dict) -> dict:
    console.print("[cyan]Orchestrator Node[/cyan]: Starting agency-grade entity decomposition analysis...")
    
    # ── 1. INITIALIZE ALL FALLBACK LOCALS SAFELY ──
    brand_name = "Unavailable"
    primary_industry = "Unavailable"
    secondary_revenue_streams = []
    target_industry = "Unavailable"
    target_audience_summary = "Unavailable"
    persona_matrix = {}
    intent_type = "Transactional"
    discovered_location = "Worldwide"
    service_zones = []
    orchestrator_notes = ""
    
    url = state.get("url", "")
    locale = state.get("locale", "en")
    
    # ── Ground-Truth Content (populated by content_fetcher which runs BEFORE this node) ──
    site_content = state.get("client_content_clean", "")
    page_title = state.get("page_title", "")
    og_tags = state.get("og_tags", {})
    content_snippet = site_content[:3000] if site_content else "(No content extracted)"
    
    # Defaults — no longer dependent on a CLI --type flag
    type_config = copy.deepcopy(BUSINESS_TYPE_MAP.get("tech", BUSINESS_TYPE_MAP["tech"]))
    scale_level = type_config.get("scale_default", "Local")

    gemini_key = os.getenv("GEMINI_API_KEY")
    if not gemini_key:
        console.print("[bold red]NODE_FAILED[/bold red]: API Key missing. Using fallbacks.")
        state.update({
            "target_industry": target_industry,
            "primary_industry": primary_industry,
            "secondary_revenue_streams": secondary_revenue_streams,
            "brand_name": brand_name,
            "target_audience_summary": target_audience_summary,
            "persona_matrix": persona_matrix,
            "scale_level": scale_level,
            "intent_type": intent_type,
            "discovered_location": discovered_location,
            "service_zones": service_zones,
            "type_config": type_config,
            "orchestrator_notes": "API Key Missing"
        })
        return state

    client = genai.Client(api_key=gemini_key)
    from nodes.api_utils import execute_with_backoff
    
    prompt = f"""
    Analyze the following URL and its ACTUAL SCRAPED CONTENT to determine the business profile for a v4.5 Agency-Grade GEO audit.
    URL: {url}
    Locale: {locale}
    Page Title: {page_title}
    OG Description: {og_tags.get('og:description', 'N/A')}
    
    === BEGIN SCRAPED SITE CONTENT (first 3000 chars) ===
    {content_snippet}
    === END SCRAPED SITE CONTENT ===
    
    CRITICAL INSTRUCTION: Base your analysis ONLY on the scraped content above. DO NOT guess from the URL alone.
    
    ANTI-HALLUCINATION PROTOCOL: You must first identify the EXACT physical or digital product sold. If the company sells physical rooms, accommodations, or local services, your `target_audience_summary` and `persona_matrix` MUST reflect individuals booking physical spaces. DO NOT hallucinate software, SaaS, or IT personas unless the website explicitly sells code or digital platforms.
    
    ENTITY DECOMPOSITION RULE: Do not mash multiple distinct business models or practice areas into a single industry string. Identify the single most dominant 'primary_industry'. If the business is a hybrid or a professional service with multiple verticals, list those distinct branches in 'secondary_revenue_streams'. Your generated 'persona_matrix' must reflect users for ALL identified streams.
    
    VALIDATION INSTRUCTION: Ensure the `scale_level` is set to 'Local' if a physical address, city, or regional target is detected. Only use 'Global' for true location-agnostic software/ecommerce.
    
    LOCATION EXTRACTION RULE: For 'discovered_location', you MUST output ONLY the primary City and Country (e.g., 'Milano, Italy' or 'Rome, Italy'). DO NOT output conversational text, explanations, or lists of regions. If national/global brand, set to 'National' or 'Worldwide'.
    
    HYPER-LOCAL ZONES RULE: If the business operates in specific neighborhoods (e.g., 'Prati', 'Trastevere'), or has secondary clinic/store locations, list them exclusively in the 'service_zones' array. Do not include them in the main 'discovered_location' string.
    
    GEOGRAPHIC RULE: If a brand has clinics/stores in multiple cities, set 'discovered_location' to 'National', and list the individual cities cleanly inside the 'service_zones' array. No parentheses.
    """
    
    # ── 2. STRUCTURED OUTPUT CALL WITH PYDANTIC SCHEMA ──
    def _req():
        return client.models.generate_content(
            model='gemini-2.5-flash-lite',
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=OrchestratorOutput,
            )
        )

    try:
        response = execute_with_backoff(_req, max_retries=2, initial_delay=2.0)
        
        # Try native parsed output first, fallback to manual JSON parsing
        output = None
        if hasattr(response, 'parsed') and response.parsed:
            parsed: OrchestratorOutput = response.parsed
            output = parsed.model_dump()
        else:
            output = _safe_parse_json_response(response.text)
        
        if output:
            # ── Entity Decomposition Fields ──
            primary_industry = output.get("primary_industry", primary_industry)
            secondary_revenue_streams = output.get("secondary_revenue_streams", secondary_revenue_streams)
            
            # Backward-compatible: set target_industry = primary_industry for downstream nodes
            target_industry = primary_industry
            
            brand_name = output.get("brand_name", brand_name)
            target_audience_summary = output.get("target_audience_summary", target_audience_summary)
            
            # Handle persona_matrix — normalize from list[PersonaEntry] or raw dict/list
            raw_personas = output.get("persona_matrix", persona_matrix)
            if isinstance(raw_personas, list):
                persona_matrix = {
                    entry.get("persona_label", f"Persona_{i}"): {
                        "intent": entry.get("intent", "unknown"),
                        "revenue_stream": entry.get("revenue_stream", primary_industry)
                    }
                    for i, entry in enumerate(raw_personas)
                    if isinstance(entry, dict)
                }
            elif isinstance(raw_personas, dict):
                persona_matrix = raw_personas
            
            scale_level = output.get("scale_level", scale_level)
            # Normalize enum values to plain strings
            if hasattr(scale_level, 'value'):
                scale_level = scale_level.value
                
            intent_type = output.get("intent_type", intent_type)
            if hasattr(intent_type, 'value'):
                intent_type = intent_type.value
                
            raw_loc = output.get("discovered_location", discovered_location)
            # Strip everything inside parentheses and keep only the first item before a comma
            clean_loc = re.sub(r'\(.*?\)', '', raw_loc).split(',')[0].strip()
            # Fallback if empty
            discovered_location = clean_loc if clean_loc else "National"
            service_zones = output.get("service_zones", service_zones)
            if not isinstance(service_zones, list):
                service_zones = []
            
            streams_display = f" + {', '.join(secondary_revenue_streams)}" if secondary_revenue_streams else ""
            console.print(f"   [green]Success[/green]: Mapped [cyan]{brand_name}[/cyan] as [yellow]{primary_industry}{streams_display}[/yellow] ({scale_level}).")
        else:
            orchestrator_notes = "Prompt output empty or malformed; using defaults."
            console.print(f"   [yellow]Warning[/yellow]: {orchestrator_notes}")
            
    except Exception as e:
        orchestrator_notes = f"Orchestrator prompt failed: {str(e)}"
        console.print(f"   [bold red]NODE_FAILED[/bold red]: {orchestrator_notes}")

    # ── 3. HEURISTIC SAFETY CHECKS ──
    is_global_heuristic = (scale_level == "Global" or "Worldwide" in str(discovered_location))
    global_tlds = [".com", ".net", ".org", ".io", ".co", ".ai"]
    if any(tld + "/" in url.lower() or url.lower().endswith(tld) for tld in global_tlds) and scale_level != "Local":
        is_global_heuristic = True

    if is_global_heuristic:
        scale_level = "Global"
        type_config["location_enforce"] = False

    # ── 4. FINAL STATE UPDATE (Guaranteed Integrity) ──
    state.update({
        "target_industry": target_industry,
        "primary_industry": primary_industry,
        "secondary_revenue_streams": secondary_revenue_streams,
        "brand_name": brand_name,
        "target_audience_summary": target_audience_summary,
        "persona_matrix": persona_matrix,
        "scale_level": scale_level,
        "intent_type": intent_type,
        "discovered_location": discovered_location,
        "service_zones": service_zones,
        "type_config": type_config,
        "orchestrator_notes": orchestrator_notes
    })
    
    return state
