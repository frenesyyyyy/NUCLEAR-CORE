import os
import time
import json
import re
import copy
from rich.console import Console
from google import genai
from google.genai import types

console = Console()

# Definitive Agency-Grade Taxonomy
BUSINESS_TYPE_MAP = {
    "tech":          {"scale_default": "Global", "query_style": "technical_authority", "location_enforce": False},
    "local_tech":    {"scale_default": "National", "query_style": "geo_technical", "location_enforce": True},
    "food":          {"scale_default": "Local", "query_style": "sensory_experiential", "location_enforce": True},
    "food_blog":     {"scale_default": "National", "query_style": "narrative_recipe", "location_enforce": False},
    "freelancer":    {"scale_default": "Local", "query_style": "personal_expertise", "location_enforce": True},
    "dentist":       {"scale_default": "Local", "query_style": "medical_trust", "location_enforce": True},
    "blog":          {"scale_default": "National", "query_style": "thought_leadership", "location_enforce": False},
}

def _safe_parse_json_response(raw: str) -> dict:
    """ defensive JSON parser to strip markdown fences and handle malformed strings. """
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
        # Fallback to finding the first { and last }
        match = re.search(r'(\{.*\})', clean, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except:
                pass
    return {}

def process(state: dict) -> dict:
    console.print("[cyan]Orchestrator Node[/cyan]: Starting agency-grade persona and industry analysis...")
    
    # ── 1. INITIALIZE ALL FALLBACK LOCALS SAFELY (v4.5 Hotfix) ──
    brand_name = "Unavailable"
    target_industry = "Unavailable"
    target_audience_summary = "Unavailable"
    persona_matrix = {}
    intent_type = "Transactional"
    discovered_location = "Worldwide"
    orchestrator_notes = ""
    
    url = state.get("url", "")
    locale = state.get("locale", "en")
    business_type = state.get("business_type", "tech")
    
    # Defaults based on type
    type_config = copy.deepcopy(BUSINESS_TYPE_MAP.get(business_type, BUSINESS_TYPE_MAP["tech"]))
    scale_level = type_config.get("scale_default", "Local")

    gemini_key = os.getenv("GEMINI_API_KEY")
    if not gemini_key:
        console.print("[bold red]NODE_FAILED[/bold red]: API Key missing. Using fallbacks.")
        state.update({
            "target_industry": target_industry,
            "brand_name": brand_name,
            "target_audience_summary": target_audience_summary,
            "persona_matrix": persona_matrix,
            "scale_level": scale_level,
            "intent_type": intent_type,
            "discovered_location": discovered_location,
            "type_config": type_config,
            "orchestrator_notes": "API Key Missing"
        })
        return state

    client = genai.Client(api_key=gemini_key)
    from nodes.api_utils import execute_with_backoff
    
    prompt = f"""
    Analyze the following URL and determine the business profile for a v4.5 Agency-Grade GEO audit.
    URL: {url}
    Locale: {locale}
    Assumed Type: {business_type} (Style: {type_config['query_style']}).
    
    Rules for Location Discovery:
    - If national/global brand, set discovered_location to 'National' or 'Worldwide'.
    - If local business, force city-level location (e.g. "Milano").
    
    Respond STRICTLY in JSON:
    {{
        "target_industry": string,
        "brand_name": string,
        "target_audience_summary": string,
        "persona_matrix": dict,
        "scale_level": "Local" | "National" | "Global",
        "intent_type": "Informational" | "Transactional" | "Navigational",
        "discovered_location": string
    }}
    """
    
    def _req():
        return client.models.generate_content(
            model='gemini-2.5-flash-lite',
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )

    # ── 2. DEFENSIVE EXECUTION & PARSING ──
    try:
        response = execute_with_backoff(_req, max_retries=2, initial_delay=2.0)
        output = _safe_parse_json_response(response.text)
        
        if output:
            target_industry = output.get("target_industry", target_industry)
            brand_name = output.get("brand_name", brand_name)
            target_audience_summary = output.get("target_audience_summary", target_audience_summary)
            persona_matrix = output.get("persona_matrix", persona_matrix)
            scale_level = output.get("scale_level", scale_level)
            intent_type = output.get("intent_type", intent_type)
            discovered_location = output.get("discovered_location", discovered_location)
            console.print(f"   [green]Success[/green]: Mapped {brand_name} as {target_industry} ({scale_level}).")
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
        "brand_name": brand_name,
        "target_audience_summary": target_audience_summary,
        "persona_matrix": persona_matrix,
        "scale_level": scale_level,
        "intent_type": intent_type,
        "discovered_location": discovered_location,
        "type_config": type_config,
        "orchestrator_notes": orchestrator_notes
    })
    
    return state
