import os
import time
import json
from rich.console import Console
from google import genai
from google.genai import types

console = Console()

# Definitve Agency-Grade Taxonomy
BUSINESS_TYPE_MAP = {
    "tech":          {"scale_default": "Global", "query_style": "technical_authority", "location_enforce": False},
    "local_tech":    {"scale_default": "National", "query_style": "geo_technical", "location_enforce": True},
    "food":          {"scale_default": "Local", "query_style": "sensory_experiential", "location_enforce": True},
    "food_blog":     {"scale_default": "National", "query_style": "narrative_recipe", "location_enforce": False},
    "freelancer":    {"scale_default": "Local", "query_style": "personal_expertise", "location_enforce": True},
    "dentist":       {"scale_default": "Local", "query_style": "medical_trust", "location_enforce": True},
    "blog":          {"scale_default": "National", "query_style": "thought_leadership", "location_enforce": False},
}

def process(state: dict) -> dict:
    console.print("[cyan]Orchestrator Node[/cyan]: Starting agency-grade persona and industry analysis...")
    
    # Setup safe fallbacks according to contract
    target_industry = "Unavailable"
    target_audience_summary = "Unavailable"
    persona_matrix = {}
    brand_name = "Unavailable"
    scale_level = "Local"
    intent_type = "Transactional" # Default safe fallback
    
    url = state.get("url", "")
    locale = state.get("locale", "en")
    business_type = state.get("business_type", "tech")
    
    # Get type-specific defaults
    type_config = BUSINESS_TYPE_MAP.get(business_type, BUSINESS_TYPE_MAP["tech"])
    scale_level = type_config["scale_default"]
    location_enforce = type_config["location_enforce"]

    gemini_key = os.getenv("GEMINI_API_KEY")
    if not gemini_key:
        console.print("[bold red]NODE_FAILED[/bold red]: Orchestrator (GEMINI_API_KEY missing). Using fallbacks.")
        state["target_industry"] = target_industry
        state["target_audience_summary"] = target_audience_summary
        state["persona_matrix"] = persona_matrix
        state["scale_level"] = scale_level
        state["intent_type"] = intent_type
        state["brand_name"] = brand_name
        state["discovered_location"] = "Worldwide"
        state["type_config"] = type_config
        return state

    client = genai.Client(api_key=gemini_key)
    
    # Strict 5-second sleep before API call as per specification
    console.print("[yellow]Waiting 5 seconds to respect Gemini API rate limits...[/yellow]")
    time.sleep(5)
    
    prompt = f"""
    Analyze the following URL and determine the business profile for a v4.1 Agency-Grade GEO audit.
    URL: {url}
    Locale: {locale} (Analyze and return results natively in this locale language)
    Assumed Type: {business_type} (Style: {type_config['query_style']}).
    
    Rules for Location Discovery & Scale:
    - Assess the actual brand scale from the URL. Even if business_type is 'food' or 'dentist', if the brand operates nationally or globally (e.g. a multinational chain), set scale_level to 'National' or 'Global' and DO NOT force a local city.
    - If it is genuinely a local business, force discovered_location to city-level (e.g. "Roma, Lazio") and set scale_level to 'Local'.
    - If 'tech' or similar global reach, prioritize Global/Worldwide.

    Respond STRICTLY in JSON format with exactly these keys:
    "target_industry": string (The main industry or niche)
    "brand_name": string (The recognizable name of the business)
    "target_audience_summary": string (A concise summary of the primary target audience)
    "persona_matrix": dict (A structured breakdown of 1-3 buyer personas)
    "scale_level": string (ONE OF: "Local", "National", "Global")
    "intent_type": string (ONE OF: "Informational", "Transactional", "Navigational")
    "discovered_location": string (The main city, province or region. Return 'Worldwide' or 'National' if it operates broadly.)
    """
    
    max_retries = 1
    for attempt in range(max_retries + 1):
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash-lite',
                contents=prompt,
                config=genai.types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            output = json.loads(response.text)
            
            target_industry = output.get("target_industry", "Unavailable")
            brand_name = output.get("brand_name", "Unavailable")
            target_audience_summary = output.get("target_audience_summary", "Unavailable")
            persona_matrix = output.get("persona_matrix", {})
            scale_level = output.get("scale_level", scale_level)
            intent_type = output.get("intent_type", "Transactional")
            discovered_location = output.get("discovered_location", "Worldwide")
            
            console.print(f"[green]Orchestrator Node[/green]: Successfully mapped {business_type} industry for {brand_name} ({discovered_location}).")
            break
        except Exception as e:
            console.print(f"[yellow]Orchestrator Node[/yellow]: Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries:
                console.print("[yellow]Waiting 5 seconds before retry...[/yellow]")
                time.sleep(5)
    
    # v4.3 Global vs Local Fix (Critical Bug)
    is_global_heuristic = False
    if scale_level == "Global" or "Worldwide" in discovered_location:
        is_global_heuristic = True
    
    # If the domain is a major generic TLD and the LLM didn't force a strict local town
    global_tlds = [".com", ".net", ".org", ".io", ".co", ".ai"]
    if any(tld + "/" in url.lower() or url.lower().endswith(tld) for tld in global_tlds) and scale_level != "Local":
        is_global_heuristic = True

    if is_global_heuristic:
        scale_level = "Global"
        type_config["location_enforce"] = False
        console.print("[cyan]Orchestrator Node[/cyan]: Global heuristic triggered. Overriding location_enforce to False.")

    # Apply to state
    state["target_industry"] = target_industry
    state["brand_name"] = brand_name
    state["target_audience_summary"] = target_audience_summary
    state["persona_matrix"] = persona_matrix
    state["scale_level"] = scale_level
    state["intent_type"] = intent_type
    state["discovered_location"] = discovered_location
    state["type_config"] = type_config
    
    return state
