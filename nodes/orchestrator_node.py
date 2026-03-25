import os
import time
import json
from rich.console import Console
from google import genai
from google.genai import types

console = Console()

def process(state: dict) -> dict:
    console.print("[cyan]Orchestrator Node[/cyan]: Starting persona and industry analysis...")
    
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
        return state

    client = genai.Client(api_key=gemini_key)
    
    # Strict 5-second sleep before API call as per specification
    console.print("[yellow]Waiting 5 seconds to respect Gemini API rate limits...[/yellow]")
    time.sleep(5)
    
    prompt = f"""
    Analyze the following URL and determine the business profile.
    URL: {url}
    Locale: {locale} (Analyze and return results natively in this locale language)
    Assumed Type: {business_type} (If 'tech', assume Global/Worldwide. If 'food' or 'freelancer', prioritize local city/region discovery).
    
    Respond STRICTLY in JSON format with exactly these keys:
    "target_industry": string (The main industry or niche)
    "brand_name": string (The recognizable name of the business)
    "target_audience_summary": string (A concise summary of the primary target audience)
    "persona_matrix": dict (A structured breakdown of 1-3 buyer personas)
    "scale_level": string (ONE OF: "Local", "National", "Global")
    "intent_type": string (ONE OF: "Informational", "Transactional", "Navigational")
    "discovered_location": string (The main city, province or region of the business, e.g. 'Roma' or 'Milano'. Return 'Worldwide' if it is a global tech brand.)
    """
    
    max_retries = 1
    for attempt in range(max_retries + 1):
        try:
            response = client.models.generate_content(
                model='gemini-3.1-flash-lite-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            output = json.loads(response.text)
            
            target_industry = output.get("target_industry", "Unavailable")
            brand_name = output.get("brand_name", "Unavailable")
            target_audience_summary = output.get("target_audience_summary", "Unavailable")
            persona_matrix = output.get("persona_matrix", {})
            scale_level = output.get("scale_level", "Local")
            intent_type = output.get("intent_type", "Transactional")
            discovered_location = output.get("discovered_location", "Worldwide")
            
            console.print(f"[green]Orchestrator Node[/green]: Successfully mapped industry, personas and location ({discovered_location}).")
            break
        except Exception as e:
            console.print(f"[yellow]Orchestrator Node[/yellow]: Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries:
                console.print("[yellow]Waiting 5 seconds before retry...[/yellow]")
                time.sleep(5)
            else:
                console.print("[bold red]NODE_FAILED[/bold red]: Orchestrator. Using fallbacks.")
    
    # Apply to state
    state["target_industry"] = target_industry
    state["brand_name"] = brand_name
    state["target_audience_summary"] = target_audience_summary
    state["persona_matrix"] = persona_matrix
    state["scale_level"] = scale_level
    state["intent_type"] = intent_type
    state["discovered_location"] = discovered_location
    
    return state
