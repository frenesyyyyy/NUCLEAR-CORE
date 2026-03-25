import os
import time
import re
import json
import requests
from rich.console import Console

console = Console()

def call_serper(query: str, api_key: str) -> list:
    console.print("[yellow]Waiting 5 seconds to respect Serper API rate limits...[/yellow]")
    time.sleep(5)
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query, "num": 10})
    headers = {
        'X-API-KEY': api_key,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get("organic", [])
    except Exception as e:
        console.print(f"[yellow]Serper API Error[/yellow]: {e}")
        return []

def call_perplexity(system_prompt: str, user_prompt: str, api_key: str) -> str:
    console.print("[yellow]Waiting 5 seconds to respect Perplexity API rate limits...[/yellow]")
    time.sleep(5)
    url = "https://api.perplexity.ai/chat/completions"
    payload = {
        "model": "sonar-pro",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        console.print(f"[yellow]Perplexity API Error[/yellow]: {e}")
        return ""

def process(state: dict) -> dict:
    console.print("[cyan]Prospector Node[/cyan]: Starting single-call Agency-Grade GEO search...")

    # Fallbacks and default schema strictly defined
    raw_data_complete = {
        "perplexity_summary": "",
        "serper_results": [],
        "competitor_entities": [],
        "authority_entities": [],
        "topic_gaps": [],
        "faq_patterns": [],
        "source_urls": [],
        "raw_notes": []
    }
    
    external_sources = []
    prospector_notes = "Default fallbacks used."
    external_data_quality = "high"
    
    url = state.get("url", "Unknown")
    locale = state.get("locale", "en")
    target_industry = state.get("target_industry", "general content")
    scale_level = state.get("scale_level", "National")
    type_config = state.get("type_config", {})
    
    serper_api_key = os.getenv("SERPER_API_KEY")
    perplexity_api_key = os.getenv("PERPLEXITY_API_KEY")
    
    if not serper_api_key or not perplexity_api_key:
        console.print("[bold red]NODE_FAILED[/bold red]: Prospector (API Keys missing). Using fallbacks.")
        state["raw_data_complete"] = raw_data_complete
        state["external_sources"] = external_sources
        state["prospector_notes"] = "Failed: Missing API keys"
        state["external_data_quality"] = "LOW"
        return state

    # Perform Serper Call (Grounding Research)
    console.print("Fetching SERPER results for grounding...")
    location_enforce = type_config.get("location_enforce", False)
    discovered_location = state.get("discovered_location", "")
    
    # Do not force local market query for global/national brands
    is_local = (scale_level == "Local") and location_enforce and discovered_location and discovered_location.lower() not in ["worldwide", "national"]

    if locale == "it":
        if is_local:
            serper_query = f"eccellenze per {target_industry} a {discovered_location}"
        else:
            if scale_level == "Global":
                serper_query = f"top brand mondiali {target_industry}"
            else:
                serper_query = f"eccellenze e leader per {target_industry}"
    else:
        if is_local:
            serper_query = f"top {target_industry} businesses in {discovered_location}"
        else:
            if scale_level == "Global":
                serper_query = f"top global {target_industry} industry leaders"
            else:
                serper_query = f"top {target_industry} industry leaders"
            
    def is_valid_result(res):
        link = res.get("link", "").lower()
        title = res.get("title", "").lower()
        invalid_domains = ['amazon', 'tripadvisor', 'yelp', 'trustpilot', 'booking', 'expedia', 'zillow', 'yellowpages', 'facebook', 'instagram', 'linkedin', 'thefork', 'justeat', 'deliveroo', 'glovo']
        if any(d in link for d in invalid_domains): return False
        if "directory" in title or "top 10" in title or "best 10" in title or "migliori 10" in title: return False
        return True

    for attempt in range(2):
        s_results = call_serper(serper_query, str(serper_api_key))
        if s_results:
            seen_domains = set()
            filtered_results = []
            for r in s_results:
                if is_valid_result(r):
                    domain_match = re.search(r'https?://(?:www\.)?([^/]+)', r.get("link", ""))
                    d = domain_match.group(1).lower() if domain_match else r.get("link", "")
                    if d not in seen_domains:
                        seen_domains.add(d)
                        filtered_results.append(r)
            raw_data_complete["serper_results"] = filtered_results
            external_sources = [r.get("link") for r in filtered_results if r.get("link")]
            
            if len(filtered_results) < 4:
                external_data_quality = "low"
            elif len(filtered_results) < 7:
                external_data_quality = "medium"
            break
        else:
            console.print("[yellow]Serper retry...[/yellow]")
            if attempt == 1:
                external_data_quality = "low"

    # Single-Stage Tier-1 GEO Intelligence Call
    system_role = "You are a Tier-1 GEO Intelligence Analyst. You return ONLY strict JSON. No conversational filler."
    lang_instruction = "Respond entirely in Italian while preserving proper nouns and brand names." if locale == "it" else "Respond in English."
    
    # Agency Prompt Construction
    location_clause = ""
    if location_enforce and discovered_location:
        location_clause = f"CRITICAL: Context is strictly limited to the location '{discovered_location}'. Extract only nearby results."

    anti_noise = "FATAL RULE: Strictly exclude all review sites, leaderboards, blog/news media, and aggregators (Amazon, Tripadvisor, Yelp, Trustpilot). Extract ONLY proprietary business entities."

    agency_query = f"""
    You are a Tier-1 GEO Intelligence Analyst. 
    Target: {url} | Industry: {target_industry} | Scale: {scale_level} | Business Type: {state.get('business_type')}
    {location_clause}
    {anti_noise}
    {lang_instruction}

    Analyze the market and return STRICT JSON with exactly these keys:
    "competitor_entities": list of top 8 proprietary brand names (companies) competing directly.
    "authority_entities": list of 12 industry-defining technical standards, certifications, or specialized protocols.
    "topic_gaps": list of 10 specific technical or content gaps vs leaders.
    "faq_patterns": list of 5 most common user intent patterns.
    """

    console.print(f"Executing Tier-1 Intelligence Retrieval for {scale_level} scope...")
    raw_intelligence = call_perplexity(system_role, agency_query, str(perplexity_api_key))
    
    # Extract JSON
    try:
        if "INSUFFICIENT" in raw_intelligence or "insufficient" in raw_intelligence.lower():
            external_data_quality = "low"
            
        match = re.search(r'```(?:json)?(.*?)```', raw_intelligence, re.DOTALL | re.IGNORECASE)
        json_str = match.group(1).strip() if match else raw_intelligence.strip()
        intel_data = json.loads(json_str)
        
        def normalize_entity(e):
            return re.sub(r'[^\w\s]', '', str(e).lower().strip())
            
        directories = ['yelp', 'tripadvisor', 'amazon', 'booking', 'expedia', 'zillow', 'yellowpages', 'facebook', 'thefork', 'justeat', 'deliveroo', 'glovo', 'trustpilot', 'ebay', 'etsy']
        
        raw_comp = [str(e).strip() for e in intel_data.get("competitor_entities", [])]
        raw_auth = [str(e).strip() for e in intel_data.get("authority_entities", [])]
        
        raw_data_complete["competitor_entities"] = [normalize_entity(e) for e in raw_comp if not any(d in e.lower() for d in directories)]
        raw_data_complete["authority_entities"] = [normalize_entity(e) for e in raw_auth if not any(d in e.lower() for d in directories)]
        raw_data_complete["topic_gaps"] = intel_data.get("topic_gaps", [])
        raw_data_complete["faq_patterns"] = intel_data.get("faq_patterns", [])
        raw_data_complete["perplexity_summary"] = f"Intelligence Mapping Success: {len(raw_data_complete['competitor_entities'])} competitors."
        
        comp_len = len(raw_data_complete['competitor_entities'])
        if comp_len < 2:
            external_data_quality = "low"
        elif comp_len < 5 and external_data_quality != "low":
            external_data_quality = "medium"
            
        console.print(f"[green]Prospector Node[/green]: Successfully mapped {comp_len} competitors and {len(raw_data_complete['authority_entities'])} authority nodes.")
    except Exception as e:
        console.print(f"[bold red]Agency Intelligence Parsing Failed[/bold red]: {e}")
        raw_data_complete["perplexity_summary"] = "Intelligence Retrieval Failed."
        external_data_quality = "low"

    raw_data_complete["source_urls"] = external_sources
    raw_data_complete["raw_notes"] = [raw_intelligence]
    
    state["raw_data_complete"] = raw_data_complete
    state["external_sources"] = external_sources
    state["prospector_notes"] = f"Successfully executed Tier-1 {scale_level} Intelligence Retrieval."
    state["external_data_quality"] = external_data_quality
    
    return state
