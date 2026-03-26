import os
import time
import re
import json
import requests
from rich.console import Console
from google import genai
from nodes.api_utils import execute_with_backoff

console = Console()

def call_serper(query: str, api_key: str) -> list:
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query, "num": 10})
    headers = {
        'X-API-KEY': api_key,
        'Content-Type': 'application/json'
    }
    def _req():
        response = requests.post(url, headers=headers, data=payload, timeout=15)
        response.raise_for_status()
        return response.json().get("organic", [])
    try:
        return execute_with_backoff(_req, max_retries=3, initial_delay=2.0)
    except Exception as e:
        console.print(f"[yellow]Serper API Error[/yellow]: {e}")
        return []

def call_perplexity(system_prompt: str, user_prompt: str, api_key: str) -> str:
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
    def _req():
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    try:
        return execute_with_backoff(_req, max_retries=3, initial_delay=3.0)
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

    # --- CLASSIFICATION VALIDATION LAYER (PHASE 2) ---
    # Moved here so Prospector searches for the TRUE industry, preventing Market Poisoning.
    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key:
        console.print("[cyan]Prospector Node[/cyan]: Validating orchestrator's blind classification against deep content before searching...")
        try:
            client = genai.Client(api_key=gemini_key)
            initial_brand = state.get("brand_name", "Unknown")
            initial_ind = state.get("target_industry", "Unknown")
            initial_loc = state.get("discovered_location", "Unknown")
            
            client_content = state.get("client_content_clean", "")
            og_title = state.get("og_tags", {}).get("og:title", "")
            schema_counts = state.get("schema_type_counts", {})
            schema_str = ", ".join(schema_counts.keys()) if schema_counts else "None"
            
            val_prompt = f"""
            You are an AI data classification expert validating the initial blind categorization of a website.
            Initial Classification (guessed from URL only):
            - Brand: {initial_brand}
            - Industry: {initial_ind}
            - Location: {initial_loc}

            Deep Evidence (Scraped HTML & Schema):
            - Schema Types: {schema_str}
            - OG Title: {og_title}
            - Content Snippet: {client_content[:5000]}
            
            Evaluate if the initial classification is accurate. Generic assumptions like "Tech" often mask a specific niche (e.g., "Restaurant Delivery Software"). Give the TRUE, HIGHLY SPECIFIC industry and the exact proper brand name used on the site.
            
            CRITICAL RULES FOR CLASSIFICATION:
            1. Scrutinize the H1/H2 headings in the Content Snippet. They often contain the exact value proposition.
            2. If 'Organization' or 'LocalBusiness' Schema is present, trust its industry definition immediately.
            3. Ignore generic marketing boilerplate (e.g. "We deliver excellence"); focus on the literal product/service described.
            4. Only set Location to a city/region if the business relies on foot traffic or local territory constraints. If it is global SaaS, return 'Worldwide'.
            
            Return STRICTLY JSON with exact keys:
            "validated_brand_name": string (The exact, correct brand name)
            "validated_industry": string (The highly specific industry/niche)
            "validated_target_audience_summary": string (A concise summary of the primary target audience matching the validated industry)
            "validated_persona_matrix": dict (A structured breakdown of 1-3 buyer personas matching the validated industry)
            "validated_location": string (The operating location, or 'Worldwide' if global)
            "classification_notes": string (Why you changed it, or 'Confirmed initial classification.')
            """
            def _val_req():
                return client.models.generate_content(
                    model='gemini-2.5-flash-lite',
                    contents=val_prompt,
                    config={"response_mime_type": "application/json"}
                )
            val_res = execute_with_backoff(_val_req, max_retries=2, initial_delay=2.0)
            clean_val = val_res.text.strip()
            if clean_val.startswith("```json"): clean_val = clean_val[7:]
            if clean_val.startswith("```"): clean_val = clean_val[3:]
            if clean_val.endswith("```"): clean_val = clean_val[:-3]
            
            val_data = json.loads(clean_val.strip())
            
            # Immediately overwrite the generic state variables
            target_industry = val_data.get("validated_industry", initial_ind)
            state["brand_name"] = val_data.get("validated_brand_name", initial_brand)
            state["target_industry"] = target_industry
            state["discovered_location"] = val_data.get("validated_location", initial_loc)
            if val_data.get("validated_target_audience_summary"):
                state["target_audience_summary"] = val_data.get("validated_target_audience_summary")
            if val_data.get("validated_persona_matrix"):
                state["persona_matrix"] = val_data.get("validated_persona_matrix")
            state["classification_notes"] = val_data.get("classification_notes", "")
            
            console.print(f"  [green]Validated Brand[/green]: {state['brand_name']} | [green]Validated Niche[/green]: {target_industry}")
        except Exception as e:
            console.print(f"[yellow]Classification validation failed, continuing with initial guesses: {e}[/yellow]")
    # ------------------------------------------------

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

    anti_noise = "FATAL RULE: Strictly exclude all review sites, software directories (like Capterra, G2), leaderboards, blog/news media, and aggregators (Amazon, Tripadvisor, Yelp, Trustpilot). Extract ONLY proprietary business entities."

    agency_query = f"""
    You are a Tier-1 GEO Intelligence Analyst. 
    Target: {url} | Highly Specific Industry/Niche: {target_industry} | Scale: {scale_level} | Business Type: {state.get('business_type')}
    {location_clause}
    {anti_noise}
    {lang_instruction}

    Analyze the precise niche market and return STRICT JSON with exactly these keys:
    "competitor_entities": list of top 8 proprietary brand names (companies) competing directly IN THIS EXACT NICHE. Do NOT return generic massive companies unless they perfectly match this specific niche.
    "authority_entities": list of 12 industry-defining technical standards, certifications, or specialized protocols for this niche.
    "topic_gaps": list of 10 highly specific technical or content gaps vs niche leaders.
    "faq_patterns": list of 5 most common user intent patterns for this niche.
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
