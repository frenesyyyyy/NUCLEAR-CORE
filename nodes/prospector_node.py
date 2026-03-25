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
    console.print("[cyan]Prospector Node[/cyan]: Starting multi-call GEO search...")

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
    
    url = state.get("url", "Unknown")
    locale = state.get("locale", "en")
    target_industry = state.get("target_industry", "general content")
    scale_level = state.get("scale_level", "National")
    
    serper_api_key = os.getenv("SERPER_API_KEY")
    perplexity_api_key = os.getenv("PERPLEXITY_API_KEY")
    
    if not serper_api_key or not perplexity_api_key:
        console.print("[bold red]NODE_FAILED[/bold red]: Prospector (API Keys missing). Using fallbacks.")
        state["raw_data_complete"] = raw_data_complete
        state["external_sources"] = external_sources
        state["prospector_notes"] = "Failed: Missing API keys"
        return state

    # --- Helper: Parse and Structure Data via JSON extraction ---
    def extract_json_array(text: str, key: str) -> list:
        try:
            match = re.search(r'```(?:json)?(.*?)```', text, re.DOTALL | re.IGNORECASE)
            json_str = match.group(1).strip() if match else text.strip()
            data = json.loads(json_str)
            return data.get(key, [])
        except Exception as e:
            console.print(f"[red]JSON Parse Failed for key '{key}'[/red]: {e}")
            return []

    # Perform Serper Call
    console.print("Fetching SERPER results...")
    serper_query = f"{target_industry} best websites" if locale == "en" else f"migliori siti web per {target_industry}"
    for attempt in range(2):
        s_results = call_serper(serper_query, str(serper_api_key))
        if s_results:
            raw_data_complete["serper_results"] = s_results
            external_sources = [r.get("link") for r in s_results if "link" in r]
            break
        else:
            console.print("[yellow]Serper retry...[/yellow]")

    # Prepare Perplexity Prompts
    system_role = "You are a strict Data Miner API. You only extract hard empirical data from SERP results. No SEO/GEO analysis. Strict JSON only."
    lang_instruction = "Respond entirely in Italian while preserving proper nouns and brand names." if locale == "it" else "Respond in English."

    adaptive_scope = f"The business scale is '{scale_level}'. If Local, extract only nearby competitors. If Global/National, focus on major market leaders."

    # Multi-Stage Waterfall Search
    anti_noise = "FATAL RULE: Strictly exclude all review sites, leaderboards, blog/news media, and aggregators (Amazon, Tripadvisor, Yelp, Trustpilot). Extract ONLY proprietary business entities."
    
    # Call 1: Brand Axis (Direct Competitors)
    user_query_1 = f"Identify the top 5 proprietary brand names (companies) competing directly with {url} in the {target_industry} market. {anti_noise} {lang_instruction} Respond with JSON: {{ \"competitors\": [\"Name 1\", \"Name 2\"] }}"
    
    # Call 2: Authority Axis (Technical Nodes)
    user_query_2 = f"Identify 5 industry-defining technical standards, certifications, and specialized service protocols that define authority for {target_industry} leaders. {lang_instruction} Respond with JSON: {{ \"industry_entities\": [\"Standard 1\", \"Cert 2\"] }}"

    # Call 3: Gap & Intent Axis
    user_query_3 = f"Analyze deep content gaps and user FAQs for {target_industry} compared to market leaders. {lang_instruction} Respond with JSON: {{ \"topic_gaps\": [\"Gap 1\"], \"faq_patterns\": [\"FAQ 1\"] }}"

    console.print("Waterfall Phase 1: Brand Axis...")
    res_1 = call_perplexity(system_role, user_query_1, str(perplexity_api_key))
    
    console.print("Waterfall Phase 2: Authority Axis...")
    res_2 = call_perplexity(system_role, user_query_2, str(perplexity_api_key))
    
    console.print("Waterfall Phase 3: Gap & Intent Axis...")
    res_3 = call_perplexity(system_role, user_query_3, str(perplexity_api_key))

    # Parse raw results
    raw_brands = extract_json_array(res_1, "competitors")
    raw_authority = extract_json_array(res_2, "industry_entities")
    gaps = extract_json_array(res_3, "topic_gaps")
    faqs = extract_json_array(res_3, "faq_patterns")

    # PHASE 4: Taxonomy Classification (The Knowledge Filter)
    all_raw_entities = list(set(raw_brands + raw_authority))
    console.print(f"Waterfall Phase 4: Classifying {len(all_raw_entities)} entities for noise reduction...")
    
    classification_query = f"Classify this list of entities related to {target_industry} into a strict taxonomy: PROPRIETARY_BRAND, TECHNICAL_STANDARD, or NOISE (aggregators, review sites, news). Entities: {all_raw_entities}. respond ONLY with JSON: {{ \"classified\": [{{ \"name\": \"...\", \"type\": \"...\" }}] }}"
    res_class = call_perplexity(system_role, classification_query, str(perplexity_api_key))
    
    classified_data = extract_json_array(res_class, "classified")
    
    # Filter: Keep only high-value types and separate them
    clean_brands = []
    clean_authority = []
    for item in classified_data:
        if isinstance(item, dict):
            e_type = item.get("type", "").upper()
            if e_type == "PROPRIETARY_BRAND":
                clean_brands.append(item.get("name"))
            elif e_type in ["TECHNICAL_STANDARD", "AUTHORITY_NODE"]:
                clean_authority.append(item.get("name"))

    raw_data_complete["competitor_entities"] = clean_brands if clean_brands else ["No brands passed classification"]
    raw_data_complete["authority_entities"] = clean_authority if clean_authority else ["No authority standards passed classification"]
    raw_data_complete["topic_gaps"] = gaps
    raw_data_complete["faq_patterns"] = faqs
    
    summary_text = ""
    if res_1 and res_2 and res_3:
        summary_text = f"Waterfall Discovery Complete. Phases 1-4 executed."
    else:
        summary_text = f"Partial Waterfall Discovery. Errors in phases."
        
    raw_data_complete["perplexity_summary"] = summary_text
    raw_data_complete["source_urls"] = external_sources
    raw_data_complete["raw_notes"] = [res_1, res_2, res_3, res_class]
    
    prospector_notes = "Successfully ran Waterfall GEO context retrieval & classification."
    if not res_1 and not res_2 and not s_results:
        console.print("[bold red]NODE_FAILED[/bold red]: Prospector retrieved no external data.")
        prospector_notes = "Failed to retrieve any data."
    
    # Assign safely
    state["raw_data_complete"] = raw_data_complete
    state["external_sources"] = external_sources
    state["prospector_notes"] = prospector_notes
    
    return state
