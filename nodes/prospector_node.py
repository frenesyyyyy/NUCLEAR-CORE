import os
import time
import re
import json
import requests
from rich.console import Console
from google import genai
from nodes.api_utils import execute_with_backoff
from nodes.business_profiles import DEFAULT_PROFILE_KEY, normalize_profile_key, get_local_trust_profiles, get_platform_like_profiles

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

def _is_platform_like_context(state: dict) -> bool:
    """Determine if the audited business is a platform/aggregator to adjust filtering."""
    profile_key = state.get("business_profile_key", "")
    profile_key = normalize_profile_key(profile_key)
    target_ind = str(state.get("target_industry", "")).lower()
    bus_type = str(state.get("business_type", "")).lower()
    evidence = " ".join(state.get("classification_evidence", [])).lower()
    
    # Structural clues
    platform_keywords = ["marketplace", "platform", "aggregator", "delivery", "booking", "piattaforma", "onboarding", "network"]
    if profile_key in get_platform_like_profiles(): return True
    if any(k in target_ind for k in platform_keywords): return True
    if any(k in bus_type for k in platform_keywords): return True
    if any(k in evidence for k in platform_keywords): return True
    
    return False

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

def _corroborate_location(state: dict, val_data: dict, serper_results: list) -> tuple[str, str, str, list]:
    """
    Agency-Grade Geo Corroboration.
    Requires:
    1. On-site address match OR
    2. Multiple external directory/review matches
    """
    discovered_loc = val_data.get("validated_location", "Unknown")
    on_site_address = state.get("extracted_on_site_address", "")
    source_mode = state.get("source_of_truth_mode", "hybrid")
    
    evidence = []
    confidence = "low"
    inference_mode = "unverified_offsite"

    if not discovered_loc or discovered_loc.lower() in ["unknown", "worldwide", "national"]:
        return "Unknown", "low", "unverified_offsite", []

    # 1. On-site Corroboration
    if on_site_address:
        # Simple cross-check: see if discovered city is in the extracted address
        city_parts = [p.strip().lower() for p in discovered_loc.split(",")]
        addr_low = on_site_address.lower()
        if any(p in addr_low for p in city_parts if len(p) > 2):
            evidence.append(f"On-site Corroboration: Address '{on_site_address}' matches '{discovered_loc}'")
            confidence = "high"
            inference_mode = "site_verified"

    # 2. External Corroboration (Serper/Review/Directory)
    external_matches = 0
    # Common local directory tokens
    local_tokens = ["miodottore", "idoctors", "prontopro", "paginegialle", "paginebianche", "yelp", "tripadvisor", "dentisti-italia", "avvocati-italia"]
    
    for res in serper_results:
        snippet = res.get("snippet", "").lower()
        title = res.get("title", "").lower()
        link = res.get("link", "").lower()
        
        city_parts = [p.strip().lower() for p in discovered_loc.split(",")]
        found_in_res = any(p in snippet or p in title for p in city_parts if len(p) > 2)
        
        if found_in_res:
            is_directory = any(t in link for t in local_tokens)
            if is_directory:
                external_matches += 1.5 # Weighted heavier
                evidence.append(f"Directory Match: {link}")
            else:
                external_matches += 1
                evidence.append(f"SERP Mention: {res.get('title')}")

    if external_matches >= 2:
        if confidence != "high":
            confidence = "medium"
            inference_mode = "corroborated_offsite"
    
    # Final Decision
    if confidence == "low" and source_mode == "offsite_only":
        # Hard stop for offsite_only with no corroboration
        return "Unknown", "low", "unverified_offsite", evidence

    return discovered_loc, confidence, inference_mode, evidence

def process(state: dict) -> dict:
    console.print("[cyan]Prospector Node[/cyan]: Starting single-call Agency-Grade GEO search...")

    # SAFE NORMALIZE LIST OBJECTS TO PREVENT ATTRIBUTE ERROR
    raw_payload = state.get("client_content_raw", "")
    if isinstance(raw_payload, list):
        normalized = " ".join([str(p.get("html", "")).lower() for p in raw_payload if isinstance(p, dict)])
        state["client_content_raw"] = normalized
    elif isinstance(raw_payload, str):
        state["client_content_raw"] = raw_payload.lower()

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
    source_mode = state.get("source_of_truth_mode", "hybrid")
    
    serper_api_key = os.getenv("SERPER_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")
    
    if not serper_api_key or not gemini_key:
        console.print("[bold red]NODE_FAILED[/bold red]: Prospector (API Keys missing). Using fallbacks.")
        state["raw_data_complete"] = raw_data_complete
        state["external_sources"] = external_sources
        state["prospector_notes"] = "Failed: Missing API keys"
        state["external_data_quality"] = "LOW"
        return state

    # --- CLASSIFICATION VALIDATION LAYER (PHASE 2) ---
    val_data = {}
    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key:
        console.print("[cyan]Prospector Node[/cyan]: Validating orchestrator's blind classification...")
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
            Analyze site evidence to validate industry/brand/location.
            URL: {url} | Initial Brand: {initial_brand} | Initial Industry: {initial_ind}
            On-site Content Fragment: {client_content[:5000]} | Schema: {schema_str}
            Locale: {locale}
            
            CRITICAL RULES:
            1. 'validated_location' MUST be in the target Locale ({locale}).
            2. 'validated_location' MUST be a single City and Country (e.g., 'Milano, Italia') OR 'National'. NO conversational text. NO lists of regions.
            
            Return JSON: validated_brand_name, validated_industry, validated_target_audience_summary, validated_persona_matrix, validated_location, classification_notes.
            """
            def _val_req():
                return client.models.generate_content(
                    model='gemini-2.5-flash-lite',
                    contents=val_prompt,
                    config={"response_mime_type": "application/json"}
                )
            val_res = execute_with_backoff(_val_req, max_retries=2, initial_delay=2.0)
            val_data = json.loads(val_res.text.strip())
            
            target_industry = val_data.get("validated_industry", initial_ind)
            state["brand_name"] = val_data.get("validated_brand_name", initial_brand)
            state["target_industry"] = target_industry
            state["classification_notes"] = val_data.get("classification_notes", "")
        except Exception as e:
            console.print(f"[yellow]Classification validation failed: {e}[/yellow]")

    # --- SERPER GROUNDING ---
    is_platform_context = _is_platform_like_context(state)
    location_enforce = type_config.get("location_enforce", False)
    discovered_location = val_data.get("validated_location", state.get("discovered_location", "Unknown"))
    
    # Preliminary search with potentially unverified location
    is_local_candidate = (scale_level == "Local") and location_enforce and discovered_location and str(discovered_location).lower() not in ["worldwide", "national", "unknown"]

    if locale == "it":
        serper_query = f"migliori {target_industry} a {discovered_location}" if is_local_candidate else f"leader settore {target_industry}"
    else:
        serper_query = f"top {target_industry} in {discovered_location}" if is_local_candidate else f"top {target_industry} industry leaders"

    def generate_external_source_queries(b_name, p_key, b_locale):
        p_key = normalize_profile_key(p_key)
        if p_key == "marketplace_aggregator":
            return [
                f"{b_name} app store",
                f"{b_name} google play",
                f"{b_name} recensioni",
                f"{b_name} trustpilot",
                f"{b_name} news",
                f"{b_name} wikipedia",
                f"{b_name} rider recensioni",
                f"{b_name} partner ristoranti"
            ]
        elif p_key in ("b2b_saas_tech", "professional_services"):
            return [
                f"{b_name} g2",
                f"{b_name} capterra",
                f"{b_name} review",
                f"{b_name} product hunt",
                f"{b_name} integrations",
                f"{b_name} github"
            ]
        elif p_key in get_local_trust_profiles().union({"general_local_business"}):
            return [
                f"{b_name} recensioni",
                f"{b_name} google maps",
                f"{b_name} opinioni",
                f"{b_name} dove si trova"
            ]
        else:
            return [
                f"{b_name} recensioni",
                f"{b_name} wikipedia",
                f"{b_name} news"
            ]

    brand_for_query = state.get("brand_name", "")
    profile_for_query = state.get("business_profile_key", "unknown")
    external_queries = generate_external_source_queries(brand_for_query, profile_for_query, locale)
    all_queries = [serper_query] + external_queries[:5]

    def is_valid_result(res, is_platform_context, is_external_query=False):
        link = res.get("link", "").lower()
        title = res.get("title", "").lower()
        soft_exclusions = ['tripadvisor', 'yelp', 'booking', 'expedia', 'thefork', 'justeat', 'deliveroo', 'glovo', 'ubereats', 'airbnb']
        hard_exclusions = ['amazon', 'trustpilot', 'facebook', 'instagram', 'linkedin', 'github', 'youtube']
        
        # If we are explicitly searching for external ecosystems, we WANT trustpilot, github, etc.
        if not is_external_query and any(d in link for d in hard_exclusions): return False
        
        if not is_platform_context and not is_external_query:
            if any(d in link for d in soft_exclusions): return False
            if "directory" in title or "top 10" in title: return False
        return True

    s_results = []
    seen_domains = set()
    filtered_results = []

    for q_idx, q in enumerate(all_queries):
        is_ext_query = (q_idx > 0)
        for attempt in range(2):
            res_chunk = call_serper(q, str(serper_api_key))
            if res_chunk:
                for r in res_chunk:
                    if is_valid_result(r, is_platform_context, is_external_query=is_ext_query):
                        domain_match = re.search(r'https?://(?:www\.)?([^/]+)', r.get("link", ""))
                        d = domain_match.group(1).lower() if domain_match else r.get("link", "")
                        if d not in seen_domains:
                            seen_domains.add(d)
                            filtered_results.append(r)
                            s_results.append(r)
                break
            else:
                time.sleep(1)

    if filtered_results:
        raw_data_complete["serper_results"] = filtered_results
        external_sources_raw = [
            {
                "url": r.get("link"),
                "title": r.get("title"),
                "snippet": r.get("snippet")
            }
            for r in filtered_results if r.get("link")
        ]
        state["external_sources_raw"] = external_sources_raw
        external_sources = [r.get("link") for r in filtered_results if r.get("link")]
        external_data_quality = "high" if len(filtered_results) >= 5 else "medium"
    else:
        external_data_quality = "low"
        external_sources = []
        state["external_sources_raw"] = []

    # --- GEO CORROBORATION (CRITICAL PATCH) ---
    final_loc, loc_conf, geo_mode, geo_ev = _corroborate_location(state, val_data, s_results)
    state["discovered_location"] = final_loc
    state["location_confidence"] = loc_conf
    state["geo_inference_mode"] = geo_mode
    state["location_evidence"] = geo_ev
    
    # Re-evaluate is_local based on corroboration
    is_local = (scale_level == "Local") and location_enforce and final_loc and final_loc.lower() not in ["worldwide", "national", "unknown"]

    # --- PERPLEXITY INTELLIGENCE ---
    system_role = "You are a Tier-1 GEO Intelligence Analyst. Return ONLY strict JSON."
    lang_instruction = "Respond in Italian." if locale == "it" else "Respond in English."
    anti_noise = "Exclude generic directories and social media."
    if not is_platform_context:
        anti_noise += " Also exclude massive aggregators like TripAdvisor, Booking, JustEat."
    else:
        anti_noise += " Allow direct platform competitors in this category."

    loc_context = f" targeting {final_loc}" if is_local else ""
    agency_query = f"""
    Analyze {target_industry} market for {url}{loc_context}. Scale: {scale_level}.
    {anti_noise} | {lang_instruction}
    Return JSON: competitor_entities (list of 8 proprietary brands), authority_entities (12 protocols/standards), topic_gaps (10), faq_patterns (5).
    """

    console.print(f"Executing Tier-1 Intelligence Retrieval (Geo: {final_loc} | Conf: {loc_conf})...")
    
    # LEGACY PATH: Perplexity implementation retained intentionally for optional future reactivation.
    # raw_intelligence = call_perplexity(system_role, agency_query, str(perplexity_api_key))
    
    # ACTIVE PATH: Serper + Gemini 2.5 Flash Lite
    try:
        gemini_client = genai.Client(api_key=gemini_key)
        serper_context = json.dumps([{"title": r.get("title"), "snippet": r.get("snippet")} for r in s_results])
        
        full_query = f"{system_role}\n\n{agency_query}\n\nContext from live search:\n{serper_context}"
        
        def _intel_req():
            return gemini_client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=full_query,
                config={"response_mime_type": "application/json"}
            )
        res = execute_with_backoff(_intel_req, max_retries=3, initial_delay=3.0)
        raw_intelligence = res.text
    except Exception as e:
        console.print(f"[yellow]Gemini Intelligence Error[/yellow]: {e}")
        raw_intelligence = "{}"
    
    try:
        match = re.search(r'```(?:json)?(.*?)```', raw_intelligence, re.DOTALL | re.IGNORECASE)
        json_str = match.group(1).strip() if match else raw_intelligence.strip()
        intel_data = json.loads(json_str)
        def _force_flat_strings(raw_list: list) -> list[str]:
            """Flattens dicts into strings (taking the first value) to prevent downstream join() crashes."""
            clean = []
            for item in raw_list:
                if isinstance(item, str):
                    clean.append(item.strip())
                elif isinstance(item, dict):
                    # Extract the first valid string from the dictionary (e.g. {'name': 'Humanitas'} -> 'Humanitas')
                    vals = [str(v).strip() for v in item.values() if v]
                    if vals:
                        clean.append(vals[0])
            return clean

        # Extract and sanitize all arrays
        comp_list = _force_flat_strings(intel_data.get("competitor_entities", []))
        auth_list = _force_flat_strings(intel_data.get("authority_entities", []))
        topic_list = _force_flat_strings(intel_data.get("topic_gaps", []))
        faq_list = _force_flat_strings(intel_data.get("faq_patterns", []))

        # Apply the junk filter to competitors
        junk = ['facebook', 'instagram', 'linkedin', 'twitter', 'amazon', 'youtube']
        if not is_platform_context:
            junk.extend(['yelp', 'tripadvisor', 'booking', 'justeat', 'deliveroo', 'glovo'])
            
        raw_data_complete["competitor_entities"] = [c for c in comp_list if not any(j in c.lower() for j in junk)]
        raw_data_complete["authority_entities"] = auth_list
        raw_data_complete["topic_gaps"] = topic_list
        raw_data_complete["faq_patterns"] = faq_list
        raw_data_complete["perplexity_summary"] = f"Mapped {len(raw_data_complete['competitor_entities'])} competitors."
    except Exception as e:
        console.print(f"[bold red]Intelligence Parsing Failed[/bold red]: {e}")
        external_data_quality = "low"

    raw_data_complete["source_urls"] = external_sources
    state.update({
        "raw_data_complete": raw_data_complete,
        "external_sources": external_sources,
        "prospector_notes": f"v4.5 Hardening: Geo-corroboration applied ({geo_mode}).",
        "external_data_quality": external_data_quality
    })
    
    return state
