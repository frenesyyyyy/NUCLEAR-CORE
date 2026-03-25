import os
# Must be set BEFORE down-stream imports initialize the Hub constants
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "0"
os.environ["HF_HOME"] = os.path.join(os.getcwd(), "hf_cache")

import time
import re
import json
import requests
import chromadb
from sentence_transformers import SentenceTransformer
from rich.console import Console
from google import genai

console = Console()

def process(state: dict) -> dict:
    console.print("[cyan]Researcher Node[/cyan]: Starting deep GEO analysis & metrics calculation...")
    
    # Fallbacks and default returns defined early
    metrics = {
        "Entity Consensus": 0,
        "Information Gain": 0,
        "Hallucination Risk": 100
    }
    citation_status = "Low Verification"
    projected_traffic_lift = "0%"
    geo_recommendation_pack = "Unavailable"
    research_output = "Research failed or skipped."

    url = state.get("url", "Unknown")
    locale = state.get("locale", "en")
    target_industry = state.get("target_industry", "Unknown")
    
    gemini_key = os.getenv("GEMINI_API_KEY")
    if not gemini_key:
        console.print("[bold red]NODE_FAILED[/bold red]: Researcher (GEMINI_API_KEY missing). Using fallbacks.")
        state["metrics"] = metrics
        state["citation_status"] = citation_status
        state["projected_traffic_lift"] = projected_traffic_lift
        state["geo_recommendation_pack"] = geo_recommendation_pack
        state["research_output"] = research_output
        return state

    try:
        # 1. Setup ChromaDB Local Persistent Storage
        console.print("[cyan]Researcher Node[/cyan]: Initializing ChromaDB...")
        os.makedirs("chroma_db", exist_ok=True)
        chroma_client = chromadb.PersistentClient(path="./chroma_db/")
        
        model_name = "intfloat/multilingual-e5-large" if locale == "it" else "all-MiniLM-L6-v2"
        console.print(f"[cyan]Researcher Node[/cyan]: Loading embedding model ({model_name})...")
        
        local_cache = os.path.join(os.getcwd(), "hf_cache")
        embedding_model = SentenceTransformer(model_name, cache_folder=local_cache)
        
        class CustomEmbeddingFunction(chromadb.EmbeddingFunction):
            def __call__(self, input: chromadb.Documents) -> chromadb.Embeddings:
                return embedding_model.encode(input).tolist()
                
        emb_fn = CustomEmbeddingFunction()
        
        collection_name = f"geo_audit_{int(time.time())}"
        collection = chroma_client.get_or_create_collection(
            name=collection_name, 
            embedding_function=emb_fn
        )
        
        # 2. Ingest Data
        client_content = state.get("client_content_clean", "")
        prospector_notes = state.get("raw_data_complete", {}).get("perplexity_summary", "")
        
        if client_content:
            content_to_embed = client_content[:10000]
            chunks = [content_to_embed[i:i+500] for i in range(0, len(content_to_embed), 500)]
            if chunks:
                collection.upsert(
                    documents=chunks,
                    metadatas=[{"source": "client_website", "url": url}] * len(chunks),
                    ids=[f"client_content_{state.get('run_id', '0')}_{i}" for i in range(len(chunks))]
                )
            
        console.print("[cyan]Researcher Node[/cyan]: Data successfully embedded.")

        # 3. v7.0 METRICS ENGINE (The 50/50 Scalable Matrix)
        raw_data = state.get("raw_data_complete", {})
        competitor_ents = raw_data.get("competitor_entities", [])
        authority_ents = raw_data.get("authority_entities", [])
        topic_gaps = raw_data.get("topic_gaps", [])
        
        brand_name = state.get("brand_name", "Unknown")
        scale_level = state.get("scale_level", "National")
        intent_type = state.get("intent_type", "Transactional")

        def call_perplexity_researcher(system_prompt: str, user_prompt: str) -> str:
            api_key = os.getenv("PERPLEXITY_API_KEY")
            if not api_key: return ""
            time.sleep(5)
            payload = {
                "model": "sonar-pro",
                "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}]
            }
            try:
                res = requests.post("https://api.perplexity.ai/chat/completions", 
                                  json=payload, 
                                  headers={"Authorization": f"Bearer {api_key}"}, 
                                  timeout=30)
                return res.json()["choices"][0]["message"]["content"]
            except: return ""

        # --- HEURISTIC 1: GROUNDING SCANNER (25%) ---
        console.print("[cyan]Researcher Node[/cyan]: Scanning Grounding Anchors (v7.0)...")
        grounding_score = 0
        content_lower = client_content.lower()
        client_content_lower = content_lower # Variable fix
        
        if re.search(r'\d{11}', content_lower): grounding_score += 7  # VAT
        if any(x in content_lower for x in ["pec:", "@pec"]): grounding_score += 5
        if any(x in content_lower for x in ["via ", "piazza ", "viale ", "street ", "road "]): grounding_score += 8
        if any(x in content_lower for x in ["sede legale", "privacy policy", "registered office"]): grounding_score += 5
        
        # --- HEURISTIC 2: AI VISIBILITY WATERFALL (50%) ---
        console.print(f"[cyan]Researcher Node[/cyan]: Executing AI Stress Test for '{brand_name}'...")
        visibility_points = 0
        
        business_type = state.get("business_type", "tech")
        discovered_location = state.get("discovered_location", "Worldwide")
        
        lang_name = "Italian" if locale == "it" else "English"
        
        # Location Enforcement Logic for v7.2
        location_clause = ""
        if business_type in ["food", "freelancer"] and discovered_location != "Worldwide":
            location_clause = f"CRITICAL: Every query MUST explicitly include the location '{discovered_location}' (e.g., 'a {discovered_location}' or 'in {discovered_location}')."
        
        waterfall_gen_prompt = f"""
        Generate 4 highly relevant search queries in {lang_name} to test the AI visibility of '{brand_name}' in the '{target_industry}' sector. 
        Brand scale: '{scale_level}'. Business Type: '{business_type}'. Discovered Location: '{discovered_location}'.
        
        {location_clause}
        
        CRITICAL RULES:
        1. All queries MUST be in {lang_name}.
        2. DO NOT mention the brand name '{brand_name}' in any query.
        3. Level 1 specificity MUST be high (start at niche-depth).
        4. Specificity Levels:
           - L1: Niche intent (niche-depth solution).
           - L2: Benefit/Scenario-oriented (specific user problem).
           - L3: Regional/Contextual (deeply grounded in '{discovered_location}').
           - L4: Hyper-specific technical value/Authority production.
        
        Respond ONLY with a JSON object: {{'queries': [{{'level': 1, 'query': '...'}}, ...]}}
        """
        
        gemini_client = genai.Client(api_key=gemini_key)
        try:
            w_res = gemini_client.models.generate_content(model='gemini-3.1-flash-lite-preview', contents=waterfall_gen_prompt, config={"response_mime_type": "application/json"})
            queries = json.loads(w_res.text).get("queries", [])
            levels_points = {1: 20, 2: 15, 3: 10, 4: 5}
            
            for q_obj in queries:
                lvl = q_obj.get("level", 1)
                q_text = q_obj.get("query", "")
                console.print(f"  Level {lvl} Query: {q_text}")
                perp_res = call_perplexity_researcher("Search engine assistant.", q_text)
                if brand_name.lower() in perp_res.lower() or url.split(".")[1] in perp_res.lower():
                    visibility_points += levels_points.get(lvl, 0)
                    console.print(f"  [green]MATCH FOUND[/green] at Level {lvl}")
        except Exception as e:
            console.print(f"[yellow]Waterfall Error[/yellow]: {e}")

        # --- HEURISTIC 3: AUTHORITY ENTITIES (25%) ---
        matched_auth = sum(1 for a in authority_ents if str(a).lower() in content_lower)
        matched_comp = sum(1 for c in competitor_ents if str(c).lower() in content_lower)
        
        auth_score = (matched_auth / max(len(authority_ents), 1) * 20) + (matched_comp / max(len(competitor_ents), 1) * 5)
        
        # FINAL SCORES
        total_trust = float(grounding_score + visibility_points + auth_score)
        metrics["Hallucination Risk"] = int(max(2.0, 100.0 - total_trust))
        metrics["Entity Consensus"] = int((matched_auth / max(len(authority_ents), 1) * 80) + (matched_comp / max(len(competitor_ents), 1) * 20))
        
        # Information Gain
        gaps_covered = 0
        limited_gaps = topic_gaps[:10]
        for gap in limited_gaps:
            try:
                res = collection.query(query_texts=[str(gap)], n_results=1)
                if res.get("distances", [[1.0]])[0][0] < 0.85:
                    gaps_covered += 1
            except: pass
        metrics["Information Gain"] = int(100 * (gaps_covered / max(len(limited_gaps), 1)))

        # Status
        if metrics["Entity Consensus"] > 50 and grounding_score > 15:
            citation_status = "Verified"
        elif metrics["Entity Consensus"] > 20:
            citation_status = "Partially Verified"
        else:
            citation_status = "Unverified"
        
        projected_traffic_lift = f"{int(metrics['Information Gain'] * 0.4)}%–{int(metrics['Information Gain'] * 0.6 + 5)}%"

    except Exception as e:
        console.print(f"[bold red]NODE_FAILED[/bold red]: Researcher logic failed: {e}")

    # 4. Recommendations
    try:
        client = genai.Client(api_key=gemini_key)
        time.sleep(5)
        lang_instruction = "Generate in Italian." if locale == "it" else "Generate in English."
        prompt = f"Act as GEO Strategist. Metrics: Consensus {metrics['Entity Consensus']}%, Info Gain {metrics['Information Gain']}%, Risk {metrics['Hallucination Risk']}%. URL: {url}. Write 3 Markdown recommendations. {lang_instruction}"
        
        response = client.models.generate_content(model='gemini-3.1-flash-lite-preview', contents=prompt)
        geo_recommendation_pack = response.text
        research_output = "Successful v7.0 Scalable Matrix analysis."

    except Exception as e:
        console.print(f"[bold red]NODE_FAILED[/bold red]: Final LLM failed: {e}")

    state["metrics"] = metrics
    state["citation_status"] = citation_status
    state["projected_traffic_lift"] = projected_traffic_lift
    state["geo_recommendation_pack"] = geo_recommendation_pack
    state["research_output"] = research_output

    return state
