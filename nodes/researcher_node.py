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
    console.print("[cyan]Researcher Node[/cyan]: Starting deep v4.0 Agency-Grade analysis...")
    
    # Defaults and status
    metrics = {
        "Entity Consensus": 0,
        "Information Gain": 0,
        "Hallucination Risk": 100,
        "Citation Readiness": "Needs Work"
    }
    citation_status = "Low Verification"
    projected_traffic_lift = "0%"
    geo_recommendation_pack = "Unavailable"
    research_output = "Research failed or skipped."

    url = state.get("url", "Unknown")
    locale = state.get("locale", "en")
    target_industry = state.get("target_industry", "Unknown")
    brand_name = state.get("brand_name", "Unknown")
    scale_level = state.get("scale_level", "National")
    business_type = state.get("business_type", "tech")
    discovered_location = state.get("discovered_location", "Worldwide")
    lang_name = "Italian" if locale == "it" else "English"
    
    # Calculate confidence score early
    extraction_warnings = state.get("extraction_warnings", [])
    structured_data = state.get("structured_data_extract", [])
    depth = state.get("client_content_depth", {})
    
    confidence_points = 100
    if isinstance(depth, dict):
        quality = depth.get("extraction_quality", "Low")
        if quality == "Low": confidence_points -= 40
        elif quality == "Medium": confidence_points -= 20
        if depth.get("js_heavy_detected", False): confidence_points -= 20
    else:
        confidence_points -= 40 # fallback
        
    if not structured_data: confidence_points -= 10
    if extraction_warnings: confidence_points -= 10
    
    external_data_quality = state.get("external_data_quality", "HIGH")
    if external_data_quality == "LOW":
        confidence_points -= 30
        
    confidence_score = max(10, min(100, confidence_points))
    
    gemini_key = os.getenv("GEMINI_API_KEY")
    perplexity_key = os.getenv("PERPLEXITY_API_KEY")

    if not gemini_key or not perplexity_key:
        console.print("[bold red]NODE_FAILED[/bold red]: Researcher (API Keys missing). Using fallbacks.")
        state["metrics"] = metrics
        return state

    try:
        # 1. Setup ChromaDB for Information Gain Analysis
        os.makedirs("chroma_db", exist_ok=True)
        chroma_client = chromadb.PersistentClient(path="./chroma_db/")
        model_name = "intfloat/multilingual-e5-large" if locale == "it" else "all-MiniLM-L6-v2"
        local_cache = os.path.join(os.getcwd(), "hf_cache")
        embedding_model = SentenceTransformer(model_name, cache_folder=local_cache)
        
        class CustomEmbeddingFunction(chromadb.EmbeddingFunction):
            def __call__(self, input: chromadb.Documents) -> chromadb.Embeddings:
                return embedding_model.encode(input).tolist()
        
        collection_name = f"geo_audit_{int(time.time())}"
        collection = chroma_client.get_or_create_collection(name=collection_name, embedding_function=CustomEmbeddingFunction())
        
        client_content = state.get("client_content_clean", "")
        if client_content:
            chunks = [client_content[i:i+500] for i in range(0, min(len(client_content), 15000), 500)]
            collection.upsert(
                documents=chunks,
                metadatas=[{"source": "client_website"}] * len(chunks),
                ids=[f"chunk_{i}" for i in range(len(chunks))]
            )

        # 2. GROUNDING SCANNER v2.0 (70% Weight)
        console.print("[cyan]Researcher Node[/cyan]: Scanning Grounding Anchors v2.0...")
        grounding_score = 0
        content_lower = client_content.lower()
        
        # Heuristics
        if re.search(r'p\.?i\.?v\.?a\.?\s*[:=]?\s*(\d{11})', content_lower): grounding_score += 15
        if re.search(r'\d{11}', content_lower): grounding_score += 5 # General 11-digit match (VAT)
        if any(x in content_lower for x in ["pec:", "@pec", "posta certificata"]): grounding_score += 10
        if any(x in content_lower for x in ["via ", "piazza ", "viale ", "street ", "road ", "ave "]): grounding_score += 10
        if "schema.org" in content_lower or "ld+json" in content_lower: grounding_score += 15
        if any(x in content_lower for x in ["localbusiness", "organization", "dentist", "restaurant"]): grounding_score += 15
        
        # Structured Data enhancements
        if any(x in structured_data for x in ["LocalBusiness", "Organization"]): grounding_score += 15
        if any(x in structured_data for x in ["ContactPoint", "PostalAddress"]): grounding_score += 15
        
        grounding_score = min(100, grounding_score)

        # 3. 10-QUERY AGENCY ENGINE (20% Weight)
        console.print(f"[cyan]Researcher Node[/cyan]: Executing 10-Query Agency Stress Test for '{brand_name}'...")
        visibility_points = 0
        
        # Generate 10 queries via Gemini
        waterfall_gen_prompt = f"""
        You are the Chief GEO Stress-Test Architect for a $2M retainer client.
        Business: {brand_name} | Type: {business_type} | Location: {discovered_location} | Scale: {scale_level}

        Generate EXACTLY 10 independent search queries in {lang_name} that would be asked by real users in the wild.
        Rules:
        - ZERO mention of the brand name '{brand_name}' or URL '{url}' in any query.
        - Queries must be 100% natural and varied.
        - Group them into 5 categories (2 queries each):
          1. Niche Intent (L1)
          2. Scenario / Pain (L2)
          3. Geo-Contextual (L3) – MUST include location '{discovered_location}' if local business
          4. Authority / Comparison (L4)
          5. Future / Emerging (L5)

        Return ONLY valid JSON: {{ "queries": [{{ "category": "...", "level": 1, "query": "..." }}] }}
        """
        
        gemini_client = genai.Client(api_key=gemini_key)
        w_res = gemini_client.models.generate_content(model='gemini-2.5-flash-lite', contents=waterfall_gen_prompt, config={"response_mime_type": "application/json"})
        queries_dict = json.loads(w_res.text).get("queries", [])
        
        # Category Weights
        cat_points = {
            "Niche Intent": 12,
            "Scenario / Pain": 12,
            "Geo-Contextual": 18,
            "Authority / Comparison": 8,
            "Future / Emerging": 8
        }
        
        # Execute sequentially (strictly synchronous)
        for q_obj in queries_dict[:10]:
            cat = q_obj.get("category", "Niche Intent")
            q_text = q_obj.get("query", "")
            console.print(f"  Testing Query [{cat}]: {q_text}")
            
            # Call Perplexity
            time.sleep(5)
            payload = {
                "model": "sonar-pro",
                "messages": [{"role": "system", "content": "Search engine assistant."}, {"role": "user", "content": q_text}]
            }
            try:
                res = requests.post("https://api.perplexity.ai/chat/completions", json=payload, headers={"Authorization": f"Bearer {perplexity_key}"}, timeout=30)
                ans = res.json()["choices"][0]["message"]["content"].lower()
                if brand_name.lower() in ans or url.split(".")[1] in ans:
                    visibility_points += cat_points.get(cat, 10)
                    console.print(f"    [green]MATCH FOUND[/green]")
            except: pass

        visibility_score = min(100, (visibility_points / 140) * 100)

        # 4. AUTHORITY MATCH (10% Weight) - Normalized Fuzzy Match
        raw_data = state.get("raw_data_complete", {})
        authority_ents = raw_data.get("authority_entities", [])
        competitor_ents = raw_data.get("competitor_entities", [])
        
        def fuzzy_match(entity, text):
            e_norm = re.sub(r'[^\w\s]', '', str(entity).lower().strip())
            t_norm = re.sub(r'[^\w\s]', '', text)
            if not e_norm: return False
            if e_norm in t_norm: return True
            parts = e_norm.split()
            if len(parts) > 1 and sum(1 for p in parts if p in t_norm) >= len(parts) * 0.5:
                return True
            return False
            
        matched_auth = sum(1 for a in authority_ents if fuzzy_match(a, content_lower))
        matched_comp = sum(1 for c in competitor_ents if fuzzy_match(c, content_lower))
        
        auth_ratio = matched_auth / max(len(authority_ents), 1)
        comp_ratio = matched_comp / max(len(competitor_ents), 1)
        
        authority_match_score = (auth_ratio * 70) + (comp_ratio * 30)
        
        if authority_match_score > 0:
            authority_match_score = 15 + (authority_match_score * 0.7)
        if authority_match_score > 90 and auth_ratio < 0.9:
            authority_match_score = 85
            
        authority_match_score = max(5, min(95, authority_match_score))

        # 5. FINAL AGENCY VISIBILITY SCORE
        total_visibility = (grounding_score * 0.7) + (visibility_score * 0.2) + (authority_match_score * 0.1)
        
        metrics["Hallucination Risk"] = max(0, 100 - int(total_visibility))
        metrics["Entity Consensus"] = int(authority_match_score)
        metrics["Citation Readiness"] = "Enterprise-Ready" if total_visibility > 92 else "Agency-Ready" if total_visibility > 75 else "Needs Work"
        
        # Information Gain Calculation (Upside from MISSING topics)
        topic_gaps = raw_data.get("topic_gaps", [])
        gaps_missing = 0
        for gap in topic_gaps[:10]:
            try:
                res = collection.query(query_texts=[str(gap)], n_results=1)
                if res.get("distances", [[1.0]])[0][0] >= 0.85:
                    gaps_missing += 1
            except: pass
            
        raw_gain = (gaps_missing / max(len(topic_gaps[:10]), 1)) * 100
        ig = int(raw_gain * (confidence_score / 100))
        
        if confidence_score < 40:
            ig = min(ig, 65)
        elif confidence_score < 75:
            ig = min(ig, 85)
            
        if ig == 0 and len(topic_gaps) > 0:
            ig = 5
            
        metrics["Information Gain"] = ig

        # Metadata updates (Do not emit Unverified, use Low Verification)
        if confidence_score < 40: citation_status = "Low Verification"
        elif total_visibility > 60 and confidence_score >= 80: citation_status = "Verified"
        elif total_visibility > 30: citation_status = "Partially Verified"
        else: citation_status = "Low Verification"
        
        ig = metrics["Information Gain"]
        if confidence_score < 40:
            projected_traffic_lift = "0–5%"
        elif confidence_score < 75:
            projected_traffic_lift = "8–18%" if ig > 40 else "3–8%"
        else:
            projected_traffic_lift = "18–30%" if ig > 60 else "8–18%"

    except Exception as e:
        console.print(f"[bold red]Researcher Logic Failure: {e}[/bold red]")

    # 6. Final Recommendation Pack
    try:
        time.sleep(5)
        rec_prompt = f"""
        Act as Senior GEO Strategist. 
        Agency Metrics: Visibility {total_visibility:.1f}/100, Info Gain {metrics['Information Gain']}%, Risk {metrics['Hallucination Risk']}%. Readiness: {metrics['Citation Readiness']}.
        Confidence in Evidence Extraction: {confidence_score}/100.
        URL: {url}. 
        
        Write an Agency-Level recommendation pack in {lang_name} using STRICTLY the evidence provided. 
        MANDATORY RULES:
        - DO NOT invent or mention technical SEO issues (like crawl budget, toxic backlinks, penalties, severe SEO failures) unless explicitly present in the data.
        - Use agency-safe language like "limited evidence suggests", "inferred opportunity", or "manual verification recommended".
        - If confidence is below 50, ensure recommendations are extremely conservative.
        
        Ensure your response explicitly separates into:
        1. Observed findings 
        2. Inferred opportunities
        3. Unknown / needs manual verification
        
        Provide 3-5 conservative, factual recommendations in professional Markdown.
        """
        gemini_client = genai.Client(api_key=gemini_key)
        res_rec = gemini_client.models.generate_content(model='gemini-2.5-flash-lite', contents=rec_prompt)
        geo_recommendation_pack = res_rec.text
        research_output = f"v4.2 Agency-Grade Analysis Complete. Readiness: {metrics['Citation Readiness']}."
    except Exception as e:
        console.print(f"[yellow]Recommendation generation skipped or failed: {e}[/yellow]")
        pass

    state["metrics"] = metrics
    state["citation_status"] = citation_status
    state["projected_traffic_lift"] = projected_traffic_lift
    state["geo_recommendation_pack"] = geo_recommendation_pack
    state["research_output"] = research_output
    
    # Additive fields
    state["confidence_score"] = confidence_score
    state["evidence_quality"] = depth.get("extraction_quality", "Low") if isinstance(depth, dict) else "Low"
    state["visibility_score"] = int(total_visibility) if 'total_visibility' in locals() else 0
    state["grounding_score"] = grounding_score if 'grounding_score' in locals() else 0
    state["authority_match_score"] = int(authority_match_score) if 'authority_match_score' in locals() else 0
    state["extraction_quality"] = depth.get("extraction_quality", "Low") if isinstance(depth, dict) else "Low"
    state["evidence_limitations"] = "Low confidence due to parsing issues or lack of structured data." if confidence_score < 50 else "Adequate confidence."
    
    return state
