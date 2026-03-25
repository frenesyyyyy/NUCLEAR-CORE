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
    
    # v4.4 Brand Authority Signals
    schema_type_counts = state.get("schema_type_counts", {})
    hreflang_count = state.get("hreflang_count", 0)
    
    authority_signals = []
    auth_score = 0
    if hreflang_count > 3:
        authority_signals.append(f"{hreflang_count} hreflang international targets")
        auth_score += 2
    
    if "Organization" in schema_type_counts or "WebSite" in schema_type_counts:
        authority_signals.append("Strong Organization/WebSite Schema Identity")
        auth_score += 2
        
    if "Product" in schema_type_counts or "Offer" in schema_type_counts or "AggregateOffer" in schema_type_counts:
        authority_signals.append("Commerce Schema Present")
        auth_score += 1
        
    if scale_level == "Global":
        authority_signals.append("Self-identified Global Market Scale")
        auth_score += 1
        
    authority_strength = "high" if auth_score >= 4 else ("medium" if auth_score >= 2 else "low")
    is_global_brand = (authority_strength == "high") and (hreflang_count > 0 or scale_level == "Global")
    
    brand_authority_signals = {
        "is_global_brand": is_global_brand,
        "authority_strength": authority_strength,
        "score": auth_score,
        "signals": authority_signals
    }
    state["brand_authority_signals"] = brand_authority_signals

    # v4.4 Calculate strict confidence score
    extraction_warnings = state.get("extraction_warnings", [])
    structured_data = state.get("structured_data_extract", [])
    depth = state.get("client_content_depth", {})
    
    quality = depth.get("extraction_quality", "low") if isinstance(depth, dict) else "low"
    schema_blk = depth.get("schema_block_count", 0) if isinstance(depth, dict) else 0
    words = depth.get("word_count", 0) if isinstance(depth, dict) else 0
    pages = depth.get("page_count", 1) if isinstance(depth, dict) else 1
    external_data_quality = state.get("external_data_quality", "high")
    
    # Base Weights
    conf_base = 0
    conf_base += 40 if quality == "high" else (20 if quality == "medium" else 5)
    conf_base += min(20, schema_blk * 5)
    conf_base += min(20, int((words / 1000) * 10 + (pages * 5))) 
    conf_base += 20 if external_data_quality == "high" else (10 if external_data_quality == "medium" else 5)
    
    confidence_score = conf_base
    
    # Strict Caps (v4.4 bounds)
    if pages == 1:
        confidence_score = min(confidence_score, 65)
    if external_data_quality == "low":
        confidence_score = min(confidence_score, 60)
    if schema_blk < 1:
        confidence_score = min(confidence_score, 70)
    if quality in ["low", "medium"]:
        confidence_score = min(confidence_score, 75)
        
    if extraction_warnings: confidence_score -= 10
    
    # Exceeding 85 requires absolute strength
    if confidence_score > 85:
        if pages < 3 or quality != "high" or schema_blk < 2 or external_data_quality == "low":
            confidence_score = 85
            
    confidence_score = max(10, min(100, confidence_score))
    
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

        # 4. AUTHORITY MATCH (10% Weight) - Normalized Fuzzy Match v4.3
        raw_data = state.get("raw_data_complete", {})
        authority_ents = raw_data.get("authority_entities", [])
        competitor_ents = raw_data.get("competitor_entities", [])
        total_ents = authority_ents + competitor_ents
        
        def fuzzy_match(entity, text):
            e_norm = re.sub(r'[^\w\s]', '', str(entity).lower().strip())
            # strip suffix noise
            suffixes = [" spa", " s.p.a.", " ltd", " inc", " srl", " llc", " gmbh", " s.r.l."]
            for s in suffixes:
                if e_norm.endswith(s):
                    e_norm = e_norm[:-len(s)].strip()
                    
            t_norm = re.sub(r'[^\w\s]', '', text)
            if not e_norm: return False
            if e_norm in t_norm: return True
            parts = e_norm.split()
            if len(parts) > 1 and sum(1 for p in parts if p in t_norm) >= len(parts) * 0.6:
                return True
            return False
            
        matched_ents = sum(1 for e in total_ents if fuzzy_match(e, content_lower))
        match_ratio = matched_ents / max(len(total_ents), 1)
        
        base_consensus = 15 + (match_ratio * 70)
        
        # Floor Protection for strong brands
        if is_global_brand or authority_strength == "high":
            base_consensus = max(base_consensus, 60)
            
        authority_match_score = max(15, min(95, int(base_consensus)))

        # 5. FINAL AGENCY VISIBILITY SCORE
        total_visibility = (grounding_score * 0.7) + (visibility_score * 0.2) + (authority_match_score * 0.1)
        
        # v4.4 Hallucination Risk
        base_risk = 100 - int(total_visibility)
        
        if authority_strength == "high" and external_data_quality != "low":
            risk_score = min(base_risk, 30)
        elif authority_strength == "high" and external_data_quality == "low":
            risk_score = min(base_risk, 55)
        elif authority_strength == "low" and external_data_quality == "low" and schema_blk == 0:
            risk_score = max(base_risk, 80)
        else:
            rm = 0.7 if quality == "high" else (0.85 if quality == "medium" else 1.0)
            risk_score = int(base_risk * rm)
            
        risk_score = max(5, min(95, risk_score))
        metrics["Hallucination Risk"] = risk_score
        
        metrics["Entity Consensus"] = int(authority_match_score)
        metrics["Citation Readiness"] = "Enterprise-Ready" if total_visibility > 92 else "Agency-Ready" if total_visibility > 75 else "Needs Work"
        
        # v4.3 Information Gain Calculation (Upside from MISSING topics)
        topic_gaps = raw_data.get("topic_gaps", [])
        total_possible = max(len(topic_gaps[:10]), 1)
        gaps_missing = 0
        for gap in topic_gaps[:10]:
            try:
                res = collection.query(query_texts=[str(gap)], n_results=1)
                if res.get("distances", [[1.0]])[0][0] >= 0.85:
                    gaps_missing += 1
            except: pass
            
        ig = int(100 * (gaps_missing / total_possible))
        if confidence_score < 50:
            ig = min(ig, 40)
            
        if ig == 0 and len(topic_gaps) > 0:
            ig = 5
            
        metrics["Information Gain"] = ig

        # Metadata updates (Do not emit Unverified, use Low Verification)
        if confidence_score < 40: citation_status = "Low Verification"
        elif total_visibility > 60 and confidence_score >= 80: citation_status = "Verified"
        elif total_visibility > 30: citation_status = "Partially Verified"
        else: citation_status = "Low Verification"
        
        ig = metrics["Information Gain"]
        if confidence_score < 40 or citation_status == "Low Verification":
            projected_traffic_lift = "0–5%"
        elif confidence_score < 75:
            projected_traffic_lift = "3–8%" if ig < 40 else "8–18%"
        else:
            projected_traffic_lift = "8–18%" if ig < 60 else "18–30%"

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
        - DO NOT invent or mention technical SEO issues (like crawl budget, toxic backlinks, penalties) unless explicitly present in the data.
        - Use agency-safe language like "limited evidence suggests", "inferred opportunity", or "manual verification recommended".
        - If evidence is missing, state it clearly. NEVER invent facts.
        
        ALL recommendations MUST be returned STRICTLY as a JSON array of objects, exactly like this:
        [
          {
            "title": "Clear Actionable Title",
            "rationale": "Why this matters based on the data",
            "priority": "High|Medium|Low",
            "implementation_type": "Technical|Content|Authority"
          }
        ]
        """
        gemini_client = genai.Client(api_key=gemini_key)
        res_rec = gemini_client.models.generate_content(model='gemini-2.5-flash-lite', contents=rec_prompt, config={"response_mime_type": "application/json"})
        
        try:
            clean_text = res_rec.text.strip()
            if clean_text.startswith("```json"):
                clean_text = clean_text[7:]
            if clean_text.startswith("```"):
                clean_text = clean_text[3:]
            if clean_text.endswith("```"):
                clean_text = clean_text[:-3]
                
            geo_recommendation_pack_json = json.loads(clean_text.strip())
            geo_recommendation_pack = json.dumps(geo_recommendation_pack_json, indent=2)
        except:
            geo_recommendation_pack = res_rec.text
            
        research_output = f"v4.3 Agency-Grade Analysis Complete. Readiness: {metrics['Citation Readiness']}."
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
