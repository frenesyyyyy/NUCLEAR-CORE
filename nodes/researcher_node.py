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
from typing import Any
from google import genai
from nodes.api_utils import execute_with_backoff

console = Console()


# ──────────────────────────────────────────────────────────────────────────────
# Stress Test Helpers (v4.5 Agency-Grade Tiered Architecture)
# ──────────────────────────────────────────────────────────────────────────────

def _safe_stringify_list(items: list[Any]) -> list[str]:
    """
    Agency-grade safety helper to normalize lists of mixed types (str, dict, list)
    into a flat list of strings for prompt interpolation or joining.
    """
    if not items: return []
    safe = []
    for item in items:
        if not item: continue
        if isinstance(item, str):
            val = item.strip()
            if val: safe.append(val)
        elif isinstance(item, dict):
            # Convert dict to key:value string for context
            val = "; ".join([f"{k}: {v}" for k, v in item.items() if v])
            if val: safe.append(f"[{val}]")
        else:
            val = str(item).strip()
            if val: safe.append(val)
    return safe

def _extract_brand_tokens(brand_name: str, url: str, og_title: str) -> set[str]:
    """
    Extracts restricted tokens that must NOT appear in T1/T2 queries.
    """
    from urllib.parse import urlparse
    tokens = set()
    
    # 1. Full brand name
    b = brand_name.lower().strip()
    if b and b != "unknown":
        tokens.add(b)
        # Split tokens (e.g., "Just Eat" -> "just", "eat")
        for part in b.split():
            if len(part) > 2: tokens.add(part)

    # 2. Domain token
    try:
        domain = urlparse(url).netloc.replace("www.", "").split(".")[0].lower()
        if domain and len(domain) > 2:
            tokens.add(domain)
    except: pass

    # 3. OG Title hints (significant words only)
    if og_title and og_title != "N/A":
        title_words = re.findall(r'\b\w{4,}\b', og_title.lower())
        # Only add words that aren't common generic terms
        stop_words = {"home", "welcome", "italia", "italy", "page", "website", "online", "servizi", "services"}
        for word in title_words:
            if word not in stop_words:
                tokens.add(word)

    return tokens

def _query_has_brand_leakage(query: str, brand_tokens: set[str]) -> bool:
    """Checks if a query contains any forbidden brand identifiers."""
    q_low = query.lower()
    for token in brand_tokens:
        # Use word boundaries to avoid false positives (e.g. "eat" in "weather")
        if re.search(rf'\b{re.escape(token)}\b', q_low):
            return True
    return False

def _inject_geo_context(query: str, location: str, locale: str) -> str:
    """Forces geo-anchoring into a query for local businesses."""
    if not location or location.lower() in ["worldwide", "national", "unknown"]:
        return query
    
    # Simplify location (e.g. "Rome, Italy" -> "Roma" if locale IT)
    city = location.split(",")[0].strip()
    if locale == "it" and city.lower() == "rome": city = "Roma"
    if locale == "it" and city.lower() == "milan": city = "Milano"
    
    q = query.strip()
    # Check if already has geo
    if city.lower() in q.lower():
        return q
    
    # Natural injection patterns
    if locale == "it":
        return f"{q} {city}"
    else:
        return f"{q} in {city}"

def _is_realistic_query(query: str, industry: str) -> bool:
    """Rejects taxonomical, corporate, or awkward AI-generated phrasing."""
    q = query.lower()
    # 1. Reject raw taxonomy tokens if they appear as the sole subject
    bad_tokens = ["food delivery service", "delivery platform", "marketplace platform", "software as a service", "marketing agency"]
    if q.strip() in bad_tokens: return False
    
    # 2. Reject awkward corporate labels that aren't natural Italian/English
    if "service" in q and industry.lower() in q and len(q.split()) < 4: return False
    
    # 3. Reject placeholder-like brackets if they leaked
    if "[" in q or "]" in q: return False
    
    return True

def _sanitize_or_reject_query(
    query: str, 
    tier: str, 
    brand_tokens: set[str], 
    is_local: bool, 
    location: str, 
    locale: str,
    industry: str = "business",
    location_confidence: str = "high"
) -> tuple[str | None, str]:
    """
    Agency Gate: Rejects or cleans queries based on tier-integrity and realism rules.
    Returns (sanitized_query_or_None, reason).
    """
    if not query or len(query.strip()) < 5:
        return None, "Too short"

    q = query.strip()

    # REALISM GATE: Reject taxonomical/awkward phrasing
    if not _is_realistic_query(q, industry):
        return None, "FAILED REALISM GATE: Taxonomical or awkward phrasing"

    # Tier 1 & 2: Brand Leakage check
    if tier in ["blind_discovery", "contextual_discovery"]:
        if _query_has_brand_leakage(q, brand_tokens):
            if tier == "blind_discovery":
                return None, f"T1 HARD BAN: Brand leakage detected ({q})"
            else:
                # For T2, try one quick scrub if possible, otherwise reject
                for token in brand_tokens:
                    q = re.sub(rf'\b{re.escape(token)}\b', '', q, flags=re.IGNORECASE).strip()
                if _query_has_brand_leakage(q, brand_tokens) or len(q) < 10:
                    return None, "T2 Leakage: Scrub failed or corrupted context"

    # Local Enforcement (CRITICAL PATCH)
    # Only inject geo if is_local AND we have corroborated confidence (high/medium)
    if is_local and location_confidence in ["high", "medium"]:
        q = _inject_geo_context(q, location, locale)
    elif is_local:
        # Log suppression for debugging
        # console.print(f"      [dim]Geo-injection suppressed (Conf: {location_confidence})[/dim]")
        pass

    return q, "Success"

def _build_blind_queries(
    faq_patterns: list, 
    authority_entities: list, 
    brand_tokens: set[str],
    is_local: bool,
    location: str,
    locale: str,
    gemini_client,
    budget: int = 8
) -> list:
    """
    Tier 1: Blind Discovery.
    Uses LLM to convert patterns into brand-neutral category-intent queries.
    """
    lang_name = "Italian" if locale == "it" else "English"
    seeds = _safe_stringify_list(faq_patterns + authority_entities)[:10]
    
    queries = []
    if seeds:
        try:
            prompt = f"""
            Convert these market identifiers/FAQs into {budget} truly BLIND discovery queries.
            RULES:
            1. NO brand names, NO domain tokens, NO company specific IDs.
            2. Focus on "best [category]", "how to [problem]", "[category] platform".
            3. Must sound like a real user who DOES NOT know the brand exists.
            4. Language: {lang_name}.
            5. Output ONLY a JSON array of strings: ["query1", "query2", ...]
            
            Seeds: {json.dumps(seeds)}
            """
            res = gemini_client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=prompt,
                config={"response_mime_type": "application/json"}
            )
            raw_qs = json.loads(res.text)
            if isinstance(raw_qs, dict): raw_qs = raw_qs.get("queries", [])
            
            for q in raw_qs:
                sanitized, reason = _sanitize_or_reject_query(q, "blind_discovery", brand_tokens, is_local, location, locale, industry="market")
                if sanitized:
                    queries.append({"query": sanitized, "tier": "blind_discovery", "points": 25})
        except Exception as e:
            console.print(f"      [yellow]T1 Gen Error: {e}[/yellow]")

    return queries

def _build_contextual_queries(
    topic_gaps: list,
    brand_tokens: set[str],
    is_local: bool,
    location: str,
    locale: str,
    gemini_client,
    budget: int = 10,
    persona_templates: list = None
) -> list:
    """
    Tier 2: Contextual Discovery.
    Strictly neutral queries derived from topic gaps.
    """
    lang_name = "Italian" if locale == "it" else "English"
    if not topic_gaps: return []

    gaps = [str(g).strip() for g in topic_gaps[:budget] if g]
    
    try:
        persona_context = ""
        if persona_templates:
            pt_list = [f"{p.get('persona', '')} ({p.get('intent', '')})" for p in persona_templates if p.get('persona')]
            persona_context = f"\nPersonas: {', '.join(pt_list)}."

        prompt = f"""
        Convert these content gaps into {len(gaps)} brand-neutral search queries.
        RULES:
        1. STRICTLY FORBIDDEN: Naming the audited brand or its website.
        2. Allowed: Category, use case, budget, geography.
        3. NO investor/analyst phrasing. Sound like a user search.
        4. Language: {lang_name}.{persona_context}
        5. Output ONLY JSON: {{"questions": ["...", "..."]}}
        
        Gaps: {json.dumps(gaps)}
        """
        res = gemini_client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        questions = json.loads(res.text).get("questions", [])
        
        queries = []
        for q in questions:
            sanitized, reason = _sanitize_or_reject_query(q, "contextual_discovery", brand_tokens, is_local, location, locale)
            if sanitized:
                queries.append({"query": sanitized, "tier": "contextual_discovery", "points": 15})
        return queries
    except Exception as e:
        console.print(f"      [yellow]T2 Gen Error: {e}[/yellow]")
        return []


def _build_branded_queries(
    brand_name: str,
    target_industry: str,
    discovered_location: str,
    scale_level: str,
    locale: str,
    competitor_entities: list,
    budget: int = 6,
) -> list:
    """
    Tier 3: Branded Validation.
    Tests whether the brand is recognised by AI when searched directly.
    Expanded from 3 fixed queries to a configurable budget with 6 distinct query types.
    Points value: 20.
    """
    queries = []
    b = brand_name.strip()
    ind = target_industry.strip()
    loc = discovered_location.strip()
    top_comp = competitor_entities[0].strip() if competitor_entities else ""
    sec_comp = competitor_entities[1].strip() if len(competitor_entities) > 1 else ""

    is_local = scale_level == "Local" and loc and loc.lower() not in ["worldwide", "national"]

    if locale == "it":
        candidate_queries = [
            # Core brand recognition (always first)
            {"query": f"{b} opinioni",                                   "tier": "branded_validation", "points": 20},
            {"query": f"{b} {ind}",                                       "tier": "branded_validation", "points": 20},
            # Competitor comparison (high intent)
            {"query": f"{b} vs" + (f" {top_comp}" if top_comp else f" {ind}"), "tier": "branded_validation", "points": 20},
            # Use-case / problem-aware query
            {"query": f"come funziona {b}",                               "tier": "branded_validation", "points": 20},
            # Location signal (if local, otherwise national market probe)
            {"query": (f"{b} a {loc}" if is_local else f"{b} italia"),   "tier": "branded_validation", "points": 20},
            # Alternative/authority probe using second competitor
            {"query": f"alternativa a {sec_comp or top_comp or ind} per {ind}", "tier": "branded_validation", "points": 15},
        ]
    else:
        candidate_queries = [
            {"query": f"{b} reviews",                                    "tier": "branded_validation", "points": 20},
            {"query": f"what is {b}",                                     "tier": "branded_validation", "points": 20},
            {"query": f"{b} vs" + (f" {top_comp}" if top_comp else f" {ind} competitors"), "tier": "branded_validation", "points": 20},
            {"query": f"how does {b} work",                              "tier": "branded_validation", "points": 20},
            {"query": (f"{b} in {loc}" if is_local else f"{b} pricing"), "tier": "branded_validation", "points": 20},
            {"query": f"alternative to {sec_comp or top_comp or ind} for {ind}", "tier": "branded_validation", "points": 15},
        ]

    return candidate_queries[:budget]


def _brand_mentioned(brand_name: str, url: str, og_title: str, answer_text: str, gemini_client=None) -> bool:
    """
    Agency-Grade Semantic Brand Evaluation.
    First runs deterministic string signals. If those fail, runs a semantic LLM
    evaluator to determine if the brand is conceptually present.
    """
    from urllib.parse import urlparse as _urlparse
    ans = answer_text.lower()
    b = brand_name.lower().strip()

    # Fast Path 1: Exact brand name
    if b and b in ans:
        return True

    # Fast Path 2: Domain token
    try:
        domain_token = _urlparse(url).netloc.replace("www.", "").split(".")[0].lower()
        if domain_token and len(domain_token) > 2 and domain_token in ans:
            return True
    except Exception:
        pass

    # Fallback Path 3: Semantic LLM Evaluator
    if gemini_client:
        try:
            prompt = f"""
            You are a semantic evaluator.
            We asked an AI search engine a query. The engine returned:
            "{answer_text}"
            
            Is this answer recommending or prominently mentioning the brand "{brand_name}" (or its exact website {url})?
            Sometimes engines misspell the name, use an acronym, or reference its core proprietary product.
            
            Return ONLY "yes" if the brand/site is functionally represented as an answer, and "no" if it is entirely absent or represented as a negative.
            """
            def _sem_req():
                return gemini_client.models.generate_content(
                    model="gemini-2.5-flash-lite",
                    contents=prompt,
                    config={"response_mime_type": "text/plain"}
                )
            res = execute_with_backoff(_sem_req, max_retries=2, initial_delay=1.0)
            if "yes" in res.text.lower().strip()[:10]:
                return True
        except Exception:
            pass

    return False


def _run_stress_test(
    all_queries: list,
    perplexity_key: str,
    brand_name: str,
    url: str,
    og_title: str,
    gemini_client=None
) -> tuple:
    """
    Unified sequential Perplexity executor.
    Returns (visibility_points, total_possible, stress_test_log, tier_stats).
    """
    visibility_points = 0
    total_possible = sum(q["points"] for q in all_queries)
    log = []
    
    tier_stats = {
        "blind_discovery": {"queries": 0, "matches": 0, "pts": 0, "max": 0},
        "contextual_discovery": {"queries": 0, "matches": 0, "pts": 0, "max": 0},
        "branded_validation": {"queries": 0, "matches": 0, "pts": 0, "max": 0},
    }

    for q_obj in all_queries:
        tier = q_obj["tier"]
        q_text = q_obj["query"]
        pts = q_obj["points"]

        tier_label = {
            "blind_discovery":     "T1-Blind",
            "contextual_discovery": "T2-Context",
            "branded_validation":  "T3-Brand",
        }.get(tier, tier)

        console.print(f"  [{tier_label}] {q_text}")

        tier_stats[tier]["queries"] += 1
        tier_stats[tier]["max"] += pts

        matched = False
        try:
            payload = {
                "model": "sonar-pro",
                "messages": [
                    {"role": "system", "content": "You are a helpful search assistant."},
                    {"role": "user",   "content": q_text}
                ]
            }
            def _perp_req():
                response = requests.post(
                    "https://api.perplexity.ai/chat/completions",
                    json=payload,
                    headers={"Authorization": f"Bearer {perplexity_key}"},
                    timeout=30
                )
                response.raise_for_status()
                return response.json()["choices"][0]["message"]["content"]

            answer = execute_with_backoff(_perp_req, max_retries=3, initial_delay=3.0)
            matched = _brand_mentioned(brand_name, url, og_title, answer, gemini_client)
            if matched:
                visibility_points += pts
                tier_stats[tier]["matches"] += 1
                tier_stats[tier]["pts"] += pts
                console.print(f"    [green]✓ MATCH ({pts}pts)[/green]")
            else:
                console.print(f"    [yellow]✗ no match[/yellow]")
        except Exception as qe:
            console.print(f"    [yellow]Query error: {qe}[/yellow]")

        log.append({
            "tier":    tier,
            "query":   q_text,
            "matched": matched,
            "points":  pts if matched else 0,
            "max_pts": pts,
        })

    return visibility_points, total_possible, log, tier_stats

def process(state: dict) -> dict:
    console.print("[cyan]Researcher Node[/cyan]: Starting deep v4.5 Agency-Grade analysis...")
    
    # Defaults and status
    metrics = {
        "Defensible Evidence Depth": 0,
        "Entity Consensus": 0,
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
    
    # ── Audit Integrity Gating v4.5 ─────────────────────────────────────────────
    integrity_status = state.get("audit_integrity_status", "valid")
    integ_reasons = state.get("audit_integrity_reasons", [])
    
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

    # v4.5 Rigorous Confidence Score
    extraction_warnings = state.get("extraction_warnings", [])
    depth = state.get("client_content_depth", {})
    quality = depth.get("extraction_quality", "low") if isinstance(depth, dict) else "low"
    words = depth.get("word_count", 0) if isinstance(depth, dict) else 0
    pages = depth.get("page_count", 1) if isinstance(depth, dict) else 1

    js_fallback = state.get("js_fallback_used", False)
    robots = state.get("robots_txt_status", "not_found")
    ext_quality = state.get("external_data_quality", "high")

    # ── SCORE A: Extraction Integrity ──────────────────────────────────────────
    ei_score = 100
    ei_score -= (len(extraction_warnings) * 10)
    if quality == "low":
        ei_score -= 15
    if js_fallback and quality != "high":
        ei_score -= 25    # fallback triggered but content still thin
    if robots == "restricted":
        ei_score -= 20    # AI engines can't legally crawl this site
    if pages >= 5 and words > 1000:
        ei_score = max(50, ei_score)   # floor: critical mass of text was extracted
    extraction_integrity = max(10, min(100, ei_score))

    # ── SCORE B: Context Reliability ──────────────────────────────────────────
    cr_score = 100
    s_counts_cr = state.get("schema_type_counts", {})
    generic_only = all(k in {"WebSite", "WebPage", "BreadcrumbList", "SiteLinksSearchBox"} for k in s_counts_cr)
    if not s_counts_cr:
        cr_score -= 20
    elif generic_only:
        cr_score -= 10
    
    og_title_cr = state.get("og_tags", {}).get("og:title", "")
    templated_signals = ["home", "welcome", "untitled", "sample", "default", "page"]
    if og_title_cr and any(sig in og_title_cr.lower() for sig in templated_signals):
        cr_score -= 15

    class_notes = state.get("classification_notes", "")
    if class_notes and "confirmed" not in class_notes.lower():
        cr_score -= 10

    if ext_quality == "low":
        cr_score -= 30
    elif ext_quality == "medium":
        cr_score -= 10

    context_reliability = max(10, min(100, cr_score))

    # ── BLENDED FINAL SCORE + INTEGRITY CAP v4.5 ──────────────────────────────
    raw_confidence = int(0.6 * extraction_integrity + 0.4 * context_reliability)
    
    confidence_integrity_cap = 100
    if integrity_status == "invalid":
        confidence_integrity_cap = 25
    elif integrity_status == "degraded":
        confidence_integrity_cap = 60
        
    confidence_score = min(raw_confidence, confidence_integrity_cap)
    
    # Export scores for downstream use
    state["confidence_score"] = confidence_score
    state["extraction_integrity"] = extraction_integrity
    state["context_reliability"] = context_reliability
    state["confidence_integrity_cap"] = confidence_integrity_cap
    
    gemini_key = os.getenv("GEMINI_API_KEY")
    perplexity_key = os.getenv("PERPLEXITY_API_KEY")

    if not gemini_key or not perplexity_key:
        console.print("[bold red]NODE_FAILED[/bold red]: Researcher (API Keys missing). Using fallbacks.")
        state["metrics"] = metrics
        return state

    try:
        # 1. Setup ChromaDB for Evidence Analysis
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

        # Stage 2: Expand ChromaDB corpus with market intelligence
        raw_data_for_corpus = state.get("raw_data_complete", {})
        corpus_docs, corpus_meta, corpus_ids = [], [], []

        for i, gap in enumerate(raw_data_for_corpus.get("topic_gaps", [])):
            corpus_docs.append(f"Topic gap in this market: {gap}")
            corpus_meta.append({"source": "topic_gap"})
            corpus_ids.append(f"gap_{i}")

        for i, faq in enumerate(raw_data_for_corpus.get("faq_patterns", [])):
            corpus_docs.append(f"User question in this market: {faq}")
            corpus_meta.append({"source": "faq_pattern"})
            corpus_ids.append(f"faq_{i}")

        for i, comp in enumerate(raw_data_for_corpus.get("competitor_entities", [])):
            corpus_docs.append(f"Competitor in this market: {comp}")
            corpus_meta.append({"source": "competitor"})
            corpus_ids.append(f"comp_{i}")

        if corpus_docs:
            collection.upsert(documents=corpus_docs, metadatas=corpus_meta, ids=corpus_ids)
            console.print(f"[cyan]Researcher Node[/cyan]: ChromaDB corpus enriched with {len(corpus_docs)} market intelligence documents.")

        # 3. THREE-TIER STRESS TEST ENGINE (v4.5 Integrity Hardened)
        console.print(f"[cyan]Researcher Node[/cyan]: Executing 3-Tier Evidence-Derived Stress Test for '{brand_name}'...")
        
        raw_data_st = state.get("raw_data_complete", {})
        faq_patterns_st    = raw_data_st.get("faq_patterns", [])
        topic_gaps_st      = raw_data_st.get("topic_gaps", [])
        competitor_ents_st = raw_data_st.get("competitor_entities", [])
        authority_ents_st  = raw_data_st.get("authority_entities", [])
        og_title_st        = state.get("og_tags", {}).get("og:title", "")
        
        is_local = (scale_level == "Local" or state.get("business_profile", {}).get("location_enforce", False))
        brand_tokens = _extract_brand_tokens(brand_name, url, og_title_st)
        gemini_client_st = genai.Client(api_key=gemini_key)

        STRESS_TEST_BUDGET = {
            "blind":       int(os.getenv("GEO_T1_BUDGET", "8")),
            "contextual":  int(os.getenv("GEO_T2_BUDGET", "10")),
            "branded":     int(os.getenv("GEO_T3_BUDGET", "6")),
        }

        # Query Generation
        loc_conf = state.get("location_confidence", "high")
        t1_raw = _build_blind_queries(faq_patterns_st, authority_ents_st, brand_tokens, is_local, discovered_location, locale, gemini_client_st, STRESS_TEST_BUDGET["blind"])
        t2_raw = _build_contextual_queries(topic_gaps_st, brand_tokens, is_local, discovered_location, locale, gemini_client_st, STRESS_TEST_BUDGET["contextual"], state.get("business_profile", {}).get("persona_templates", []))
        
        # Patch query calls to include location_confidence
        for q_obj in t1_raw:
            q_text = q_obj.get("query")
            if q_text:
                sanitized, reason = _sanitize_or_reject_query(q_text, "blind_discovery", brand_tokens, is_local, discovered_location, locale, industry="market", location_confidence=loc_conf)
                q_obj["query"] = sanitized

        for q_obj in t2_raw:
            q_text = q_obj.get("query")
            if q_text:
                sanitized, reason = _sanitize_or_reject_query(q_text, "contextual_discovery", brand_tokens, is_local, discovered_location, locale, location_confidence=loc_conf)
                q_obj["query"] = sanitized

        t3_queries = _build_branded_queries(brand_name, target_industry, discovered_location, scale_level, locale, competitor_ents_st, STRESS_TEST_BUDGET["branded"])

        # ── FAIL-SAFE LINT GATE & BACKFILL ─────────────────────────────────────
        t1_queries = [q for q in t1_raw if q.get("query")]
        t2_queries = [q for q in t2_raw if q.get("query")]

        t1_fallback_count = 0
        t2_fallback_count = 0

        # BACKFILL Logic (Agency-Grade)
        def _get_fallback_queries(type_key, profile, loc, locale_code, budget_limit, existing_count):
            fallback_map = profile.get(f"{type_key}_fallback_templates", {})
            templates = fallback_map.get(locale_code, fallback_map.get("en", []))
            needed = budget_limit - existing_count
            added = []
            if needed > 0 and templates:
                for t in templates[:needed]:
                    q_text = _inject_geo_context(t, loc, locale_code) if is_local else t
                    added.append({"query": q_text, "tier": f"{type_key}_discovery", "points": 25 if type_key == "blind" else 15})
            return added

        if len(t1_queries) < 3:
            console.print("      [yellow]T1 Integrity Guard: Backfilling realistic profile templates.[/yellow]")
            added = _get_fallback_queries("blind", state.get("business_profile", {}), discovered_location, locale, 5, len(t1_queries))
            t1_queries.extend(added)
            t1_fallback_count = len(added)

        if len(t2_queries) < 3:
            console.print("      [yellow]T2 Integrity Guard: Backfilling realistic profile templates.[/yellow]")
            added = _get_fallback_queries("contextual", state.get("business_profile", {}), discovered_location, locale, 5, len(t2_queries))
            t2_queries.extend(added)
            t2_fallback_count = len(added)

        all_queries = t1_queries[:STRESS_TEST_BUDGET["blind"]] + t2_queries[:STRESS_TEST_BUDGET["contextual"]] + t3_queries
        
        # Diagnostics
        total_q = len(all_queries)
        total_fallback = t1_fallback_count + t2_fallback_count
        intent_buckets = set()
        for q in all_queries:
            # Simple heuristic for intent diversity
            tokens = q["query"].lower().split()
            if any(t in tokens for t in ["miglior", "best", "top"]): intent_buckets.add("best_of")
            if any(t in tokens for t in ["come", "how", "perché", "why"]): intent_buckets.add("educational")
            if any(t in tokens for t in ["costo", "prezzo", "cost", "price"]): intent_buckets.add("transactional")
            if len(tokens) > 5: intent_buckets.add("long_tail")
        
        state["stress_test_diagnostics"] = {
            "query_count": total_q,
            "fallback_count": total_fallback,
            "bucket_diversity": len(intent_buckets),
            "point_conversion": sum(q.get("points", 0) for q in all_queries) / total_q if total_q > 0 else 0
        }
        
        visibility_points, total_possible, stress_test_log, tier_stats = _run_stress_test(
            all_queries, str(perplexity_key), brand_name, url, og_title_st, gemini_client_st
        )

        visibility_score = min(100, int((visibility_points / max(total_possible, 1)) * 100))

        stress_test_summary = {
            "t1_blind_queries":       len(t1_queries),
            "t2_contextual_queries":  len(t2_queries),
            "t3_branded_queries":     len(t3_queries),
            "total_queries":          len(all_queries),
            "total_possible_points":  total_possible,
            "matched_points":         visibility_points,
            "tier_metrics":           tier_stats,
            "provenance": "Direct Context Search" if visibility_score > 0 else "Profile Inferred"
        }
        state["stress_test_log"]     = stress_test_log
        state["stress_test_summary"] = stress_test_summary
        state["stress_test_tier_stats"] = tier_stats

        # 4. AUTHORITY MATCH v4.5
        console.print("[cyan]Researcher Node[/cyan]: Calculating Authority Match...")
        raw_data = state.get("raw_data_complete", {})
        authority_ents = raw_data.get("authority_entities", [])
        
        def fuzzy_match(entity, text):
            e_norm = re.sub(r'[^\w\s]', '', str(entity).lower().strip())
            suffixes = [" spa", " s.p.a.", " ltd", " inc", " srl", " llc", " gmbh", " s.r.l."]
            for s in suffixes:
                if e_norm.endswith(s): e_norm = e_norm[:-len(s)].strip()
            t_norm = re.sub(r'[^\w\s]', '', text.lower())
            if not e_norm: return False
            if e_norm in t_norm: return True
            parts = e_norm.split()
            return len(parts) > 1 and sum(1 for p in parts if p in t_norm) >= len(parts) * 0.6
            
        matched_auth = sum(1 for e in authority_ents if fuzzy_match(e, client_content))
        auth_cov = (matched_auth / max(len(authority_ents), 1)) * 40.0

        tech_trust = 0
        if locale == "it":
            content_lower = client_content.lower()
            if re.search(r'p\.?\s*i\.?\s*v\.?\s*a\.?\s*[:=]?\s*\d{11}', content_lower): tech_trust += 10
            if re.search(r'@pec\.|posta\s+certificata', content_lower): tech_trust += 10
            if re.search(r'\brea\b\s*[:=]?\s*[a-z]{2}\s*\d+', content_lower) or 'camera di commercio' in content_lower: tech_trust += 10
        else:
            if state.get("robots_txt_status") == "allowed": tech_trust += 15
            if state.get("hreflang_count", 0) > 0: tech_trust += 15

        schema_auth = 0
        s_counts = state.get("schema_type_counts", {})
        if any(k in s_counts for k in ["Organization", "LocalBusiness"]): schema_auth += 10
        if any(k in s_counts for k in ["Person", "Author"]): schema_auth += 10
        if any(k in s_counts for k in ["Article", "NewsArticle", "FAQPage"]): schema_auth += 10

        authority_match_score = max(15, min(100, int(auth_cov + tech_trust + schema_auth)))

        # 5. DEFENSIBLE EVIDENCE DEPTH (Formerly Information Gain)
        console.print("[cyan]Researcher Node[/cyan]: Calculating Defensible Evidence Depth...")
        eg_score = 0.0
        faqs = raw_data.get("faq_patterns", [])
        topic_gaps = raw_data.get("topic_gaps", [])
        frameworks = state.get("original_frameworks", [])
        
        try:
            ig_prompt = f"""
            Evaluate the Defensible Evidence Depth of this website against its precise market.
            This measures unique, factual, and proprietary value provided by the site.
            
            Market Topic Gaps: {topic_gaps[:5]}
            Market FAQs: {faqs[:5]}
            Site's Original Frameworks/IP: {frameworks}
            
            Site Content Snippet: {client_content[:6000]}
            
            Score from 0 to 100 based on this rigorous rubric:
            1. Solutions for Market Topic Gaps (0-30 pts)
            2. Comprehensive FAQ answers (0-30 pts)
            3. Unique frameworks or proprietary data (0-40 pts)
            
            Return ONLY a single integer.
            """
            def _ig_req():
                return gemini_client_st.models.generate_content(
                    model="gemini-2.5-flash-lite",
                    contents=ig_prompt,
                )
            ig_res = execute_with_backoff(_ig_req, max_retries=2, initial_delay=2.0)
            ig_match = re.search(r'\d+', ig_res.text)
            if ig_match:
                eg_score = float(ig_match.group(0))
        except Exception as e:
            eg_score = 30.0 # conservative fallback
            
        metrics["Defensible Evidence Depth"] = int(min(100, max(0, eg_score)))

        # 6. FINAL VISIBILITY & HALLUCINATION RISK
        total_visibility = (visibility_score * 0.45) + (authority_match_score * 0.30) + (metrics["Defensible Evidence Depth"] * 0.25)
        
        base_risk = 100 - total_visibility
        confidence_penalty = (100 - confidence_score) * 0.5
        risk_score = min(95, max(5, int(base_risk + confidence_penalty)))
        
        metrics["Hallucination Risk"] = risk_score
        metrics["Entity Consensus"] = int(authority_match_score)
        
        status_label = "Enterprise-Ready" if total_visibility > 85 else "Agency-Ready" if total_visibility > 65 else "Needs Work"
        metrics["Citation Readiness"] = f"{status_label} [PROVISIONAL]" if integrity_status != "valid" else status_label

        if confidence_score < 40: citation_status = "Low Verification"
        elif total_visibility > 60 and confidence_score >= 80: citation_status = "Verified"
        elif total_visibility > 30: citation_status = "Partially Verified"
        else: citation_status = "Low Verification"
        
        ed = metrics["Defensible Evidence Depth"]
        if confidence_score < 40 or citation_status == "Low Verification":
            projected_traffic_lift = "0–5%"
        elif confidence_score < 75:
            projected_traffic_lift = "3–8%" if ed < 40 else "8–18%"
        else:
            projected_traffic_lift = "8–18%" if ed < 60 else "18–30%"

    except Exception as e:
        console.print(f"[bold red]Researcher Logic Failure: {e}[/bold red]")

    # 7. Final Recommendation Pack
    try:
        time.sleep(5)
        grounding_context = state.get("grounding_context", "")
        e_e_a_t_gaps_str = "; ".join(_safe_stringify_list(state.get("e_e_a_t_gaps", []))) or "none identified"
        frameworks_str = "; ".join(_safe_stringify_list(state.get("original_frameworks", []))) or "none identified"
        rec_content_str = "; ".join(_safe_stringify_list(state.get("recommended_content", []))) or "none identified"

        rec_prompt = f"""
        Act as Senior GEO Strategist. Write an agency deliverable.
        Context: {grounding_context}
        E-E-A-T gaps: {e_e_a_t_gaps_str}
        IP/Frameworks: {frameworks_str}
        Rec Content: {rec_content_str}
        
        Metrics: Visibility: {total_visibility:.1f}/100 | Evidence Depth: {metrics['Defensible Evidence Depth']}% | Confidence: {confidence_score}/100
        Integrity Status: {integrity_status} ({', '.join(integ_reasons)})
        
        MANDATORY RULES:
        - Ground EVERY recommendation in market data.
        - If integrity_status is not 'valid', tag titles with [PROVISIONAL].
        - Output language: {lang_name}
        
        Return STRICTLY a JSON array of objects with keys: title, rationale, priority, implementation_type.
        """
        gemini_client = genai.Client(api_key=gemini_key)
        res_rec = gemini_client.models.generate_content(model='gemini-2.5-flash-lite', contents=rec_prompt, config={"response_mime_type": "application/json"})
        
        try:
            clean_text = res_rec.text.strip()
            if clean_text.startswith("```json"): clean_text = clean_text[7:]
            if clean_text.startswith("```"): clean_text = clean_text[3:]
            if clean_text.endswith("```"): clean_text = clean_text[:-3]
            geo_recommendation_pack_json = json.loads(clean_text.strip())
            geo_recommendation_pack = json.dumps(geo_recommendation_pack_json, indent=2)
        except:
            geo_recommendation_pack = res_rec.text
            
        research_output = f"v4.5 Analysis Complete. Integrity: {integrity_status}."
    except Exception as e:
        console.print(f"[yellow]Recommendation generation skipped: {e}[/yellow]")

    state["metrics"] = metrics
    state["citation_status"] = citation_status
    state["projected_traffic_lift"] = projected_traffic_lift
    state["geo_recommendation_pack"] = geo_recommendation_pack
    state["research_output"] = research_output
    state["visibility_score"] = int(total_visibility) if 'total_visibility' in locals() else 0
    state["authority_match_score"] = int(authority_match_score) if 'authority_match_score' in locals() else 0
    state["evidence_limitations"] = "Low confidence due to parsing issues." if confidence_score < 50 else "Adequate confidence."

    if state.get("locale") == "it":
        _content = state.get("client_content_clean", "").lower()
        state["italian_trust_signals"] = {
            "piva_detected": bool(re.search(r'p\.?\s*i\.?\s*v\.?\s*a\.?\s*[:=]?\s*\d{11}', _content)),
            "pec_detected": bool(re.search(r'@pec\.|posta\s+certificata', _content)),
            "rea_detected": bool(re.search(r'\brea\b\s*[:=]?\s*[a-z]{2}\s*\d+', _content)),
            "camera_commercio_detected": 'camera di commercio' in _content,
        }
    else:
        state["italian_trust_signals"] = {}
        
    return state
