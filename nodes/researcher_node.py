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
from nodes.api_utils import execute_with_backoff

console = Console()


# ──────────────────────────────────────────────────────────────────────────────
# Stress Test Helpers (3-Tier Evidence-Derived Architecture)
# ──────────────────────────────────────────────────────────────────────────────

def _build_blind_queries(faq_patterns: list, authority_entities: list, lang_name: str, budget: int = 8) -> list:
    """
    Tier 1: Blind Discovery.
    Uses FAQ patterns (no brand mentioned) as pure organic intent probes.
    Also probes up to 3 authority entities as secondary blind signals.
    Points value: 25 (highest — hardest signal to earn).
    """
    queries = []
    for faq in faq_patterns[:budget]:
        if faq and len(str(faq).strip()) > 8:
            queries.append({
                "query": str(faq).strip(),
                "tier": "blind_discovery",
                "points": 25
            })

    # Secondary blind probes: use authority/certification entities as search terms
    # These never mention the brand; they test whether AI answers about the market mention it.
    auth_budget = max(0, budget - len(queries))  # fill remaining budget
    for ent in authority_entities[:min(3, auth_budget)]:
        ent_q = str(ent).strip()
        if ent_q and len(ent_q) > 5:
            question = (
                f"quale azienda è leader in {ent_q}" if lang_name == "Italian"
                else f"which company leads in {ent_q}"
            )
            queries.append({
                "query": question,
                "tier": "blind_discovery",
                "points": 20  # slightly lower — less pure
            })
    return queries


def _build_contextual_queries(
    topic_gaps: list,
    lang_name: str,
    gemini_client,
    budget: int = 10,
) -> list:
    """
    Tier 2: Contextual Discovery.
    Converts confirmed topic gaps into natural user questions using a single
    Gemini call. Topics are fixed from validated evidence; only the phrasing is LLM.
    Falls back to the raw gap string if the LLM call fails.
    Points value: 15.
    """
    if not topic_gaps:
        return []

    gaps_to_use = [str(g).strip() for g in topic_gaps[:budget] if g and len(str(g).strip()) > 5]
    if not gaps_to_use:
        return []

    try:
        phrasing_prompt = (
            f"Convert each of these content gap descriptions into a single natural search query "
            f"that a real user would type. Language: {lang_name}. "
            f"Output ONLY valid JSON: {{\"questions\": [\"...\", \"...\"]}}.\n"
            f"Gaps: {json.dumps(gaps_to_use)}"
        )
        def _q_req():
            return gemini_client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=phrasing_prompt,
                config={"response_mime_type": "application/json"}
            )
        res = execute_with_backoff(_q_req, max_retries=2, initial_delay=2.0)
        questions = json.loads(res.text).get("questions", [])
    except Exception:
        questions = []  # fallback to raw gap strings below

    queries = []
    for i, gap in enumerate(gaps_to_use):
        q_text = questions[i].strip() if i < len(questions) and questions[i].strip() else gap
        queries.append({
            "query": q_text,
            "tier": "contextual_discovery",
            "points": 15
        })
    return queries


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
    Unified sequential Perplexity executor for all three tiers.
    Returns (visibility_points, total_possible, stress_test_log).
    """
    visibility_points = 0
    total_possible = sum(q["points"] for q in all_queries)
    log = []

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

    return visibility_points, total_possible, log

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
    # Measures how faithfully we captured the site. Only site-side defects apply here.
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
    # Measures how trustworthy the BUSINESS CONTEXT we assembled is.
    # This is independent of extraction quality — a well-scraped site can still
    # have a polluted or fabricated market frame.
    cr_score = 100
    
    # Penalise absent or trivially generic schema
    s_counts_cr = state.get("schema_type_counts", {})
    generic_only = all(k in {"WebSite", "WebPage", "BreadcrumbList", "SiteLinksSearchBox"} for k in s_counts_cr)
    if not s_counts_cr:
        cr_score -= 20   # No schema at all — AI engines have no entity anchor
    elif generic_only:
        cr_score -= 10   # Schema exists but carries zero entity or product specifics

    # Penalise templated / low-quality metadata
    og_title_cr = state.get("og_tags", {}).get("og:title", "")
    templated_signals = ["home", "welcome", "untitled", "sample", "default", "page"]
    if og_title_cr and any(sig in og_title_cr.lower() for sig in templated_signals):
        cr_score -= 15   # OG title looks auto-generated or unconfigured

    # Penalise when our validator corrected the orchestrator's classification
    # (correction itself is good, but it means initial framing was wrong — context is less stable)
    class_notes = state.get("classification_notes", "")
    if class_notes and "confirmed" not in class_notes.lower():
        cr_score -= 10   # industry was silently changed mid-pipeline

    # Penalise polluted external market intelligence
    if ext_quality == "low":
        cr_score -= 30   # Prospector returned too few or irrelevant competitors
    elif ext_quality == "medium":
        cr_score -= 10

    context_reliability = max(10, min(100, cr_score))

    # ── BLENDED FINAL SCORE ───────────────────────────────────────────────────
    confidence_score = int(0.5 * extraction_integrity + 0.5 * context_reliability)
    
    # Export both sub-scores for downstream use
    state["extraction_integrity"] = extraction_integrity
    state["context_reliability"] = context_reliability
    
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

        # Stage 2: Expand ChromaDB corpus with market intelligence
        # This makes Information Gain queries measure client content vs. real market landscape.
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

        # Grounding Scanner v2.0 retired (signals moved into Authority Match)

        # 3. THREE-TIER STRESS TEST ENGINE (20% Weight)
        # T1: blind_discovery   — from faq_patterns (no LLM, pure intent signal)
        # T2: contextual_discovery — from topic_gaps  (one small Gemini phrasing call)
        # T3: branded_validation — from brand context (fully deterministic, no LLM)
        console.print(f"[cyan]Researcher Node[/cyan]: Executing 3-Tier Evidence-Derived Stress Test for '{brand_name}'...")
        visibility_points = 0
        stress_test_log = []

        raw_data_st = state.get("raw_data_complete", {})
        faq_patterns_st    = raw_data_st.get("faq_patterns", [])
        topic_gaps_st      = raw_data_st.get("topic_gaps", [])
        competitor_ents_st = raw_data_st.get("competitor_entities", [])
        authority_ents_st  = raw_data_st.get("authority_entities", [])
        og_title_st        = state.get("og_tags", {}).get("og:title", "")

        # Configurable per-tier budgets (agency operators can set via .env)
        STRESS_TEST_BUDGET = {
            "blind":       int(os.getenv("GEO_T1_BUDGET", "8")),
            "contextual":  int(os.getenv("GEO_T2_BUDGET", "10")),
            "branded":     int(os.getenv("GEO_T3_BUDGET", "6")),
        }

        gemini_client_st = genai.Client(api_key=gemini_key)

        t1_queries = _build_blind_queries(faq_patterns_st, authority_ents_st, lang_name, STRESS_TEST_BUDGET["blind"])
        t2_queries = _build_contextual_queries(topic_gaps_st, lang_name, gemini_client_st, STRESS_TEST_BUDGET["contextual"])
        t3_queries = _build_branded_queries(
            brand_name, target_industry, discovered_location, scale_level, locale,
            competitor_ents_st, STRESS_TEST_BUDGET["branded"]
        )

        all_queries = t1_queries + t2_queries + t3_queries
        console.print(
            f"  Tiers: T1={len(t1_queries)} blind | "
            f"T2={len(t2_queries)} contextual | "
            f"T3={len(t3_queries)} branded | "
            f"Total={len(all_queries)} queries"
        )

        visibility_points, total_possible, stress_test_log = _run_stress_test(
            all_queries, str(perplexity_key), brand_name, url, og_title_st, gemini_client_st
        )

        # Dynamic ceiling — scales with actual query count, no hardcoded /140
        visibility_score = min(100, int((visibility_points / max(total_possible, 1)) * 100))

        # Tier breakdown for state
        stress_test_summary = {
            "t1_blind_queries":       len(t1_queries),
            "t2_contextual_queries":  len(t2_queries),
            "t3_branded_queries":     len(t3_queries),
            "total_queries":          len(all_queries),
            "total_possible_points":  total_possible,
            "matched_points":         visibility_points,
        }
        state["stress_test_log"]     = stress_test_log
        state["stress_test_summary"] = stress_test_summary
        console.print(
            f"[green]Stress Test[/green]: {visibility_points}/{total_possible}pts → "
            f"visibility_score={visibility_score}"
        )

        # 4. AUTHORITY MATCH v4.5 (Multi-dimensional Trust)
        console.print("[cyan]Researcher Node[/cyan]: Calculating Authority Match...")
        raw_data = state.get("raw_data_complete", {})
        authority_ents = raw_data.get("authority_entities", [])
        
        # Dimension A: Entity Cohabitation (40%)
        # Do they write about the same entities the authorities write about?
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

        # Dimension B: Technical Trust Signals (30%)
        tech_trust = 0
        if locale == "it":
            content_lower = client_content.lower()
            if re.search(r'p\.?\s*i\.?\s*v\.?\s*a\.?\s*[:=]?\s*\d{11}', content_lower): tech_trust += 10
            if re.search(r'@pec\.|posta\s+certificata', content_lower): tech_trust += 10
            if re.search(r'\brea\b\s*[:=]?\s*[a-z]{2}\s*\d+', content_lower) or 'camera di commercio' in content_lower: tech_trust += 10
        else:
            if state.get("robots_txt_status") == "allowed": tech_trust += 15
            if state.get("hreflang_count", 0) > 0: tech_trust += 15

        # Dimension C: Structured Data Authority (30%)
        schema_auth = 0
        s_counts = state.get("schema_type_counts", {})
        if any(k in s_counts for k in ["Organization", "LocalBusiness"]): schema_auth += 10
        if any(k in s_counts for k in ["Person", "Author"]): schema_auth += 10
        if any(k in s_counts for k in ["Article", "NewsArticle", "FAQPage"]): schema_auth += 10

        authority_match_score = max(15, min(100, int(auth_cov + tech_trust + schema_auth)))

        # 5. INFORMATION GAIN v4.5 (Rigorous LLM Evaluation)
        console.print("[cyan]Researcher Node[/cyan]: Calculating Information Gain rigorously...")
        ig_score = 0.0
        faqs = raw_data.get("faq_patterns", [])
        topic_gaps = raw_data.get("topic_gaps", [])
        frameworks = state.get("original_frameworks", [])
        
        try:
            ig_prompt = f"""
            Evaluate the Information Gain of this website against its precise market.
            Information Gain is the unique, proprietary value this site provides that competitors do not.
            
            Market Topic Gaps: {topic_gaps[:5]}
            Market FAQs: {faqs[:5]}
            Site's Original Frameworks/IP: {frameworks}
            
            Site Content Snippet: {client_content[:6000]}
            
            Score the site's Information Gain from 0 to 100 based on this rigorous rubric:
            1. Does the site explicitly address the Market Topic Gaps with specific solutions? (0-30 pts)
            2. Does it answer the exact Market FAQs comprehensively? (0-30 pts)
            3. Does it possess clearly identified unique frameworks, original data, or proprietary technology? (0-40 pts)
            
            Return ONLY a single integer between 0 and 100.
            """
            def _ig_req():
                return gemini_client_st.models.generate_content(
                    model="gemini-2.5-flash-lite",
                    contents=ig_prompt,
                )
            ig_res = execute_with_backoff(_ig_req, max_retries=2, initial_delay=2.0)
            ig_match = re.search(r'\d+', ig_res.text)
            if ig_match:
                ig_score = float(ig_match.group(0))
        except Exception as e:
            console.print(f"[yellow]IG Math fallback due to error: {e}[/yellow]")
            ig_score = 30.0 # conservative fallback
            
        metrics["Information Gain"] = int(min(100, max(0, ig_score)))

        # 6. FINAL VISIBILITY & HALLUCINATION RISK v4.5
        # The stress test (which runs externally on Perplexity) is the heaviest realistic signal
        total_visibility = (visibility_score * 0.45) + (authority_match_score * 0.30) + (metrics["Information Gain"] * 0.25)
        
        # Risk is the inverse of visibility, amplified by how low our confidence is
        # If confidence is low, the stated hallucination risk increases proportionately
        base_risk = 100 - total_visibility
        confidence_penalty = (100 - confidence_score) * 0.5
        risk_score = min(95, max(5, int(base_risk + confidence_penalty)))
        
        metrics["Hallucination Risk"] = risk_score
        metrics["Entity Consensus"] = int(authority_match_score)
        metrics["Citation Readiness"] = "Enterprise-Ready" if total_visibility > 85 else "Agency-Ready" if total_visibility > 65 else "Needs Work"

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

        # Stage 1: pull grounding context assembled by ContentStrategist
        # Falls back gracefully to an empty string if ContentStrategist was skipped.
        grounding_context = state.get("grounding_context", "")

        # Enrich grounding with fields only available post-Researcher
        e_e_a_t_gaps_str = "; ".join(state.get("e_e_a_t_gaps", [])) or "none identified"
        frameworks_str = "; ".join(state.get("original_frameworks", [])) or "none identified"
        rec_content_str = "; ".join(state.get("recommended_content", [])) or "none identified"

        rec_prompt = f"""
        Act as Senior GEO Strategist writing an agency deliverable for a paying client.

        ## Collected Market Intelligence
        {grounding_context}

        ## ContentStrategist Findings
        - E-E-A-T gaps identified: {e_e_a_t_gaps_str}
        - Proprietary IP/frameworks on site: {frameworks_str}
        - Recommended new content (from strategist): {rec_content_str}

        ## Agency Metrics
        Visibility: {total_visibility:.1f}/100 | Info Gain: {metrics['Information Gain']}% | Hallucination Risk: {metrics['Hallucination Risk']}%
        Citation Readiness: {metrics['Citation Readiness']} | Confidence: {confidence_score}/100
        URL: {url}

        MANDATORY RULES:
        - Ground EVERY recommendation in the market intelligence above. Name specific gaps, competitors, or FAQ patterns.
        - DO NOT invent technical SEO issues not present in the data.
        - Use agency-safe hedging: "evidence suggests", "inferred opportunity", "manual verification recommended".
        - If evidence is missing for a dimension, say so explicitly.
        - Output language: {lang_name}

        Return STRICTLY a JSON array of recommendation objects:
        [
          {{
            "title": "Clear Actionable Title",
            "rationale": "Why this matters, grounded in the market data above",
            "priority": "High|Medium|Low",
            "implementation_type": "Technical|Content|Authority"
          }}
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

    # Stage 3: Italian trust signals stored for Finalizer and downstream use
    if state.get("locale") == "it":
        _content = state.get("client_content_clean", "").lower()
        import re as _re
        state["italian_trust_signals"] = {
            "piva_detected":            bool(_re.search(r'p\.?\s*i\.?\s*v\.?\s*a\.?\s*[:=]?\s*\d{11}', _content)),
            "pec_detected":             bool(_re.search(r'@pec\.|posta\s+certificata', _content)),
            "rea_detected":             bool(_re.search(r'\brea\b\s*[:=]?\s*[a-z]{2}\s*\d+', _content)),
            "camera_commercio_detected": 'camera di commercio' in _content,
            "codice_fiscale_detected":   bool(_re.search(r'codice\s+fiscale\s*[:=]?\s*[a-z0-9]{16}', _content)),
        }
    else:
        state["italian_trust_signals"] = {}
    
    return state
