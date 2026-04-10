import os
import re
import json
import time
from rich.console import Console
from google import genai

console = Console()


def _build_grounding_context(state: dict) -> str:
    """
    Stage 1: Assemble a structured grounding block from all available state evidence.
    Injected into LLM prompts to anchor generation to real, collected data.
    Returns a Markdown-formatted string ready for prompt injection.
    """
    raw_data = state.get("raw_data_complete", {})
    depth = state.get("client_content_depth", {})
    locale = state.get("locale", "en")
    content = state.get("client_content_clean", "").lower()

    competitors = raw_data.get("competitor_entities", [])[:6]
    authority = raw_data.get("authority_entities", [])[:8]
    topic_gaps = raw_data.get("topic_gaps", [])[:8]
    faq_patterns = raw_data.get("faq_patterns", [])[:5]
    schema_counts = state.get("schema_type_counts", {})
    og_title = state.get("og_tags", {}).get("og:title", "")
    robots = state.get("robots_txt_status", "not_found")
    extraction_quality = depth.get("extraction_quality", "unknown") if isinstance(depth, dict) else "unknown"
    page_count = depth.get("page_count", 1) if isinstance(depth, dict) else 1
    js_fallback = state.get("js_fallback_used", False)

    lines = [
        "## Brand Context",
        f"- Brand: {state.get('brand_name', 'Unknown')} | Industry: {state.get('target_industry', 'Unknown')}",
        f"- Scale: {state.get('scale_level', 'Unknown')} | Location: {state.get('discovered_location', 'Unknown')}",
        f"- OG Title: {og_title or 'not set'} | robots.txt: {robots}",
        f"- Schema types present: {', '.join(schema_counts.keys()) if schema_counts else 'none detected'}",
        "",
        "## Competitor Landscape",
        f"- Direct competitors: {', '.join(competitors) if competitors else 'none identified'}",
        f"- Authority standards/entities: {', '.join(authority) if authority else 'none identified'}",
        "",
        "## Content Gaps & Market Intelligence",
        f"- Confirmed topic gaps: {'; '.join(topic_gaps) if topic_gaps else 'none identified'}",
        f"- User FAQ patterns in market: {'; '.join(faq_patterns) if faq_patterns else 'none identified'}",
        "",
        "## Evidence Quality",
        f"- Extraction quality: {extraction_quality} | Pages crawled: {page_count}",
        f"- JS rendering fallback used: {js_fallback}",
    ]

    # Stage 3: Italian trust signals
    if locale == "it":
        piva = bool(re.search(r'p\.?\s*i\.?\s*v\.?\s*a\.?\s*[:=]?\s*\d{11}', content))
        pec = bool(re.search(r'@pec\.|posta\s+certificata', content))
        rea = bool(re.search(r'\brea\b\s*[:=]?\s*[a-z]{2}\s*\d+', content))
        cam = 'camera di commercio' in content
        lines += [
            "",
            "## Italian Trust Signals",
            f"- P.IVA detected: {piva} | PEC email: {pec} | REA: {rea} | Camera di Commercio: {cam}",
            f"- Location: {state.get('discovered_location', 'unknown')}",
            "- Note: missing Italian legal anchors (P.IVA, PEC, PostalAddress schema) are critical GEO gaps for Italian AI engines.",
        ]

    return "\n".join(lines)

def process(state: dict) -> dict:
    console.print("[cyan]Content Strategist Node[/cyan]: Extracting structured data and E-E-A-T anchors...")
    
    # Defaults for new fields - only adding to state
    schema_objects = []
    original_frameworks = []
    e_e_a_t_gaps = []
    recommended_content = []
    structured_data_extract = []
    evidence_summary = ""
    extraction_warnings = []
    
    url = state.get("url", "")
    client_content = state.get("client_content_clean", "")
    gemini_key = os.getenv("GEMINI_API_KEY")
    
    # Deterministic Schema Extraction
    json_ld_blocks = state.get("json_ld_blocks", [])
    for block in json_ld_blocks:
        try:
            parsed = json.loads(block)
            if isinstance(parsed, dict):
                stype = parsed.get("@type")
                if stype:
                    structured_data_extract.append(str(stype))
                if "@graph" in parsed and isinstance(parsed["@graph"], list):
                    for item in parsed["@graph"]:
                        if isinstance(item, dict) and "@type" in item:
                            structured_data_extract.append(str(item["@type"]))
            elif isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        stype = item.get("@type")
                        if stype:
                            structured_data_extract.append(str(stype))
        except:
            extraction_warnings.append("Failed to parse a JSON-LD block")
            pass
            
    schema_objects = list(set(structured_data_extract))
    evidence_summary = f"Detected {len(schema_objects)} schema types. Page Title: {state.get('page_title', 'Missing')}."
    
    if not gemini_key or not client_content:
        console.print("[yellow]Content Strategist Node: Skipping due to missing API key or content.[/yellow]")
        state["schema_objects"] = schema_objects
        state["original_frameworks"] = original_frameworks
        state["e_e_a_t_gaps"] = e_e_a_t_gaps
        state["recommended_content"] = recommended_content
        state["structured_data_extract"] = structured_data_extract
        state["evidence_summary"] = evidence_summary
        state["extraction_warnings"] = extraction_warnings
        return state

    client = genai.Client(api_key=gemini_key)
    from nodes.api_utils import execute_with_backoff

    # Stage 1: assemble grounding context from all collected evidence
    grounding_context = _build_grounding_context(state)

    prompt = f"""
    You are a Senior GEO Content Auditor conducting an agency-grade audit.
    Your goal is to convert extracted evidence into structured action opportunities.

    ## Collected Market Intelligence
    {grounding_context}

    ## Target Website Evidence
    URL: {url}
    Detected Schema: {json.dumps(schema_objects)}
    Content Length: {len(client_content)} chars
    
    ## Primary Extraction (First 12,000 chars)
    {client_content[:12000]}

    Identify the specific gaps that prevent this brand from being the "authoritative answer" for the target industry in AI search engines (GEO).
    
    RULES FOR ACTION GENERATION:
    1. Every action MUST include:
       - "evidence_origin": ("on_site" | "off_site" | "query_gap" | "profile_inference" | "mixed")
       - "evidence_confidence": ("high" | "medium" | "low")
       - "supporting_signals": [list of 1-3 concrete evidence strings]
    2. Use "on_site" for signals discovered on this URL.
    3. Use "query_gap" or "off_site" if market context shows a gap the site doesn't cover.
    4. Use "profile_inference" if the recommendation is a best-practice for the industry but not directly triggered by a specific site-fail.
    5. Generate at least 2-3 specific "missing_page_types".
    6. Identify MUST-HAVE "trust_signal_gaps" for the specific locale (e.g., P.IVA/PEC for Italy).

    Return STRICT JSON with exactly these keys:
    "missing_page_types": list of [page_name, why_needed, evidence_origin, evidence_confidence, supporting_signals]
    "trust_signal_gaps": list of [signal, evidence_basis, evidence_origin, evidence_confidence, supporting_signals]
    "discovery_intent_gaps": list of [intent, suggestion, evidence_origin, evidence_confidence, supporting_signals]
    "entity_trust_gaps": list of [entity, relation, evidence_origin, evidence_confidence, supporting_signals]
    "local_visibility_gaps": list of [gap, fix, evidence_origin, evidence_confidence, supporting_signals]
    "competitor_gap_opportunities": list of [competitor_strength, our_counter_strategy, evidence_origin, evidence_confidence, supporting_signals]
    "original_frameworks": list of proprietary methodologies actually present on this site.
    """
    
    try:
        def _req():
            return client.models.generate_content(
                model='gemini-2.5-flash-lite',
                contents=prompt,
                config={"response_mime_type": "application/json"}
            )
        response = execute_with_backoff(_req, max_retries=2, initial_delay=2.0)
        clean_text = response.text.strip()
        if clean_text.startswith("```json"):
            clean_text = clean_text[7:]
        if clean_text.startswith("```"):
            clean_text = clean_text[3:]
        if clean_text.endswith("```"):
            clean_text = clean_text[:-3]
            
        output = json.loads(clean_text.strip())
        
        # New Structured Output
        state["missing_page_types"] = output.get("missing_page_types", [])
        state["trust_signal_gaps"] = output.get("trust_signal_gaps", [])
        state["discovery_intent_gaps"] = output.get("discovery_intent_gaps", [])
        state["entity_trust_gaps"] = output.get("entity_trust_gaps", [])
        state["local_visibility_gaps"] = output.get("local_visibility_gaps", [])
        state["competitor_gap_opportunities"] = output.get("competitor_gap_opportunities", [])
        
        # Legacy/Support Output
        state["original_frameworks"] = output.get("original_frameworks", [])
        state["e_e_a_t_gaps"] = [f"{g[0]}: {g[1]}" for g in output.get("trust_signal_gaps", [])] # for backward compat
        state["recommended_content"] = [f"{p[0]} ({p[1]})" for p in output.get("missing_page_types", [])]
        
        console.print(f"[green]Content Strategist Node[/green]: Successfully mapped {len(state['trust_signal_gaps'])} trust gaps and {len(state['missing_page_types'])} page opportunities.")
    except Exception as e:
        console.print(f"[yellow]Content Strategist Node Failure: {e}[/yellow]")
        extraction_warnings.append(str(e))

    # Existing field support
    state["schema_objects"] = schema_objects
    state["structured_data_extract"] = structured_data_extract
    state["evidence_summary"] = evidence_summary
    state["extraction_warnings"] = extraction_warnings
    state["grounding_context"] = grounding_context
    
    return state
