import json
import re
import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from rich.console import Console
import os

console = Console()

# ---------------------------------------------------------------------------
# Refined Fetch Helpers
# ---------------------------------------------------------------------------

def _primary_fetch(url: str, headers: dict, timeout: int = 12) -> tuple[requests.Response | None, str]:
    """Execute a standard requests.get fetch."""
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        return response, f"HTTP {response.status_code}"
    except Exception as e:
        return None, f"Exception: {str(e)}"

def _scraperapi_fetch(url: str, locale_code: str = "en") -> tuple[str, str]:
    """Execute a ScraperAPI rendered fetch with basic retry."""
    api_key = os.getenv("SCRAPER_API_KEY", "").strip()
    if not api_key:
        return "", "ScraperAPI Key Missing"
    
    last_reason = "ScraperAPI Timeout"
    for attempt in range(2):
        try:
            # Construct ScraperAPI URL via params for robustness
            params = {
                "api_key": api_key,
                "url": url,
                "render": "true"
            }
            if locale_code == "it":
                params["country_code"] = "it"
                
            console.print(f"   [yellow]ScraperAPI Rescue[/yellow] (Attempt {attempt+1}): Rendering {url}...")
            r = requests.get("https://api.scraperapi.com/", params=params, timeout=60)
            if r.status_code == 200:
                return r.text, "ScraperAPI Success"
            
            last_reason = f"ScraperAPI Error: HTTP {r.status_code}"
            if r.status_code in [500, 502, 503, 504]:
                time.sleep(2)
                continue
            else:
                break # Non-retryable error
        except Exception as e:
            last_reason = f"ScraperAPI Exception: {str(e)}"
            time.sleep(1)
            
    return "", last_reason

def _looks_blocked_response(status_code: int, html: str, text: str) -> bool:
    """Detect if the response is a block/challenge page."""
    if status_code in [401, 403, 429, 503]:
        return True
    
    html_lower = html.lower()
    block_patterns = [
        "access denied", "forbidden", "captcha", "verify you are human",
        "attention required", "cloudflare", "akamai", "bot protection",
        "request blocked", "enable javascript and cookies", "security check",
        "please wait while we verify", "incapsula", "distil networks"
    ]
    
    # If patterns exist and content is very thin, it's likely a block
    if any(p in html_lower for p in block_patterns) and len(text) < 500:
        return True
    
    return False

def _looks_thin_or_shell(html: str, text: str) -> bool:
    """Detect if the response is an empty JS shell or extremely thin content."""
    word_count = len(text.split())
    # Generic shell signals
    if word_count < 120 and len(html) > 5000:
        return True
    if "noscript" in html.lower() and word_count < 100:
        return True
    return False

def _choose_best_html(primary_html: str, rendered_html: str, primary_text: str, rendered_text: str) -> tuple[str, str, str]:
    """
    Select the highest quality HTML based on semantic criteria.
    Returns: (chosen_html, source_label, debug_note)
    """
    if not rendered_html:
        return primary_html, "primary", "ScraperAPI failed or skipped; using primary."
    
    if not primary_html:
        return rendered_html, "scraperapi_rendered", "Primary failed; using ScraperAPI."

    primary_wc = len(primary_text.split())
    rendered_wc = len(rendered_text.split())
    
    # Heuristic: If rendered has >= 2x text or crosses the "thin" threshold while primary doesn't
    if rendered_wc > primary_wc * 1.5 and rendered_wc > 150:
        return rendered_html, "scraperapi_rendered", f"Rendered HTML selected (Richness: {rendered_wc} vs {primary_wc} words)."
    
    return primary_html, "primary", f"Primary HTML retained (Sufficient richness: {primary_wc} words)."

# ---------------------------------------------------------------------------
# Semantic Extraction
# ---------------------------------------------------------------------------

def _extract_body_text(soup: BeautifulSoup) -> str:
    """Paragraph-level extraction with preservation of semantic markers."""
    # Create a copy to avoid destroying the original soup used for diagnostics
    s = BeautifulSoup(str(soup), "html.parser")
    
    # Selective noise removal: Do NOT decompose headers/footers entirely yet 
    # as they often contain critical Marketplace signals (onboarding/CTA)
    for noise in s(["script", "style", "noscript", "aside", "form"]):
        noise.decompose()
        
    body_parts = []
    # Targeted extraction including CTA buttons and list items often used for benefit cards
    for tag in s.find_all(["p", "li", "h1", "h2", "h3", "h4", "blockquote", "td", "dt", "dd", "span", "a"]):
        # Special handling for Span/A: only if they look like CTAs or bold emphasis
        if tag.name in ["span", "a"]:
            text = tag.get_text(strip=True)
            if len(text) > 10 and (tag.name == "a" or any(c in str(tag).lower() for c in ["btn", "cta", "button", "hero"])):
                body_parts.append(f"[{text}]")
            continue
            
        text = tag.get_text(separator=" ", strip=True)
        if len(text) > 30:
            body_parts.append(text)
            
    return " ".join(body_parts)

def _detect_semantic_signals(soup: BeautifulSoup, word_count: int, schema_counts: dict) -> dict:
    """Count structural signals to calibrate evidence density even for thin content."""
    signals = {
        "heading_count": len(soup.find_all(["h1", "h2", "h3"])),
        "cta_count": len(soup.find_all(["a", "button"], string=re.compile(r'Sign up|Log in|Order|Start|Join|Register|Become|Partner|Accedi|Ordina|Entra|Inizia|Diventa', re.I))),
        "semantic_section_count": len(soup.find_all(["section", "article", "main"])),
        "schema_signal_count": sum(schema_counts.values()),
        "word_count": word_count
    }
    
    # Identify specific marketplace signals
    summary = []
    if signals["cta_count"] > 2: summary.append("High CTA Density")
    if signals["schema_signal_count"] > 2: summary.append("Rich Schema Foundations")
    if signals["heading_count"] > 3: summary.append("Structured Argumentation")
    if signals["semantic_section_count"] > 2: summary.append("Modular Content Layout")
    
    signals["extraction_signal_summary"] = ", ".join(summary) if summary else "Linear/Unstructured"
    return signals
def _parse_schema_blocks(soup: BeautifulSoup, json_ld_blocks: list, schema_type_counts: dict):
    """Parses JSON-LD blocks and updates schema counts."""
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text()
        if not raw: continue
        try:
            block = json.loads(raw.strip())
            blocks = block if isinstance(block, list) else [block]
            for b in blocks:
                if not isinstance(b, dict): continue
                types = b.get("@type", [])
                if isinstance(types, str): types = [types]
                for t in types: schema_type_counts[t] = schema_type_counts.get(t, 0) + 1
                
                # Check for @graph
                for item in b.get("@graph", []):
                    if isinstance(item, dict):
                        itpes = item.get("@type", [])
                        if isinstance(itpes, str): itpes = [itpes]
                        for it in itpes: schema_type_counts[it] = schema_type_counts.get(it, 0) + 1
            json_ld_blocks.append(raw.strip())
        except Exception: pass

def _extract_business_address(soup: BeautifulSoup, json_ld_blocks: list) -> str:
    """Extracts business address from schema or body for geo-corroboration."""
    # 1. Try JSON-LD first
    for block_raw in json_ld_blocks:
        try:
            b = json.loads(block_raw)
            blocks = b if isinstance(b, list) else [b]
            for item in blocks:
                # Direct or @graph
                candidates = [item] + item.get("@graph", [])
                for c in candidates:
                    if not isinstance(c, dict): continue
                    addr = c.get("address")
                    if isinstance(addr, dict):
                        parts = [addr.get("streetAddress"), addr.get("addressLocality"), addr.get("postalCode")]
                        found = ", ".join([str(p) for p in parts if p])
                        if found: return found
                    elif isinstance(addr, str):
                        return addr
        except: pass
    
    # 2. Try Microdata (simplified)
    addr_tag = soup.find(attrs={"itemprop": "address"})
    if addr_tag:
        return addr_tag.get_text(separator=" ", strip=True)
        
    return ""

def _extract_metadata(soup: BeautifulSoup) -> dict:
    meta = {}
    meta["title"] = soup.title.string.strip() if soup.title and soup.title.string else ""
    desc_tag = soup.find("meta", attrs={"name": "description"})
    meta["description"] = desc_tag.get("content", "").strip() if desc_tag else ""
    can_tag = soup.find("link", rel="canonical")
    meta["canonical"] = can_tag.get("href", "").strip() if can_tag else ""
    
    og = {}
    for m in soup.find_all("meta"):
        prop = m.get("property", "") or m.get("name", "")
        if prop.startswith("og:"): og[prop] = m.get("content", "").strip()
    meta["og"] = og

    tw = {}
    for m in soup.find_all("meta"):
        name = m.get("name", "")
        if name.startswith("twitter:"): tw[name] = m.get("content", "").strip()
    meta["twitter"] = tw
    
    return meta

def process(state: dict) -> dict:
    console.print("[cyan]Content Fetcher Node[/cyan]: Starting v4.5 Anti-Block Rescue Fetch...")

    # A. INITIALIZE DEFAULTS & DEBUG FIELDS
    fetch_debug_log = []
    fetch_strategy_used = "failed"
    primary_fetch_status = None
    scraperapi_used = False
    scraperapi_success = False
    rendered_fetch_used = False
    
    client_content_raw = ""
    client_content_clean = ""
    json_ld_blocks = []
    schema_type_counts = {}
    
    url = state.get("url", "")
    locale_code = state.get("locale", "en")
    
    if not url:
        state.update({
            "audit_integrity_status": "invalid", 
            "audit_integrity_reasons": ["No URL provided."],
            "source_of_truth_mode": "failed"
        })
        return state

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7" if locale_code == "it" else "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        # B. PRIMARY FETCH
        primary_res, primary_reason = _primary_fetch(url, headers)
        primary_html = primary_res.text if primary_res and primary_res.status_code == 200 else ""
        primary_fetch_status = primary_res.status_code if primary_res else None
        
        # Preliminary parse for primary
        primary_text = ""
        if primary_html:
            p_soup = BeautifulSoup(primary_html, "html.parser")
            primary_text = _extract_body_text(p_soup)
        
        fetch_debug_log.append(f"Primary Fetch Outcome: {primary_reason}")

        # C. ESCALATION DETECTION
        should_escalate = False
        if primary_fetch_status and _looks_blocked_response(primary_fetch_status, primary_html, primary_text):
            should_escalate = True
            fetch_debug_log.append(f"Escalation Triggered: Blocked/Challenge detected ({primary_fetch_status})")
        elif _looks_thin_or_shell(primary_html, primary_text):
            should_escalate = True
            fetch_debug_log.append("Escalation Triggered: Thin content/JS-shell detected.")
        elif primary_fetch_status is None or primary_fetch_status != 200:
            should_escalate = True
            fetch_debug_log.append(f"Escalation Triggered: Primary fetch problem ({primary_reason})")

        # D. SCRAPERAPI RESCUE
        rendered_html = ""
        if should_escalate:
            scraperapi_used = True
            rendered_html, s_reason = _scraperapi_fetch(url, locale_code)
            fetch_debug_log.append(f"ScraperAPI Rescue Outcome: {s_reason}")
            if rendered_html:
                scraperapi_success = True
                rendered_fetch_used = True

        # E. SOURCE SELECTION
        rendered_text = ""
        if rendered_html:
            r_soup = BeautifulSoup(rendered_html, "html.parser")
            rendered_text = _extract_body_text(r_soup)
            
        chosen_html, strategy, selection_note = _choose_best_html(primary_html, rendered_html, primary_text, rendered_text)
        fetch_strategy_used = strategy
        fetch_debug_log.append(f"Strategy Selected: {strategy} | {selection_note}")
        
        # F. FINAL PARSING (FROM CHOSEN SOURCE)
        if not chosen_html or len(chosen_html.strip()) < 200:
            fetch_debug_log.append("Hard failure: No usable HTML recovered after all attempts.")
            state.update({
                "audit_integrity_status": "invalid",
                "audit_integrity_reasons": ["Site extraction failed: Zero or unusable HTML recovered."],
                "source_of_truth_mode": "offsite_only",
                "fetch_debug_log": fetch_debug_log
            })
            return state

        final_soup = BeautifulSoup(chosen_html, "html.parser")
        client_content_raw = chosen_html
        client_content_clean = _extract_body_text(final_soup) 
        word_count = len(client_content_clean.split())
        
        meta = _extract_metadata(final_soup)
        _parse_schema_blocks(final_soup, json_ld_blocks, schema_type_counts)
        found_address = _extract_business_address(final_soup, json_ld_blocks)
        
        # New: Semantic Signal Detection
        signals = _detect_semantic_signals(final_soup, word_count, schema_counts=schema_type_counts)
        
        # G. INTEGRITY ASSESSMENT
        reasons = []
        source_of_truth = "hybrid" # default

        # Recalibrated Integrity Logic
        has_rich_signals = signals["schema_signal_count"] >= 2 or signals["cta_count"] >= 2
        
        if word_count < 150:
            if has_rich_signals:
                integrity_status = "degraded"
                reasons.append(f"Thin extraction ({word_count} words) but meaningful structured signals found.")
            else:
                integrity_status = "invalid"
                source_of_truth = "offsite_only"
                reasons.append(f"Critical data deficiency: Extracted word count ({word_count}) is below usable threshold.")
        elif word_count < 400:
            if signals["heading_count"] < 2 and not has_rich_signals:
                integrity_status = "degraded"
                reasons.append("Weak content structure (insufficient headings/signals).")
            else:
                integrity_status = "valid"
        else:
            integrity_status = "valid"

        if not client_content_clean.strip():
            integrity_status = "invalid"
            source_of_truth = "offsite_only"
            reasons.append("No meaningful site-native text extracted.")

        # Rescue reporting rule: 
        final_note = f"Integrity: {integrity_status} | Strategy: {strategy}"
        if strategy == "scraperapi_rendered" and primary_fetch_status in [403, 429]:
            final_note = f"Primary fetch blocked ({primary_fetch_status}); ScraperAPI rendered fallback succeeded."

        state.update({
            "client_content_raw": client_content_raw,
            "client_content_clean": client_content_clean,
            "client_content_depth": {
                "word_count": word_count,
                "extraction_quality": "high" if word_count > 600 else "medium" if word_count > 250 else "low",
                "schema_block_count": len(json_ld_blocks),
                "heading_count": signals["heading_count"],
                "cta_count": signals["cta_count"],
                "semantic_signals": signals
            },
            "fetch_strategy_used": fetch_strategy_used,
            "primary_fetch_status": primary_fetch_status,
            "scraperapi_used": scraperapi_used,
            "scraperapi_success": scraperapi_success,
            "rendered_fetch_used": rendered_fetch_used,
            "fetch_debug_log": fetch_debug_log,
            "audit_integrity_status": integrity_status,
            "audit_integrity_reasons": reasons,
            "source_of_truth_mode": source_of_truth,
            "extracted_on_site_address": found_address,
            "content_fetch_notes": final_note,
            "page_title": meta["title"],
            "meta_description": meta["description"],
            "canonical_url": meta["canonical"],
            "og_tags": meta["og"],
            "twitter_tags": meta["twitter"],
            "json_ld_blocks": json_ld_blocks,
            "schema_type_counts": schema_type_counts
        })

    except Exception as e:
        fetch_debug_log.append(f"CRITICAL FETCH CRASH: {str(e)}")
        state.update({
            "audit_integrity_status": "invalid",
            "audit_integrity_reasons": [f"Fetcher Node Crash: {str(e)}"],
            "source_of_truth_mode": "offsite_only",
            "fetch_debug_log": fetch_debug_log
        })

    console.print(f"   [green]Content Fetcher Complete[/green] | Mode: {state.get('source_of_truth_mode')} | Word count: {state.get('client_content_depth', {}).get('word_count', 0)}")
    return state
