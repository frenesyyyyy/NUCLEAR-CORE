import json
import re
import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from rich.console import Console
import os

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

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
        return primary_html, "primary", "Render fallback failed or skipped; using primary."
    
    if not primary_html:
        return rendered_html, "rendered", "Primary failed; using Render fallback."

    primary_wc = len(primary_text.split())
    rendered_wc = len(rendered_text.split())
    
    # Heuristic: If rendered has >= 1.5x text or crosses the "thin" threshold while primary doesn't
    if rendered_wc > primary_wc * 1.5 and rendered_wc > 150:
        return rendered_html, "rendered", f"Rendered HTML selected (Richness: {rendered_wc} vs {primary_wc} words)."
    
    return primary_html, "primary", f"Primary HTML retained (Sufficient richness: {primary_wc} words)."

def _detect_js_heavy_suspect(html: str, text: str) -> tuple[bool, bool]:
    """
    Heuristic to detect JS-heavy/Shell-heavy pages and anti-bot blocks.
    Returns: (is_js_heavy_suspect, is_anti_bot_detected)
    """
    word_count = len(text.split())
    html_len = len(html)
    script_count = html.count("<script")
    visible_text_ratio = len(text) / max(1, len(html))
    
    triggers = 0
    if word_count < 180: triggers += 1
    if html_len > 80000: triggers += 1
    if script_count > 20: triggers += 1
    if visible_text_ratio < 0.02: triggers += 1
    
    app_shell_markers = ["_NEXT_DATA_", "_next/static", "webpack", "data-reactroot", "window._INITIAL_STATE_"]
    if any(m in html for m in app_shell_markers): triggers += 1
    
    html_lower = html.lower()
    anti_bot_markers = ["px-cloud", "captcha", "challenge", "cf-", "verify you are human"]
    is_anti_bot = any(m in html_lower for m in anti_bot_markers)
    if is_anti_bot: triggers += 1
    
    return (triggers >= 2), is_anti_bot

# ---------------------------------------------------------------------------
# Multi-Page Content Fetcher v2.1 Helpers
# ---------------------------------------------------------------------------

def _fingerprint_site(html: str, text: str, url: str) -> dict:
    html_lower = html.lower()
    
    has_cart = any(x in html_lower for x in ['add to cart', 'checkout', 'carrello', 'aggiungi al carrello', '🛒'])
    has_pricing = any(x in html_lower for x in ['pricing', 'prezzi', 'plans', 'abbonamenti'])
    has_book = any(x in html_lower for x in ['book now', 'prenota', 'appointment', 'appuntamento', 'schedule'])
    has_vendor = any(x in html_lower for x in ['become a partner', 'sell with us', 'become a driver', 'diventa partner', 'rider', 'vendor'])
    
    if has_cart:
        site_class = "ecommerce"
    elif has_vendor:
        site_class = "marketplace"
    elif has_pricing and ("software" in html_lower or "saas" in html_lower or "api " in html_lower):
        site_class = "saas"
    elif has_book:
        site_class = "local_services"
    elif len(text.split()) > 1500 and ("investor" in html_lower or "corporate" in html_lower):
        site_class = "enterprise_corporate"
    else:
        site_class = "brochure_local"
        
    js_suspect, anti_bot = _detect_js_heavy_suspect(html, text)
    
    return {
        "site_class": site_class,
        "js_heavy_suspect": js_suspect,
        "anti_bot_detected": anti_bot,
    }

def _determine_acquisition_policy(fingerprint: dict) -> dict:
    sc = fingerprint.get("site_class", "brochure_local")
    
    if sc == "brochure_local":
        max_pages = 3
        boosts = ["about", "contact", "services", "servizi", "chi siamo"]
    elif sc == "local_services":
        max_pages = 6
        boosts = ["services", "servizi", "book", "prenota", "team", "faq", "trattamenti"]
        boosts += [
            "sedi", "locations", "clinics",
            "specialita", "specialties",
            "equipe", "team", "medici",
            "faq", "documenti", "convenzioni"
        ]
    elif sc == "saas":
        max_pages = 5
        boosts = ["pricing", "features", "integrations", "docs", "security", "prezzi"]
    elif sc in ["ecommerce", "marketplace"]:
        max_pages = 5
        boosts = ["shop", "category", "returns", "help", "faq", "partner", "seller", "rider", "vendor"]
    elif sc == "enterprise_corporate":
        max_pages = 5
        boosts = ["about", "solutions", "investors", "legal", "contact"]
    else:
        max_pages = 3
        boosts = ["about", "contact"]
        
    return {
        "max_pages_to_fetch": min(max_pages, 6), # MAX GLOBAL LIMIT
        "boosted_page_tokens": boosts
    }

def _extract_and_score_links(soup: BeautifulSoup, base_url: str, policy: dict) -> list[dict]:
    boosts = policy.get("boosted_page_tokens", [])
    generic_high_value = ["about", "services", "solutions", "product", "pricing", "contact", "faq", "legal", "privacy", "terms", "team", "chi siamo", "contatti"]
    junk_patterns = ["login", "cart", "account", "checkout", "signup", "?", "#", "password", "forgot"]
    
    unique_links = {}
    
    for a in soup.find_all("a", href=True):
        href = a['href']
        text = a.get_text(strip=True).lower()
        full_url = urljoin(base_url, href)
        
        if urlparse(full_url).netloc != urlparse(base_url).netloc: continue
        
        href_lower = href.lower()
        if any(j in href_lower for j in junk_patterns): continue
            
        score = 0
        assigned_type = "internal"
        
        for b in boosts:
            if b in href_lower or b in text:
                score += 50
                assigned_type = b
        
        for g in generic_high_value:
            if g in href_lower or g in text:
                score += 20
                assigned_type = g if assigned_type == "internal" else assigned_type

        if any(k in href_lower for k in ["sedi", "locations"]):
            score += 60

        if any(k in href_lower for k in ["specialita", "specialties"]):
            score += 50
                
        if score > 0:
            if full_url not in unique_links or unique_links[full_url]['score'] < score:
                unique_links[full_url] = {"url": full_url, "score": score, "type": assigned_type}
                
    sorted_links = sorted(unique_links.values(), key=lambda x: x['score'], reverse=True)
    needed = policy.get("max_pages_to_fetch", 3) - 1
    return sorted_links[:max(3, needed)]

def _playwright_fetch(url: str) -> tuple[str, str]:
    """Execute a Playwright rendered fetch with fallback rendering capabilities."""
    if sync_playwright is None:
        return "", "Playwright uninstalled"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            console.print(f"   [cyan]Playwright Tier 2[/cyan]: Rendering {url}...")
            
            # Go to page with strict 30s timeout
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            # Wait for content. Try multiple common semantic anchors, fallback to generic wait if none found
            selectors = ["main", "[role='main']", "h1", "form", "footer", "input[type='text']"]
            for sel in selectors:
                try:
                    page.wait_for_selector(sel, state="attached", timeout=10000)
                    break
                except PlaywrightTimeoutError:
                    continue
            else:
                # If all standard selectors timeout, allow a few seconds to let any JS finish loading
                page.wait_for_timeout(3000)

            content = page.content()
            browser.close()
            return content, "Playwright Success"
            
    except Exception as e:
        return "", f"Playwright Exception: {e}"

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

def _extract_legal_metadata(html: str) -> str:
    """Extracts critical business footprints (P.IVA, PEC, REA) directly from raw HTML via regex."""
    piva_match = re.search(r'(?:P\.IVA|Partita IVA|VAT(?:[\s]*ID)?|IVA|C\.F\.)[\s\:\.\-]*([0-9]{11})', html, re.IGNORECASE)
    pec_match = re.search(r'([a-zA-Z0-9\.\-\_]+\@(?:pec|legalmail|cert)\.[a-zA-Z\.]{2,})', html, re.IGNORECASE)
    rea_match = re.search(r'(?:REA)[\s\:\.\-]*([a-zA-Z]{2}\s*[\-]?\s*[0-9]{5,6})', html, re.IGNORECASE)
    
    findings = []
    if piva_match:
        findings.append(f"P.IVA: {piva_match.group(1)}")
    if pec_match:
        findings.append(f"PEC: {pec_match.group(1)}")
    if rea_match:
        findings.append(f"REA: {rea_match.group(1)}")
        
    if findings:
        return f"\n\n[CRITICAL_METADATA_FOOTPRINT] {' | '.join(findings)}"
    return ""

def _execute_tier_fetch(url: str, locale_code: str, force_playwright: bool = False, max_time: int = 60) -> dict:
    """Encapsulates the 3-Tier fetch logic for a single URL."""
    fetch_debug_log = []
    start_time = time.time()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7" if locale_code == "it" else "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    
    # B. PRIMARY FETCH
    primary_res, primary_reason = _primary_fetch(url, headers, timeout=6)
    primary_html = primary_res.text if primary_res and primary_res.status_code == 200 else ""
    primary_fetch_status = primary_res.status_code if primary_res else None
    
    primary_text = ""
    if primary_html:
        p_soup = BeautifulSoup(primary_html, "html.parser")
        primary_text = _extract_body_text(p_soup)
        
    fetch_debug_log.append(f"Primary fetch [{url}]: {primary_reason}")

    # C. ESCALATION DETECTION
    should_escalate = force_playwright
    if primary_fetch_status and _looks_blocked_response(primary_fetch_status, primary_html, primary_text):
        should_escalate = True
        fetch_debug_log.append(f"Escalation: Blocked/Challenge.")
    elif _looks_thin_or_shell(primary_html, primary_text):
        should_escalate = True
        fetch_debug_log.append("Escalation: Thin/JS-shell.")
    elif primary_fetch_status is None or primary_fetch_status != 200:
        should_escalate = True
        fetch_debug_log.append(f"Escalation: Primary failed.")

    js_suspect, anti_bot = _detect_js_heavy_suspect(primary_html, primary_text)
    if js_suspect and not force_playwright:
        should_escalate = True
        fetch_debug_log.append("Escalation: JS-heavy markers.")
        
    rendered_html = ""
    rendered_text = ""
    render_success = False
    render_source = "primary"
    scraperapi_used = False
    
    # D. PLAYWRIGHT TIER 2
    if should_escalate and (time.time() - start_time) < max_time:
        rendered_html, p_reason = _playwright_fetch(url)
        fetch_debug_log.append(f"Playwright Tier 2: {p_reason}")
        if rendered_html:
            r_soup = BeautifulSoup(rendered_html, "html.parser")
            rendered_text = _extract_body_text(r_soup)
            render_wc = len(rendered_text.split())
            render_ratio = len(rendered_text) / max(1, len(rendered_html))
            if render_wc < 180 and render_ratio < 0.02:
                fetch_debug_log.append("Playwright valid failed (too thin). Cascading to Tier 3.")
                rendered_html = ""
            else:
                render_success = True
                render_source = "playwright"

    # E. SCRAPERAPI TIER 3
    if should_escalate and not render_success and (time.time() - start_time) < max_time:
        scraperapi_used = True
        render_source = "scraperapi"
        rendered_html, s_reason = _scraperapi_fetch(url, locale_code)
        fetch_debug_log.append(f"ScraperAPI Tier 3: {s_reason}")
        if rendered_html:
            r_soup = BeautifulSoup(rendered_html, "html.parser")
            rendered_text = _extract_body_text(r_soup)

    chosen_html, strategy, selection_note = _choose_best_html(primary_html, rendered_html, primary_text, rendered_text)
    fetch_debug_log.append(f"Strategy Selected: {strategy} | {selection_note}")
    
    return {
        "chosen_html": chosen_html,
        "strategy": strategy,
        "primary_fetch_status": primary_fetch_status,
        "js_heavy_suspect": js_suspect,
        "anti_bot": anti_bot,
        "scraperapi_used": scraperapi_used,
        "render_source": render_source if strategy == "rendered" else "primary",
        "fetch_debug_log": fetch_debug_log
    }

def process(state: dict) -> dict:
    console.print("[cyan]Content Fetcher Node[/cyan]: Starting v5.0 Multi-Page Adaptive Fetch...")

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
        start_time_global = time.time()
        MAX_GLOBAL_TIME = 60
        
        client_content_raw_list = []
        client_content_clean_parts = []
        json_ld_blocks = []
        schema_type_counts = {}
        fetched_page_urls = []
        fetched_page_types = []
        master_fetch_log = []
        
        # 1. Fetch Homepage
        hp_res = _execute_tier_fetch(url, locale_code)
        master_fetch_log.extend(hp_res["fetch_debug_log"])
        
        if not hp_res["chosen_html"]:
            master_fetch_log.append("Hard failure: No usable HTML recovered for homepage.")
            state.update({
                "audit_integrity_status": "invalid",
                "audit_integrity_reasons": ["Site extraction failed: Zero or unusable HTML recovered."],
                "source_of_truth_mode": "offsite_only",
                "fetch_debug_log": master_fetch_log
            })
            return state
            
        hp_soup = BeautifulSoup(hp_res["chosen_html"], "html.parser")
        hp_text = _extract_body_text(hp_soup)
        
        # 2. Fingerprint & Policy
        fingerprint = _fingerprint_site(hp_res["chosen_html"], hp_text, url)
        policy = _determine_acquisition_policy(fingerprint)
        
        # 3. Add homepage evidence
        client_content_raw_list.append({"url": url, "html": hp_res["chosen_html"]})
        client_content_clean_parts.append(hp_text)
        fetched_page_urls.append(url)
        fetched_page_types.append("homepage")
        _parse_schema_blocks(hp_soup, json_ld_blocks, schema_type_counts)
        meta = _extract_metadata(hp_soup)
        found_address = _extract_business_address(hp_soup, json_ld_blocks)
        
        # 4. Extract and Fetch Links
        links = _extract_and_score_links(hp_soup, url, policy)
        playwright_force = fingerprint["js_heavy_suspect"]
        
        for link_obj in links:
            if (time.time() - start_time_global) > MAX_GLOBAL_TIME:
                master_fetch_log.append("Global fetch timeout reached. Aborting further pages.")
                break
                
            inner_url = link_obj["url"]
            inner_res = _execute_tier_fetch(inner_url, locale_code, force_playwright=playwright_force, max_time=25)
            master_fetch_log.extend(inner_res["fetch_debug_log"])
            
            if inner_res["chosen_html"]:
                i_soup = BeautifulSoup(inner_res["chosen_html"], "html.parser")
                i_text = _extract_body_text(i_soup)
                
                if len(i_text) > 100 and i_text not in client_content_clean_parts:
                    client_content_raw_list.append({"url": inner_url, "html": inner_res["chosen_html"]})
                    client_content_clean_parts.append(i_text)
                    fetched_page_urls.append(inner_url)
                    fetched_page_types.append(link_obj["type"])
                    _parse_schema_blocks(i_soup, json_ld_blocks, schema_type_counts)

        # 5. Aggregate logic
        merged_clean = "\n\n".join(client_content_clean_parts)
        
        # Inject raw footprint directly into semantic clean string
        legal_footprint = _extract_legal_metadata(hp_res["chosen_html"])
        if legal_footprint:
            merged_clean += legal_footprint
            
        word_count = len(merged_clean.split())
        
        signals = _detect_semantic_signals(hp_soup, word_count, schema_counts=schema_type_counts)
        
        # 6. Integrity
        reasons = []
        source_of_truth = "hybrid"
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
            
        if not merged_clean.strip():
            integrity_status = "invalid"
            source_of_truth = "offsite_only"
            reasons.append("No meaningful site-native text extracted.")
            
        strategy = hp_res["strategy"]
        final_note = f"Integrity: {integrity_status} | Strategy: {strategy} (Pages: {len(fetched_page_urls)})"

        state.update({
            "client_content_raw": client_content_raw_list,
            "client_content_clean": merged_clean,
            "client_content_depth": {
                "word_count": word_count,
                "extraction_quality": "high" if word_count > 600 else "medium" if word_count > 250 else "low",
                "schema_block_count": len(json_ld_blocks),
                "heading_count": signals["heading_count"],
                "cta_count": signals["cta_count"],
                "semantic_signals": signals
            },
            "fetch_strategy_used": strategy,
            "primary_fetch_status": hp_res["primary_fetch_status"],
            "scraperapi_used": hp_res["scraperapi_used"],
            "js_heavy_suspect": fingerprint["js_heavy_suspect"],
            "anti_bot_detected": fingerprint["anti_bot_detected"],
            "render_source": hp_res["render_source"],
            "fetched_page_urls": fetched_page_urls,
            "fetched_page_types": fetched_page_types,
            "site_fingerprint": fingerprint,
            "acquisition_mode": "multi_page",
            "page_selection_notes": f"Policy selected max {policy['max_pages_to_fetch']} pages.",
            "fetch_debug_log": master_fetch_log,
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
        fetch_debug_log = [f"CRITICAL FETCH CRASH: {str(e)}"]
        state.update({
            "audit_integrity_status": "invalid",
            "audit_integrity_reasons": [f"Fetcher Node Crash: {str(e)}"],
            "source_of_truth_mode": "offsite_only",
            "fetch_debug_log": fetch_debug_log
        })

    console.print(f"   [green]Content Fetcher Complete[/green] | Mode: {state.get('source_of_truth_mode')} | Word count: {state.get('client_content_depth', {}).get('word_count', 0)}")
    return state
