import json
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from rich.console import Console
import os

console = Console()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_body_text(soup: BeautifulSoup) -> str:
    """
    Stage 1 fix: paragraph-level extraction instead of soup.get_text().
    Targets only semantic content tags; skips nav/footer/script noise.
    Requires a minimum token length to avoid button labels and breadcrumbs.
    """
    body_parts = []
    for tag in soup.find_all(["p", "li", "h1", "h2", "h3", "h4", "blockquote", "td", "dt", "dd"]):
        text = tag.get_text(separator=" ", strip=True)
        if len(text) > 40:          # drop link labels / button text / nav items
            body_parts.append(text)
    return " ".join(body_parts)


def _parse_schema_blocks(soup: BeautifulSoup, json_ld_blocks: list, schema_type_counts: dict) -> None:
    """
    Parse JSON-LD blocks from a soup object in-place into the shared accumulators.
    Handles single objects, lists, and @graph containers.
    """
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            block = json.loads(raw.strip())
            blocks = block if isinstance(block, list) else [block]
            for b in blocks:
                if not isinstance(b, dict):
                    continue
                # Direct @type
                for t in ([b["@type"]] if isinstance(b.get("@type"), str) else (b.get("@type") or [])):
                    schema_type_counts[t] = schema_type_counts.get(t, 0) + 1
                # @graph children
                for item in b.get("@graph", []):
                    if isinstance(item, dict):
                        for t in ([item["@type"]] if isinstance(item.get("@type"), str) else (item.get("@type") or [])):
                            schema_type_counts[t] = schema_type_counts.get(t, 0) + 1
            json_ld_blocks.append(raw.strip())
        except Exception:
            pass


def _extract_og_tags(soup: BeautifulSoup) -> dict:
    """Stage 2: Extract OpenGraph metadata."""
    og = {}
    for meta in soup.find_all("meta"):
        prop = meta.get("property", "") or meta.get("name", "")
        if prop.startswith("og:"):
            og[prop] = meta.get("content", "").strip()
    return og


def _extract_twitter_tags(soup: BeautifulSoup) -> dict:
    """Stage 2: Extract Twitter/X card metadata."""
    tw = {}
    for meta in soup.find_all("meta"):
        name = meta.get("name", "")
        if name.startswith("twitter:"):
            tw[name] = meta.get("content", "").strip()
    return tw


def _check_robots_txt(base_url: str, headers: dict) -> str:
    """
    Stage 2: Fetch /robots.txt and assess AI crawler policy.
    Returns: 'restricted', 'partial', or 'allowed'.
    """
    try:
        scheme = urlparse(base_url).scheme
        domain = urlparse(base_url).netloc
        robots_url = f"{scheme}://{domain}/robots.txt"
        r = requests.get(robots_url, headers=headers, timeout=5)
        if r.status_code != 200:
            return "not_found"
        content = r.text.lower()
        ai_bots = ["gptbot", "perplexitybot", "anthropic-ai", "claudebot", "googlebot-extended"]
        disallowed_for_all = "user-agent: *" in content and "disallow: /" in content
        blocked_bots = [b for b in ai_bots if b in content]
        if disallowed_for_all:
            return "restricted"
        if blocked_bots:
            return "partial"
        return "allowed"
    except Exception:
        return "not_found"


def _discover_sitemap_urls(base_url: str, headers: dict, limit: int = 12) -> list:
    """
    Stage 2: Attempt to read sitemap.xml and return internal page URLs.
    Falls back gracefully if the sitemap is absent or malformed.
    """
    try:
        scheme = urlparse(base_url).scheme
        domain = urlparse(base_url).netloc
        sitemap_url = f"{scheme}://{domain}/sitemap.xml"
        r = requests.get(sitemap_url, headers=headers, timeout=8)
        if r.status_code != 200:
            return []
        # Extract <loc> tags - works for both sitemap index and urlset
        locs = re.findall(r"<loc>(.*?)</loc>", r.text, re.IGNORECASE)
        internal = [l.strip() for l in locs if urlparse(l.strip()).netloc == domain]
        return internal[:limit]
    except Exception:
        return []


def _is_js_heavy_page(word_count: int, raw_html_len: int) -> bool:
    """
    Stage 1 fix: a page is JS-heavy when it has thin text AND a small HTML payload.
    (Small payload = likely a shell div with no pre-rendered content.)
    Previous logic had this inverted.
    """
    return word_count < 100 and raw_html_len < 40000


def _js_fallback(url: str, headers: dict) -> str:
    """
    Stage 3: Attempt ScraperAPI JS-rendered fetch.
    Returns rendered HTML string or empty string on any failure.
    """
    api_key = os.getenv("SCRAPER_API_KEY", "").strip()
    if not api_key:
        return ""
    try:
        scraper_url = (
            f"https://api.scraperapi.com/"
            f"?api_key={api_key}&url={requests.utils.quote(url, safe='')}&render=true"
        )
        console.print("   [yellow]JS Fallback[/yellow]: Attempting ScraperAPI render...")
        r = requests.get(scraper_url, timeout=45)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        console.print(f"   [yellow]JS Fallback skipped[/yellow]: {e}")
    return ""


# ---------------------------------------------------------------------------
# Node entry point
# ---------------------------------------------------------------------------

def process(state: dict) -> dict:
    console.print("[cyan]Content Fetcher Node[/cyan]: Fetching url content...")

    url = state.get("url", "")
    locale_code = state.get("locale", "en")

    # ── defaults (state contract: all existing keys preserved) ──
    client_content_raw = ""
    client_content_clean = ""
    client_content_depth = {
        "word_count": 0,
        "paragraph_count": 0,
        "heading_count": 0,
        "js_heavy_detected": False,
        "extraction_quality": "Failed",
        "schema_block_count": 0
    }
    content_fetch_notes = ""
    page_title = ""
    meta_description = ""
    canonical_url = ""
    hreflang_count = 0
    json_ld_blocks = []
    schema_type_counts = {}
    fetched_page_urls = []

    # ── new state keys (Stage 2+3) ──
    og_tags = {}
    twitter_tags = {}
    robots_txt_status = "not_found"
    sitemap_urls = []
    js_fallback_used = False

    if not url:
        console.print("[bold red]NODE_FAILED[/bold red]: Content Fetcher (No URL provided). Using fallbacks.")
        content_fetch_notes = "Failed: No URL"
    else:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": (
                "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7"
                if locale_code == "it"
                else "en-US,en;q=0.9"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        # ── Stage 2: robots.txt (early signal, independent of page fetch) ──
        robots_txt_status = _check_robots_txt(url, headers)
        if robots_txt_status == "restricted":
            console.print("   [yellow]Warning[/yellow]: robots.txt restricts all crawlers. AI indexability may be blocked.")

        # ── Stage 2: sitemap discovery ──
        sitemap_urls = _discover_sitemap_urls(url, headers)
        if sitemap_urls:
            console.print(f"   [green]Sitemap[/green]: Found {len(sitemap_urls)} URLs via sitemap.xml")

        # ── Primary fetch ──
        response = None
        for attempt in range(2):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                break
            except Exception as e:
                console.print(f"[yellow]Content Fetcher Node[/yellow]: Attempt {attempt + 1} failed: {e}")
                if attempt == 1:
                    console.print("[bold red]NODE_FAILED[/bold red]: Content Fetcher. Using fallbacks.")
                    content_fetch_notes = f"Failed to fetch content: {str(e)}"

        if response and response.status_code == 200:
            try:
                client_content_raw = response.text
                soup = BeautifulSoup(client_content_raw, "html.parser")

                # ── Homepage metadata (deterministic) ──
                page_title = soup.title.string.strip() if soup.title and soup.title.string else ""
                desc_tag = soup.find("meta", attrs={"name": "description"})
                if desc_tag:
                    meta_description = desc_tag.get("content", "").strip()
                can_tag = soup.find("link", rel="canonical")
                if can_tag:
                    canonical_url = can_tag.get("href", "").strip()
                hreflang_count = len(soup.find_all("link", hreflang=True))

                # Stage 2: OG + Twitter from homepage
                og_tags = _extract_og_tags(soup)
                twitter_tags = _extract_twitter_tags(soup)

                # Schema from homepage
                _parse_schema_blocks(soup, json_ld_blocks, schema_type_counts)

                # ── Stage 2: build crawl queue (sitemap-first) ──
                base_domain = urlparse(url).netloc
                junk_keywords = [
                    "cart", "login", "account", "checkout", "password",
                    "search", "register", "?", "=", "logout", "wishlist"
                ]
                priority_keywords = [
                    "category", "product", "blog", "about", "services",
                    "chi-siamo", "storia", "collection", "editorial",
                    "recipe", "contatti", "contact", "team", "menu"
                ]

                # Anchor-based discovery (homepage)
                anchor_links = []
                for a_tag in soup.find_all("a", href=True):
                    link = urljoin(url, a_tag["href"])
                    parsed = urlparse(link)
                    if parsed.netloc != base_domain or link == url:
                        continue
                    if any(junk in link.lower() for junk in junk_keywords):
                        continue
                    if any(kw in link.lower() for kw in priority_keywords):
                        anchor_links.insert(0, link)
                    elif len(anchor_links) < 20:
                        anchor_links.append(link)

                # Merge sitemap URLs (deduplicated, sitemap gets lower priority weight)
                seen = {url}
                internal_links = []
                for link in anchor_links:
                    if link not in seen:
                        seen.add(link)
                        internal_links.append(link)

                for link in sitemap_urls:
                    if link not in seen and not any(junk in link.lower() for junk in junk_keywords):
                        seen.add(link)
                        internal_links.append(link)

                # Stage 2: up to 6 sub-pages (was 3)
                pages_to_fetch = [url] + internal_links[:6]
                fetched_page_urls = pages_to_fetch

                # ── Multi-page fetch ──
                merged_clean_text = ""
                total_heading_count = 0
                page_count = 0
                js_heavy_signal_count = 0

                for p_url in pages_to_fetch:
                    try:
                        console.print(f"   - Fetching: {p_url}")
                        if p_url == url:
                            p_response = response
                        else:
                            p_response = requests.get(p_url, headers=headers, timeout=8)

                        if p_response.status_code != 200:
                            continue

                        page_count += 1
                        p_soup = BeautifulSoup(p_response.text, "html.parser")

                        # Schema from sub-pages
                        if p_url != url:
                            _parse_schema_blocks(p_soup, json_ld_blocks, schema_type_counts)

                        total_heading_count += len(
                            p_soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"])
                        )

                        # Stage 1: paragraph-level extraction
                        # Remove non-content elements first
                        for tag in p_soup(["script", "style", "noscript", "header",
                                           "footer", "nav", "aside", "form"]):
                            tag.decompose()

                        clean_text = _extract_body_text(p_soup)
                        p_word_count = len(clean_text.split())

                        # Stage 1: JS heavy signal accumulation (was boolean inversion)
                        if _is_js_heavy_page(p_word_count, len(p_response.text)):
                            js_heavy_signal_count += 1

                        merged_clean_text += " " + clean_text

                    except Exception as loop_e:
                        console.print(f"   [yellow]Warning[/yellow]: Failed to fetch {p_url}: {loop_e}")

                # ── Stage 3: JS fallback via ScraperAPI ──
                js_heavy_detected = js_heavy_signal_count >= 3
                if js_heavy_detected:
                    rendered_html = _js_fallback(url, headers)
                    if rendered_html:
                        r_soup = BeautifulSoup(rendered_html, "html.parser")
                        for tag in r_soup(["script", "style", "noscript", "header",
                                           "footer", "nav", "aside", "form"]):
                            tag.decompose()
                        fallback_text = _extract_body_text(r_soup)
                        fallback_wc = len(fallback_text.split())
                        current_wc = len(merged_clean_text.split())
                        if fallback_wc > current_wc * 1.5:
                            # Rendered version is meaningfully richer — use it
                            merged_clean_text = fallback_text + " " + merged_clean_text
                            js_fallback_used = True
                            console.print(
                                f"   [green]JS Fallback[/green]: Enriched from {current_wc} → {fallback_wc} words"
                            )
                        else:
                            console.print("   [yellow]JS Fallback[/yellow]: Rendered content not richer. Discarded.")

                client_content_clean = merged_clean_text.strip()
                word_count = len(client_content_clean.split())

                # ── Stage 1: tightened extraction_quality thresholds ──
                # Require genuine content depth, not just raw volume.
                # "high" requires both adequate word count AND multi-page coverage.
                if word_count > 600 and page_count >= 3:
                    extraction_quality = "high"
                elif word_count > 250 or page_count >= 2:
                    extraction_quality = "medium"
                else:
                    extraction_quality = "low"

                client_content_depth = {
                    "word_count": word_count,
                    "page_count": page_count,
                    "heading_count": total_heading_count,
                    "js_heavy_detected": js_heavy_detected,
                    "js_heavy_signal_count": js_heavy_signal_count,
                    "extraction_quality": extraction_quality,
                    "schema_block_count": len(json_ld_blocks)
                }

                if extraction_quality == "low" or js_heavy_detected:
                    detail = " JS-heavy site detected." if js_heavy_detected else ""
                    content_fetch_notes = (
                        f"Warning: Content is thin or limited.{detail} Downstream confidence reduced."
                    )
                else:
                    content_fetch_notes = (
                        f"Content fetched from {page_count} pages successfully."
                    )

                console.print(
                    f"[green]Content Fetcher Node[/green]: "
                    f"Extracted {word_count} words across {page_count} pages | "
                    f"Quality: {extraction_quality} | "
                    f"JS-heavy signals: {js_heavy_signal_count}/{'7'} | "
                    f"robots.txt: {robots_txt_status}"
                )

            except Exception as e:
                console.print(f"[bold red]NODE_FAILED[/bold red]: Content Fetcher parse error: {e}")
                content_fetch_notes = f"Parse error: {str(e)}"

    # ── Assign to state (all existing keys + new keys) ──
    # Existing keys (preserved, no renames)
    state["client_content_raw"] = client_content_raw
    state["client_content_clean"] = client_content_clean
    state["client_content_depth"] = client_content_depth
    state["content_fetch_notes"] = content_fetch_notes
    state["page_title"] = page_title
    state["meta_description"] = meta_description
    state["canonical_url"] = canonical_url
    state["hreflang_count"] = hreflang_count
    state["json_ld_blocks"] = json_ld_blocks
    state["schema_type_counts"] = schema_type_counts
    state["fetched_page_urls"] = fetched_page_urls
    state["multi_page_coverage_summary"] = f"Fetched {len(fetched_page_urls)} pages."

    # New keys (Stage 2+3)
    state["og_tags"] = og_tags
    state["twitter_tags"] = twitter_tags
    state["robots_txt_status"] = robots_txt_status
    state["sitemap_urls"] = sitemap_urls
    state["js_fallback_used"] = js_fallback_used

    return state
