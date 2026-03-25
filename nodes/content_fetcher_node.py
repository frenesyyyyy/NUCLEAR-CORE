import requests
from bs4 import BeautifulSoup
from rich.console import Console

console = Console()

def process(state: dict) -> dict:
    console.print("[cyan]Content Fetcher Node[/cyan]: Fetching url content...")
    
    url = state.get("url", "")
    
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
    
    if not url:
        console.print("[bold red]NODE_FAILED[/bold red]: Content Fetcher (No URL provided). Using fallbacks.")
        content_fetch_notes = "Failed: No URL"
    else:
        locale_code = state.get("locale", "en")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7" if locale_code == "it" else "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        }
        
        response = None
        max_retries = 1
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                break
            except Exception as e:
                console.print(f"[yellow]Content Fetcher Node[/yellow]: Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries:
                    console.print("[bold red]NODE_FAILED[/bold red]: Content Fetcher. Using fallbacks.")
                    content_fetch_notes = f"Failed to fetch content: {str(e)}"
        
        if response and response.status_code == 200:
            try:
                # Page 1 (Homepage)
                client_content_raw = response.text
                soup = BeautifulSoup(client_content_raw, 'html.parser')
                
                # Deterministic Parsing for homepage
                page_title = soup.title.string.strip() if soup.title and soup.title.string else ""
                desc_tag = soup.find("meta", attrs={"name": "description"})
                if desc_tag: meta_description = desc_tag.get("content", "").strip()
                can_tag = soup.find("link", rel="canonical")
                if can_tag: canonical_url = can_tag.get("href", "").strip()
                hreflang_count = len(soup.find_all("link", hreflang=True))
                
                for script in soup.find_all("script", type="application/ld+json"):
                    content = script.text
                    if content:
                        try:
                            block = json.loads(content.strip())
                            # Handle both single objects and lists
                            blocks = block if isinstance(block, list) else [block]
                            for b in blocks:
                                t = b.get("@type")
                                if t:
                                    t_str = str(t)
                                    schema_type_counts[t_str] = schema_type_counts.get(t_str, 0) + 1
                            json_ld_blocks.append(content.strip())
                        except:
                            pass
                
                # Fetch deeper pages
                from urllib.parse import urljoin, urlparse
                base_domain = urlparse(url).netloc
                internal_links = []
                junk_keywords = ["cart", "login", "account", "checkout", "password", "search", "register", "?", "="]
                priority_keywords = ["category", "product", "blog", "about", "services", "chi-siamo", "storia", "collection", "editorial", "recipe"]
                
                for a_tag in soup.find_all("a", href=True):
                    link = urljoin(url, a_tag['href'])
                    if urlparse(link).netloc == base_domain and link != url and link not in internal_links:
                        if any(junk in link.lower() for junk in junk_keywords):
                            continue
                        if any(kw in link.lower() for kw in priority_keywords):
                            internal_links.insert(0, link) # priority push
                        elif len(internal_links) < 15: 
                            internal_links.append(link)
                
                # Priority links to fetch (up to 3 extra)
                pages_to_fetch = [url] + internal_links[:3]
                fetched_page_urls = pages_to_fetch
                page_count = 0
                merged_clean_text = ""
                total_heading_count = 0
                all_js_heavy = True
                
                for p_url in pages_to_fetch:
                    try:
                        console.print(f"   - Fetching: {p_url}")
                        if p_url == url:
                            p_response = response 
                        else:
                            p_response = requests.get(p_url, headers=headers, timeout=5)
                        
                        if p_response.status_code == 200:
                            page_count += 1
                            p_soup = BeautifulSoup(p_response.text, 'html.parser')
                            
                            # schema
                            if p_url != url:
                                for script in p_soup.find_all("script", type="application/ld+json"):
                                    content = script.text
                                    if content:
                                        try:
                                            block = json.loads(content.strip())
                                            blocks = block if isinstance(block, list) else [block]
                                            for b in blocks:
                                                t = b.get("@type")
                                                if t:
                                                    t_str = str(t)
                                                    schema_type_counts[t_str] = schema_type_counts.get(t_str, 0) + 1
                                            json_ld_blocks.append(content.strip())
                                        except:
                                            pass
                            
                            total_heading_count += len(p_soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]))
                            
                            for script_tag in p_soup(["script", "style", "noscript", "header", "footer", "nav"]):
                                script_tag.extract()
                            
                            p_text = p_soup.get_text(separator=' ')
                            lines = (line.strip() for line in p_text.splitlines())
                            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                            clean_text = ' '.join(chunk for chunk in chunks if chunk)
                            merged_clean_text += " " + clean_text
                            
                            p_word_count = len(clean_text.split())
                            if p_word_count >= 150 or len(p_response.text) <= 50000:
                                all_js_heavy = False # At least one page is ok
                                
                    except Exception as loop_e:
                        console.print(f"   [yellow]Warning[/yellow]: Failed to fetch {p_url}: {loop_e}")
                
                client_content_clean = merged_clean_text.strip()
                word_count = len(client_content_clean.split())
                js_heavy = all_js_heavy
                
                extraction_quality = "low"
                if word_count > 800 or page_count >= 3:
                    extraction_quality = "high"
                elif word_count > 300 or page_count >= 2:
                    extraction_quality = "medium"
                    
                client_content_depth = {
                    "word_count": word_count,
                    "page_count": page_count,
                    "heading_count": total_heading_count,
                    "js_heavy_detected": js_heavy,
                    "extraction_quality": extraction_quality,
                    "schema_block_count": len(json_ld_blocks)
                }
                
                if extraction_quality == "low" or js_heavy:
                    content_fetch_notes = "Warning: Content is thin or JS-heavy. Downstream confidence reduced."
                else:
                    content_fetch_notes = f"Content fetched from {page_count} pages successfully."
                    
                console.print(f"[green]Content Fetcher Node[/green]: Successfully extracted {word_count} words across {page_count} pages | Quality: {extraction_quality}.")
            except Exception as e:
                console.print(f"[bold red]NODE_FAILED[/bold red]: Content Fetcher parse error: {e}")
                content_fetch_notes = f"Parse error: {str(e)}"
    
    # Assign safely
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
    
    return state
