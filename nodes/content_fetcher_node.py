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
    
    if not url:
        console.print("[bold red]NODE_FAILED[/bold red]: Content Fetcher (No URL provided). Using fallbacks.")
        content_fetch_notes = "Failed: No URL"
    else:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
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
                client_content_raw = response.text
                
                soup = BeautifulSoup(client_content_raw, 'html.parser')
                
                # Deterministic Parsing
                page_title = soup.title.string.strip() if soup.title and soup.title.string else ""
                
                desc_tag = soup.find("meta", attrs={"name": "description"})
                if desc_tag: meta_description = desc_tag.get("content", "").strip()
                
                can_tag = soup.find("link", rel="canonical")
                if can_tag: canonical_url = can_tag.get("href", "").strip()
                
                hreflang_count = len(soup.find_all("link", hreflang=True))
                
                for script in soup.find_all("script", type="application/ld+json"):
                    if script.string:
                        json_ld_blocks.append(script.string.strip())
                
                heading_count = len(soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]))
                paragraph_count = len(soup.find_all("p"))
                
                # Remove unneeded elements for clean text
                for script in soup(["script", "style", "noscript", "header", "footer", "nav"]):
                    script.extract()
                    
                text = soup.get_text(separator=' ')
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                client_content_clean = ' '.join(chunk for chunk in chunks if chunk)
                
                word_count = len(client_content_clean.split())
                
                js_heavy = word_count < 150 and len(client_content_raw) > 50000
                extraction_quality = "High" if word_count > 300 else ("Medium" if word_count > 100 else "Low")
                
                client_content_depth = {
                    "word_count": word_count,
                    "paragraph_count": paragraph_count,
                    "heading_count": heading_count,
                    "js_heavy_detected": js_heavy,
                    "extraction_quality": extraction_quality,
                    "schema_block_count": len(json_ld_blocks)
                }
                
                if extraction_quality == "Low" or js_heavy:
                    content_fetch_notes = "Warning: Content is thin or JS-heavy. Downstream confidence reduced."
                else:
                    content_fetch_notes = "Content fetched and parsed successfully."
                    
                console.print(f"[green]Content Fetcher Node[/green]: Successfully extracted {word_count} words | Quality: {extraction_quality}.")
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
    
    return state
