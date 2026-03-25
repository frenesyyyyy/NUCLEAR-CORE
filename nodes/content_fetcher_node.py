import requests
from bs4 import BeautifulSoup
from rich.console import Console

console = Console()

def process(state: dict) -> dict:
    console.print("[cyan]Content Fetcher Node[/cyan]: Fetching url content...")
    
    url = state.get("url", "")
    
    client_content_raw = ""
    client_content_clean = ""
    client_content_depth = 0
    content_fetch_notes = ""
    
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
                
                # Remove unneeded elements
                for script in soup(["script", "style", "noscript", "header", "footer", "nav"]):
                    script.extract()
                    
                text = soup.get_text(separator=' ')
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                client_content_clean = ' '.join(chunk for chunk in chunks if chunk)
                
                client_content_depth = len(client_content_clean.split())
                content_fetch_notes = "Content fetched successfully."
                console.print(f"[green]Content Fetcher Node[/green]: Successfully extracted {client_content_depth} words.")
            except Exception as e:
                console.print(f"[bold red]NODE_FAILED[/bold red]: Content Fetcher parse error: {e}")
                content_fetch_notes = f"Parse error: {str(e)}"
    
    # Assign safely
    state["client_content_raw"] = client_content_raw
    state["client_content_clean"] = client_content_clean
    state["client_content_depth"] = client_content_depth
    state["content_fetch_notes"] = content_fetch_notes
    
    return state
