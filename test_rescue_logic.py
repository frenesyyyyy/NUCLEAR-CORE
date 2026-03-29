import sys
import os
from unittest.mock import MagicMock, patch

# Add current dir to path
sys.path.append(os.getcwd())

from nodes.content_fetcher_node import process

def test_rescue_transitions():
    print("Testing ScraperAPI Rescue Logic Transitions (Refined)...")
    
    # 1. Mock Case: Successful Rich Rescue
    state_rich = {"url": "https://example.com/blocked", "locale": "it"}
    
    with patch("nodes.content_fetcher_node._primary_fetch") as mock_primary, \
         patch("nodes.content_fetcher_node._scraperapi_fetch") as mock_scraper:
        
        mock_res = MagicMock()
        mock_res.status_code = 403
        mock_res.text = "<html>Forbidden</html>"
        mock_primary.return_value = (mock_res, "HTTP 403")
        
        # Truly rich HTML > 400 words
        rich_html = "<html><body>"
        rich_html += "<h1>Just Eat Italia: Il leader della consegna a domicilio</h1>"
        rich_html += "<p>" + "L'Osservatorio Just Eat è lo strumento di analisi proprietario che monitora le tendenze del mercato del food delivery in Italia. " * 10 + "</p>"
        rich_html += "<h2>Come diventare partner di Just Eat</h2>"
        rich_html += "<p>" + "Entra a far parte del network di ristoranti leader in Italia. Gestisci i tuoi ordini con semplicità e raggiungi nuovi clienti ogni giorno. " * 10 + "</p>"
        rich_html += "<h3>Vantaggi per i ristoranti</h3>"
        rich_html += "<p>" + "Visibilità nazionale, logistica integrata, supporto dedicato e strumenti di marketing avanzati. Just Eat supporta la digitalizzazione della ristorazione italiana. " * 10 + "</p>"
        rich_html += "<h2>Servizi e Logistica</h2>"
        rich_html += "<p>" + "La nostra rete di riders copre oltre 1000 comuni in tutta Italia, garantendo tempi di consegna rapidi e qualità del prodotto preservata. " * 10 + "</p>"
        rich_html += "</body></html>"
        
        mock_scraper.return_value = (rich_html, "ScraperAPI Success")
        
        result = process(state_rich)
        
        wc = result['client_content_depth']['word_count']
        status = result['audit_integrity_status']
        print(f"Rich Rescue -> Status: {status} | WC: {wc}")
        assert status == "valid", f"Should be valid for rich content (Got {status})"
        assert result["fetch_strategy_used"] == "scraperapi_rendered"

    # 2. Mock Case: Successful Thin Rescue
    state_thin = {"url": "https://example.com/blocked", "locale": "it"}
    with patch("nodes.content_fetcher_node._primary_fetch") as mock_primary, \
         patch("nodes.content_fetcher_node._scraperapi_fetch") as mock_scraper:
        
        mock_res = MagicMock()
        mock_res.status_code = 403
        mock_primary.return_value = (mock_res, "HTTP 403")
        
        # ScraperAPI returns Thin HTML (less than 150 words)
        thin_html = "<html><body><h1>Just Eat Italia</h1><p>Siamo la piattaforma leader per ordinare cibo a domicilio online in tutta Italia. Ordina ora!</p></body></html>"
        mock_scraper.return_value = (thin_html, "ScraperAPI Success")
        
        result = process(state_thin)
        wc = result['client_content_depth']['word_count']
        status = result['audit_integrity_status']
        print(f"Thin Rescue -> Status: {status} | WC: {wc}")
        assert status == "degraded", f"Should be degraded for thin content (Got {status})"

    print("\n[SUCCESS] Integrity transition logic verified.")

if __name__ == "__main__":
    test_rescue_transitions()
