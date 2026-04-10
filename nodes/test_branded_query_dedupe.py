import sys
import os
import unittest

# Ensure the root directory is accessible.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nodes.researcher_node import _build_branded_queries

class TestBrandedQueryDedupe(unittest.TestCase):
    def test_prevent_self_vs_self_queries(self):
        # Setup NaturaSì case with itself included in competitors
        brand_name = "NaturaSì"
        target_industry = "Organic Retail"
        discovered_location = "Milano"
        scale_level = "National"
        locale = "it"
        
        # Test input where the brand is its own top competitor and also has case differences
        competitor_entities = [
            "Naturasi",     # Duplicate without accent
            " NaturaSì ",   # Duplicate with spaces
            "Esselunga",    # Valid competitor
            "Carrefour"     # Second valid competitor
        ]
        
        queries = _build_branded_queries(
            brand_name,
            target_industry,
            discovered_location,
            scale_level,
            locale,
            competitor_entities,
            budget=6
        )
        
        # We need to verify that "NaturaSì vs Esselunga" is generated, AND NOT "NaturaSì vs NaturaSì" or "NaturaSì vs Naturasi"
        vs_query = [q["query"] for q in queries if " vs " in q["query"]][0]
        
        self.assertNotIn("NaturaSì vs Natura", vs_query, "The generated query contains self-comparison!")
        self.assertNotIn("Naturasi", vs_query.lower(), "Normalized name leaked into top competitor.")
        self.assertEqual("NaturaSì vs Esselunga", vs_query, "Top competitor was not correctly filtered out to Esselunga.")

    def test_missing_competitors(self):
        # Ensure it falls back to industry if no valid competitors exist
        queries = _build_branded_queries(
            "Acme", "Anvils", "Desert", "National", "en",
            ["Acme", " ACME "] # Only self
        )
        
        vs_query = [q["query"] for q in queries if " vs " in q["query"]][0]
        self.assertEqual("Acme vs Anvils competitors", vs_query, "Did not fallback to industry correctly when all competitors were filtered out.")

if __name__ == "__main__":
    unittest.main()
