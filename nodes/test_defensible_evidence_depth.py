import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Ensure the root directory is accessible.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nodes.researcher_node import process

class TestDefensibleEvidenceDepth(unittest.TestCase):
    @patch('nodes.researcher_node.genai')
    @patch('nodes.api_utils.execute_with_backoff')
    def test_evidence_depth_partial_credits(self, mock_execute, mock_genai):
        # Simulate environment
        os.environ["GEMINI_API_KEY"] = "dummy_key_for_test"
        os.environ["SERPER_API_KEY"] = "dummy_serper_key"
        
        # Craft a state matching NaturaSì scenario with 0 ig_score from Gemini but strong upstream evidence
        dummy_state = {
            "url": "https://www.naturasi.it",
            "locale": "it",
            "brand_name": "NaturaSì",
            "business_profile_key": "ecommerce_retail",
            "client_content_clean": "Alimenti biologici" * 100,
            "raw_data_complete": {
                "competitor_entities": ["Esselunga"],
                "authority_entities": ["Organic Farming Std"],
                "topic_gaps": ["prodotti biologici"],
                "faq_patterns": ["come funziona naturaSì"]
            },
            # Upstream evidence triggers
            "missing_page_types": ["Punti Vendita"],
            "trust_signal_gaps": [{"signal": "P.IVA", "status": "missing"}],
            "schema_type_counts": {"Organization": 1},
            "external_data_quality": "high",
            "client_content_depth": {"extraction_quality": "high"},
            "discovery_intent_gaps": ["navigational"],
            # To ensure it doesn't crash elsewhere
            "target_industry": "Retail",
            "scale_level": "National"
        }
        
        # Mock responses to return empty or 0 to force fallback/partial credit evaluation
        def mock_side_effect(*args, **kwargs):
            mock_res = MagicMock()
            # If prompt asks for IG score, intentionally return 0 to trigger our new logic
            mock_res.text = "0"
            return mock_res
            
        mock_execute.side_effect = mock_side_effect
        
        # Run process
        result_state = process(dummy_state)
        
        # Assert Evidence Depth is correctly calculated via partial credits
        depth = result_state.get("metrics", {}).get("Defensible Evidence Depth", -1)
        
        # Base logic expects:
        # trust/missing_pages = 10
        # visibility > 0 -> let's say 0 for this mock
        # external_data_quality = 5
        # schema_type_counts = 10
        # extraction_quality = 10
        # Expected minimum combined score without visibility is 35
        # Plus the guard asserts it's at least 15.
        
        self.assertGreaterEqual(depth, 15, "Evidence depth failed to trigger the minimum guard.")
        self.assertGreaterEqual(depth, 35, "Evidence depth failed to award partial credits.")

if __name__ == "__main__":
    unittest.main()
