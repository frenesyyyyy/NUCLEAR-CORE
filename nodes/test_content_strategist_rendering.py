import sys
import os
import json
import unittest
from unittest.mock import patch, MagicMock

# Ensure the root directory is accessible.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nodes.content_strategist_node import process, _build_grounding_context

class TestContentStrategistPrompt(unittest.TestCase):
    @patch('nodes.content_strategist_node.genai')
    @patch('nodes.api_utils.execute_with_backoff')
    def test_prompt_rendering_no_exception(self, mock_execute, mock_genai):
        os.environ["GEMINI_API_KEY"] = "dummy_key_for_test"
        
        dummy_state = {
            "url": "https://example.com",
            "client_content_clean": "This is some dummy content text." * 10,
            "json_ld_blocks": [],
            "raw_data_complete": {
                "competitor_entities": ["Comp A"],
                "authority_entities": ["Auth A"],
                "topic_gaps": ["Gap 1"],
                "faq_patterns": ["FAQ 1"]
            },
            "client_content_depth": {"page_count": 1, "extraction_quality": "high"},
            "locale": "en"
        }
        
        # Mock the execution to return a simple JSON response to prevent further errors
        mock_response = MagicMock()
        mock_response.text = '{"missing_page_types": []}'
        mock_execute.return_value = mock_response
        
        try:
            result = process(dummy_state)
            self.assertIn("missing_page_types", result)
        except ValueError as e:
            self.fail(f"Prompt rendering raised ValueError: {e}")
        except Exception as e:
             self.fail(f"Prompt rendering raised unexpected Exception: {e}")

if __name__ == "__main__":
    unittest.main()
