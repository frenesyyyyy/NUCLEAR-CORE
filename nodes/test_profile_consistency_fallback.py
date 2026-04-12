import os
import sys
import unittest

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nodes.researcher_node import _assemble_tier_queries

class TestProfileConsistencyFallback(unittest.TestCase):

    def setUp(self):
        # Setting up a scenario where a medical site was classified as B2B SaaS
        # This creates a strong entity conflict.
        self.state = {
            "business_profile_key": "b2b_saas_tech",
            "business_profile": {
                "blind_fallback_templates": {"it": ["Miglior {core_noun} software", "Software per cliniche"]},
                "contextual_fallback_templates": {"it": ["Recensioni {core_noun} platform"]},
                "persona_templates": []
            },
            "raw_data_complete": {},
            "target_industry": "sanità",
            "primary_industry": "terapia",
            "locale": "it",
            "discovered_location": "Roma",
            "client_content_clean": "terapia del dolore clinica"
        }
        self.brand_tokens = {"centro", "terapia_brand"}
        self.anchors = {
            "primary_anchors": {"terapia software", "software per cliniche", "terapia"}, # added terapia to pass validation
            "secondary_anchors": {"cura"},
            "brand_anchors": {"centro"}
        }

    @unittest.mock.patch("nodes.researcher_node._score_query_candidate")
    def test_profile_fallback_blocked(self, mock_score):
        """
        Verify that when a profile is contradicted by domain evidence, 
        pure profile_fallback templates are blocked, but entity_fallback ones are still processed.
        And verify the state flag is set.
        """
        mock_score.return_value = 100.0  # Force candidate to pass threshold
        model_queries = [] # No model queries, forcing fallback passage
        
        accepted, metrics = _assemble_tier_queries(
            "blind_discovery", model_queries, self.state, self.brand_tokens,
            True, "Roma", "high", "it", self.anchors, regen_context={}
        )
        
        # State flag should be active
        self.assertTrue(self.state.get("profile_fallback_blocked_due_to_entity_conflict", False), 
                        "The block flag must be set to True due to contradiction.")
        
        # "Software per cliniche" is pure profile_fallback (no {services} template).
        # "Miglior {services} software" is an entity_fallback.
        # So the accepted queries should ONLY contain the entity_fallback one.
        
        sources = [q["source"] for q in accepted]
        self.assertNotIn("profile_fallback", sources, "Pure profile_fallback must be blocked.")
        self.assertIn("entity_fallback", sources, "Entity fallback should still be allowed if templates are capable.")
        
        # Verify the actual generated query
        queries = [q["query"].lower() for q in accepted]
        self.assertFalse(any("software per cliniche" == q for q in queries), "Pure profile-based template was generated despite conflict.")

if __name__ == "__main__":
    unittest.main()
