import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nodes.researcher_node import _assemble_tier_queries, _build_regeneration_context

class TestTierRegeneration(unittest.TestCase):

    def setUp(self):
        self.state = {
            "business_profile_key": "local_healthcare_ymyl",
            "business_profile": {
                "blind_fallback_templates": {"it": ["Come curare {services} a Roma"]},
                "contextual_fallback_templates": {"it": ["Migliori trattamenti per {services}"]},
                "persona_templates": [{"persona": "Paziente", "intent": "Cura"}]
            },
            "raw_data_complete": {
                "faq_patterns": ["Come curare il mal di schiena?"],
                "topic_gaps": ["Trattamenti innovativi per ernia"],
                "authority_entities": ["Dott. Rossi"]
            },
            "target_industry": "Medicina",
            "service_zones": ["Roma"]
        }
        self.brand_tokens = {"centro", "terapia"}
        self.anchors = {
            "primary_anchors": {"terapia del dolore", "clinica"},
            "secondary_anchors": {"ernia", "disco"},
            "brand_anchors": {"centro"}
        }
        self.gemini_mock = MagicMock()

    def test_regeneration_not_triggered_when_min_met(self):
        """Case 4: accepted_count >= min but < target -> No regeneration triggered."""
        model_queries = [
            {"query": "Terapia dolore cronico Roma", "source": "model"},
            {"query": "Clinica specializzata ernia Roma", "source": "model"},
            {"query": "Visita specialistica schiena Roma", "source": "model"}
        ]
        # Policy for contextual is min=3, target=5
        accepted, metrics = _assemble_tier_queries(
            "contextual_discovery", model_queries, self.state, self.brand_tokens,
            True, "Roma", "high", "it", self.anchors, regen_context={}
        )
        self.assertEqual(len(accepted), 3)
        self.assertEqual(metrics["regeneration_rounds_used"], 0)
        self.assertFalse(metrics["generation_degraded"])

    @patch("nodes.researcher_node._build_contextual_queries")
    def test_round_1_recovery(self, mock_build):
        """Case 1: first pass fails, round 1 recovers minimum."""
        # Initial pass provides 1 query (min is 3)
        model_queries = [{"query": "Query valida 1", "source": "model"}]
        
        # Mock Round 1 to return 2 valid queries
        mock_build.return_value = [
            {"query": "Regen query 1", "source": "model"},
            {"query": "Regen query 2", "source": "model"}
        ]
        
        regen_ctx = _build_regeneration_context(self.state, self.gemini_mock)
        accepted, metrics = _assemble_tier_queries(
            "contextual_discovery", model_queries, self.state, self.brand_tokens,
            True, "Roma", "high", "it", self.anchors, regen_context=regen_ctx
        )
        
        self.assertEqual(len(accepted), 3)
        self.assertEqual(metrics["regeneration_rounds_used"], 1)
        self.assertFalse(metrics["generation_degraded"])

    @patch("nodes.researcher_node._build_contextual_queries")
    def test_round_2_recovery(self, mock_build):
        """Case 2: first pass + round 1 fail, round 2 recovers minimum."""
        model_queries = [] # 0 queries
        mock_build.return_value = [] # Round 1 fails
        
        # We need to mock _fill_profile_fallback_template or ensure self.state has what's needed
        # Round 2 uses entity fallback.
        regen_ctx = _build_regeneration_context(self.state, self.gemini_mock)
        
        accepted, metrics = _assemble_tier_queries(
            "contextual_discovery", model_queries, self.state, self.brand_tokens,
            True, "Roma", "high", "it", self.anchors, regen_context=regen_ctx
        )
        
        # Should have found enough via Round 2 (Entity Fallback)
        self.assertGreaterEqual(len(accepted), 1) # Depends on templates
        self.assertEqual(metrics["regeneration_rounds_used"], 2)

    def test_total_failure_degradation(self):
        """Case 3: both rounds fail -> generation_degraded = True."""
        # Force all scoring to 0 or provide no data
        empty_state = {
            "business_profile_key": "unknown",
            "business_profile": {},
            "raw_data_complete": {},
            "target_industry": "None"
        }
        accepted, metrics = _assemble_tier_queries(
            "contextual_discovery", [], empty_state, set(),
            False, "None", "low", "en", {"primary_anchors": [], "secondary_anchors": []},
            regen_context={}
        )
        self.assertTrue(metrics["generation_degraded"])

    def test_profile_guard_violation(self):
        """Case 5: Wrong-profile case -> Profile-based regeneration blocked."""
        # Medical anchors but SaaS profile
        medical_anchors = {"primary_anchors": ["clinica medica"]}
        saas_profile_key = "b2b_saas_tech"
        
        from nodes.researcher_node import _profile_consistency_guard
        is_consistent = _profile_consistency_guard(saas_profile_key, medical_anchors)
        self.assertFalse(is_consistent, "Medical anchors should contradict B2B SaaS profile")

if __name__ == "__main__":
    unittest.main()
