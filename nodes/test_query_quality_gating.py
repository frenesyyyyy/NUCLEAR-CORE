"""
Regression tests for Quality-Gated, Confidence-Aware Tier Construction.

Tests validate that the system degrades gracefully instead of backfilling junk.
"""
import os
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nodes.researcher_node import (
    _score_query_candidate,
    _assemble_tier_queries,
    _contains_required_anchor,
    TIER_POLICY,
    QUALITY_THRESHOLDS,
    FALLBACK_CAPS,
)
from nodes.model_analytics_node import _apply_provenance_weighting
from nodes.validator_node import _detect_contradictions, _compute_verdict


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

def _make_state(**overrides):
    """Create a minimal pipeline state for testing."""
    state = {
        "scale_level": "National",
        "discovered_location": "Milano, Italy",
        "locale": "en",
        "target_business": "dental clinic",
        "target_industry": "dental",
        "brand_name": "SmileCare",
        "business_profile_key": "local_healthcare_ymyl",
        "business_profile": {
            "location_enforce": False,
            "blind_fallback_templates": {
                "en": [
                    "best dentist near me",
                    "trusted dental clinic nearby",
                    "urgent dental care",
                ],
                "it": [
                    "miglior dentista in zona",
                    "clinica dentale affidabile vicino a me",
                    "guardia dentale urgente",
                ],
            },
            "contextual_fallback_templates": {
                "en": [
                    "cost of dental consultation",
                    "how to choose the best dentist",
                ],
                "it": [
                    "costo della visita dentistica",
                    "come scegliere il migliore dentista",
                ],
            },
        },
        "raw_data_complete": {
            "orchestrator_entities": ["implants", "whitening"],
        },
        "og_tags": {"og:title": "SmileCare Dental Clinic"},
        "grounding_entities": [],
    }
    state.update(overrides)
    return state


def _make_anchors():
    return {
        "primary_anchors": {"dental", "dentist", "dentista"},
        "secondary_anchors": {"implants", "whitening", "cleaning"},
        "brand_anchors": {"smilecare"},
    }


def _make_model_queries(queries, tier="blind_discovery"):
    """Build model query objects from text list."""
    return [
        {
            "query": q,
            "tier": tier,
            "points": 25 if tier == "blind_discovery" else 15,
            "source": "model",
            "raw_query": q,
            "profile_key": "local_healthcare_ymyl",
        }
        for q in queries
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: High-quality model generation — full tier, no degradation
# ─────────────────────────────────────────────────────────────────────────────

def test_high_quality_model_generation():
    """All model queries pass scoring → full tier size, no degradation flag."""
    state = _make_state()
    anchors = _make_anchors()

    model_queries = _make_model_queries([
        "best dental implants for adults over 50",
        "how to choose a dental clinic for whitening",
        "dental cleaning frequency recommendation",
        "dental implants cost comparison guide",
        "difference between dental veneers and implants",
        "how dental whitening works at a clinic",
    ])

    accepted, metrics = _assemble_tier_queries(
        "blind_discovery", model_queries, state, {"smilecare"},
        False, "Milano, Italy", "high", "en", anchors
    )

    assert metrics["generation_degraded"] is False, "Should NOT be degraded"
    assert metrics["model_query_count"] >= 3, f"Need ≥3 model queries, got {metrics['model_query_count']}"
    assert metrics["entity_fallback_count"] == 0, "Should have 0 entity fallback"
    assert metrics["profile_fallback_count"] == 0, "Should have 0 profile fallback"
    assert len(accepted) >= TIER_POLICY["blind_discovery"]["min"], f"Need ≥{TIER_POLICY['blind_discovery']['min']} accepted"
    print(f"  [PASS] Test 1 PASSED: {len(accepted)} accepted, {metrics['model_query_count']} model, no degradation")


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: Partial failure — some model + some fallback, no degradation
# ─────────────────────────────────────────────────────────────────────────────

def test_partial_failure():
    """Some model queries + entity fallback → reduced tier, degradation=false if min met."""
    state = _make_state()
    anchors = _make_anchors()

    # Only 2 good model queries (below target but may still meet min with fallback)
    model_queries = _make_model_queries([
        "best dental implants for adults over 50",
        "dental whitening procedure explained step by step",
    ])

    accepted, metrics = _assemble_tier_queries(
        "blind_discovery", model_queries, state, {"smilecare"},
        False, "Milano, Italy", "high", "en", anchors
    )

    assert metrics["model_query_count"] >= 1, "Should have at least 1 model query"
    total_fb = metrics["entity_fallback_count"] + metrics["profile_fallback_count"]
    # If we have enough (min=3), degradation should be false
    if len(accepted) >= TIER_POLICY["blind_discovery"]["min"]:
        assert metrics["generation_degraded"] is False, "Min met, should NOT be degraded"
    print(f"  [PASS] Test 2 PASSED: {len(accepted)} accepted, model={metrics['model_query_count']}, fb={total_fb}, degraded={metrics['generation_degraded']}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: Heavy fallback — profile fallback dominates, cap enforced
# ─────────────────────────────────────────────────────────────────────────────

def test_heavy_fallback_cap_enforcement():
    """No model queries pass → fallback capped, degradation=true if below min."""
    state = _make_state()
    anchors = _make_anchors()

    # All model queries are junk (too short/generic → will fail scoring)
    model_queries = _make_model_queries([
        "hi",
        "abc",
    ])

    accepted, metrics = _assemble_tier_queries(
        "blind_discovery", model_queries, state, {"smilecare"},
        False, "Milano, Italy", "high", "en", anchors
    )

    # Model queries should all be rejected
    assert metrics["model_query_count"] == 0, "Junk model queries should be rejected"

    # With 0 model queries and caps enforced, very few fallbacks should be accepted
    total = len(accepted)
    total_fb = metrics["entity_fallback_count"] + metrics["profile_fallback_count"]

    # The cap logic prevents adding MORE fallback once share would exceed limit
    # With 0 model queries, the tier should be degraded (below minimum)
    assert metrics["generation_degraded"] is True, "Should be degraded when no model queries pass"
    
    # The system should NOT have filled up to target with junk
    assert total <= TIER_POLICY["blind_discovery"]["target"], \
        f"Should not exceed target with pure fallback, got {total}"

    print(f"  [PASS] Test 3 PASSED: {len(accepted)} accepted, fb={total_fb}, degraded={metrics['generation_degraded']}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: Malformed fallback rejection — no junk accepted
# ─────────────────────────────────────────────────────────────────────────────

def test_malformed_fallback_rejection():
    """Bad candidates rejected → below target but above minimum, no junk accepted."""
    state = _make_state()
    anchors = _make_anchors()

    # 3 decent model queries
    model_queries = _make_model_queries([
        "best dental implants for adults over 50",
        "dental whitening procedure explained step by step",
        "how to choose a dental clinic for your family",
    ])

    accepted, metrics = _assemble_tier_queries(
        "blind_discovery", model_queries, state, {"smilecare"},
        False, "Milano, Italy", "high", "en", anchors
    )

    # All accepted queries should have quality_score >= threshold
    for q in accepted:
        source = q.get("source", "model")
        threshold = QUALITY_THRESHOLDS.get(source, 55)
        score = q.get("quality_score", 0)
        assert score >= threshold, f"Query '{q['query'][:30]}...' score {score} < threshold {threshold}"

    print(f"  [PASS] Test 4 PASSED: All {len(accepted)} accepted queries pass quality thresholds")


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: Parse failure case — recovery + metrics recorded
# ─────────────────────────────────────────────────────────────────────────────

def test_parse_failure_recovery():
    """Model output malformed → recovered queries + validated fallback → metrics recorded."""
    state = _make_state()
    anchors = _make_anchors()

    # Simulate parse failure: empty model queries (model returned garbage)
    model_queries = _make_model_queries([])

    accepted, metrics = _assemble_tier_queries(
        "blind_discovery", model_queries, state, {"smilecare"},
        False, "Milano, Italy", "high", "en", anchors
    )

    assert metrics["model_query_count"] == 0, "No model queries should pass"
    assert metrics["query_construction_reliability"] is not None, "Reliability should be recorded"
    assert isinstance(metrics["generation_degraded"], bool), "Degradation flag must be boolean"
    assert metrics["accepted_count"] == len(accepted), "Count mismatch"

    print(f"  [PASS] Test 5 PASSED: Parse failure recovered, {len(accepted)} accepted via fallback, degraded={metrics['generation_degraded']}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: Provenance weighting — profile_fallback miss < model miss
# ─────────────────────────────────────────────────────────────────────────────

def test_provenance_weighting():
    """Verify profile_fallback miss contributes less than model miss."""

    # Scenario A: All model queries, none matched
    log_model_only = [
        {"tier": "blind_discovery", "query": "q1", "matched": False, "points": 0, "max_pts": 25, "source": "model"},
        {"tier": "blind_discovery", "query": "q2", "matched": False, "points": 0, "max_pts": 25, "source": "model"},
    ]

    # Scenario B: All profile_fallback queries, none matched
    log_fallback_only = [
        {"tier": "blind_discovery", "query": "q1", "matched": False, "points": 0, "max_pts": 25, "source": "profile_fallback"},
        {"tier": "blind_discovery", "query": "q2", "matched": False, "points": 0, "max_pts": 25, "source": "profile_fallback"},
    ]

    result_model = _apply_provenance_weighting(log_model_only)
    result_fallback = _apply_provenance_weighting(log_fallback_only)

    # Model misses should have higher weighted_total (more evidence weight)
    assert result_model["weighted_total"] > result_fallback["weighted_total"], \
        f"Model weighted_total ({result_model['weighted_total']}) should exceed fallback ({result_fallback['weighted_total']})"

    # Both have 0% visibility since nothing matched
    assert result_model["weighted_visibility_pct"] == 0.0
    assert result_fallback["weighted_visibility_pct"] == 0.0

    print(f"  [PASS] Test 6 PASSED: Model weighted_total={result_model['weighted_total']}, Fallback={result_fallback['weighted_total']}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 7: Degradation flag triggers C7 contradiction
# ─────────────────────────────────────────────────────────────────────────────

def test_degradation_triggers_c7():
    """C7 contradiction fires when tier is degraded and discovery is zero."""
    state = {
        "tier_query_reliability": {
            "blind_discovery": {
                "generation_degraded": True,
                "model_query_count": 0,
                "entity_fallback_count": 1,
                "profile_fallback_count": 1,
                "rejected_query_count": 5,
                "query_construction_reliability": 0.15,
            },
            "contextual_discovery": {
                "generation_degraded": False,
                "model_query_count": 4,
                "entity_fallback_count": 0,
                "profile_fallback_count": 0,
                "rejected_query_count": 1,
                "query_construction_reliability": 0.80,
            },
        }
    }

    flags, reasons = _detect_contradictions(
        candidate_verdict="NOT CLIENT READY",
        confidence=55,
        penalized_gaps=[],
        trust_mix="Moderate",
        trust_conf="Low",
        blind_rate=0.0,        # Zero blind discovery
        contextual_rate=30.0,  # Some contextual discovery
        auth_composite=40.0,
        schema_conf="Low",
        integrity_status="valid",
        consensus=50.0,
        state=state,
    )

    assert "C7" in flags, f"C7 should fire when tier is degraded and blind_rate=0. Got flags: {flags}"
    print(f"  [PASS] Test 7 PASSED: C7 fired correctly. Flags: {flags}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 8: Query scoring dimensions
# ─────────────────────────────────────────────────────────────────────────────

def test_query_scoring_dimensions():
    """Verify scoring function punishes known bad patterns and rewards good ones."""
    state = _make_state()
    anchors = _make_anchors()

    # Good query: has anchor, specific, grammatical, not generic
    good_score = _score_query_candidate(
        "best dental implants for adults over 50",
        "blind_discovery", state, "model", anchors
    )

    # Bad query: too short
    short_score = _score_query_candidate(
        "hi",
        "blind_discovery", state, "model", anchors
    )

    # Bad query: unresolved placeholder
    placeholder_score = _score_query_candidate(
        "best {core_noun} near me",
        "blind_discovery", state, "model", anchors
    )

    # Bad query: comma blob
    comma_score = _score_query_candidate(
        "a , Italia Roma dental clinic",
        "blind_discovery", state, "model", anchors
    )

    # Generic shell
    generic_score = _score_query_candidate(
        "best products online",
        "blind_discovery", state, "model", anchors
    )

    assert good_score > 55, f"Good query should score >55, got {good_score}"
    assert short_score < 20, f"Short query should score <20, got {short_score}"
    assert placeholder_score < good_score, f"Placeholder query should score less than good query"
    assert comma_score < good_score, f"Comma blob should score less than good query"
    assert generic_score < good_score, f"Generic shell should score less than good query"

    print(f"  [PASS] Test 8 PASSED: good={good_score}, short={short_score}, placeholder={placeholder_score}, comma={comma_score}, generic={generic_score}")


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("  REGRESSION TESTS: Quality-Gated Tier Construction")
    print("=" * 70 + "\n")

    tests = [
        test_high_quality_model_generation,
        test_partial_failure,
        test_heavy_fallback_cap_enforcement,
        test_malformed_fallback_rejection,
        test_parse_failure_recovery,
        test_provenance_weighting,
        test_degradation_triggers_c7,
        test_query_scoring_dimensions,
    ]

    passed = 0
    failed = 0
    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except AssertionError as e:
            print(f"  [FAIL] {test_fn.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"  [FAIL] {test_fn.__name__} ERROR: {e}")
            failed += 1

    print(f"\n{'=' * 70}")
    print(f"  RESULTS: {passed} passed, {failed} failed out of {len(tests)} tests")
    print(f"{'=' * 70}\n")

    sys.exit(1 if failed > 0 else 0)
