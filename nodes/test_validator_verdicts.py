"""
Regression Tests for Validator Verdict Hardening (v5.0)
Tests 3 scenarios: Strong audit, Weak audit, Contradictory audit.
"""
import sys, os
sys.path.append(os.getcwd())

from nodes.validator_node import _compute_verdict, _compute_data_confidence

def _make_state(**overrides):
    """Build a baseline valid state, then apply overrides."""
    base = {
        "audit_integrity_status": "valid",
        "source_of_truth_mode": "hybrid",
        "confidence_score": 80,
        "business_profile_key": "b2b_saas_tech",
        "classification_reliability": "high",
        "metrics": {
            "Data Confidence Score": 80,
            "Defensible Evidence Depth": 55,
            "Entity Consensus": 50,
        },
        "source_taxonomy": {
            "trust_mix": "Strong",
            "penalized_relevant_gaps": [],
            "owned_count": 3,
            "earned_count": 5,
            "review_count": 4,
            "directory_count": 2,
            "forum_count": 1,
            "unknown_count": 0,
        },
        "stress_test_tier_stats": {
            "blind_discovery": {"matches": 5, "queries": 10, "max": 10},
            "contextual_discovery": {"matches": 7, "queries": 10, "max": 10},
            "branded_validation": {"matches": 8, "queries": 8, "max": 8},
        },
        "client_content_depth": {
            "extraction_quality": "high",
            "word_count": 1200,
            "schema_block_count": 4,
            "semantic_signals": {"cta_count": 3, "heading_count": 5},
        },
        "schema_type_counts": {"Organization": 1, "SoftwareApplication": 1, "FAQPage": 1, "WebSite": 1},
        "earned_media": {"first_party_inferred_families": [], "strength_score": 60, "profile_aware_strength": 60},
    }
    base.update(overrides)
    return base


def test_strong_audit():
    """Case 1: Everything is strong -> CLIENT READY"""
    state = _make_state()
    verdict, reason = _compute_verdict(state)
    assert verdict == "CLIENT READY", f"FAIL Strong Audit: got '{verdict}' — {reason}"
    assert "All gates passed" in reason
    print(f"  PASS: Strong Audit -> {verdict} | {reason}")


def test_weak_audit_invalid():
    """Case 2a: Invalid integrity -> NOT CLIENT READY"""
    state = _make_state(audit_integrity_status="invalid")
    verdict, reason = _compute_verdict(state)
    assert verdict == "NOT CLIENT READY", f"FAIL Weak(invalid): got '{verdict}'"
    assert "V1" in reason
    print(f"  PASS: Weak Audit (invalid) -> {verdict} | {reason}")


def test_weak_audit_zero_discovery():
    """Case 2b: Zero discovery -> NOT CLIENT READY"""
    state = _make_state(
        stress_test_tier_stats={
            "blind_discovery": {"matches": 0, "queries": 10, "max": 10},
            "contextual_discovery": {"matches": 0, "queries": 10, "max": 10},
        }
    )
    verdict, reason = _compute_verdict(state)
    assert verdict == "NOT CLIENT READY", f"FAIL Weak(zero): got '{verdict}'"
    assert "V4" in reason
    print(f"  PASS: Weak Audit (zero discovery) -> {verdict} | {reason}")


def test_weak_audit_low_confidence():
    """Case 2c: Confidence < 40 -> NOT CLIENT READY"""
    state = _make_state(confidence_score=30)
    verdict, reason = _compute_verdict(state)
    assert verdict == "NOT CLIENT READY", f"FAIL Weak(confidence): got '{verdict}'"
    assert "V3" in reason
    print(f"  PASS: Weak Audit (low confidence) -> {verdict} | {reason}")


def test_contradictory_high_discovery_low_confidence():
    """Case 3a: Good discovery, but confidence < 70 -> REQUIRES ANALYST REVIEW"""
    state = _make_state(confidence_score=55)
    verdict, reason = _compute_verdict(state)
    assert verdict == "REQUIRES ANALYST REVIEW", f"FAIL Contradiction(conf): got '{verdict}'"
    assert "S1" in reason
    print(f"  PASS: Contradictory (high disc / low conf) -> {verdict} | {reason}")


def test_contradictory_weak_trust():
    """Case 3b: Strong discovery, but weak trust -> REQUIRES ANALYST REVIEW"""
    state = _make_state()
    state["source_taxonomy"]["trust_mix"] = "Weak — Insufficient off-site authority"
    verdict, reason = _compute_verdict(state)
    assert verdict == "REQUIRES ANALYST REVIEW", f"FAIL Contradiction(trust): got '{verdict}'"
    assert "S4" in reason
    print(f"  PASS: Contradictory (strong disc / weak trust) -> {verdict} | {reason}")


def test_contradictory_many_gaps():
    """Case 3c: Many gaps -> REQUIRES ANALYST REVIEW"""
    state = _make_state()
    state["source_taxonomy"]["penalized_relevant_gaps"] = ["Gap A", "Gap B"]
    verdict, reason = _compute_verdict(state)
    assert verdict == "REQUIRES ANALYST REVIEW", f"FAIL Contradiction(gaps): got '{verdict}'"
    assert "S2" in reason
    print(f"  PASS: Contradictory (2 gaps) -> {verdict} | {reason}")


def test_local_profile_low_combined_rate():
    """Case 3d: Local healthcare profile with poor combined T1+T2 -> REQUIRES ANALYST REVIEW"""
    state = _make_state(
        business_profile_key="local_healthcare_ymyl",
        stress_test_tier_stats={
            "blind_discovery": {"matches": 1, "queries": 10, "max": 10},
            "contextual_discovery": {"matches": 1, "queries": 10, "max": 10},
        },
    )
    verdict, reason = _compute_verdict(state)
    assert verdict == "REQUIRES ANALYST REVIEW", f"FAIL Local(low combined): got '{verdict}'"
    assert "S7" in reason
    print(f"  PASS: Local Profile (low combined rate) -> {verdict} | {reason}")


def test_contradiction_blind_vs_contextual():
    """Case 3e: blind < 15% but contextual > 50% -> contradiction C4"""
    state = _make_state(
        stress_test_tier_stats={
            "blind_discovery": {"matches": 1, "queries": 10, "max": 10},
            "contextual_discovery": {"matches": 6, "queries": 10, "max": 10},
        }
    )
    verdict, reason = _compute_verdict(state)
    assert verdict == "REQUIRES ANALYST REVIEW", f"FAIL Contradiction(C4): got '{verdict}'"
    assert "C4" in reason
    print(f"  PASS: Contradiction (blind 10% / contextual 60%) -> {verdict} | {reason}")


if __name__ == "__main__":
    print("=== Validator Verdict Regression Tests ===\n")
    tests = [
        test_strong_audit,
        test_weak_audit_invalid,
        test_weak_audit_zero_discovery,
        test_weak_audit_low_confidence,
        test_contradictory_high_discovery_low_confidence,
        test_contradictory_weak_trust,
        test_contradictory_many_gaps,
        test_local_profile_low_combined_rate,
        test_contradiction_blind_vs_contextual,
    ]
    passed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {t.__name__} — {e}")
        except Exception as e:
            print(f"  ERROR: {t.__name__} — {e}")

    print(f"\n{'='*50}")
    print(f"Passed {passed}/{len(tests)} tests.")
    if passed == len(tests):
        print("=== ALL VALIDATOR VERDICT TESTS PASSED ===")
    else:
        print(f"!!! {len(tests) - passed} test(s) FAILED !!!")
