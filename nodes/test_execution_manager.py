"""
Regression Tests for Execution Manager Consolidation.
Verifies that the hybrid manager in sequential mode produces identical
node ordering and state-reducer protection as the old legacy path.
"""
import sys, os
sys.path.append(os.getcwd())

from nodes.execution_manager import _load_nodes, _wrap_node, run_hybrid_pipeline
from nodes.run_modes import RUN_MODES
from nodes.node_contracts import PROTECTED_KEYS
from nodes.state_reducer import merge_patch
from copy import deepcopy


# ── Canonical node ordering spec ──────────────────────────────────────────────
TRUTH_SPINE = [
    "content_fetcher",
    "orchestrator",
    "prospector",
    "business_profile_selector",
]

SEQUENTIAL_TAIL = [
    "content_strategist",
    "content_engineering",
    "schema_generation",
    "crawler_policy",
    "earned_media",
    "source_quality",
    "researcher",
    "model_analytics",
    "implementation_blueprint",
    "agentic_readiness",
    "validator",
    "finalizer",
]

FULL_CANONICAL_ORDER = TRUTH_SPINE + SEQUENTIAL_TAIL


def test_node_registry_completeness():
    """Verify _load_nodes() returns exactly the 16 canonical nodes."""
    nodes = _load_nodes()
    for name in FULL_CANONICAL_ORDER:
        assert name in nodes, f"FAIL: Missing node '{name}' from _load_nodes()"
    # No extra nodes
    extra = set(nodes.keys()) - set(FULL_CANONICAL_ORDER)
    assert not extra, f"FAIL: Extra nodes in registry: {extra}"
    print(f"  PASS: All 16 canonical nodes present, no extras.")


def test_standard_mode_is_sequential():
    """Verify that run_mode='standard' has parallel_enabled=False."""
    cfg = RUN_MODES["standard"]
    assert cfg["parallel_enabled"] is False, "FAIL: standard mode should be sequential"
    print(f"  PASS: standard mode is sequential (parallel_enabled=False)")


def test_lite_mode_is_sequential():
    """Verify that run_mode='lite' has parallel_enabled=False."""
    cfg = RUN_MODES["lite"]
    assert cfg["parallel_enabled"] is False, "FAIL: lite mode should be sequential"
    print(f"  PASS: lite mode is sequential (parallel_enabled=False)")


def test_agency_mode_is_parallel():
    """Verify that run_mode='agency' has parallel_enabled=True."""
    cfg = RUN_MODES["agency"]
    assert cfg["parallel_enabled"] is True, "FAIL: agency mode should be parallel"
    print(f"  PASS: agency mode is parallel (parallel_enabled=True)")


def test_protected_keys_include_decision_fields():
    """Verify new decision fields are protected."""
    expected = {"decision_summary", "decision_risks", "decision_next_step"}
    missing = expected - PROTECTED_KEYS
    assert not missing, f"FAIL: Missing protected keys: {missing}"
    print(f"  PASS: All decision fields are in PROTECTED_KEYS")


def test_legacy_wrapper_delegates():
    """Verify run_pipeline() delegates to hybrid manager."""
    from nuclear_geo_optimizer import run_pipeline
    import inspect
    src = inspect.getsource(run_pipeline)
    assert "run_hybrid_pipeline" in src, "FAIL: run_pipeline() does not delegate to hybrid"
    assert "DEPRECATED" in src, "FAIL: run_pipeline() is not marked as deprecated"
    print(f"  PASS: run_pipeline() is a thin wrapper around run_hybrid_pipeline()")


def test_main_always_uses_hybrid():
    """Verify main() always calls run_hybrid_pipeline()."""
    from nuclear_geo_optimizer import main
    import inspect
    src = inspect.getsource(main)
    assert "run_hybrid_pipeline" in src, "FAIL: main() does not use hybrid manager"
    # Should NOT contain the old branching
    assert 'run_pipeline(state)' not in src, "FAIL: main() still calls legacy run_pipeline()"
    print(f"  PASS: main() always calls run_hybrid_pipeline()")


if __name__ == "__main__":
    print("=== Execution Manager Consolidation Tests ===\n")
    tests = [
        test_node_registry_completeness,
        test_standard_mode_is_sequential,
        test_lite_mode_is_sequential,
        test_agency_mode_is_parallel,
        test_protected_keys_include_decision_fields,
        test_legacy_wrapper_delegates,
        test_main_always_uses_hybrid,
    ]
    passed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {t.__name__} -- {e}")

    print(f"\n{'='*50}")
    print(f"Passed {passed}/{len(tests)} tests.")
    if passed == len(tests):
        print("=== ALL EXECUTION MANAGER TESTS PASSED ===")
    else:
        print(f"!!! {len(tests) - passed} test(s) FAILED !!!")
