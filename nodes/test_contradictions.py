import sys
import os
import traceback

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from nodes.validator_node import process

def run_test(name, state_patch):
    print(f"\n[TEST] {name}")
    base_state = {
        "url": "https://example.com",
        "brand_name": "Test Brand",
        "audit_integrity_status": "valid",
        "source_of_truth_mode": "hybrid",
        # Extraction signal for confidence
        "client_content_depth": {
            "extraction_quality": "high",
            "word_count": 1000,
            "schema_block_count": 5
        },
        # Schema signals for confidence
        "schema_type_counts": {"Organization": 1, "LocalBusiness": 1, "MedicalClinic": 1, "WebPage": 1},
        "metrics": {"Data Confidence Score": 80, "Defensible Evidence Depth": 50, "Entity Consensus": 80},
        "source_taxonomy": {
            "trust_mix": "Strong",
            "earned_count": 5,
            "review_count": 3,
            "owned_count": 2,
            "penalized_relevant_gaps": [],
            "relevant_gap_count": 0
        },
        "model_analytics": {"authority_composite": 80},
        "schema_confidence": "High",
        "trust_confidence": "High",
        "stress_test_tier_stats": {
            "blind_discovery": {"matches": 5, "queries": 10},
            "contextual_discovery": {"matches": 5, "queries": 10}
        }
    }
    state = {**base_state, **state_patch}
    
    try:
        result = process(state)
        verdict = result["agency_verdict"]
        flags = result.get("contradiction_flags", [])
        reasons = result.get("contradiction_reasons", [])
        
        print(f"  Verdict: {verdict}")
        print(f"  Flags: {flags}")
        if reasons:
            print(f"  Reasons: {reasons[0]}...")
            
        return result
    except Exception:
        traceback.print_exc()
        return None

# Test Cases
t1 = run_test("Clean Strong Audit", {})
# Expected: CLIENT READY, No flags

t2 = run_test("C2: High Gaps + High Trust", {
    "source_taxonomy": {
        "trust_mix": "Strong",
        "earned_count": 5,
        "review_count": 3,
        "owned_count": 2,
        "penalized_relevant_gaps": ["Gap1", "Gap2"],
        "relevant_gap_count": 2
    },
    "trust_confidence": "High"
})
# Expected: REQUIRES ANALYST REVIEW, Flag C2

t3 = run_test("C3: Low Discovery + High Authority", {
    "model_analytics": {"authority_composite": 90},
    "stress_test_tier_stats": {
        "blind_discovery": {"matches": 0, "queries": 10},
        "contextual_discovery": {"matches": 1, "queries": 10}
    }
})
# Expected: REQUIRES ANALYST REVIEW, Flag C3

t4 = run_test("C5: Degraded Integrity + High Consensus", {
    "audit_integrity_status": "degraded",
    "metrics": {"Data Confidence Score": 85, "Defensible Evidence Depth": 50, "Entity Consensus": 85}
})
# Expected: REQUIRES ANALYST REVIEW, Flag C5

t5 = run_test("C6: Blind Shadow Detection", {
    "stress_test_tier_stats": {
        "blind_discovery": {"matches": 0, "queries": 10},
        "contextual_discovery": {"matches": 8, "queries": 10}
    }
})
# Expected: REQUIRES ANALYST REVIEW, Flag C6 (Overrides candidate READY)

# Assertions
print("\n[VERIFICATION RESULTS]")
if t1["agency_verdict"] == "CLIENT READY" and not t1["contradiction_flags"]:
    print("[PASS] Clean Audit")
else:
    print("[FAIL] Clean Audit")

if t2["agency_verdict"] == "REQUIRES ANALYST REVIEW" and "C2" in t2["contradiction_flags"]:
    print("[PASS] C2: High Gaps + High Trust Dissonance")
else:
    print("[FAIL] C2: High Gaps + High Trust Dissonance")

if t3["agency_verdict"] == "REQUIRES ANALYST REVIEW" and "C3" in t3["contradiction_flags"]:
    print("[PASS] C3: Low Discovery + High Authority Drift")
else:
    print("[FAIL] C3: Low Discovery + High Authority Drift")

if t4["agency_verdict"] == "REQUIRES ANALYST REVIEW" and "C5" in t4["contradiction_flags"]:
    print("[PASS] C5: Degraded Integrity + High Consensus")
else:
    print("[FAIL] C5: Degraded Integrity + High Consensus")

if t5["agency_verdict"] == "REQUIRES ANALYST REVIEW" and "C6" in t5["contradiction_flags"]:
    print("[PASS] C6: Blind Shadow Detection")
else:
    print("[FAIL] C6: Blind Shadow Detection")

