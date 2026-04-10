import sys
import os

from nodes.bootstrap_checks import validate_profile_registries
from nodes.business_profiles import BUSINESS_INTELLIGENCE_PROFILES

def test_registry_validation_passes():
    """By default, the codebase should pass the validation without raising SystemExit."""
    try:
        validate_profile_registries()
        passed = True
    except SystemExit:
        passed = False
    
    assert passed, "Codebase currently fails pre-flight validation!"

def test_registry_validation_fails_fast_on_bad_key(monkeypatch):
    """If we monkeypatch a registry, it should fail fast."""
    
    # 1. We monkeypatch source_matrix.SOURCE_PACKS
    import nodes.source_matrix
    mock_packs = dict(nodes.source_matrix.SOURCE_PACKS)
    mock_packs["rogue_legacy_key_123"] = {}
    monkeypatch.setattr(nodes.source_matrix, "SOURCE_PACKS", mock_packs)
    
    try:
        validate_profile_registries()
        failed = False
    except SystemExit as exc:
        failed = True
        assert exc.code == 1
    
    assert failed, "Pre-flight validation did not fail when a bad key was introduced."

if __name__ == "__main__":
    print("Running registry validation test directly to see the UI...")
    
    import nodes.source_matrix
    original = dict(nodes.source_matrix.SOURCE_PACKS)
    nodes.source_matrix.SOURCE_PACKS["my_rogue_lawyer_key"] = {}
    
    try:
        validate_profile_registries()
    except SystemExit:
        print("SystemExit caught successfully! Check the table above.")
        
    # Restore
    nodes.source_matrix.SOURCE_PACKS = original
    
    print("\nRunning clean verification...")
    validate_profile_registries()
    print("Clean verification passed!")
