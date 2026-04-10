import sys
from rich.console import Console
from rich.table import Table

console = Console()

def validate_profile_registries():
    """
    Scans all statically defined mappings, lists, and packs across nodes to ensure
    no file is referencing a non-canonical, non-aliased profile key.
    Fails fast on startup (SystemExit) if bad keys are found.
    """
    
    # Core registries to validate against
    from nodes.business_profiles import BUSINESS_INTELLIGENCE_PROFILES, _LEGACY_ALIASES
    
    valid_keys = set(BUSINESS_INTELLIGENCE_PROFILES.keys()).union(set(_LEGACY_ALIASES.keys()))
    
    invalid_entries = []  # List of tuples: (Registry_Name, Invalid_Key)
    
    # 1. Source packs
    try:
        from nodes.source_matrix import SOURCE_PACKS
        for k in SOURCE_PACKS.keys():
            if k not in valid_keys and k != "unknown":
                invalid_entries.append(("source_matrix.SOURCE_PACKS", k))
    except ImportError:
        pass

    # 2. Schema templates
    try:
        from nodes.schema_generation_node import PROFILE_SCHEMA_MAP
        for k in PROFILE_SCHEMA_MAP.keys():
            if k not in valid_keys:
                invalid_entries.append(("schema_generation_node.PROFILE_SCHEMA_MAP", k))
    except ImportError:
        pass

    # 3. Profile selector keywords
    try:
        from nodes.profile_selector import PROFILE_KEYWORDS
        for k in PROFILE_KEYWORDS.keys():
            if k not in valid_keys:
                invalid_entries.append(("profile_selector.PROFILE_KEYWORDS", k))
    except ImportError:
        pass

    # 4. Crawler policies / platform inference lists inside business_profiles
    try:
        from nodes.business_profiles import get_platform_like_profiles, get_local_trust_profiles
        for k in get_platform_like_profiles():
            if k not in valid_keys:
                invalid_entries.append(("business_profiles.get_platform_like_profiles", k))
        for k in get_local_trust_profiles():
            if k not in valid_keys:
                invalid_entries.append(("business_profiles.get_local_trust_profiles", k))
    except ImportError:
        pass

    # 5. Crawler policy specific sets
    try:
        from nodes.crawler_policy_node import _HIGH_IP_PROFILES
        for k in _HIGH_IP_PROFILES:
            if k not in valid_keys:
                invalid_entries.append(("crawler_policy_node._HIGH_IP_PROFILES", k))
    except ImportError:
        pass


    if invalid_entries:
        console.print("\n[bold red]🚨 PRE-FLIGHT VALIDATION FAILED: Ontology Drift Detected 🚨[/bold red]")
        console.print("[red]One or more node registries reference invalid business profile keys.[/red]")
        console.print("All hardcoded keys MUST exist in either BUSINESS_INTELLIGENCE_PROFILES (canonical) or _LEGACY_ALIASES (alias).")
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Registry Location")
        table.add_column("Invalid Key Found", style="red")
        
        for registry, bad_key in invalid_entries:
            table.add_row(registry, bad_key)
            
        console.print(table)
        console.print("\n[yellow]Action: Fix the keys in the offending files or register them in nodes/business_profiles.py[/yellow]\n")
        
        # Fail fast
        sys.exit(1)
