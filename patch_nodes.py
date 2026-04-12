import os

target_files = [
    'nodes/researcher_node.py',
    'nodes/prospector_node.py',
    'nodes/profile_selector.py',
    'nodes/model_analytics_node.py',
    'nodes/earned_media_node.py',
    'nodes/crawler_policy_node.py',
    'nodes/content_engineering_node.py',
]

def add_import(content):
    if 'from nodes.business_profiles import DEFAULT_PROFILE_KEY' not in content:
        lines = content.split('\n')
        # find the last import line
        last_import = -1
        for i, line in enumerate(lines):
            if line.startswith('import ') or line.startswith('from '):
                last_import = i
        if last_import != -1:
            lines.insert(last_import + 1, 'from nodes.business_profiles import DEFAULT_PROFILE_KEY')
        else:
            lines.insert(0, 'from nodes.business_profiles import DEFAULT_PROFILE_KEY')
        return '\n'.join(lines)
    return content

for file in target_files:
    if os.path.exists(file):
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # apply changes
        content = add_import(content)
        
        # Specific replacements
        if file == 'nodes/crawler_policy_node.py':
            content = content.replace('profile_key: str      = state.get("business_profile_key", "b2b_saas")', 'profile_key: str      = state.get("business_profile_key", DEFAULT_PROFILE_KEY)')
            content = content.replace('"b2b_saas", "consumer_saas", "ecommerce_brand", "marketplace",', '"b2b_saas_tech", "b2b_saas", "consumer_saas", "ecommerce_brand", "marketplace", "ecommerce_retail",')
        elif file == 'nodes/prospector_node.py':
            content = content.replace('elif p_key in ("b2b_saas", "consumer_saas", "local_tech_provider"):', 'elif p_key in ("b2b_saas_tech", "b2b_saas", "consumer_saas", "local_tech_provider", "professional_services"):')
        elif file == 'nodes/profile_selector.py':
            content = content.replace('return "b2b_saas", {"reliability": "medium"', 'return DEFAULT_PROFILE_KEY, {"reliability": "medium"')
            content = content.replace('return "b2b_saas", {"reliability": "low"', 'return DEFAULT_PROFILE_KEY, {"reliability": "low"')
        elif file == 'nodes/researcher_node.py':
            content = content.replace('profile_key = state.get("business_profile_key", "b2b_saas")', 'profile_key = state.get("business_profile_key", DEFAULT_PROFILE_KEY)')
        elif file == 'nodes/model_analytics_node.py':
            content = content.replace('profile_key     = state.get("business_profile_key", "b2b_saas")', 'profile_key     = state.get("business_profile_key", DEFAULT_PROFILE_KEY)')
        elif file == 'nodes/earned_media_node.py':
            content = content.replace('profile_key: str       = state.get("business_profile_key", "b2b_saas")', 'profile_key: str       = state.get("business_profile_key", DEFAULT_PROFILE_KEY)')
        elif file == 'nodes/content_engineering_node.py':
            content = content.replace('def _evaluate_evidence_density(content: str, profile_key: str = "b2b_saas") -> int:', 'def _evaluate_evidence_density(content: str, profile_key: str = DEFAULT_PROFILE_KEY) -> int:')
            content = content.replace('profile_key = state.get("business_profile_key", "b2b_saas")', 'profile_key = state.get("business_profile_key", DEFAULT_PROFILE_KEY)')

        if content != original_content:
            with open(file, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Patched: {file}")
