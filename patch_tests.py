import os
import glob

test_files = glob.glob('nodes/test_*.py')

for file in test_files:
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    # Simple string replacement for test fixtures and assertions
    content = content.replace('"b2b_saas"', '"b2b_saas_tech"')
    # Crawler policy checks specific test assertions:
    content = content.replace("for b2b_saas", "for b2b_saas_tech")
    
    if content != original_content:
        with open(file, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Patched tests in: {file}")
