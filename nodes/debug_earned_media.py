import sys
import traceback
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from nodes.earned_media_node import process

tests = {
    'noise_split_regression': {
        'brand_name': 'Acme Dental', 
        'target_industry': 'Healthcare',
        'url': 'https://acmedental.com',
        'business_profile_key': 'local_healthcare_ymyl',
        'raw_data_complete': {
            'source_urls': [
                # 1. Known review/editorial source
                'https://trustpilot.com/review/acmedental.com',
                # 2. Owned brand source
                'https://acmedental.com/about-us',
                # 3. Generic search/junk URL -> noise
                'https://google.com/search?q=acme+dental',
                'https://youtube.com/watch?v=123',
                # 4. Unrecognized but plausible branded article URL -> unclassified candidate
                'https://randomlocalblog.net/acmedental-review',
                # 5. Google Maps path -> classified local/review, not noise
                'https://maps.google.com/?q=acme+dental+clinic'
            ]
        },
        'external_sources_raw': [],
        '_assertions': lambda em: (
            em['source_breakdown']['review_sentiment'] == 2,  # Trustpilot, Maps
            em['source_breakdown']['owned_property'] == 1,    # acmedental.com
            em['source_breakdown']['noise'] == 2,             # google.com/search, youtube.com
            em['source_breakdown']['unclassified'] == 1,      # randomlocalblog.net with 'acmedental' slug
            # 6. Noisy input set should not inflate total_sources (Total = 6 URLs - 2 noise = 4 sources)
            (em['source_breakdown']['review_sentiment'] + em['source_breakdown']['owned_property'] + em['source_breakdown']['unclassified']) == 4,
        ),
    },
}

if __name__ == "__main__":
    for name, state in tests.items():
        assertions_fn = state.pop('_assertions')
        try:
            print(f'\\n[RUN] {name}')
            r = process(state)
            em = r['earned_media']
            
            # Debug mention mapping
            for m in list(em.get("mentions", [])):
                print(f"Mention Debug: {m.get('url')} -> ?")
            
            print(f'Result Breakdown: {em["source_breakdown"]}')
            
            results = assertions_fn(em)
            if not isinstance(results, tuple):
                results = (results,)
                
            all_passed = True
            for i, res in enumerate(results):
                if not res:
                    print(f'  [FAIL] Assertion {i} failed.')
                    all_passed = False
                else:
                    print(f'  [PASS] Assertion {i} OK.')
                    
            if all_passed:
                print(f'=== {name} PASSED ===')
        except Exception as e:
            print(f'EXCEPTION in {name}:')
            traceback.print_exc()
