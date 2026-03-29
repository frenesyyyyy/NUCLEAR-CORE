import sys, traceback
sys.path.insert(0, 'nodes')
from earned_media_node import process

tests = {
    'test2': {
        'brand_name': 'Acme CRM', 'target_industry': 'SaaS', 'discovered_location': 'Global',
        'business_profile': {}, 'raw_data_complete': {
            'source_urls': ['https://techcrunch.com/2024/01/acme', 'https://forbes.com/acme-review',
                            'https://g2.com/products/acme', 'https://trustpilot.com/review/acme-crm.com',
                            'https://reddit.com/r/saas/acme']},
        'external_sources': ['https://capterra.com/software/acme', 'https://crunchbase.com/organization/acme'],
        'url': 'https://acme-crm.com',
        '_assertions': lambda em: (em['source_breakdown']['review'] == 2, em['source_breakdown']['editorial'] == 2, em['source_breakdown']['forum'] == 1, em['source_breakdown']['directory'] == 2),
    },
    'test3': {
        'brand_name': 'Shady', 'target_industry': 'Finance', 'discovered_location': 'Unknown',
        'business_profile': {}, 'raw_data_complete': {}, 'url': 'https://shadycorp.com',
        'external_sources': ['https://scam-alert.com/shady-corp', 'https://trustpilot.com/review/shadycorp', 'https://ripoff-report.com/shady'],
        '_assertions': lambda em: (em['reputation_risk_score'] > 0, em['warning_effect_risk'] == True,),
    },
    'test4': {
        'brand_name': 'Acme', 'target_industry': 'Tech', 'discovered_location': 'Worldwide',
        'business_profile': {}, 'raw_data_complete': {}, 'url': 'https://acme.com',
        'external_sources': ['https://acme.com/blog/post', 'https://acme.com/about'],
        '_assertions': lambda em: (em['source_breakdown']['owned'] == 2, em['strength_score'] < 10,),
    },
}

for name, state in tests.items():
    assertions_fn = state.pop('_assertions')
    try:
        r = process(state)
        em = r['earned_media']
        print(f'{name}: score={em["strength_score"]} risk={em["reputation_risk_score"]} warning={em["warning_effect_risk"]} breakdown={em["source_breakdown"]}')
        results = assertions_fn(em)
        for i, res in enumerate(results):
            if not res:
                print(f'  ASSERTION {i} FAILED!')
            else:
                print(f'  assertion {i} OK')
    except Exception as e:
        print(f'{name} EXCEPTION:')
        traceback.print_exc()
