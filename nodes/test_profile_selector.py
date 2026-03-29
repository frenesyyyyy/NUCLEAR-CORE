import json
from nodes.profile_selector import select_business_profile

def run_tests():
    test_cases = [
        {
            "name": "Dentist schema",
            "kwargs": {
                "business_type": "unknown",
                "target_industry": "healthcare",
                "scale_level": "Local",
                "schema_type_counts": {"Dentist": 1},
                "discovered_location": "Rome"
            },
            "expected": "local_dentist"
        },
        {
            "name": "Lawyer keyword with local context",
            "kwargs": {
                "business_type": "unknown",
                "target_industry": "studio legale immobiliare",
                "scale_level": "Local",
                "schema_type_counts": {},
                "discovered_location": "Rome"
            },
            "expected": "local_law_firm"
        },
        {
            "name": "Lawyer keyword without local context",
            "kwargs": {
                "business_type": "unknown",
                "target_industry": "studio legale",
                "scale_level": "National",
                "schema_type_counts": {},
                "discovered_location": ""
            },
            "expected": "b2b_saas" # Or whatever default if not local
        },
        {
            "name": "Boutique hotel in Rome",
            "kwargs": {
                "business_type": "unknown",
                "target_industry": "boutique hotel in Rome",
                "scale_level": "Local",
                "schema_type_counts": {},
                "discovered_location": "Rome"
            },
            "expected": "restaurant_hospitality"
        },
        {
            "name": "Habit tracking mobile app",
            "kwargs": {
                "business_type": "software",
                "target_industry": "habit tracking mobile app",
                "scale_level": "Global",
                "schema_type_counts": {},
                "discovered_location": ""
            },
            "expected": "consumer_saas"
        },
        {
            "name": "Booking platform for short-term rentals",
            "kwargs": {
                "business_type": "platform",
                "target_industry": "booking platform for short-term rentals",
                "scale_level": "Global",
                "schema_type_counts": {},
                "discovered_location": ""
            },
            "expected": "marketplace"
        },
        {
            "name": "Agency",
            "kwargs": {
                "business_type": "services",
                "target_industry": "SEO agency",
                "scale_level": "National",
                "schema_type_counts": {},
                "discovered_location": ""
            },
            "expected": "agency_marketing"
        },
        {
            "name": "Ecommerce",
            "kwargs": {
                "business_type": "retail",
                "target_industry": "fashion brand store",
                "scale_level": "National",
                "schema_type_counts": {"Product": 5},
                "discovered_location": ""
            },
            "expected": "ecommerce_brand"
        },
        {
            "name": "Local Tech Override",
            "kwargs": {
                "business_type": "local_tech",
                "target_industry": "IT services",
                "scale_level": "Local",
                "schema_type_counts": {},
                "discovered_location": "Milan"
            },
            "expected": "local_tech_provider"
        },
        {
            "name": "SaaS Default for Software",
            "kwargs": {
                "business_type": "software",
                "target_industry": "B2B CRM",
                "scale_level": "Global",
                "schema_type_counts": {},
                "discovered_location": ""
            },
            "expected": "b2b_saas"
        },
         {
            "name": "Default Fallback",
            "kwargs": {
                "business_type": "unknown",
                "target_industry": "unknown",
                "scale_level": "Global",
                "schema_type_counts": {},
                "discovered_location": ""
            },
            "expected": "b2b_saas"
        }
    ]

    passed = 0
    for tc in test_cases:
        result = select_business_profile(**tc["kwargs"])
        if result == tc["expected"]:
            passed += 1
            print(f"PASS: {tc['name']}")
        else:
            print(f"FAIL: {tc['name']} - Expected {tc['expected']}, got {result}")

    print(f"\nPassed {passed}/{len(test_cases)} tests.")

if __name__ == "__main__":
    run_tests()
