"""
Test suite for the Profile Selector — validates canonical key output
against the 6 Canonical Agency Profiles architecture.

All expected values MUST be one of the 6 canonical keys:
  b2b_saas_tech, local_service_ymyl, ecommerce_retail,
  hospitality_travel, publisher_media, professional_services
"""

from nodes.profile_selector import select_business_profile


def run_tests():
    test_cases = [
        {
            "name": "Dentist schema boost",
            "kwargs": {
                "target_industry": "healthcare",
                "scale_level": "Local",
                "schema_type_counts": {"Dentist": 1},
                "discovered_location": "Rome",
            },
            "expected": "local_healthcare_ymyl",
        },
        {
            "name": "Lawyer keyword with local context",
            "kwargs": {
                "target_industry": "studio legale immobiliare",
                "scale_level": "Local",
                "schema_type_counts": {},
                "discovered_location": "Rome",
            },
            "expected": "local_legal_ymyl",
        },
        {
            "name": "Lawyer keyword without local context (still YMYL)",
            "kwargs": {
                "target_industry": "studio legale",
                "scale_level": "National",
                "schema_type_counts": {},
                "discovered_location": "",
            },
            "expected": "local_legal_ymyl",
        },
        {
            "name": "Boutique hotel in Rome",
            "kwargs": {
                "target_industry": "boutique hotel in Rome",
                "scale_level": "Local",
                "schema_type_counts": {},
                "discovered_location": "Rome",
            },
            "expected": "hospitality_travel",
        },
        {
            "name": "Habit tracking mobile app",
            "kwargs": {
                "target_industry": "habit tracking mobile app",
                "scale_level": "Global",
                "schema_type_counts": {"SoftwareApplication": 1},
                "discovered_location": "",
            },
            "expected": "b2b_saas_tech",
        },
        {
            "name": "Booking platform for short-term rentals",
            "kwargs": {
                "target_industry": "booking platform for short-term rentals",
                "scale_level": "Global",
                "schema_type_counts": {},
                "discovered_location": "",
            },
            "expected": "hospitality_travel",
        },
        {
            "name": "SEO Agency maps to professional_services",
            "kwargs": {
                "target_industry": "SEO agency",
                "scale_level": "National",
                "schema_type_counts": {},
                "discovered_location": "",
            },
            "expected": "professional_services",
        },
        {
            "name": "Fashion brand store maps to ecommerce_retail",
            "kwargs": {
                "target_industry": "fashion brand store",
                "scale_level": "National",
                "schema_type_counts": {"Product": 5},
                "discovered_location": "",
            },
            "expected": "ecommerce_retail",
        },
        {
            "name": "Local IT services maps to professional_services",
            "kwargs": {
                "target_industry": "IT services provider",
                "scale_level": "Local",
                "schema_type_counts": {},
                "discovered_location": "Milan",
            },
            "expected": "professional_services",
        },
        {
            "name": "B2B CRM SaaS",
            "kwargs": {
                "target_industry": "B2B CRM",
                "scale_level": "Global",
                "schema_type_counts": {},
                "discovered_location": "",
            },
            "expected": "b2b_saas_tech",
        },
        {
            "name": "Zero-score fallback to professional_services",
            "kwargs": {
                "target_industry": "unknown",
                "scale_level": "Global",
                "schema_type_counts": {},
                "discovered_location": "",
            },
            "expected": "professional_services",
        },
        {
            "name": "News publisher",
            "kwargs": {
                "target_industry": "online news magazine",
                "scale_level": "Global",
                "schema_type_counts": {"NewsArticle": 3},
                "discovered_location": "",
            },
            "expected": "publisher_media",
        },
        {
            "name": "Coliving campus (Dot Campus scenario)",
            "kwargs": {
                "target_industry": "coliving campus accommodation",
                "scale_level": "Local",
                "schema_type_counts": {"LodgingBusiness": 1},
                "discovered_location": "Rome",
                "extra_context": {
                    "client_content_clean": "rooms booking guests check-in campus coliving accommodation breakfast"
                },
            },
            "expected": "hospitality_travel",
        },
    ]

    passed = 0
    total = len(test_cases)

    for tc in test_cases:
        result_key, result_meta = select_business_profile(**tc["kwargs"])
        expected = tc["expected"]

        if result_key == expected:
            passed += 1
            print(f"PASS: {tc['name']} -> {result_key} [{result_meta['reliability']}]")
        else:
            print(f"FAIL: {tc['name']} -> Expected '{expected}', got '{result_key}' {result_meta['evidence']}")

    print(f"\nPassed {passed}/{total} tests.")
    if passed == total:
        print("=== ALL PROFILE SELECTOR TESTS PASSED ===")
    else:
        print(f"!!! {total - passed} test(s) FAILED !!!")


if __name__ == "__main__":
    run_tests()
