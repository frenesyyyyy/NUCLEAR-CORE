import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from agentic_readiness_node import process

def test_semantic_perfection():
    print("--- Test 1: Semantic Perfection ---")
    state = {
        "client_content_raw": """
        <html>
            <body>
                <button>Schedule a Demo</button>
                <form id="contact">
                    <label for="email">Email:</label>
                    <input type="email" id="email" />
                    <button type="submit">Send Message</button>
                </form>
            </body>
        </html>
        """,
        "business_profile_summary": {"label": "B2B SaaS"}
    }
    result = process(state)
    ar = result["agentic_readiness"]
    assert ar["button_semantics_score"] >= 90
    assert ar["form_readability_score"] >= 90
    assert ar["cta_clarity_score"] >= 90
    assert "Excellent agentic operability" in ar["notes"]
    print("  PASSED\n")

def test_legacy_mess():
    print("--- Test 2: Legacy / Non-Semantic HTML ---")
    state = {
        "client_content_raw": """
        <html>
            <body>
                <div role="button">Click Here</div>
                <form>
                    <input type="text" placeholder="Your name" />
                    <input type="submit" value="Go" />
                </form>
            </body>
        </html>
        """,
        "business_profile_summary": {"label": "Local Dentist"}
    }
    result = process(state)
    ar = result["agentic_readiness"]
    # Vague button + Unlabeled input (placeholder is checked but let's see)
    assert ar["button_semantics_score"] < 80
    assert ar["cta_clarity_score"] < 100 # Missing 'book', 'reserve' etc.
    print("  PASSED\n")

def test_no_content():
    print("--- Test 3: No Content Fallback ---")
    state = {}
    result = process(state)
    ar = result["agentic_readiness"]
    assert ar["button_semantics_score"] == 0
    assert "Audit failed" in ar["notes"]
    print("  PASSED\n")

if __name__ == "__main__":
    test_semantic_perfection()
    test_legacy_mess()
    test_no_content()
    print("=== ALL AGENTIC READINESS TESTS PASSED ===")
