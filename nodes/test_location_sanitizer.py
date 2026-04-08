"""Quick validation of _sanitize_location edge cases."""
from nodes.researcher_node import _sanitize_location

tests = [
    ("Roma, Italia",               "Roma"),
    ("  , Roma, , Italia, ",       "Roma"),
    (", , ",                       ""),
    ("  ",                         ""),
    (None,                         ""),
    ("unknown",                    ""),
    ("Milano (Center)",            "Milano"),
    ("Rome, Italy",                "Rome"),
    ("  Roma  ,  ,  Italia  ",     "Roma"),
    ("Worldwide",                  ""),
    ("N/A",                        ""),
]

passed = 0
for raw, expected in tests:
    result = _sanitize_location(raw)
    status = "PASS" if result == expected else f"FAIL (expected {expected!r})"
    if result == expected:
        passed += 1
    print(f"  {raw!r:35s} -> {result!r:25s} {status}")

print(f"\n{passed}/{len(tests)} sanitizer tests passed.")
