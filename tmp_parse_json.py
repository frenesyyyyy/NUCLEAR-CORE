import json

filepath = r"C:\Projects\NUCLEAR CORE\exports\geo_audit_b2149406-a99c-41c0-b143-74686de33b63.json"

with open(filepath, "r", encoding="utf-8") as f:
    data = json.load(f)

print("Competitor Entities:")
for e in data.get("competitor_entities", []):
    print(f" - {e}")

print("\nTopic Gaps:")
for g in data.get("topic_gaps", []):
    print(f" - {g}")

print("\nMetrics:")
print(json.dumps(data.get("metrics", {}), indent=2))

print("\nCitation Status:")
print(data.get("citation_status"))
