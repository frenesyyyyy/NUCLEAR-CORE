import os
# Must be set BEFORE down-stream imports initialize the Hub constants
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "0"
os.environ["HF_HOME"] = os.path.join(os.getcwd(), "hf_cache")

import time
import re
import json
import requests
import chromadb
from sentence_transformers import SentenceTransformer
from rich.console import Console
from typing import Any
from google import genai
from datetime import datetime
from nodes.api_utils import execute_with_backoff
from nodes.business_profiles import DEFAULT_PROFILE_KEY

console = Console()

# ──────────────────────────────────────────────────────────────────────────────
# Quality-Gated Tier Construction — Constants
# ──────────────────────────────────────────────────────────────────────────────

TIER_POLICY = {
    "blind_discovery":      {"min": 3, "target": 5, "max": 8},
    "contextual_discovery": {"min": 3, "target": 5, "max": 8},
}

QUALITY_THRESHOLDS = {
    "model":             55,
    "entity_fallback":   60,
    "profile_fallback":  70,
}

FALLBACK_CAPS = {
    "max_fallback_share":          0.60,
    "max_profile_fallback_share":  0.40,
}


# ──────────────────────────────────────────────────────────────────────────────
# Stress Test Helpers (v4.5 Agency-Grade Tiered Architecture)
# ──────────────────────────────────────────────────────────────────────────────

def _safe_stringify_list(items: list[Any]) -> list[str]:
    """
    Agency-grade safety helper to normalize lists of mixed types (str, dict, list)
    into a flat list of strings for prompt interpolation or joining.
    """
    if not items: return []
    safe = []
    for item in items:
        if not item: continue
        if isinstance(item, str):
            val = item.strip()
            if val: safe.append(val)
        elif isinstance(item, dict):
            # Convert dict to key:value string for context
            val = "; ".join([f"{k}: {v}" for k, v in item.items() if v])
            if val: safe.append(f"[{val}]")
        else:
            val = str(item).strip()
            if val: safe.append(val)
    return safe


def _sanitize_location(raw: str) -> str:
    """
    Universal location sanitizer — prevents string interpolation bugs
    in downstream prompts and query builders.

    Handles:
      - Leading/trailing commas and whitespace
      - Collapsed multiple separators (e.g. ', , ' -> ', ')
      - Garbage patterns like 'a , Italia Roma'
      - Empty, None, or placeholder values

    Returns a clean location string or '' if invalid.
    """
    if not raw or not isinstance(raw, str):
        return ""

    loc = raw.strip()

    # Strip out any text inside parentheses
    loc = re.sub(r'\(.*?\)', '', loc)

    # Strip leading/trailing commas and spaces before splitting
    loc = loc.strip(", ")

    # Split by comma and keep only the first item
    loc = loc.split(',')[0]

    # Collapse multiple spaces
    loc = re.sub(r'\s{2,}', ' ', loc)

    # Final trim
    loc = loc.strip()

    # Reject known placeholders
    if loc.lower() in {"none", "n/a", "unknown", "unavailable", "worldwide", "national", ""}:
        return ""

    # Reject if result is a single character or pure punctuation
    if len(loc) < 2 or not any(c.isalpha() for c in loc):
        return ""

    return loc


def _extract_brand_tokens(brand_name: str, url: str, og_title: str) -> set[str]:
    """
    Extracts restricted tokens that must NOT appear in T1/T2 queries.
    """
    from urllib.parse import urlparse
    tokens = set()
    
    # 1. Full brand name
    b = brand_name.lower().strip()
    if b and b != "unknown":
        tokens.add(b)
        # Split tokens (e.g., "Just Eat" -> "just", "eat")
        for part in b.split():
            if len(part) > 2: tokens.add(part)

    # 2. Domain token
    try:
        domain = urlparse(url).netloc.replace("www.", "").split(".")[0].lower()
        if domain and len(domain) > 2:
            tokens.add(domain)
    except: pass

    # 3. OG Title hints (significant words only)
    if og_title and og_title != "N/A":
        title_words = re.findall(r'\b\w{4,}\b', og_title.lower())
        # Only add words that aren't common generic terms
        stop_words = {"home", "welcome", "italia", "italy", "page", "website", "online", "servizi", "services"}
        for word in title_words:
            if word not in stop_words:
                tokens.add(word)

    return tokens

def _query_has_brand_leakage(query: str, brand_tokens: set[str]) -> bool:
    """Checks if a query contains any forbidden brand identifiers."""
    q_low = query.lower()
    for token in brand_tokens:
        t_low = token.lower()
        if re.search(rf'\b{re.escape(t_low)}\b', q_low):
            return True
    return False

def _inject_geo_context(query: str, location: str, locale: str) -> str:
    """Forces geo-anchoring into a query for local businesses."""
    if not location or location.lower() in ["worldwide", "national", "unknown"]:
        return query
    
    # Simplify location (e.g. "Rome, Italy" -> "Roma" if locale IT)
    city = location.split(",")[0].strip()
    if locale == "it" and city.lower() == "rome": city = "Roma"
    if locale == "it" and city.lower() == "milan": city = "Milano"
    
    q = query.strip()
    # Check if already has geo
    if city.lower() in q.lower():
        return q
    
    # Natural injection patterns
    if locale == "it":
        return f"{q} {city}"
    else:
        return f"{q} in {city}"

def _is_realistic_query(query: str, industry: str) -> bool:
    """Rejects taxonomical, corporate, or awkward AI-generated phrasing."""
    q = query.lower()
    # 1. Reject raw taxonomy tokens if they appear as the sole subject
    bad_tokens = ["food delivery service", "delivery platform", "marketplace platform", "software as a service", "marketing agency"]
    if q.strip() in bad_tokens: return False
    
    # 2. Reject awkward corporate labels that aren't natural Italian/English
    if "service" in q and industry.lower() in q and len(q.split()) < 4: return False
    
    # 3. Reject placeholder-like brackets if they leaked
    if "[" in q or "]" in q: return False
    
    return True

def _fill_profile_fallback_template(template: str, entity_terms: dict) -> str | None:
    """
    Fills placeholders in a profile fallback template.
    Placeholders: {core_noun}, {quality_modifier}, {location}, {use_case}, 
                 {comparison_modifier}, {packaging_modifier}
    
    Rules:
    1. Reject if unresolved placeholders remain.
    2. Reject if no core_noun is present in entity_terms.
    3. Ensure no trailing/leading garbage.
    """
    if not template or not entity_terms:
        return None
        
    core_noun = entity_terms.get("core_noun")
    if not core_noun:
        return None
        
    filled = template
    try:
        # Check if all placeholders in the template have a match in entity_terms
        placeholders = re.findall(r'\{(.*?)\}', template)
        for p in placeholders:
            if p not in entity_terms or not entity_terms.get(p):
                # Special case: if it's {location} and it's empty, we might allow it if the template allows or just skip
                # For now, strict: all must be present.
                return None 
        
        filled = template.format(**entity_terms)
        
        # Double check for uncleaned braces
        if "{" in filled or "}" in filled:
            return None
            
        filled = re.sub(r'\s+', ' ', filled).strip()
        return filled if len(filled) > 5 else None
    except Exception:
        return None

def _extract_entity_terms(state: dict) -> dict:
    """Extracts semantic placeholders from state for template filling."""
    terms = {}
    locale = state.get("locale", "en")
    
    # Core Noun extraction
    industry = str(state.get("target_industry", "")).lower()
    if "suppl" in industry:
        terms["core_noun"] = industry.replace("supplier", "").replace("supplies", "").strip()
    elif industry and industry != "unknown":
        terms["core_noun"] = industry
    else:
        terms["core_noun"] = state.get("primary_industry", "prodotti" if locale == "it" else "products")
        
    # Sanitize core_noun
    if terms["core_noun"]:
        terms["core_noun"] = terms["core_noun"].strip(",. ")

    # Modifiers
    terms["quality_modifier"] = "certificati" if locale == "it" else "certified"
    
    loc = _sanitize_location(state.get("discovered_location", ""))
    terms["location"] = loc if loc else ""
    
    content = state.get("client_content_clean", "").lower()
    if locale == "it":
        terms["use_case"] = "riscaldamento" if "pellet" in content or "caldaia" in content else "uso professionale"
        terms["comparison_modifier"] = "migliore"
        terms["packaging_modifier"] = "all'ingrosso" if "bulk" in content or "sfuso" in content else "in sacchi"
    else:
        terms["use_case"] = "heating" if "pellet" in content or "boiler" in content else "professional use"
        terms["comparison_modifier"] = "best"
        terms["packaging_modifier"] = "bulk" if "bulk" in content else "packaged"
        
    return terms


def _normalize_query_candidate(candidate: dict, tier: str) -> dict:
    """
    Guarantees a stable schema for query candidates before assembly.
    Default points: T1=25, T2=15, T3=20.
    """
    tier_pts = 25 if tier == "blind_discovery" else 15
    if tier == "branded_validation": tier_pts = 20
    
    norm = {
        "query": str(candidate.get("query", "")).strip(),
        "tier": tier,
        "points": candidate.get("points", tier_pts),
        "source": candidate.get("source", "model"),
        "raw_query": candidate.get("raw_query", candidate.get("query", "")),
        "profile_key": candidate.get("profile_key", "unknown"),
        "quality_score": candidate.get("quality_score", 0.0),
        "quality_gate": candidate.get("quality_gate", "PASS"),
    }
    # Carry over optional fields
    for k in ["location_applied", "sanitized_from", "regen_round", "anchors_used"]:
        if k in candidate: norm[k] = candidate[k]
        
    return norm


def _normalize_stress_test_result(result: dict, candidate: dict) -> dict:
    """
    Guarantees a stable schema for executed stress-test results.
    """
    norm = {
        "query": result.get("query", candidate.get("query", "")),
        "tier": result.get("tier", candidate.get("tier", "unknown")),
        "points": result.get("points", candidate.get("points", 0)),
        "max_pts": result.get("max_pts", candidate.get("points", 0)),
        "matched": result.get("matched", False),
        "source": result.get("source", candidate.get("source", "unknown")),
        "raw_response": result.get("raw_response", ""),
        "execution_date": result.get("execution_date", datetime.now().isoformat()),
    }
    # Carry over construction provenance
    for k in ["quality_score", "regen_round", "anchors_used", "profile_key"]:
        if k in candidate: norm[k] = candidate[k]
        
    return norm


def _normalize_query_text_for_dedupe(query: str) -> str:
    """
    Normalized dedupe key: lowercase, strip punctuation, collapse whitespace.
    """
    if not query: return ""
    # 1. Lowercase and strip outer whitespace
    q = query.lower().strip()
    # 2. Collapse internal whitespace
    q = re.sub(r'\s+', ' ', q)
    # 3. Strip trailing/leading punctuation
    q = q.strip('?!.,;: ')
    # 4. Normalize quote/apostrophe variants
    q = q.replace("'", "").replace("\"", "").replace("’", "")
    return q


def _score_query_candidate(
    query: str,
    tier: str,
    state: dict,
    provenance: str,
    anchors: dict = None,
) -> float:
    """
    Quality-gate scoring for a query candidate.
    Returns a normalized score 0–100 across 9 dimensions.
    """
    if not query or len(query.strip()) < 5:
        return 0.0

    q = query.strip()
    q_lower = q.lower()
    score = 0.0

    # ── 1. Required anchor presence (20 pts) ─────────────────────────────────
    if anchors:
        is_valid, hits = _contains_required_anchor(q, anchors)
        if is_valid:
            primary_hits = [h for h in hits if h.startswith("primary:")]
            score += 20.0 if primary_hits else 10.0

    # ── 2. Grammatical sanity (10 pts) ────────────────────────────────────────
    gram_ok = True
    # Orphan prepositions at start/end
    orphan_preps = ["in", "a", "per", "di", "for", "the", "of"]
    tokens = q_lower.split()
    if tokens and tokens[-1] in orphan_preps:
        gram_ok = False
    if tokens and tokens[0] in ["and", "or", "e", "o", "vs"]:
        gram_ok = False
    # Bracket leaks
    if "[" in q or "]" in q or "{" in q or "}" in q:
        gram_ok = False
    # Consecutive punctuation
    if re.search(r'[,;:]{2,}', q):
        gram_ok = False
    score += 10.0 if gram_ok else 0.0

    # ── 3. Specificity (15 pts) ──────────────────────────────────────────────
    word_count = len(tokens)
    if word_count >= 6:
        score += 15.0
    elif word_count >= 4:
        score += 10.0
    elif word_count >= 3:
        score += 5.0
    # Very short queries get 0

    # ── 4. Generic-shell penalty (15 pts) ────────────────────────────────────
    generic_shells = [
        "best products online", "where to buy", "top rated online store",
        "migliori prodotti online", "dove comprare", "miglior negozio online",
        "how to guides", "guide approfondite", "expert opinions",
        "opinioni degli esperti", "best places to stay", "best consulting firms",
        "best platforms to find services", "compare online vendors",
        "directory for reviews", "best b2b software",
    ]
    is_generic = any(q_lower.strip() == g or q_lower.strip().startswith(g + " ") for g in generic_shells)
    score += 0.0 if is_generic else 15.0

    # ── 5. Domain/entity relevance (15 pts) ──────────────────────────────────
    if anchors:
        all_anchors = anchors.get("primary_anchors", set()) | anchors.get("secondary_anchors", set())
        overlap = sum(1 for a in all_anchors if a in q_lower)
        if overlap >= 2:
            score += 15.0
        elif overlap >= 1:
            score += 10.0

    # ── 6. Local fit (10 pts) ────────────────────────────────────────────────
    is_local = state.get("scale_level", "National") == "Local" or state.get("business_profile", {}).get("location_enforce", False)
    if is_local:
        location = _sanitize_location(state.get("discovered_location", ""))
        if location:
            city = location.split(",")[0].strip().lower()
            if city and city in q_lower:
                score += 10.0
            else:
                score += 2.0  # Partial: local query without geo token
        else:
            score += 5.0  # No location to validate against
    else:
        score += 10.0  # Non-local: full points automatically

    # ── 7. No unresolved placeholders (5 pts) ────────────────────────────────
    has_placeholders = bool(re.search(r'\{\w+\}', q))
    score += 0.0 if has_placeholders else 5.0

    # ── 8. No comma-joined anchor blobs (5 pts) ─────────────────────────────
    has_comma_blob = bool(re.search(r',\s*,|\s,\s', q)) or q.startswith(",")
    score += 0.0 if has_comma_blob else 5.0

    # ── 9. Provenance weighting (5 pts) ──────────────────────────────────────
    provenance_bonus = {"model": 5.0, "entity_fallback": 3.0, "profile_fallback": 0.0}
    score += provenance_bonus.get(provenance, 0.0)

    return min(100.0, max(0.0, score))

def _build_regeneration_context(state: dict, gemini_client) -> dict:
    """
    Consolidates metadata for regeneration rounds to avoid parameter bloat.
    """
    raw_data = state.get("raw_data_complete", {})
    profile = state.get("business_profile", {})
    return {
        "gemini_client": gemini_client,
        "faq_patterns": raw_data.get("faq_patterns", []),
        "authority_entities": raw_data.get("authority_entities", []),
        "topic_gaps": raw_data.get("topic_gaps", []),
        "persona_templates": profile.get("persona_templates", []),
        "service_zones": state.get("service_zones", []),
        "profile_key": state.get("business_profile_key", "unknown"),
        "target_industry": state.get("target_industry", "market"),
        "primary_industry": state.get("primary_industry", ""),
        "locale": state.get("locale", "en"),
        "discovered_location": state.get("discovered_location", ""),
        "client_content_clean": state.get("client_content_clean", ""),
        "business_profile": profile
    }

def _profile_consistency_guard(profile_key: str, anchors: dict) -> bool:
    """
    Returns True if the selected profile is consistent with domain anchors.
    If strong contradictions exist (e.g. medical anchors on a SaaS profile), returns False.
    """
    pa = [a.lower() for a in anchors.get("primary_anchors", [])]
    
    # 1. Medical Contradiction
    medical_anchors = ["terapia", "clinica", "medico", "paziente", "cura", "ospedale", "dentista"]
    if any(m in " ".join(pa) for m in medical_anchors):
        if profile_key not in ["local_healthcare_ymyl", "professional_services"]:
            return False
            
    # 2. SaaS Contradiction
    saas_anchors = ["software", "saas", "cloud", "api", "dashboard", "platform"]
    if any(s in " ".join(pa) for s in saas_anchors):
        if profile_key not in ["b2b_saas_tech", "professional_services"]:
            # This is softer because many businesses have "platforms"
            pass

    return True

def _regenerate_tier_candidates(
    tier_key: str,
    round_idx: int,
    ctx: dict,
    brand_tokens: set,
    is_local: bool,
    location: str,
    locale: str,
    anchors: dict,
    exclusion_context: set
) -> list:
    """
    Executes a specific regeneration round.
    Round 1: LLM Regeneration with anchor guidance.
    Round 2: Corrected Entity Fallback (alternate templates).
    """
    if not ctx or not ctx.get("gemini_client"):
        return []

    type_key = "blind" if tier_key == "blind_discovery" else "contextual"
    new_candidates = []
    
    if round_idx == 1:
        # ── Round 1: Model Regeneration ──
        # For T2, we inject top primary anchors into the prompt for better alignment
        budget = 5 # Smaller budget for regeneration
        if tier_key == "blind_discovery":
            new_candidates = _build_blind_queries(
                ctx["faq_patterns"], ctx["authority_entities"], brand_tokens,
                is_local, location, locale, ctx["gemini_client"], budget,
                ctx["service_zones"], ctx["profile_key"]
            )
        else:
            # T2 specific: use corrected anchors in prompt, avoiding those heavily used or rejected
            all_anchors = anchors.get("primary_anchors", [])
            # Priority: anchors not yet seen in exclusion context (if we could extract them)
            # Simplification: just skip the first few if they might be overused
            top_anchors = [a for a in all_anchors if a.lower().strip() not in exclusion_context][:3]
            if not top_anchors:
                top_anchors = list(all_anchors)[:3]

            new_candidates = _build_contextual_queries(
                ctx["topic_gaps"], brand_tokens, is_local, location, locale,
                ctx["gemini_client"], budget, ctx["persona_templates"],
                ctx["service_zones"], ctx["profile_key"],
                domain_anchors=top_anchors
            )
    
    elif round_idx == 2:
        # ── Round 2: Corrected Entity Fallback ──
        # Use alternate anchors and templates, excluding already used/rejected patterns
        profile = ctx.get("business_profile", {})
        fallback_map = profile.get(f"{type_key}_fallback_templates", {})
        templates = fallback_map.get(locale, fallback_map.get("en", []))
        
        # Build base entity terms
        base_terms = _extract_entity_terms(ctx)
        
        # Identify viable anchors for {services} or {core_noun} overrides
        primary = [a for a in anchors.get("primary_anchors", []) if a.lower().strip() not in exclusion_context]
        secondary = [a for a in anchors.get("secondary_anchors", []) if a.lower().strip() not in exclusion_context]
        candidate_services = primary if primary else secondary
        
        # If all anchors were excluded, relax the filter but log it
        if not candidate_services:
            candidate_services = anchors.get("primary_anchors", [])[:5]

        for t in templates:
            if "{" in t:
                # Iterate over anchors to create variety
                for svc in candidate_services[:5]:
                    current_terms = base_terms.copy()
                    current_terms["services"] = svc
                    if "{core_noun}" in t: current_terms["core_noun"] = svc
                    
                    filled = _fill_profile_fallback_template(t, current_terms)
                    if filled:
                        q_norm = _normalize_query_text_for_dedupe(filled)
                        if q_norm not in exclusion_context:
                            new_candidates.append({
                                "query": filled,
                                "source": "entity_fallback",
                                "raw_template": t
                            })
            elif _profile_consistency_guard(ctx["profile_key"], anchors):
                q_text = _inject_geo_context(t, location, locale) if is_local else t
                q_norm = _normalize_query_text_for_dedupe(q_text)
                if q_norm not in exclusion_context:
                    new_candidates.append({
                        "query": q_text,
                        "source": "profile_fallback",
                        "raw_template": t
                    })

    # Final pre-return filter
    final_filtered = []
    seen_in_batch = set()
    for c in new_candidates:
        q_norm = _normalize_query_text_for_dedupe(c.get("query", ""))
        if q_norm and q_norm not in exclusion_context and q_norm not in seen_in_batch:
            final_filtered.append(c)
            seen_in_batch.add(q_norm)
            
    return final_filtered


def _classify_intent_family(q_text: str, locale: str = "en", city: str = "") -> str:
    """
    Heuristic-based intent family classification.
    """
    q = q_text.lower()
    
    # Intent keywords (localized)
    keywords = {
        "best-of": ["best", "top", "leading", "miglior", "più", "eccellenza"],
        "local": ["near", "nearby", "vicino", " a ", " in ", "zona"],
        "trust": ["trusted", "expert", "review", "rating", "reliable", "affidabile", "opinioni", "esperto", "recensioni"],
        "action": ["book", "appointment", "contact", "pricing", "cost", "prenota", "appuntamento", "contatta", "costo", "prezzo", "visita"]
    }
    
    if any(w in q for w in keywords["best-of"]): return "best-of"
    if city and city.lower() in q: return "local"
    if any(w in q for w in keywords["local"]): return "local"
    if any(w in q for w in keywords["trust"]): return "trust"
    if any(w in q for w in keywords["action"]): return "action"
    
    return "category"

class TierSlotBuilder:
    """
    State manager for fixed-target tier query construction.
    """
    def __init__(self, target_count: int, tier_key: str):
        self.target_count = target_count
        self.tier_key = tier_key
        self.slots = [] # list of query objects
        self.accepted_norms = set()
        self.rejection_log = [] # {query, reason, wave}
        self.intent_coverage = {"best-of": 0, "local": 0, "trust": 0, "action": 0, "category": 0}
        self.waves_used = 0
        self.model_calls = 0
        self.quality_tradeoffs = 0
        self.trace = []
        
    def add_trace(self, message: str):
        self.trace.append(message)
        console.print(f"      [cyan]SLOT_BUILDER[/cyan] {message}")

    def is_full(self) -> bool:
        return len(self.slots) >= self.target_count

    def has_norm(self, norm_text: str) -> bool:
        return norm_text in self.accepted_norms

    def get_diversity_score(self) -> float:
        occupied = sum(1 for v in self.intent_coverage.values() if v > 0)
        return round(occupied / 5.0, 2)

def _get_jaccard_similarity(s1: str, s2: str) -> float:
    """
    Word-level Jaccard similarity for near-duplicate detection.
    """
    t1 = set(re.findall(r'\w+', s1.lower()))
    t2 = set(re.findall(r'\w+', s2.lower()))
    if not t1 or not t2: return 0.0
    return len(t1 & t2) / len(t1 | t2)

def _evaluate_query_contract(
    q_text: str, 
    builder: TierSlotBuilder, 
    exclusion_set: set,
    intent_family: str,
    family_preference_delta: int = 10
) -> tuple[bool, str]:
    """
    Enforces the acceptance contract for a slot candidate.
    Returns (accepted, reason).
    """
    if not q_text: return False, "Empty query text."
    
    # 1. Normalization Uniqueness
    q_norm = _normalize_query_text_for_dedupe(q_text)
    if builder.has_norm(q_norm) or q_norm in exclusion_set:
        return False, "Duplicate pattern (Normalized)."

    # 2. Grammar & Generic Sanity
    if len(q_text.split()) < 2: return False, "Too short (Generic)."
    if re.search(r'[\,\.\?\!]{2,}', q_text): return False, "Malformed punctuation."
    
    # 3. Semantic Near-Dedupe (Within the same family)
    for accepted_q in builder.slots:
        if accepted_q.get("intent_family") == intent_family:
            sim = _get_jaccard_similarity(q_text, accepted_q["query"])
            if sim > 0.7:
                return False, f"Semantic Redundancy ({sim:.2f}) in family {intent_family}."

    return True, "Passed contract."

def _wave_4_rescue_deterministic(builder: TierSlotBuilder, anchors: dict, location: str, locale: str) -> list[str]:
    """
    Wave 4: Deterministic Permutation Engine.
    Combines validated anchors with missing intent family keywords.
    """
    candidates = []
    primary = list(anchors.get("primary_anchors", []))[:3]
    if not primary: return []
    
    city = location.split(",")[0].strip() if location else ""
    
    # Map families to deterministic head/tail modifiers
    MODS = {
        "best-of": {"en": ["best", "top", "leading"], "it": ["miglior", "migliori", "eccellenza"]},
        "trust": {"en": ["reviews", "trusted", "expert"], "it": ["recensioni", "opinioni", "esperto", "affidabile"]},
        "action": {"en": ["book", "appointment", "contact"], "it": ["prenota", "appuntamento", "contatta", "visita"]},
        "local": {"en": ["near me", "in " + city], "it": ["vicino a me", "a " + city]},
    }
    
    missing = [f for f, count in builder.intent_coverage.items() if count == 0 and f != "category"]
    
    for fam in missing:
        mods = MODS.get(fam, {}).get(locale, MODS.get(fam, {}).get("en", []))
        for m in mods:
            for a in primary:
                if fam == "best-of":
                    candidates.append(f"{m} {a}")
                elif fam == "local":
                    candidates.append(f"{a} {m}")
                else:
                    candidates.append(f"{a} {m}")
                    candidates.append(f"{m} {a}")
                    
    return candidates

def _wave_4_rescue_micro_gen(builder: TierSlotBuilder, state: dict, gemini_client, locale: str, tier_key: str) -> list[str]:
    """
    Wave 4: Constrained Micro-Generation.
    Final last-mile LLM call for specific missing slots.
    """
    if not gemini_client: return []
    
    missing_count = builder.target_count - len(builder.slots)
    if missing_count <= 0: return []

    lang = "Italian" if locale == "it" else "English"
    profile = state.get("business_profile_key", "unknown")
    exclusions = list(builder.accepted_norms)[:10]
    
    prompt = f"""
    Generate EXACTLY {missing_count} highly specific search queries for the {profile} industry in {lang}.
    RULES:
    1. DO NOT use these patterns: {json.dumps(exclusions)}
    2. Focus on varied intent: technical, trust-based, or service-specific.
    3. Output ONLY a raw JSON array of strings: ["q1", "q2", ...]
    4. Quality check: No brand names, no generic fluff.
    """
    try:
        res = gemini_client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        qs = json.loads(res.text)
        if isinstance(qs, dict): qs = qs.get("queries", [])
        return [q for q in qs if isinstance(q, str)]
    except Exception:
        return []

def _assemble_tier_queries(
    tier_key: str,
    model_queries: list,
    state: dict,
    brand_tokens: set,
    is_local: bool,
    location: str,
    loc_conf: str,
    locale: str,
    anchors_raw: dict,
    gemini_client = None,
    regen_context: dict = None
) -> tuple[list, dict]:
    """
    Fixed-target, 4-wave slot-filling tier construction.
    """
    policy = TIER_POLICY.get(tier_key, {"target": 5})
    target_k = policy.get("target", 5)
    builder = TierSlotBuilder(target_k, tier_key)
    builder.add_trace(f"Starting {tier_key} assembly. Target: {target_k}")

    city = location.split(",")[0].strip() if location else ""
    exclusion_context = set() 
    
    # ── WAVE 1: MODEL OVERSAMPLE ──────────────────────────────────────────────
    builder.waves_used = 1
    builder.model_calls += 1
    
    scored_candidates = []
    for q_obj in model_queries:
        q_text = q_obj.get("query", "")
        if not q_text: continue
        family = _classify_intent_family(q_text, locale, city)
        score = _score_query_candidate(q_text, tier_key, state, "model", anchors_raw)
        scored_candidates.append({**q_obj, "score": score, "family": family})
    
    scored_candidates.sort(key=lambda x: x["score"], reverse=True)
    
    for c in scored_candidates:
        if builder.is_full(): break
        q_text, family, score = c["query"], c["family"], c["score"]
        ok, reason = _evaluate_query_contract(q_text, builder, exclusion_context, family)
        
        if ok and score >= QUALITY_THRESHOLDS["model"]:
            # Soft Diversity Rule (Preference Delta = 10)
            if builder.intent_coverage[family] > 0:
                for alt_c in scored_candidates[scored_candidates.index(c)+1:]:
                    if alt_c["score"] < (score - 10): break
                    alt_ok, _ = _evaluate_query_contract(alt_c["query"], builder, exclusion_context, alt_c["family"])
                    if alt_ok and builder.intent_coverage[alt_c["family"]] == 0:
                        c = alt_c; q_text, family, score = c["query"], c["family"], c["score"]
                        builder.quality_tradeoffs += 1
                        builder.add_trace(f"Diversity: Picked {family} ({score}) over redundant best-of.")
                        break
            
            ok, reason = _evaluate_query_contract(q_text, builder, exclusion_context, family)
            if ok:
                norm_q = _normalize_query_candidate(c, tier_key)
                norm_q.update({"quality_score": round(score, 1), "intent_family": family})
                builder.slots.append(norm_q)
                builder.accepted_norms.add(_normalize_query_text_for_dedupe(q_text))
                builder.intent_coverage[family] += 1
                builder.add_trace(f"Accepted [{family}] {q_text[:40]}...")

    # ── WAVE 2: TARGETED REGENERATION ──────────────────────────────────────────
    if not builder.is_full() and regen_context:
        builder.waves_used = 2
        builder.model_calls += 1
        builder.add_trace(f"WAVE 2: Regeneration ({len(builder.slots)}/{target_k})")
        regen_qs = _regenerate_tier_candidates(tier_key, 1, regen_context, brand_tokens, is_local, location, locale, anchors_raw, builder.accepted_norms)
        for q_obj in regen_qs:
            if builder.is_full(): break
            q_text, family = q_obj["query"], _classify_intent_family(q_obj["query"], locale, city)
            score = _score_query_candidate(q_text, tier_key, state, q_obj["source"], anchors_raw)
            ok, reason = _evaluate_query_contract(q_text, builder, exclusion_context, family)
            if ok and score >= 60:
                norm_q = _normalize_query_candidate(q_obj, tier_key); norm_q.update({"quality_score": round(score, 1), "intent_family": family})
                builder.slots.append(norm_q); builder.accepted_norms.add(_normalize_query_text_for_dedupe(q_text)); builder.intent_coverage[family] += 1
                builder.add_trace(f"Accepted [regen] {q_text[:40]}...")

    # ── WAVE 3: STRUCTURED FALLBACK ────────────────────────────────────────────
    if not builder.is_full():
        builder.waves_used = 3
        builder.add_trace(f"WAVE 3: Fallback ({len(builder.slots)}/{target_k})")
        type_key = "blind" if tier_key == "blind_discovery" else "contextual"
        profile = state.get("business_profile", {}); templates = profile.get(f"{type_key}_fallback_templates", {}).get(locale, profile.get(f"{type_key}_fallback_templates", {}).get("en", []))
        entity_terms = _extract_entity_terms(state); profile_consistent = _profile_consistency_guard(state.get("business_profile_key"), anchors_raw)
        for t in templates:
            if builder.is_full(): break
            q_text = _fill_profile_fallback_template(t, entity_terms) if "{" in t and entity_terms else (_inject_geo_context(t, location, locale) if "{" not in t and profile_consistent and is_local else (t if "{" not in t and profile_consistent else None))
            if not q_text: continue
            family = _classify_intent_family(q_text, locale, city); score = _score_query_candidate(q_text, tier_key, state, "fallback", anchors_raw)
            ok, reason = _evaluate_query_contract(q_text, builder, exclusion_context, family)
            if ok and score >= 60:
                norm_fb = _normalize_query_candidate({"query": q_text, "source": "fallback"}, tier_key); norm_fb.update({"quality_score": round(score, 1), "intent_family": family})
                builder.slots.append(norm_fb); builder.accepted_norms.add(_normalize_query_text_for_dedupe(q_text)); builder.intent_coverage[family] += 1
                builder.add_trace(f"Accepted [fallback] {q_text[:40]}...")

    # ── WAVE 4: STRICT RESCUE ──────────────────────────────────────────────────
    if not builder.is_full():
        builder.waves_used = 4
        builder.add_trace("WAVE 4: Strict Rescue Layer.")
        rescue_det = _wave_4_rescue_deterministic(builder, anchors_raw, location, locale)
        for q_text in rescue_det:
            if builder.is_full(): break
            family = _classify_intent_family(q_text, locale, city); score = _score_query_candidate(q_text, tier_key, state, "rescue_deterministic", anchors_raw)
            ok, reason = _evaluate_query_contract(q_text, builder, exclusion_context, family)
            if ok and score >= 60:
                norm_res = _normalize_query_candidate({"query": q_text, "source": "rescue_deterministic"}, tier_key); norm_res.update({"quality_score": round(score, 1), "intent_family": family})
                builder.slots.append(norm_res); builder.accepted_norms.add(_normalize_query_text_for_dedupe(q_text)); builder.intent_coverage[family] += 1
                builder.add_trace(f"Accepted [rescue_det] {q_text[:40]}...")
        
        if not builder.is_full() and gemini_client and builder.model_calls < 2:
            builder.model_calls += 1
            micro_qs = _wave_4_rescue_micro_gen(builder, state, gemini_client, locale, tier_key)
            for q_text in micro_qs:
                if builder.is_full(): break
                family = _classify_intent_family(q_text, locale, city); score = _score_query_candidate(q_text, tier_key, state, "rescue_micro", anchors_raw)
                ok, reason = _evaluate_query_contract(q_text, builder, exclusion_context, family)
                if ok and score >= 55:
                    norm_mic = _normalize_query_candidate({"query": q_text, "source": "rescue_micro"}, tier_key); norm_mic.update({"quality_score": round(score, 1), "intent_family": family})
                    builder.slots.append(norm_mic); builder.accepted_norms.add(_normalize_query_text_for_dedupe(q_text)); builder.intent_coverage[family] += 1
                    builder.add_trace(f"Accepted [rescue_micro] {q_text[:40]}...")

    # ── FINAL METRICS ─────────────────────────────────────────────────────────
    exact_match = builder.is_full()
    accepted_list = builder.slots
    q_scores = [s.get("quality_score", 0) for s in accepted_list]
    avg_q = sum(q_scores) / len(q_scores) if q_scores else 0.0
    rel_pts = 0; REL_MAP = {"model": 1.0, "regen": 0.9, "entity_fallback": 0.7, "profile_fallback": 0.5, "rescue_deterministic": 0.6, "rescue_micro": 0.4}
    for s in accepted_list: rel_pts += REL_MAP.get(s.get("source"), 0.5)
    reliability = round(rel_pts / max(builder.target_count, 1), 2)
    
    return accepted_list, {
        "query_quality_avg": round(avg_q, 1),
        "exact_target_achieved": exact_match,
        "generation_degraded": not exact_match,
        "empty_slot_count": max(0, builder.target_count - len(accepted_list)),
        "rejected_query_count": len(builder.rejection_log),
        "waves_used": builder.waves_used,
        "model_calls": builder.model_calls,
        "intent_diversity_score": builder.get_diversity_score(),
        "intent_coverage": builder.intent_coverage,
        "quality_tradeoffs": builder.quality_tradeoffs,
        "slot_filling_trace": "\n".join(builder.trace),
        "tier_query_reliability": reliability,
        "rejection_summary": builder.rejection_log[:10]
    }


def _extract_required_query_anchors(state: dict) -> dict:
    """
    Extracts semantic anchors for query validation and hardening.
    Sources: target_business, target_industry, orchestrator entities, og_tags, content.
    """
    import unicodedata
    
    def normalize_str(s: str) -> str:
        s = s.lower().strip()
        s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
        return s

    anchors = {
        "primary_anchors": set(),
        "secondary_anchors": set(),
        "brand_anchors": set()
    }
    
    # ── 1. Identify Blacklisted Tokens (Locations & Modifiers) ──
    blacklist = {
        "italy", "italia", "page", "official", "site", "home", "welcome", "website",
        "compare", "comparison", "difference", "vs", "differenza", "differenze",
        "choose", "how", "which", "scegliere", "quale", "miglior", "best", "top",
        "price", "cost", "pricing", "prezzo", "costo", "prezzi",
        "quality", "standard", "certified", "certification", "qualita", "certificato",
        "delivery", "shipping", "consegna", "spedizione",
        "why", "benefits", "risks", "come", "perche", "rischi",
        "for", "usage", "per", "uso", "compatibility", "compatibilita",
        "process", "application", "procedura", "domanda", "richiesta",
        "con", "senza", "senza", "del", "della", "delle", "dei", "dal", "dalla", "da",
        "in", "su", "tra", "fra", "di", "a", "il", "lo", "la", "i", "gli", "le", "un", "una"
    }
    
    location_raw = state.get("discovered_location", "")
    if location_raw and location_raw.lower() not in ["none", "unknown", "worldwide", "national"]:
        for p in location_raw.replace(",", " ").split():
            if len(p) > 2:
                blacklist.add(normalize_str(p))

    # 1. Brand Anchors (for branded-leak detection, not acceptance)
    brand_name = str(state.get("brand_name", "")).lower()
    if brand_name and brand_name not in ["unknown", "none"]:
        anchors["brand_anchors"].add(normalize_str(brand_name))
        for part in brand_name.split():
            if len(part) > 2: anchors["brand_anchors"].add(normalize_str(part))
            
    # 2. Primary Anchors (Core business/category/industry)
    target_bus = str(state.get("target_business", "")).lower()
    target_ind = str(state.get("target_industry", "")).lower()
    
    if target_bus and target_bus not in ["unknown", "none"]:
        anchors["primary_anchors"].add(normalize_str(target_bus))
        for p in target_bus.replace("/", " ").replace("-", " ").split():
            if len(p) > 2: anchors["primary_anchors"].add(normalize_str(p))
            
    if target_ind and target_ind not in ["unknown", "none"]:
        anchors["primary_anchors"].add(normalize_str(target_ind))
        for p in target_ind.replace("/", " ").replace("-", " ").split():
            if len(p) > 2: anchors["primary_anchors"].add(normalize_str(p))
        # Heuristic: Supplier of X -> X
        if "supplier of" in target_ind:
            anchors["primary_anchors"].add(normalize_str(target_ind.split("supplier of")[-1].strip()))
            
    primary_ind = str(state.get("primary_industry", "")).lower()
    if primary_ind and primary_ind not in ["unknown", "none"]:
        anchors["primary_anchors"].add(normalize_str(primary_ind))

    # 3. Secondary Anchors (Specifics, subcategories, entities)
    raw_data = state.get("raw_data_complete", {})
    ents = raw_data.get("orchestrator_entities", [])
    if not ents: ents = state.get("grounding_entities", [])
    
    for e in ents:
        e_low = str(e).lower().strip()
        if e_low and len(e_low) > 2:
            # The full specific entity/service is strong enough to be a primary anchor
            anchors["primary_anchors"].add(normalize_str(e_low))
            # Individual words act as secondary anchors requiring a modifier
            for p in e_low.replace("/", " ").replace("-", " ").split():
                if len(p) > 3: anchors["secondary_anchors"].add(normalize_str(p))
            
    og_title = state.get("og_tags", {}).get("og:title", "").lower()
    if og_title:
        title_words = re.findall(r'\b\w{3,}\b', og_title)
        for w in title_words:
            # We will rely on the blacklist step at the end for stripping
            anchors["secondary_anchors"].add(normalize_str(w))
                
    # Clean up and normalize
    # Remove brand tokens, location tokens, and modifier tokens from domain anchors
    for b in list(anchors["brand_anchors"]) + list(blacklist):
        anchors["primary_anchors"].discard(b)
        anchors["secondary_anchors"].discard(b)
        
    return anchors

def _contains_required_anchor(query: str, anchors: dict) -> tuple[bool, list[str]]:
    """
    Agency Gate: Ensures T2 queries have domain-specific anchors.
    Rules:
    - Must contain at least one primary anchor
    - OR contain at least one secondary anchor plus a validated modifier.
    - Brand anchors alone do NOT count towards acceptance.
    Returns (is_valid, hits).
    """
    import unicodedata
    if not query or not anchors: return False, []
    
    # Normalization (accents)
    def normalize_str(s: str) -> str:
        s = s.lower().strip()
        return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

    from re import escape
    
    q_norm = normalize_str(query)
    
    # Mask full brand phrases from the evaluation string to prevent
    # sub-strings of the brand name (which might be generic industries)
    # from falsely triggering a domain hit on purely branded queries.
    q_eval = q_norm
    # Sort to replace longer multi-word brands first
    for b in sorted(anchors.get("brand_anchors", []), key=len, reverse=True):
        if len(b.split()) > 1 and b in q_eval:
            # We replace it with spaces so tokens aren't joined accidentally
            q_eval = q_eval.replace(b, " ")
            
    hits = []
    
    # Modifier lists for secondary anchor reinforcement
    modifiers = [
        "compare", "comparison", "difference", "vs", "differenza", "differenze",
        "choose", "how to choose", "which", "scegliere", "quale", "miglior", "migliori", "best", "top",
        "price", "cost", "pricing", "prezzo", "costo", "prezzi",
        "quality", "standard", "certified", "certification", "qualita", "certificato",
        "delivery", "shipping", "consegna", "spedizione",
        "how", "why", "benefits", "risks", "come", "perche", "rischi",
        "for", "usage", "per", "compatibility", "compatibilita", "cura", "trattamento",
        "process", "application", "procedura", "domanda", "richiesta", "opinioni"
    ]

    def _has_token(text, token):
        # Handle plurals/singulars dynamically for italian (o/i, a/e, e/i) & english (s, es)
        tok_norm = normalize_str(token)
        patterns = [rf'\b{escape(tok_norm)}\b']
        
        # Plural heuristics
        if len(tok_norm) > 3:
            if tok_norm.endswith("o"): patterns.append(rf'\b{escape(tok_norm[:-1])}i\b')
            elif tok_norm.endswith("a"): patterns.append(rf'\b{escape(tok_norm[:-1])}e\b')
            elif tok_norm.endswith("e"): patterns.append(rf'\b{escape(tok_norm[:-1])}i\b')
            elif not tok_norm.endswith("s"): patterns.append(rf'\b{escape(tok_norm)}s\b')
            elif tok_norm.endswith("s"): patterns.append(rf'\b{escape(tok_norm[:-1])}\b')
            
        return any(re.search(p, text) for p in patterns)

    # 1. Primary Anchor Hit
    primary_hit = False
    for a in anchors.get("primary_anchors", []):
        if _has_token(q_eval, a):
            hits.append(f"primary:{a}")
            primary_hit = True
            
    if primary_hit:
        return True, hits
            
    # 2. Secondary Anchor + Modifier Hit
    secondary_hits = []
    for a in anchors.get("secondary_anchors", []):
        if _has_token(q_eval, a):
            secondary_hits.append(a)
            
    if secondary_hits:
        for m in modifiers:
            if _has_token(q_eval, m):
                hits.extend([f"secondary:{a}" for a in secondary_hits])
                hits.append(f"modifier:{m}")
                return True, hits
                
    return False, []

def _sanitize_or_reject_query(
    query: str, 
    tier: str, 
    brand_tokens: set[str], 
    is_local: bool, 
    location: str, 
    locale: str,
    industry: str = "business",
    location_confidence: str = "high"
) -> dict:
    """
    Agency Gate: Rejects or cleans queries based on tier-integrity and realism rules.
    Returns provenance analytics dict.
    """
    result = {
        "final_query": None,
        "raw_query": query,
        "sanitized_from": None,
        "location_applied": None,
        "rejection_code": None,
        "rejection_reason": None,
    }

    if not query or len(query.strip()) < 5:
        result["rejection_code"] = "REJECT_TOO_SHORT"
        result["rejection_reason"] = "Query is empty or functionally too short."
        return result

    q = query.strip()

    # REALISM GATE: Reject taxonomical/awkward phrasing
    if not _is_realistic_query(q, industry):
        result["rejection_code"] = "REJECT_MALFORMED_CONNECTOR"
        result["rejection_reason"] = "FAILED REALISM GATE: Taxonomical or awkward phrasing."
        return result

    # Tier 1 & 2: Brand Leakage check
    if tier in ["blind_discovery", "contextual_discovery"]:
        if _query_has_brand_leakage(q, brand_tokens):
            if tier == "blind_discovery":
                result["rejection_code"] = "REJECT_BRAND_LEAKAGE"
                result["rejection_reason"] = f"T1 HARD BAN: Brand leakage detected ({q})"
                return result
            else:
                # For T2, try one quick scrub if possible, otherwise reject
                orig_before_scrub = q
                for token in brand_tokens:
                    q = re.sub(rf'\b{re.escape(token)}\b', '', q, flags=re.IGNORECASE).strip()
                
                if _query_has_brand_leakage(q, brand_tokens) or len(q) < 10:
                    result["rejection_code"] = "REJECT_BRAND_LEAKAGE"
                    result["rejection_reason"] = "T2 Leakage: Scrub failed or corrupted context."
                    return result
                
                result["sanitized_from"] = orig_before_scrub

    # Local Enforcement (CRITICAL PATCH)
    # Only inject geo if is_local AND we have corroborated confidence (high/medium)
    if is_local and location_confidence in ["high", "medium"]:
        city = location.split(",")[0].strip() if location else ""
        if city and city.lower() in q.lower():
            result["location_applied"] = city

        orig_before_geo = q
        q = _inject_geo_context(q, location, locale)
        if q != orig_before_geo:
            result["location_applied"] = city
            # If we hadn't sanitized yet, do it now
            if not result["sanitized_from"]:
                result["sanitized_from"] = orig_before_geo
    
    result["final_query"] = q
    return result

def _build_blind_queries(
    faq_patterns: list, 
    authority_entities: list, 
    brand_tokens: set[str],
    is_local: bool,
    location: str,
    locale: str,
    gemini_client,
    budget: int = 8,
    service_zones: list = None,
    profile_key: str = "unknown"
) -> list:
    """
    Tier 1: Blind Discovery.
    Uses LLM to convert patterns into brand-neutral category-intent queries.
    """
    lang_name = "Italian" if locale == "it" else "English"
    seeds = _safe_stringify_list(faq_patterns + authority_entities)[:10]
    
    queries = []
    if seeds:
        try:
            geo_rule = "\n            6. GEOGRAPHIC ENFORCEMENT: If 'service_zones' are provided, append ONE of the zones naturally (e.g., 'a Milano'). Otherwise, do not append any macro country location."

            geo_isolation = ""
            if service_zones:
                geo_isolation = f"\n            7. GEOGRAPHIC MUTUAL EXCLUSION RULE:\n            You have a Macro Location ({location}) and Micro Zones ({service_zones}).\n            - When generating a local query, you MUST pick exactly ONE Micro Zone (e.g., 'Milano'). \n            - IF you use a Micro Zone, you MUST NOT append the Macro Location. \n            - Example GOOD: 'miglior medico generico a Milano'\n            - Example BAD: 'miglior medico generico a Milano Italy'\n            - All queries must be in flawless, conversational {lang_name}. NEVER append 'Italy' to an Italian local query."

            lang_rule = f"Language: {lang_name}. All queries must be written in perfectly natural, conversational {lang_name} (or the target locale). DO NOT append the English word 'Italy' to an Italian query. Phrasing must exactly mimic how a real human types into Google or Perplexity."

            prompt = f"""
            Convert these market identifiers/FAQs into {budget} truly BLIND discovery queries.
            RULES:
            1. NO brand names, NO domain tokens, NO company specific IDs.
            2. Focus on "best [category]", "how to [problem]", "[category] platform".
            3. Must sound like a real user who DOES NOT know the brand exists.
            4. {lang_rule}
            5. Output ONLY a JSON array of strings: ["query1", "query2", ...]{geo_rule}{geo_isolation}
            8. LOCATION FORMAT: When appending geographical locations to queries, use natural local prepositions (e.g., 'in [Location]', 'near [Location]', 'a [Location]' for Italian). NEVER output raw concatenated comma-separated strings like 'a , Italia Roma'.
            
            CRITICAL QUERY GENERATION RULES:
            1. ORGANIC LOCALE ENFORCEMENT: All generated queries MUST be written entirely in the target locale ({locale}). Do NOT mix languages.
            2. ORGANIC LOCATION INTEGRATION: Do not naively append the location at the end of the string. Weave the location ({location}) into the query exactly as a human user would type it in that language (e.g., 'dentista urgente milano', NOT 'dentista urgente Multiple locations').
            3. INTENT ANCHORING: Strictly align the search queries with the actual service/product provided by the business profile ({profile_key}). If the business is a local service (like a clinic or restaurant), DO NOT generate queries looking for B2B software, SaaS, or management platforms.
            4. STRICT GEO-FENCING: You will be provided with a 'service_zones' array {service_zones}. If this array is populated, you MUST ONLY generate localized queries for the exact cities in that list. DO NOT invent or add other major cities (e.g., if the list is [Milano, Roma], absolutely do NOT generate queries for Napoli, Torino, or Palermo).
            
            Seeds: {json.dumps(seeds)}
            """
            res = gemini_client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=prompt,
                config={"response_mime_type": "application/json"}
            )
            raw_qs = json.loads(res.text)
            if isinstance(raw_qs, dict): raw_qs = raw_qs.get("queries", [])
            
            for q in raw_qs:
                if not isinstance(q, str): continue
                san_res = _sanitize_or_reject_query(q, "blind_discovery", brand_tokens, is_local, location, locale, industry="market")
                if san_res["final_query"]:
                    queries.append({
                        "query": san_res["final_query"],
                        "tier": "blind_discovery",
                        "points": 25,
                        "source": "model",
                        "raw_query": q,
                        "profile_key": profile_key,
                        "location_applied": san_res["location_applied"],
                        "sanitized_from": san_res["sanitized_from"]
                    })
        except Exception as e:
            console.print(f"      [yellow]T1 Gen Error: {e}[/yellow]")

    return queries

def _build_contextual_queries(
    topic_gaps: list,
    brand_tokens: set[str],
    is_local: bool,
    location: str,
    locale: str,
    gemini_client,
    budget: int = 10,
    persona_templates: list = None,
    service_zones: list = None,
    profile_key: str = "unknown",
    domain_anchors: list = None
) -> list:
    """
    Tier 2: Contextual Discovery.
    Strictly neutral queries derived from topic gaps.
    """
    lang_name = "Italian" if locale == "it" else "English"
    if not topic_gaps: return []

    gaps = [str(g).strip() for g in topic_gaps[:budget] if g]
    
    try:
        persona_context = ""
        if persona_templates:
            pt_list = [f"{p.get('persona', '')} ({p.get('intent', '')})" for p in persona_templates if p.get('persona')]
            persona_context = f"\nPersonas: {', '.join(pt_list)}."

        anchor_guidance = ""
        if domain_anchors:
            anchor_guidance = f"\nGUIDANCE: Use the following domain terms where appropriate for better context: {', '.join(domain_anchors)}."

        geo_rule = "\n        6. GEOGRAPHIC ENFORCEMENT: If 'service_zones' are provided, append ONE of the zones naturally (e.g., 'a Milano'). Otherwise, do not append any macro country location."

        geo_isolation = ""
        if service_zones:
            geo_isolation = f"\n        7. GEOGRAPHIC MUTUAL EXCLUSION RULE:\n        You have a Macro Location ({location}) and Micro Zones ({service_zones}).\n        - When generating a local query, you MUST pick exactly ONE Micro Zone (e.g., 'Milano'). \n        - IF you use a Micro Zone, you MUST NOT append the Macro Location. \n        - Example GOOD: 'miglior medico generico a Milano'\n        - Example BAD: 'miglior medico generico a Milano Italy'\n        - All queries must be in flawless, conversational {lang_name}. NEVER append 'Italy' to an Italian local query."

        lang_rule = f"Language: {lang_name}. All queries must be written in perfectly natural, conversational {lang_name} (or the target locale). DO NOT append the English word 'Italy' to an Italian query. Phrasing must exactly mimic how a real human types into Google or Perplexity."

        prompt = f"""
        Convert these content gaps into {len(gaps)} brand-neutral search queries for the {profile_key} profile.
        
        STRICT RULES:
        1. NO BRAND NAMES: Do not mention the audited brand ({brand_tokens}).
        2. ANCHORING: Every query MUST contain at least one concrete business, category, product, or service noun.{anchor_guidance}
        3. NO GENERIC FLUFF: Avoid vague commerce wording like "where to buy online" without a domain anchor.
        4. INTENT: Prioritize user-centric education, comparison, and consideration intents (e.g., 'how to choose [Category]', '[Category] reviews', 'best [Category] for [Use Case]').
        5. REALISM: Phrasing must exactly mimic how a real human types into a search engine. No academic or investor phrasing.
        6. {lang_rule}{persona_context}
        7. Output ONLY JSON: {{"questions": ["...", "..."]}}{geo_rule}{geo_isolation}
        8. LOCATION FORMAT: Use natural prepositions (e.g., 'in [Location]', 'near [Location]').
        
        Gaps: {json.dumps(gaps)}
        """
        res = gemini_client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        questions = json.loads(res.text).get("questions", [])
        
        queries = []
        for q in questions:
            if not isinstance(q, str): continue
            san_res = _sanitize_or_reject_query(q, "contextual_discovery", brand_tokens, is_local, location, locale)
            
            # Note: Anchor validation happens in the main process() loop for T2
            if san_res["final_query"]:
                queries.append({
                    "query": san_res["final_query"],
                    "tier": "contextual_discovery",
                    "points": 15,
                    "source": "model",
                    "raw_query": q,
                    "profile_key": profile_key,
                    "location_applied": san_res["location_applied"],
                    "sanitized_from": san_res["sanitized_from"]
                })
        return queries
    except Exception as e:
        console.print(f"      [yellow]T2 Gen Error: {e}[/yellow]")
        return []


def _build_branded_queries(
    brand_name: str,
    target_industry: str,
    discovered_location: str,
    scale_level: str,
    locale: str,
    competitor_entities: list,
    budget: int = 6,
) -> list:
    """
    Tier 3: Branded Validation.
    Tests whether the brand is recognised by AI when searched directly.
    Expanded from 3 fixed queries to a configurable budget with 6 distinct query types.
    Points value: 20.
    """
    queries = []
    b = brand_name.strip()
    ind = target_industry.strip()
    loc = discovered_location.strip()
    
    # Self-comparison guard: deduplicate competitors who share the brand name
    import re
    import unicodedata
    def _norm(name): 
        n = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
        return re.sub(r'[^\w\s]', '', n.lower().strip())
    
    b_norm = _norm(b)
    valid_comps = []
    for c in competitor_entities:
        c_clean = c.strip()
        if _norm(c_clean) and _norm(c_clean) != b_norm and c_clean not in valid_comps:
            valid_comps.append(c_clean)
            
    top_comp = valid_comps[0] if valid_comps else ""
    sec_comp = valid_comps[1] if len(valid_comps) > 1 else ""

    is_local = scale_level == "Local" and loc and loc.lower() not in ["worldwide", "national"]

    if locale == "it":
        candidate_queries = [
            # Core brand recognition (always first)
            {"query": f"{b} opinioni",                                   "tier": "branded_validation", "points": 20},
            {"query": f"{b} {ind}",                                       "tier": "branded_validation", "points": 20},
            # Competitor comparison (high intent)
            {"query": f"{b} vs" + (f" {top_comp}" if top_comp else f" {ind}"), "tier": "branded_validation", "points": 20},
            # Use-case / problem-aware query
            {"query": f"come funziona {b}",                               "tier": "branded_validation", "points": 20},
            # Location signal (if local, otherwise national market probe)
            {"query": (f"{b} a {loc}" if is_local else f"{b} italia"),   "tier": "branded_validation", "points": 20},
            # Alternative/authority probe using second competitor
            {"query": f"alternativa a {sec_comp or top_comp or ind} per {ind}", "tier": "branded_validation", "points": 15},
        ]
    else:
        candidate_queries = [
            {"query": f"{b} reviews",                                    "tier": "branded_validation", "points": 20},
            {"query": f"what is {b}",                                     "tier": "branded_validation", "points": 20},
            {"query": f"{b} vs" + (f" {top_comp}" if top_comp else f" {ind} competitors"), "tier": "branded_validation", "points": 20},
            {"query": f"how does {b} work",                              "tier": "branded_validation", "points": 20},
            {"query": (f"{b} in {loc}" if is_local else f"{b} pricing"), "tier": "branded_validation", "points": 20},
            {"query": f"alternative to {sec_comp or top_comp or ind} for {ind}", "tier": "branded_validation", "points": 15},
        ]

    return candidate_queries[:budget]


def _brand_mentioned(brand_name: str, url: str, og_title: str, answer_text: str, gemini_client=None) -> bool:
    """
    Agency-Grade Semantic Brand Evaluation.
    First runs deterministic string signals. If those fail, runs a semantic LLM
    evaluator to determine if the brand is conceptually present.
    """
    from urllib.parse import urlparse as _urlparse
    ans = answer_text.lower()
    b = brand_name.lower().strip()

    # Fast Path 1: Exact brand name
    if b and b in ans:
        return True

    # Fast Path 2: Domain token
    try:
        domain_token = _urlparse(url).netloc.replace("www.", "").split(".")[0].lower()
        if domain_token and len(domain_token) > 2 and domain_token in ans:
            return True
    except Exception:
        pass

    # Fallback Path 3: Semantic LLM Evaluator
    if gemini_client:
        try:
            prompt = f"""
            You are a semantic evaluator.
            We asked an AI search engine a query. The engine returned:
            "{answer_text}"
            
            Is this answer recommending or prominently mentioning the brand "{brand_name}" (or its exact website {url})?
            Sometimes engines misspell the name, use an acronym, or reference its core proprietary product.
            
            Return ONLY "yes" if the brand/site is functionally represented as an answer, and "no" if it is entirely absent or represented as a negative.
            """
            def _sem_req():
                return gemini_client.models.generate_content(
                    model="gemini-2.5-flash-lite",
                    contents=prompt,
                    config={"response_mime_type": "text/plain"}
                )
            res = execute_with_backoff(_sem_req, max_retries=2, initial_delay=1.0)
            if "yes" in res.text.lower().strip()[:10]:
                return True
        except Exception:
            pass

    return False


def _run_stress_test(
    all_queries: list,
    serper_key: str,
    brand_name: str,
    url: str,
    og_title: str,
    gemini_client=None
) -> tuple:
    """
    Unified sequential Perplexity executor.
    Returns (visibility_points, total_possible, stress_test_log, tier_stats, diagnostics).
    """
    visibility_points = 0
    total_possible = sum(q.get("points", 0) for q in all_queries)
    log = []
    
    tier_stats = {
        "blind_discovery": {"queries": 0, "matches": 0, "pts": 0, "max": 0},
        "contextual_discovery": {"queries": 0, "matches": 0, "pts": 0, "max": 0},
        "branded_validation": {"queries": 0, "matches": 0, "pts": 0, "max": 0},
    }
    
    schema_repairs = 0

    for q_candidate in all_queries:
        tier = q_candidate.get("tier", "unknown")
        q_text = q_candidate.get("query", "")
        pts = q_candidate.get("points", 0)
        
        if not q_text or tier not in tier_stats:
            schema_repairs += 1
            console.print(f"      [yellow]SCHEMA_REPAIR[/yellow] Recovering defaults for malformed candidate: {str(q_text)[:30]}...")
            if not tier or tier == "unknown":
                tier = "branded_validation" # Safest default
            if pts <= 0:
                pts = 20 # T3 default
        
        tier_label = {
            "blind_discovery":     "T1-Blind",
            "contextual_discovery": "T2-Context",
            "branded_validation":  "T3-Brand",
        }.get(tier, tier)

        console.print(f"      [blue]DEEP_DISCOVERY[/blue][{tier_label}] {q_text} ...", end="")
        
        matched = False
        try:
            # ACTIVE PATH: Serper + Gemini 2.5 Flash Lite
            def _search_and_reason():
                # 1. Retrieval
                s_url = "https://google.serper.dev/search"
                s_payload = json.dumps({"q": q_text, "num": 10})
                s_headers = {'X-API-KEY': serper_key, 'Content-Type': 'application/json'}
                s_res = requests.post(s_url, headers=s_headers, data=s_payload, timeout=15)
                s_res.raise_for_status()
                s_data = s_res.json().get("organic", [])
                
                context = json.dumps([{"title": r.get("title"), "snippet": r.get("snippet")} for r in s_data[:8]])
                
                # 2. Reasoning
                prompt = f"Based on these search results for the query '{q_text}', summarize the top recommendations and specific brands/solutions mentioned in the snippets. Provide a direct, factual answer.\n\nSearch context:\n{context}"
                
                res = gemini_client.models.generate_content(
                    model="gemini-2.5-flash-lite",
                    contents=prompt
                )
                return res.text
            
            answer = execute_with_backoff(_search_and_reason, max_retries=3, initial_delay=3.0)
            
            matched = _brand_mentioned(brand_name, url, og_title, answer, gemini_client)
            
            # Increment stats
            tier_stats[tier]["queries"] += 1
            tier_stats[tier]["max"] += pts
            
            earned = 0
            if matched:
                visibility_points += pts
                tier_stats[tier]["matches"] += 1
                tier_stats[tier]["pts"] += pts
                earned = pts
                console.print(" [green]MATCH[/green]")
            else:
                console.print(" [red]NO_MATCH[/red]")

        except Exception as qe:
            console.print(f"    [yellow]Query error: {qe}[/yellow]")
            # Still increment query count for stats even if it failed
            tier_stats[tier]["queries"] += 1
            tier_stats[tier]["max"] += pts
            earned = 0

        # Normalize and log result
        result_obj = {
            "query": q_text,
            "tier": tier,
            "matched": matched,
            "points": earned,
            "max_pts": pts,
            "source": q_candidate.get("source", "unknown"),
        }
        
        norm_result = _normalize_stress_test_result(result_obj, q_candidate)
        log.append(norm_result)

    # Carry repair count for diagnostics
    diagnostics = {
        "schema_repairs": schema_repairs,
        "schema_warnings": 1 if schema_repairs > 0 else 0
    }

    return visibility_points, total_possible, log, tier_stats, diagnostics

def process(state: dict) -> dict:
    console.print("[cyan]Researcher Node[/cyan]: Starting deep v4.5 Agency-Grade analysis...")
    
    # Defaults and status
    metrics = {
        "Defensible Evidence Depth": 0,
        "Entity Consensus": 0,
        "Hallucination Risk": 100,
        "Citation Readiness": "Needs Work"
    }
    citation_status = "Low Verification"
    projected_traffic_lift = "0%"
    geo_recommendation_pack = "Unavailable"
    research_output = "Research failed or skipped."

    url = state.get("url", "Unknown")
    locale = state.get("locale", "en")
    target_industry = state.get("target_industry", "Unknown")
    brand_name = state.get("brand_name", "Unknown")
    scale_level = state.get("scale_level", "National")
    business_type = state.get("business_type", "tech")
    discovered_location = _sanitize_location(state.get("discovered_location", "")) or "Worldwide"
    service_zones = state.get("service_zones", [])
    lang_name = "Italian" if locale == "it" else "English"
    
    # ── Audit Integrity Gating v4.5 ─────────────────────────────────────────────
    integrity_status = state.get("audit_integrity_status", "valid")
    integ_reasons = state.get("audit_integrity_reasons", [])
    
    # v4.4 Brand Authority Signals
    schema_type_counts = state.get("schema_type_counts", {})
    hreflang_count = state.get("hreflang_count", 0)
    
    authority_signals = []
    auth_score = 0
    if hreflang_count > 3:
        authority_signals.append(f"{hreflang_count} hreflang international targets")
        auth_score += 2
    
    if "Organization" in schema_type_counts or "WebSite" in schema_type_counts:
        authority_signals.append("Strong Organization/WebSite Schema Identity")
        auth_score += 2
        
    if "Product" in schema_type_counts or "Offer" in schema_type_counts or "AggregateOffer" in schema_type_counts:
        authority_signals.append("Commerce Schema Present")
        auth_score += 1
        
    if scale_level == "Global":
        authority_signals.append("Self-identified Global Market Scale")
        auth_score += 1
        
    authority_strength = "high" if auth_score >= 4 else ("medium" if auth_score >= 2 else "low")
    is_global_brand = (authority_strength == "high") and (hreflang_count > 0 or scale_level == "Global")
    
    brand_authority_signals = {
        "is_global_brand": is_global_brand,
        "authority_strength": authority_strength,
        "score": auth_score,
        "signals": authority_signals
    }
    state["brand_authority_signals"] = brand_authority_signals

    # v4.5 Rigorous Confidence Score
    extraction_warnings = state.get("extraction_warnings", [])
    depth = state.get("client_content_depth", {})
    quality = depth.get("extraction_quality", "low") if isinstance(depth, dict) else "low"
    words = depth.get("word_count", 0) if isinstance(depth, dict) else 0
    pages = depth.get("page_count", 1) if isinstance(depth, dict) else 1

    js_fallback = state.get("js_fallback_used", False)
    robots = state.get("robots_txt_status", "not_found")
    ext_quality = state.get("external_data_quality", "high")

    # ── SCORE A: Extraction Integrity ──────────────────────────────────────────
    ei_score = 100
    ei_score -= (len(extraction_warnings) * 10)
    if quality == "low":
        ei_score -= 15
    if js_fallback and quality != "high":
        ei_score -= 25    # fallback triggered but content still thin
    if robots == "restricted":
        ei_score -= 20    # AI engines can't legally crawl this site
    if pages >= 5 and words > 1000:
        ei_score = max(50, ei_score)   # floor: critical mass of text was extracted
    extraction_integrity = max(10, min(100, ei_score))

    # ── SCORE B: Context Reliability ──────────────────────────────────────────
    cr_score = 100
    s_counts_cr = state.get("schema_type_counts", {})
    generic_only = all(k in {"WebSite", "WebPage", "BreadcrumbList", "SiteLinksSearchBox"} for k in s_counts_cr)
    if not s_counts_cr:
        cr_score -= 20
    elif generic_only:
        cr_score -= 10
    
    og_title_cr = state.get("og_tags", {}).get("og:title", "")
    templated_signals = ["home", "welcome", "untitled", "sample", "default", "page"]
    if og_title_cr and any(sig in og_title_cr.lower() for sig in templated_signals):
        cr_score -= 15

    class_notes = state.get("classification_notes", "")
    if class_notes and "confirmed" not in class_notes.lower():
        cr_score -= 10

    if ext_quality == "low":
        cr_score -= 30
    elif ext_quality == "medium":
        cr_score -= 10

    context_reliability = max(10, min(100, cr_score))

    # ── BLENDED FINAL SCORE + INTEGRITY CAP v4.5 ──────────────────────────────
    raw_confidence = int(0.6 * extraction_integrity + 0.4 * context_reliability)
    
    confidence_integrity_cap = 100
    if integrity_status == "invalid":
        confidence_integrity_cap = 25
    elif integrity_status == "degraded":
        confidence_integrity_cap = 60
        
    confidence_score = min(raw_confidence, confidence_integrity_cap)
    
    # Export scores for downstream use
    state["confidence_score"] = confidence_score
    state["extraction_integrity"] = extraction_integrity
    state["context_reliability"] = context_reliability
    state["confidence_integrity_cap"] = confidence_integrity_cap
    
    # Degraded mode fallback defaults
    visibility_score = 0
    total_visibility = 0.0
    authority_match_score = 0
    
    gemini_key = os.getenv("GEMINI_API_KEY")
    serper_key = os.getenv("SERPER_API_KEY")

    if not gemini_key or not serper_key:
        console.print("[bold red]NODE_FAILED[/bold red]: Researcher (API Keys missing). Using fallbacks.")
        state["metrics"] = metrics
        return state

    try:
        # 1. Setup ChromaDB for Evidence Analysis
        os.makedirs("chroma_db", exist_ok=True)
        chroma_client = chromadb.PersistentClient(path="./chroma_db/")
        model_name = "intfloat/multilingual-e5-large" if locale == "it" else "all-MiniLM-L6-v2"
        local_cache = os.path.join(os.getcwd(), "hf_cache")
        embedding_model = SentenceTransformer(model_name, cache_folder=local_cache)
        
        class CustomEmbeddingFunction(chromadb.EmbeddingFunction):
            def __call__(self, input: chromadb.Documents) -> chromadb.Embeddings:
                return embedding_model.encode(input).tolist()
        
        collection_name = f"geo_audit_{int(time.time())}"
        collection = chroma_client.get_or_create_collection(name=collection_name, embedding_function=CustomEmbeddingFunction())
        
        client_content = state.get("client_content_clean", "")
        if client_content:
            chunks = [client_content[i:i+500] for i in range(0, min(len(client_content), 15000), 500)]
            collection.upsert(
                documents=chunks,
                metadatas=[{"source": "client_website"}] * len(chunks),
                ids=[f"chunk_{i}" for i in range(len(chunks))]
            )

        # Stage 2: Expand ChromaDB corpus with market intelligence
        raw_data_for_corpus = state.get("raw_data_complete", {})
        corpus_docs, corpus_meta, corpus_ids = [], [], []

        for i, gap in enumerate(raw_data_for_corpus.get("topic_gaps", [])):
            corpus_docs.append(f"Topic gap in this market: {gap}")
            corpus_meta.append({"source": "topic_gap"})
            corpus_ids.append(f"gap_{i}")

        for i, faq in enumerate(raw_data_for_corpus.get("faq_patterns", [])):
            corpus_docs.append(f"User question in this market: {faq}")
            corpus_meta.append({"source": "faq_pattern"})
            corpus_ids.append(f"faq_{i}")

        for i, comp in enumerate(raw_data_for_corpus.get("competitor_entities", [])):
            corpus_docs.append(f"Competitor in this market: {comp}")
            corpus_meta.append({"source": "competitor"})
            corpus_ids.append(f"comp_{i}")

        if corpus_docs:
            collection.upsert(documents=corpus_docs, metadatas=corpus_meta, ids=corpus_ids)
            console.print(f"[cyan]Researcher Node[/cyan]: ChromaDB corpus enriched with {len(corpus_docs)} market intelligence documents.")

        # 3. THREE-TIER STRESS TEST ENGINE (v4.5 Integrity Hardened)
        console.print(f"[cyan]Researcher Node[/cyan]: Executing 3-Tier Evidence-Derived Stress Test for '{brand_name}'...")
        
        raw_data_st = state.get("raw_data_complete", {})
        faq_patterns_st    = raw_data_st.get("faq_patterns", [])
        topic_gaps_st      = raw_data_st.get("topic_gaps", [])
        competitor_ents_st = raw_data_st.get("competitor_entities", [])
        authority_ents_st  = raw_data_st.get("authority_entities", [])
        og_title_st        = state.get("og_tags", {}).get("og:title", "")
        
        is_local = (scale_level == "Local" or state.get("business_profile", {}).get("location_enforce", False))
        brand_tokens = _extract_brand_tokens(brand_name, url, og_title_st)
        
        # 1. NEW: Extract noun-anchors for T2 Hardening
        query_anchors = _extract_required_query_anchors(state)
        
        gemini_client_st = genai.Client(api_key=gemini_key)

        STRESS_TEST_BUDGET = {
            "blind":       12, # Oversample Wave 1
            "contextual":  12, # Oversample Wave 1
            "branded":     6,
        }

        # Query Generation
        loc_conf = state.get("location_confidence", "high")
        profile_key = state.get("business_profile_key", "unknown")
        t1_raw = _build_blind_queries(faq_patterns_st, authority_ents_st, brand_tokens, is_local, discovered_location, locale, gemini_client_st, STRESS_TEST_BUDGET["blind"], service_zones, profile_key)
        t2_raw_unfiltered = _build_contextual_queries(topic_gaps_st, brand_tokens, is_local, discovered_location, locale, gemini_client_st, STRESS_TEST_BUDGET["contextual"], state.get("business_profile", {}).get("persona_templates", []), service_zones, profile_key)
        
        # Sanitize model-generated T1 queries (location_confidence patching)
        for q_obj in t1_raw:
            q_text = q_obj.get("query")
            if q_text:
                san_res = _sanitize_or_reject_query(q_text, "blind_discovery", brand_tokens, is_local, discovered_location, locale, industry="market", location_confidence=loc_conf)
                q_obj.update(san_res)
                q_obj["query"] = san_res.get("final_query")

        # Sanitize model-generated T2 queries with anchor guard
        t2_raw = []
        rejected_t2 = []
        for q_obj in t2_raw_unfiltered:
            q_text = q_obj.get("query")
            if q_text:
                is_valid, anchor_hits = _contains_required_anchor(q_text, query_anchors)
                if is_valid:
                    q_obj["anchors_used"] = anchor_hits
                    t2_raw.append(q_obj)
                else:
                    rej = q_obj.copy()
                    rej["rejection_code"] = "REJECT_NO_REQUIRED_ANCHOR"
                    rej["rejection_reason"] = "Query lacks core domain anchors."
                    rejected_t2.append(rej)

        t3_queries = _build_branded_queries(brand_name, target_industry, discovered_location, scale_level, locale, competitor_ents_st, STRESS_TEST_BUDGET["branded"])
        for q_obj in t3_queries:
            q_obj["source"] = "model"
            q_obj["profile_key"] = profile_key

        # ── QUALITY-GATED TIER ASSEMBLY ────────────────────────────────────────
        # Consolidate context for possible regeneration rounds
        regen_ctx = _build_regeneration_context(state, gemini_client_st)

        # Filter to valid model queries first
        t1_model_valid = [q for q in t1_raw if q.get("query")]
        t2_model_valid = [q for q in t2_raw if q.get("query")]

        # Persist T2 anchor rejections
        state["stress_test_rejected_queries"] = rejected_t2

        # HARD PYTHON POST-PROCESSOR FOR STRIPPING LOCATION LEAKAGE & SYNTAX ARTIFACTS
        for q_list in [t1_model_valid, t2_model_valid, t3_queries]:
            for q_obj in q_list:
                q_text = q_obj.get("query", "")
                if isinstance(q_text, str):
                    q_text = q_text.replace(" Italy", "").replace(" Italia", "").replace("Italy", "")
                    q_text = re.sub(r'^[\,\-\.]+\s*', '', q_text)
                    q_text = re.sub(r'\s+', ' ', q_text).strip()
                    q_obj["query"] = q_text

        # Assemble T1 via quality-gated pipeline
        console.print("      [cyan]T1 Quality-Gated Assembly[/cyan]")
        t1_queries, t1_reliability = _assemble_tier_queries(
            "blind_discovery", t1_model_valid, state, brand_tokens,
            is_local, discovered_location, loc_conf, locale, query_anchors,
            gemini_client=gemini_client_st,
            regen_context=regen_ctx
        )

        # Assemble T2 via quality-gated pipeline
        console.print("      [cyan]T2 Quality-Gated Assembly[/cyan]")
        t2_queries, t2_reliability = _assemble_tier_queries(
            "contextual_discovery", t2_model_valid, state, brand_tokens,
            is_local, discovered_location, loc_conf, locale, query_anchors,
            gemini_client=gemini_client_st,
            regen_context=regen_ctx
        )

        # Persist tier reliability metrics
        state["tier_query_reliability"] = {
            "blind_discovery": t1_reliability,
            "contextual_discovery": t2_reliability,
        }

        all_queries = t1_queries + t2_queries + t3_queries
        
        # Diagnostics
        total_q = len(all_queries)
        total_empty = t1_reliability.get("empty_slot_count",0) + t2_reliability.get("empty_slot_count",0)
        
        intent_buckets = set()
        source_distribution = {}
        
        for q in all_queries:
            src = q.get("source", "unknown")
            source_distribution[src] = source_distribution.get(src, 0) + 1
            tokens = q["query"].lower().split()
            if any(t in tokens for t in ["miglior", "best", "top"]): intent_buckets.add("best_of")
            if any(t in tokens for t in ["come", "how", "perché", "why"]): intent_buckets.add("educational")
            if any(t in tokens for t in ["costo", "prezzo", "cost", "price"]): intent_buckets.add("transactional")
            if len(tokens) > 5: intent_buckets.add("long_tail")
        
        rejection_stats = {}
        for r in state.get("stress_test_rejected_queries", []):
            code = r.get("rejection_code", "UNKNOWN")
            rejection_stats[code] = rejection_stats.get(code, 0) + 1
        
        # Add quality-gate rejection stats from tier assembly
        rejection_stats["QUALITY_GATE_REJECT"] = (
            t1_reliability["rejected_query_count"] + t2_reliability["rejected_query_count"]
        )
        
        state["stress_test_query_provenance"] = all_queries
        state["stress_test_diagnostics"] = {
            "query_count": total_q,
            "empty_slots": total_empty,
            "bucket_diversity": len(intent_buckets),
            "source_distribution": source_distribution,
            "rejection_stats": rejection_stats,
            "point_conversion": sum(q.get("points", 0) for q in all_queries) / total_q if total_q > 0 else 0,
            "tier_query_reliability": state["tier_query_reliability"],
        }
        
        visibility_points, total_possible, stress_test_log, tier_stats, exec_diag = _run_stress_test(
            all_queries, str(serper_key), brand_name, url, og_title_st, gemini_client_st
        )
        
        # Merge execution diagnostics
        state["stress_test_diagnostics"].update(exec_diag)

        visibility_score = min(100, int((visibility_points / max(total_possible, 1)) * 100))

        stress_test_summary = {
            "t1_blind_queries":       len(t1_queries),
            "t2_contextual_queries":  len(t2_queries),
            "t3_branded_queries":     len(t3_queries),
            "total_queries":          len(all_queries),
            "total_possible_points":  total_possible,
            "matched_points":         visibility_points,
            "tier_metrics":           tier_stats,
            "tier_query_reliability": state["tier_query_reliability"],
            "provenance": "Direct Context Search" if visibility_score > 0 else "Profile Inferred"
        }
        state["stress_test_log"]     = stress_test_log
        state["stress_test_summary"] = stress_test_summary
        state["stress_test_tier_stats"] = tier_stats

        # 4. AUTHORITY MATCH v4.5
        console.print("[cyan]Researcher Node[/cyan]: Calculating Authority Match...")
        raw_data = state.get("raw_data_complete", {})
        authority_ents = raw_data.get("authority_entities", [])
        
        def fuzzy_match(entity, text):
            e_norm = re.sub(r'[^\w\s]', '', str(entity).lower().strip())
            suffixes = [" spa", " s.p.a.", " ltd", " inc", " srl", " llc", " gmbh", " s.r.l."]
            for s in suffixes:
                if e_norm.endswith(s): e_norm = e_norm[:-len(s)].strip()
            t_norm = re.sub(r'[^\w\s]', '', text.lower())
            if not e_norm: return False
            if e_norm in t_norm: return True
            parts = e_norm.split()
            return len(parts) > 1 and sum(1 for p in parts if p in t_norm) >= len(parts) * 0.6
            
        matched_auth = sum(1 for e in authority_ents if fuzzy_match(e, client_content))
        auth_cov = (matched_auth / max(len(authority_ents), 1)) * 40.0

        tech_trust = 0
        if locale == "it":
            content_lower = client_content.lower()
            if re.search(r'p\.?\s*i\.?\s*v\.?\s*a\.?\s*[:=]?\s*\d{11}', content_lower): tech_trust += 10
            if re.search(r'@pec\.|posta\s+certificata', content_lower): tech_trust += 10
            if re.search(r'\brea\b\s*[:=]?\s*[a-z]{2}\s*\d+', content_lower) or 'camera di commercio' in content_lower: tech_trust += 10
        else:
            if state.get("robots_txt_status") == "allowed": tech_trust += 15
            if state.get("hreflang_count", 0) > 0: tech_trust += 15

        schema_auth = 0
        s_counts = state.get("schema_type_counts", {})
        if any(k in s_counts for k in ["Organization", "LocalBusiness"]): schema_auth += 10
        if any(k in s_counts for k in ["Person", "Author"]): schema_auth += 10
        if any(k in s_counts for k in ["Article", "NewsArticle", "FAQPage"]): schema_auth += 10

        authority_match_score = max(15, min(100, int(auth_cov + tech_trust + schema_auth)))

        # 5. DEFENSIBLE EVIDENCE DEPTH (Multi-Component Aggregate)
        console.print("[cyan]Researcher Node[/cyan]: Synthesizing Defensible Evidence Depth...")
        
        # Component A: Semantic Information Gain (0-40 pts)
        semantic_ig = 0.0
        try:
            ig_prompt = f"""
            Evaluate the Defensible Evidence Depth of this website against its precise market.
            Factual uniqueness and proprietary frameworks.
            Market Topic Gaps: {raw_data.get("topic_gaps", [])[:5]}
            Market FAQs: {raw_data.get("faq_patterns", [])[:5]}
            Site Frameworks/IP: {state.get("original_frameworks", [])}
            Site Content Snippet: {client_content[:6000]}
            
            Score 0-40 (40=Exceptional intellectual property, 0=Generic/Thin).
            Return ONLY a single integer.
            """
            def _ig_req():
                return gemini_client_st.models.generate_content(
                    model="gemini-2.5-flash-lite",
                    contents=ig_prompt,
                )
            ig_res = execute_with_backoff(_ig_req, max_retries=2, initial_delay=2.0)
            ig_match = re.search(r'\d+', ig_res.text)
            if ig_match:
                semantic_ig = min(40.0, float(ig_match.group(0)))
        except Exception:
            semantic_ig = 15.0 # conservative semantic fallback
            
        # Component B: Structural & Crawl Density (0-20 pts)
        structural_score = 0.0
        depth_data = state.get("client_content_depth", {})
        wc = depth_data.get("word_count", 0)
        ext_quality = depth_data.get("extraction_quality", "low")
        s_counts = state.get("schema_type_counts", {})
        
        if wc > 1000: structural_score += 5
        elif wc > 400: structural_score += 3
        
        if ext_quality == "high": structural_score += 5
        elif ext_quality == "medium": structural_score += 3
        
        if len(s_counts) > 4: structural_score += 10
        elif len(s_counts) > 0: structural_score += 5

        # Component C: Citation & Authority Footprint (0-25 pts)
        citation_score = 0.0
        taxonomy = state.get("source_taxonomy", {})
        total_cites = (
            taxonomy.get("owned_count", 0) + 
            taxonomy.get("earned_count", 0) + 
            taxonomy.get("directory_count", 0)
        )
        if total_cites > 15: citation_score = 25
        elif total_cites > 5: citation_score = 15
        elif total_cites > 0: citation_score = 5
        
        # Component D: Strategic Readiness (0-15 pts)
        strategic_score = 0.0
        if state.get("missing_page_types") or state.get("discovery_intent_gaps"):
            strategic_score += 10
        if state.get("original_frameworks"):
            strategic_score += 5
            
        # Final Aggregate with a low-impact visibility bonus
        eg_total = semantic_ig + structural_score + citation_score + strategic_score
        if visibility_score > 0:
            eg_total += min(10.0, visibility_score * 0.1)
            
        metrics["Defensible Evidence Depth"] = int(min(100, max(0, eg_total)))

        # 6. FINAL VISIBILITY & HALLUCINATION RISK
        total_visibility = (visibility_score * 0.45) + (authority_match_score * 0.30) + (metrics["Defensible Evidence Depth"] * 0.25)
        
        base_risk = 100 - total_visibility
        confidence_penalty = (100 - confidence_score) * 0.5
        risk_score = min(95, max(5, int(base_risk + confidence_penalty)))
        
        metrics["Hallucination Risk"] = risk_score
        metrics["Entity Consensus"] = int(authority_match_score)
        
        status_label = "Enterprise-Ready" if total_visibility > 85 else "Agency-Ready" if total_visibility > 65 else "Needs Work"
        metrics["Citation Readiness"] = f"{status_label} [PROVISIONAL]" if integrity_status != "valid" else status_label

        if confidence_score < 40: citation_status = "Low Verification"
        elif total_visibility > 60 and confidence_score >= 80: citation_status = "Verified"
        elif total_visibility > 30: citation_status = "Partially Verified"
        else: citation_status = "Low Verification"
        
        ed = metrics["Defensible Evidence Depth"]
        if confidence_score < 40 or citation_status == "Low Verification":
            projected_traffic_lift = "0–5%"
        elif confidence_score < 75:
            projected_traffic_lift = "3–8%" if ed < 40 else "8–18%"
        else:
            projected_traffic_lift = "8–18%" if ed < 60 else "18–30%"

    except Exception as e:
        console.print(f"[bold red]Researcher Logic Failure: {e}[/bold red]")

    # 7. Final Recommendation Pack
    try:
        time.sleep(5)
        grounding_context = state.get("grounding_context", "")
        e_e_a_t_gaps_str = "; ".join(_safe_stringify_list(state.get("e_e_a_t_gaps", []))) or "none identified"
        frameworks_str = "; ".join(_safe_stringify_list(state.get("original_frameworks", []))) or "none identified"
        rec_content_str = "; ".join(_safe_stringify_list(state.get("recommended_content", []))) or "none identified"

        rec_prompt = f"""
        Act as Senior GEO Strategist. Write an agency deliverable.
        Context: {grounding_context}
        E-E-A-T gaps: {e_e_a_t_gaps_str}
        IP/Frameworks: {frameworks_str}
        Rec Content: {rec_content_str}
        
        Metrics: Visibility: {total_visibility:.1f}/100 | Evidence Depth: {metrics['Defensible Evidence Depth']}% | Confidence: {confidence_score}/100
        Integrity Status: {integrity_status} ({', '.join(integ_reasons)})
        
        MANDATORY RULES:
        - Ground EVERY recommendation in market data.
        - If integrity_status is not 'valid', tag titles with [PROVISIONAL].
        - Every recommendation MUST include:
          - "evidence_origin": ("on_site" | "off_site" | "query_gap" | "profile_inference" | "mixed")
          - "evidence_confidence": ("high" | "medium" | "low")
          - "supporting_signals": [list of 1-3 concrete evidence strings]
        - Output language: {lang_name}
        
        Return STRICTLY a JSON array of objects with keys: title, rationale, priority, implementation_type, evidence_origin, evidence_confidence, supporting_signals.
        """
        gemini_client = genai.Client(api_key=gemini_key)
        res_rec = gemini_client.models.generate_content(model='gemini-2.5-flash-lite', contents=rec_prompt, config={"response_mime_type": "application/json"})
        
        try:
            clean_text = res_rec.text.strip()
            if clean_text.startswith("```json"): clean_text = clean_text[7:]
            if clean_text.startswith("```"): clean_text = clean_text[3:]
            if clean_text.endswith("```"): clean_text = clean_text[:-3]
            geo_recommendation_pack_json = json.loads(clean_text.strip())
            geo_recommendation_pack = json.dumps(geo_recommendation_pack_json, indent=2)
        except:
            geo_recommendation_pack = res_rec.text
            
        research_output = f"v4.5 Analysis Complete. Integrity: {integrity_status}."
    except Exception as e:
        console.print(f"[yellow]Recommendation generation skipped: {e}[/yellow]")

    state["metrics"] = metrics
    state["citation_status"] = citation_status
    state["projected_traffic_lift"] = projected_traffic_lift
    state["geo_recommendation_pack"] = geo_recommendation_pack
    state["research_output"] = research_output
    state["visibility_score"] = int(total_visibility) if 'total_visibility' in locals() else 0
    state["authority_match_score"] = int(authority_match_score) if 'authority_match_score' in locals() else 0
    state["evidence_limitations"] = "Low confidence due to parsing issues." if confidence_score < 50 else "Adequate confidence."

    if state.get("locale") == "it":
        _content = state.get("client_content_clean", "").lower()
        state["italian_trust_signals"] = {
            "piva_detected": bool(re.search(r'p\.?\s*i\.?\s*v\.?\s*a\.?\s*[:=]?\s*\d{11}', _content)),
            "pec_detected": bool(re.search(r'@pec\.|posta\s+certificata', _content)),
            "rea_detected": bool(re.search(r'\brea\b\s*[:=]?\s*[a-z]{2}\s*\d+', _content)),
            "camera_commercio_detected": 'camera di commercio' in _content,
        }
    else:
        state["italian_trust_signals"] = {}
        
    return state
