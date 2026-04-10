"""
Earned Media Node — GEO Optimizer Pipeline.

Tracks off-site brand trust and reputation signals using available
external source data. Classifies mentions into typed buckets, computes
a brand strength score and reputation risk score, and surfaces a
warning-effect risk flag for negative review patterns.

All logic is deterministic; no external API calls.
Extensibility hooks allow future live-enrichment layers to plug in
additional mention sources without changing the core scoring logic.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse
from typing import Any
from rich.console import Console

from nodes.business_profiles import DEFAULT_PROFILE_KEY
from nodes.source_matrix import (
    classify_url_to_family,
    compute_profile_aware_strength,
    get_source_pack,
    SOURCE_FAMILIES,
    infer_families_from_site_evidence,
    get_canonical_source_urls
)

console = Console()

# ─────────────────────────────────────────────────────────────────────────────
# Domain Classification Registry & Legacy Mapping
# ─────────────────────────────────────────────────────────────────────────────

_FAMILY_TO_LEGACY_BUCKET: dict[str, str] = {
    "app_ecosystems": "review",
    "review_ecosystems": "review",
    "software_comparison_ecosystems": "review",
    "professional_directories": "directory",
    "local_directories_maps": "directory",
    "editorial_news_pr": "editorial",
    "marketplace_partner_ecosystems": "directory",
    "docs_integrations_developer": "forum",
    "forums_communities": "forum",
    "official_registries_legal": "directory",
    "employer_workforce_reputation": "review",
    "social_proof_platforms": "review",
    "owned": "owned",
    "ignored_noise": "noise",
    "unclassified_candidate": "unclassified",
}

# Bucket weights for strength score (0–100 scale)
_BUCKET_WEIGHTS: dict[str, float] = {
    "review":    3.0,   # High trust signal for AI citation
    "editorial": 2.5,   # Strong authority/E-E-A-T signal
    "directory": 1.5,   # Entity disambiguation value
    "forum":     1.0,   # Indirect mention signal
    "owned":     0.5,   # Low external value — self-published
    "unclassified": 0.5,
    "noise":     0.0,
}

# Reputation risk signals in domain hostnames
_NEGATIVE_REVIEW_PATTERNS = [
    "scam", "fraud", "complaint", "ripoff", "rip-off",
    "warning", "alert", "beware", "fake", "lawsuit",
    "report", "negative", "truffatore", "truffa",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_hostname(url: str) -> str:
    """Return the lowercase hostname from a URL string, or empty string."""
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


def _classify_url(url: str, brand_domain: str, brand_name: str = None) -> str:
    """
    Categorize a URL into a high-level reputational bucket by deriving it 
    from the canonical cross-node family classifier. This ensures buckets
    and families never drift out of sync.
    """
    family, _ = classify_url_to_family(url, brand_domain, brand_name)
    return _FAMILY_TO_LEGACY_BUCKET.get(family, "unclassified")


def _has_negative_signals(url: str) -> bool:
    """
    Check whether a URL contains known negative/reputation-risk signals.

    Args:
        url: The URL to inspect.

    Returns:
        True if a negative pattern is detected in the URL string.
    """
    url_lower = url.lower()
    return any(neg in url_lower for neg in _NEGATIVE_REVIEW_PATTERNS)


def _infer_brand_domain(url: str) -> str:
    """
    Infer the brand's own hostname from the main target URL.

    Args:
        url: The pipeline target URL.

    Returns:
        Lowercase hostname, e.g. 'acme-crm.com'.
    """
    return _extract_hostname(url)


def _compute_strength_score(
    source_breakdown: dict[str, int],
    total_sources: int,
) -> int:
    """
    Compute a 0-100 brand strength score based on weighted bucket counts.

    Args:
        source_breakdown: Dict mapping bucket name → count.
        total_sources: Total number of classified sources (excluding noise).

    Returns:
        Integer score 0–100.
    """
    if total_sources == 0:
        return 0

    raw = sum(
        count * _BUCKET_WEIGHTS.get(bucket, 0.5)
        for bucket, count in source_breakdown.items()
        if bucket != "noise"
    )
    # Normalize: cap at 100 using a soft ceiling of 30 weighted points = 100
    normalized = min(100, int((raw / 30.0) * 100))
    return normalized


def _compute_reputation_risk_score(
    negative_count: int,
    total_sources: int,
) -> int:
    """
    Compute a 0-100 reputation risk score.

    Higher = more at risk. Based on ratio of negative signals vs total sources.

    Args:
        negative_count: Number of URLs with negative patterns.
        total_sources: Total sources (excluding noise).

    Returns:
        Integer score 0–100.
    """
    if total_sources == 0:
        return 0
    if negative_count == 0:
        return 0  # No negative signals detected — no risk to report

    negative_ratio = negative_count / max(total_sources, 1)
    # Scale to 0-100 (50% negative → score of 100)
    return min(100, int(negative_ratio * 200))


# ─────────────────────────────────────────────────────────────────────────────
# Extensibility Hook — Override in future live-enrichment layers
# ─────────────────────────────────────────────────────────────────────────────

def _enrich_mentions_hook(mentions: list[dict], state: dict) -> list[dict]:
    """
    Extensibility hook for future live-enrichment of mention data.

    This function is a no-op in the current deterministic layer.
    A future implementation could call live APIs (e.g. Brandwatch,
    Mention.com, or a custom scraper) to add sentiment, date, or
    reach signal to each mention entry.

    Args:
        mentions: Current mention list to be enriched.
        state: Full pipeline state (read-only reference).

    Returns:
        Enriched mention list (currently unchanged).
    """
    return mentions


# ─────────────────────────────────────────────────────────────────────────────
# Main Process
# ─────────────────────────────────────────────────────────────────────────────

def process(state: dict) -> dict:
    """
    Analyse off-site brand trust and earned media signals.

    Reads external source URLs from the pipeline state, classifies each
    into a typed bucket (review, editorial, forum, directory, owned,
    unknown), computes brand strength and reputation risk scores, and
    flags warning-effect risk if negative signals are detected.

    Args:
        state: Pipeline state dictionary.

    Returns:
        Updated state with ``state["earned_media"]``.
    """
    console.print("[bold blue]Node: Earned Media[/bold blue] | Analysing off-site brand trust signals...")

    brand_name: str        = state.get("brand_name", "Unknown Brand")
    profile_key: str       = state.get("business_profile_key", DEFAULT_PROFILE_KEY)
    url: str               = state.get("url", "")

    inferred_domain = _infer_brand_domain(url)

    # Load profile-aware source pack
    source_pack = get_source_pack(profile_key)
    pack_weights: dict = source_pack.get("weights_override", {})

    all_source_urls = get_canonical_source_urls(state)

    # ── Site Evidence Validation ─────────────────────────────────────────────
    inferred_family_dict = infer_families_from_site_evidence(
        state.get("client_content_clean", ""),
        state.get("og_tags", {}),
        state.get("json_ld_blocks", []),
        state.get("client_content_raw", "")
    )
    first_party_inferred = [{"family": f, "tag": "first_party", "confidence": data["confidence"]} for f, data in inferred_family_dict.items()]

    console.print(f"  Brand: [cyan]{brand_name}[/cyan] | Profile: [yellow]{profile_key}[/yellow] | Sources: [yellow]{len(all_source_urls)}[/yellow]")

    # ── Classify each source URL ─────────────────────────────────────────────
    mentions: list[dict[str, Any]] = []
    earned_count = 0
    review_count = 0
    forum_count = 0
    directory_count = 0
    owned_count = 0
    unclassified_count = 0
    noise_count = 0
    family_breakdown: dict[str, int] = {}
    negative_count = 0

    for src_url in all_source_urls:
        if not src_url:
            continue

        bucket = _classify_url(src_url, inferred_domain, brand_name)
        if bucket == "noise":
            noise_count += 1
        elif bucket == "unclassified":
            unclassified_count += 1
        elif bucket == "owned":
            owned_count += 1
        elif bucket == "forum":
            forum_count += 1
        elif bucket == "review":
            review_count += 1
        elif bucket == "directory":
            directory_count += 1
        elif bucket == "editorial":
            earned_count += 1

        family, _ = classify_url_to_family(src_url, inferred_domain)
        family_breakdown[family] = family_breakdown.get(family, 0) + 1

        has_neg = _has_negative_signals(src_url)
        if has_neg:
            negative_count += 1

        mentions.append({
            "url": src_url,
            "bucket": bucket,
            "hostname": _extract_hostname(src_url),
            "negative_signal": has_neg,
        })

    source_breakdown = {
        "earned_editorial": earned_count,
        "review_sentiment": review_count,
        "forum_community": forum_count,
        "directory_listing": directory_count,
        "owned_property": owned_count,
        "unclassified": unclassified_count,
        "noise": noise_count,
    }

    # IMPORTANT: Noise is entirely excluded from the denominator.
    total_sources = sum(count for bucket, count in source_breakdown.items() if bucket != "noise")

    # Apply extensibility hook (no-op in current layer)
    mentions = _enrich_mentions_hook(mentions, state)

    strength = _compute_strength_score(source_breakdown, total_sources)
    profile_aware_strength = compute_profile_aware_strength(family_breakdown, pack_weights, total_sources)
    rep_risk = _compute_reputation_risk_score(negative_count, total_sources)

    warning_effect = negative_count > 0 and (negative_count / max(total_sources, 1)) >= 0.10

    # ── Build notes ─────────────────────────────────────────────────────────
    notes_parts: list[str] = []
    if total_sources == 0:
        notes_parts.append("No external sources detected. Earned media analysis is limited to structural signals.")
    else:
        top_buckets = sorted(source_breakdown.items(), key=lambda x: x[1], reverse=True)
        top_label = top_buckets[0][0] if top_buckets else "unclassified"
        notes_parts.append(f"Dominant mention type: '{top_label}' ({top_buckets[0][1]} sources).")
    if warning_effect:
        notes_parts.append(
            f"Warning effect risk detected: {negative_count}/{total_sources} sources contain negative signals."
        )
    if source_breakdown.get("review", 0) == 0 and total_sources > 0:
        notes_parts.append("No review-platform mentions found. Trust signal coverage is weak.")
    notes = " ".join(notes_parts)

    state["earned_media"] = {
        "strength_score": strength,
        "profile_aware_strength": profile_aware_strength,
        "reputation_risk_score": rep_risk,
        "warning_effect_risk": warning_effect,
        "mentions": mentions,
        "source_breakdown": source_breakdown,
        "first_party_inferred_families": first_party_inferred,
        "notes": notes,
    }

    console.print(
        f"[bold green]Earned Media Complete[/bold green] | "
        f"Strength: [cyan]{strength}[/cyan] | "
        f"Rep Risk: [yellow]{rep_risk}[/yellow] | "
        f"Warning Effect: [{'red' if warning_effect else 'green'}]{warning_effect}[/{'red' if warning_effect else 'green'}]"
    )

    return state
