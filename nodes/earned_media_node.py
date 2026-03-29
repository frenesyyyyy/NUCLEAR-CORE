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

console = Console()

# ─────────────────────────────────────────────────────────────────────────────
# Domain Classification Registry
# ─────────────────────────────────────────────────────────────────────────────

# Maps hostname patterns → mention bucket type.
# Ordered so more specific patterns are checked first.
_DOMAIN_BUCKET_MAP: list[tuple[str, str]] = [
    # ── Review platforms ──────────────────────────────────────────────────────
    ("trustpilot.com",   "review"),
    ("g2.com",           "review"),
    ("capterra.com",     "review"),
    ("getapp.com",       "review"),
    ("softwareadvice.com","review"),
    ("tripadvisor.",     "review"),
    ("yelp.com",         "review"),
    ("reviews.io",       "review"),
    ("sitejabber.com",   "review"),
    ("producthunt.com",  "review"),
    ("g.page",           "review"),
    ("maps.google.",     "review"),
    ("paginegialle.it",  "directory"),
    ("miodottore.it",    "review"),
    ("idoctors.it",      "review"),
    ("prontopro.it",     "review"),
    ("trovadentisti.it", "review"),
    ("paginebianche.it", "directory"),
    ("denuncia.it",      "forum"),
    # ── Directories ──────────────────────────────────────────────────────────
    ("crunchbase.com",   "directory"),
    ("linkedin.com",     "directory"),
    ("bloomberg.com",    "editorial"),
    ("clutch.co",        "directory"),
    ("dnb.com",          "directory"),
    ("kompass.com",      "directory"),
    ("manta.com",        "directory"),
    # ── Editorial / Press ─────────────────────────────────────────────────────
    ("techcrunch.com",   "editorial"),
    ("wired.com",        "editorial"),
    ("forbes.com",       "editorial"),
    ("guardian.com",     "editorial"),
    ("corriere.it",      "editorial"),
    ("ilsole24ore.com",  "editorial"),
    ("repubblica.it",    "editorial"),
    ("medium.com",       "editorial"),
    ("substack.com",     "editorial"),
    # ── Forums / Communities ─────────────────────────────────────────────────
    ("reddit.com",       "forum"),
    ("news.ycombinator.com", "forum"),
    ("quora.com",        "forum"),
    ("stackoverflow.com","forum"),
    ("discord.com",      "forum"),
    ("twitter.com",      "forum"),
    ("x.com",            "forum"),
    ("facebook.com",     "forum"),
    ("instagram.com",    "forum"),
]

# Bucket weights for strength score (0–100 scale)
_BUCKET_WEIGHTS: dict[str, float] = {
    "review":    3.0,   # High trust signal for AI citation
    "editorial": 2.5,   # Strong authority/E-E-A-T signal
    "directory": 1.5,   # Entity disambiguation value
    "forum":     1.0,   # Indirect mention signal
    "owned":     0.5,   # Low external value — self-published
    "unknown":   0.5,   # Uncategorised
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


def _classify_url(url: str, brand_domain: str) -> str:
    """
    Classify a single URL into a mention bucket.

    Priority order:
    1. Owned (contains brand domain)
    2. Known domain map
    3. Unknown fallback

    Args:
        url: The source URL to classify.
        brand_domain: The brand's own hostname for owned detection.

    Returns:
        Bucket label string.
    """
    hostname = _extract_hostname(url)
    if not hostname:
        return "unknown"

    # Owned detection
    if brand_domain and brand_domain in hostname:
        return "owned"

    # Pattern-based classification
    for pattern, bucket in _DOMAIN_BUCKET_MAP:
        if pattern in hostname:
            return bucket

    return "unknown"


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
        total_sources: Total number of classified sources.

    Returns:
        Integer score 0–100.
    """
    if total_sources == 0:
        return 0

    raw = sum(
        count * _BUCKET_WEIGHTS.get(bucket, 0.5)
        for bucket, count in source_breakdown.items()
    )
    # Normalize: cap at 100 using a soft ceiling of 30 weighted points = 100
    normalized = min(100, int((raw / 30.0) * 100))
    return normalized


def _compute_reputation_risk_score(
    negative_count: int,
    review_count: int,
    total_sources: int,
) -> int:
    """
    Compute a 0-100 reputation risk score.

    Higher = more at risk. Based on ratio of negative signals vs reviews.

    Args:
        negative_count: Number of URLs with negative patterns.
        review_count: Total review-bucket sources.
        total_sources: Total sources.

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
    target_industry: str   = state.get("target_industry", "Unknown")
    location: str          = state.get("discovered_location", "Unknown")
    profile: dict          = state.get("business_profile", {})
    raw_data: dict         = state.get("raw_data_complete", {})
    external_sources: list = state.get("external_sources", [])
    url: str               = state.get("url", "")

    brand_domain = _infer_brand_domain(url)

    # Merge: external_sources (prospector list) + source_urls from raw_data
    all_source_urls: list[str] = list(external_sources)
    raw_source_urls = raw_data.get("source_urls", [])
    for s in raw_source_urls:
        if s and s not in all_source_urls:
            all_source_urls.append(s)

    console.print(f"  Brand: [cyan]{brand_name}[/cyan] | Sources to analyse: [yellow]{len(all_source_urls)}[/yellow]")

    # ── Classify each source URL ─────────────────────────────────────────────
    mentions: list[dict[str, Any]] = []
    source_breakdown: dict[str, int] = {
        "review": 0, "editorial": 0, "forum": 0,
        "directory": 0, "owned": 0, "unknown": 0,
    }
    negative_count = 0

    for src_url in all_source_urls:
        if not src_url:
            continue

        bucket = _classify_url(src_url, brand_domain)
        source_breakdown[bucket] = source_breakdown.get(bucket, 0) + 1

        has_neg = _has_negative_signals(src_url)
        if has_neg:
            negative_count += 1

        mentions.append({
            "url": src_url,
            "bucket": bucket,
            "hostname": _extract_hostname(src_url),
            "negative_signal": has_neg,
        })

    # Apply extensibility hook (no-op in current layer)
    mentions = _enrich_mentions_hook(mentions, state)

    total = len(mentions)
    strength = _compute_strength_score(source_breakdown, total)
    rep_risk = _compute_reputation_risk_score(negative_count, source_breakdown.get("review", 0), total)
    warning_effect = negative_count > 0 and (negative_count / max(total, 1)) >= 0.10

    # ── Build notes ─────────────────────────────────────────────────────────
    notes_parts: list[str] = []
    if total == 0:
        notes_parts.append("No external sources detected. Earned media analysis is limited to structural signals.")
    else:
        top_buckets = sorted(source_breakdown.items(), key=lambda x: x[1], reverse=True)
        top_label = top_buckets[0][0] if top_buckets else "unknown"
        notes_parts.append(f"Dominant mention type: '{top_label}' ({top_buckets[0][1]} sources).")
    if warning_effect:
        notes_parts.append(
            f"Warning effect risk detected: {negative_count}/{total} sources contain negative signals."
        )
    if source_breakdown.get("review", 0) == 0 and total > 0:
        notes_parts.append("No review-platform mentions found. Trust signal coverage is weak.")
    notes = " ".join(notes_parts)

    state["earned_media"] = {
        "strength_score": strength,
        "reputation_risk_score": rep_risk,
        "warning_effect_risk": warning_effect,
        "mentions": mentions,
        "source_breakdown": source_breakdown,
        "notes": notes,
    }

    console.print(
        f"[bold green]Earned Media Complete[/bold green] | "
        f"Strength: [cyan]{strength}[/cyan] | "
        f"Rep Risk: [yellow]{rep_risk}[/yellow] | "
        f"Warning Effect: [{'red' if warning_effect else 'green'}]{warning_effect}[/{'red' if warning_effect else 'green'}]"
    )

    return state
