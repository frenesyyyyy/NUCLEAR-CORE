"""
Source Quality Node — GEO Optimizer Pipeline.

Aggregates earned media buckets into a definitive source taxonomy,
computes a text-based trust mix summary, and evaluates citation
source risk based on the business profile context.

All logic is deterministic; no external API calls.
"""

from typing import Any
from rich.console import Console

console = Console()


def _evaluate_trust_mix(
    earned_count: int,
    owned_count: int,
    forum_count: int,
    review_count: int,
    directory_count: int,
    total_sources: int,
) -> str:
    """
    Generate a text-based summary of the brand's off-site trust mix.
    """
    if total_sources == 0:
        return "No external sources discovered. Trust mix is severely lacking."

    if owned_count == total_sources:
        return "Echo chamber detected. All discovered sources are owned properties. No independent validation."

    high_trust = earned_count + review_count
    ratio = (high_trust / total_sources) * 100

    if ratio >= 50:
        return "Strong independent validation. Majority of sources are high-trust editorial or review platforms."
    elif ratio >= 20:
        return "Moderate independent validation. Some editorial/review presence, but relies heavily on directories or forums."
    else:
        return "Weak independent validation. Predominantly low-authority directory listings or owned properties."


def _evaluate_citation_risk(
    earned_count: int,
    review_count: int,
    forum_count: int,
    directory_count: int,
    total_sources: int,
    profile_key: str,
) -> list[str]:
    """
    Evaluate specific citation risks based on the business profile.
    """
    risks = []
    
    if total_sources == 0:
        risks.append("CRITICAL: Zero external footprint. AI engines have no independent context for this brand.")
        return risks

    # B2B SaaS, Ecommerce, Apps — high reliance on reviews
    if profile_key in ("b2b_saas", "consumer_saas", "ecommerce_brand", "marketplace"):
        if review_count == 0:
            risks.append("Missing validation: No review platform mentions (G2, Trustpilot, App Store). High risk of exclusion from comparison queries.")
    
    # Media/Education/Agency — high reliance on editorial
    elif profile_key in ("media_blog", "education_course_provider", "agency_marketing"):
        if earned_count == 0:
            risks.append("Authority gap: No editorial/press mentions. High risk for 'best in class' or thought-leadership queries.")
    
    # Local services — reputation proxies
    elif profile_key in ("local_dentist", "local_law_firm", "restaurant_hospitality", "local_tech_provider", "freelancer_consultant"):
        if review_count == 0 and directory_count == 0:
            risks.append("Local ghost: No local directory or review citations detected. High risk for 'near me' discovery.")
        elif review_count == 0:
            risks.append("Incomplete footprint: Discovered on directories but missing on high-trust review platforms (MioDottore, Yelp, Tripadvisor).")

    # General risks
    if (earned_count + review_count) == 0 and total_sources > 0:
        if forum_count > 0:
            risks.append("Sentiment vulnerability: Brand relies entirely on unverified forum/community chatter for independent validation.")

    return risks


def process(state: dict) -> dict:
    """
    Create a source taxonomy and trust-mix summary.

    Args:
        state: Pipeline state dictionary.

    Returns:
        Updated state with ``state["source_taxonomy"]``.
    """
    console.print("[bold blue]Node: Source Quality[/bold blue] | Building source taxonomy and trust mix...")

    earned_media: dict  = state.get("earned_media", {})
    profile: dict       = state.get("business_profile", {})
    profile_key: str    = state.get("business_profile_key", "b2b_saas")
    
    # Extract buckets from earned media node
    breakdown = earned_media.get("source_breakdown", {})
    
    owned_count     = breakdown.get("owned", 0)
    earned_count    = breakdown.get("editorial", 0)
    forum_count     = breakdown.get("forum", 0)
    review_count    = breakdown.get("review", 0)
    directory_count = breakdown.get("directory", 0)
    unknown_count   = breakdown.get("unknown", 0)

    total_sources = sum([
        owned_count, earned_count, forum_count, 
        review_count, directory_count, unknown_count
    ])

    trust_mix_summary = _evaluate_trust_mix(
        earned_count=earned_count,
        owned_count=owned_count,
        forum_count=forum_count,
        review_count=review_count,
        directory_count=directory_count,
        total_sources=total_sources,
    )

    citation_risks = _evaluate_citation_risk(
        earned_count=earned_count,
        review_count=review_count,
        forum_count=forum_count,
        directory_count=directory_count,
        total_sources=total_sources,
        profile_key=profile_key,
    )

    notes_parts = []
    if not earned_media:
        notes_parts.append("Warning: No earned_media data found in state. Taxonomy will be zeroed out.")
    if citation_risks:
        notes_parts.append(f"Identified {len(citation_risks)} citation/trust risk factor(s).")
    else:
        notes_parts.append("Citation footprint appears healthy for this profile.")

    state["source_taxonomy"] = {
        "owned_count": owned_count,
        "earned_count": earned_count,
        "forum_count": forum_count,
        "review_count": review_count,
        "directory_count": directory_count,
        "unknown_count": unknown_count,
        "trust_mix_summary": trust_mix_summary,
        "citation_source_risk": citation_risks,
        "notes": " ".join(notes_parts)
    }

    console.print(
        f"[bold green]Source Quality Complete[/bold green] | "
        f"Total Sources: [cyan]{total_sources}[/cyan] | "
        f"Trust Mix: [yellow]{trust_mix_summary.split('.')[0]}[/yellow]"
    )

    return state
