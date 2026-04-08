"""
Crawler Policy Node — GEO Optimizer Pipeline.

Transforms robots.txt detection into strategic AI crawler policy
recommendations. Uses the active business profile to determine which
AI bots should be allowed, disallowed, or rate-limited, distinguishing
between citation/retrieval value and training-only data collection.

All logic is deterministic; no external API calls.
"""

from rich.console import Console
from nodes.business_profiles import DEFAULT_PROFILE_KEY

console = Console()

# ─────────────────────────────────────────────────────────────────────────────
# Bot Registry
# ─────────────────────────────────────────────────────────────────────────────

BOT_REGISTRY = [
    {
        "bot": "GPTBot",
        "owner": "OpenAI",
        "purpose": "training",
        "description": "Used to crawl pages for model training data. Does NOT power ChatGPT citation/retrieval.",
    },
    {
        "bot": "ChatGPT-User",
        "owner": "OpenAI",
        "purpose": "citation",
        "description": "Used when ChatGPT browses the web to answer user queries in real-time. Blocking this removes your site from ChatGPT answers.",
    },
    {
        "bot": "PerplexityBot",
        "owner": "Perplexity AI",
        "purpose": "citation",
        "description": "Powers Perplexity search engine citations. Blocking removes your site from Perplexity answers and sources.",
    },
    {
        "bot": "ClaudeBot",
        "owner": "Anthropic",
        "purpose": "training",
        "description": "Used by Anthropic to crawl pages for Claude model training. Not directly tied to live retrieval.",
    },
    {
        "bot": "Googlebot-Extended",
        "owner": "Google",
        "purpose": "training",
        "description": "Used by Google for Gemini model training. Separate from core Googlebot (search indexing). Blocking this does NOT affect Google Search.",
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Profile-Aware Recommendation Logic
# ─────────────────────────────────────────────────────────────────────────────

# Profiles where citation/retrieval visibility is critical
_CITATION_CRITICAL_PROFILES = {
    "b2b_saas_tech", "b2b_saas", "consumer_saas", "ecommerce_brand", "marketplace", "ecommerce_retail",
    "agency_marketing", "education_course_provider", "local_tech_provider",
}

# Profiles where local trust and proximity matter more than broad AI training
_LOCAL_TRUST_PROFILES = {
    "local_dentist", "local_law_firm", "restaurant_hospitality",
    "freelancer_consultant",
}

# Profiles where content IP / originality is high (more reason to block training)
_HIGH_IP_PROFILES = {
    "media_blog", "education_course_provider",
}


def _infer_current_status(robots_status: str, bot_purpose: str) -> str:
    """
    Infer the current effective policy for a bot based on robots.txt status.

    Args:
        robots_status: One of 'allowed', 'restricted', 'not_found'.
        bot_purpose: 'training' or 'citation'.

    Returns:
        'Allow' or 'Disallow' or 'Unknown'.
    """
    if robots_status == "restricted":
        return "Disallow"
    if robots_status == "allowed":
        return "Allow"
    # not_found — most bots default to allowed if no robots.txt
    return "Allow (implicit — no robots.txt)"


def _recommend_for_bot(
    bot: dict,
    profile_key: str,
    geo_behavior: str,
) -> dict:
    """
    Generate a recommendation for a single bot based on profile context.

    Args:
        bot: Bot registry entry.
        profile_key: Active business profile key.
        geo_behavior: The profile's geo_behavior strategy.

    Returns:
        Dict with 'recommended' and 'reason' keys.
    """
    purpose = bot["purpose"]
    bot_name = bot["bot"]

    # ── Citation / Retrieval bots: almost always Allow ─────────────────────
    if purpose == "citation":
        if profile_key in _CITATION_CRITICAL_PROFILES:
            return {
                "recommended": "Allow",
                "reason": (
                    f"Citation-critical for '{geo_behavior}' strategy. "
                    f"Blocking {bot_name} would remove your site from live AI answers."
                ),
            }
        if profile_key in _LOCAL_TRUST_PROFILES:
            return {
                "recommended": "Allow",
                "reason": (
                    f"Local businesses benefit from AI citation for discovery queries. "
                    f"Allowing {bot_name} increases visibility in 'near me' and branded searches."
                ),
            }
        # Default for citation bots: allow
        return {
            "recommended": "Allow",
            "reason": f"Allowing {bot_name} ensures your content can be cited in live AI answers.",
        }

    # ── Training-only bots: profile-dependent ──────────────────────────────
    if purpose == "training":
        # High-IP profiles: recommend blocking training bots to protect content
        if profile_key in _HIGH_IP_PROFILES:
            return {
                "recommended": "Disallow",
                "reason": (
                    f"Your '{geo_behavior}' profile relies on original content IP. "
                    f"Blocking {bot_name} prevents training-only scraping while preserving "
                    f"citation through retrieval bots."
                ),
            }
        # Citation-critical profiles: allow training bots (broader model awareness)
        if profile_key in _CITATION_CRITICAL_PROFILES:
            return {
                "recommended": "Allow",
                "reason": (
                    f"For '{geo_behavior}' businesses, broader model awareness improves "
                    f"brand recall in AI answers. {bot_name} training data feeds future "
                    f"model knowledge of your brand."
                ),
            }
        # Local profiles: training is low-value, can safely block
        if profile_key in _LOCAL_TRUST_PROFILES:
            return {
                "recommended": "Disallow",
                "reason": (
                    f"Training-only crawling provides minimal benefit for local businesses. "
                    f"Your visibility comes from citation/retrieval bots, not training data. "
                    f"Safe to block {bot_name}."
                ),
            }
        # Fallback: conservative allow
        return {
            "recommended": "Allow",
            "reason": f"No strong reason to block {bot_name}. Default: allow for model awareness.",
        }

    # Unknown purpose fallback
    return {
        "recommended": "Allow",
        "reason": "Unknown bot purpose. Defaulting to allow.",
    }


def _generate_robots_txt(bot_matrix: list[dict]) -> str:
    """
    Generate a recommended robots.txt block from the bot matrix.

    Args:
        bot_matrix: List of per-bot recommendation dicts.

    Returns:
        Multi-line robots.txt string.
    """
    lines = [
        "# ── AI Crawler Policy (Generated by GEO Optimizer) ──",
        "# Citation/retrieval bots are allowed for AI answer visibility.",
        "# Training-only bots are configured per business profile.",
        "",
    ]

    for entry in bot_matrix:
        action = "Allow" if entry["recommended"] == "Allow" else "Disallow"
        lines.append(f"User-agent: {entry['bot']}")
        lines.append(f"{action}: /")
        lines.append("")

    # Always preserve core Googlebot
    lines.append("# Core search indexing — never block")
    lines.append("User-agent: Googlebot")
    lines.append("Allow: /")
    lines.append("")

    return "\n".join(lines)


def _generate_crawl_risk_notes(
    robots_status: str,
    bot_matrix: list[dict],
    profile_key: str,
) -> list[str]:
    """
    Generate risk notes about the current crawler configuration.

    Args:
        robots_status: Current robots.txt status from state.
        bot_matrix: Computed bot matrix.
        profile_key: Active business profile key.

    Returns:
        List of risk note strings.
    """
    notes = []

    if robots_status == "not_found":
        notes.append(
            "No robots.txt detected. All bots have implicit access. "
            "This means training bots can freely scrape your content."
        )

    if robots_status == "restricted":
        citation_bots_blocked = [
            e for e in bot_matrix
            if e["current"].startswith("Disallow") and e.get("purpose") == "citation"
        ]
        if citation_bots_blocked:
            names = ", ".join(e["bot"] for e in citation_bots_blocked)
            notes.append(
                f"CRITICAL: Citation bots ({names}) appear to be blocked. "
                f"This actively prevents your site from appearing in AI-generated answers."
            )

    training_allowed = [
        e for e in bot_matrix
        if e["recommended"] == "Allow" and e.get("purpose") == "training"
    ]
    if training_allowed and profile_key in _HIGH_IP_PROFILES:
        names = ", ".join(e["bot"] for e in training_allowed)
        notes.append(
            f"Warning: Training bots ({names}) are recommended as allowed, but your "
            f"profile has high content IP. Consider reviewing if this aligns with your "
            f"content licensing strategy."
        )

    if not notes:
        notes.append("No significant crawler policy risks detected.")

    return notes


# ─────────────────────────────────────────────────────────────────────────────
# Main Process
# ─────────────────────────────────────────────────────────────────────────────

def process(state: dict) -> dict:
    """
    Generate profile-aware AI crawler policy recommendations.

    Reads robots.txt status and the active business profile to produce
    per-bot allow/disallow recommendations, a suggested robots.txt block,
    and crawl risk notes.

    Args:
        state: Pipeline state dictionary.

    Returns:
        Updated state with ``state["crawler_policy"]``.
    """
    console.print("[bold blue]Node: Crawler Policy[/bold blue] | Generating AI crawler recommendations...")

    robots_status: str    = state.get("robots_txt_status", "not_found")
    url: str              = state.get("url", "https://example.com")
    profile: dict         = state.get("business_profile", {})
    profile_summary: dict = state.get("business_profile_summary", {})
    target_industry: str  = state.get("target_industry", "Unknown")
    profile_key: str      = state.get("business_profile_key", DEFAULT_PROFILE_KEY)

    geo_behavior = profile_summary.get("geo_behavior", profile.get("geo_behavior", "standard_retrieval"))
    label = profile_summary.get("label", profile_key)

    console.print(f"  Profile: [cyan]{label}[/cyan] | robots.txt: [yellow]{robots_status}[/yellow]")

    # Build per-bot matrix
    bot_matrix: list[dict] = []
    for bot_entry in BOT_REGISTRY:
        current = _infer_current_status(robots_status, bot_entry["purpose"])
        rec = _recommend_for_bot(bot_entry, profile_key, geo_behavior)

        bot_matrix.append({
            "bot": bot_entry["bot"],
            "owner": bot_entry["owner"],
            "purpose": bot_entry["purpose"],
            "current": current,
            "recommended": rec["recommended"],
            "reason": rec["reason"],
        })

    # Generate recommended robots.txt
    recommended_robots = _generate_robots_txt(bot_matrix)

    # Generate risk notes
    crawl_risk_notes = _generate_crawl_risk_notes(robots_status, bot_matrix, profile_key)

    # Summary note
    allowed_count = sum(1 for b in bot_matrix if b["recommended"] == "Allow")
    blocked_count = len(bot_matrix) - allowed_count
    notes = (
        f"Recommendation: Allow {allowed_count} bot(s), block {blocked_count} bot(s) "
        f"for the '{label}' profile ({geo_behavior} strategy)."
    )

    state["crawler_policy"] = {
        "robots_status": robots_status,
        "bot_matrix": bot_matrix,
        "recommended_robots_txt": recommended_robots,
        "crawl_risk_notes": crawl_risk_notes,
        "notes": notes,
    }

    console.print(
        f"[bold green]Crawler Policy Complete[/bold green] | "
        f"Allow: [green]{allowed_count}[/green] | "
        f"Block: [red]{blocked_count}[/red]"
    )

    return state
