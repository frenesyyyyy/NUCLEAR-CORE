"""
Agentic Readiness Node — GEO Optimizer Pipeline.

Audits whether the site is readable and operable for future autonomous agents.
Analyzes semantic markup, form accessibility, and CTA clarity using deterministic 
HTML heuristics.

All logic is deterministic; no external API calls.
"""

import re
from typing import Any
from bs4 import BeautifulSoup
from rich.console import Console

console = Console()

def _audit_button_semantics(soup: BeautifulSoup) -> tuple[int, list[str]]:
    """
    Check <button> and role="button" for descriptive text.
    """
    score = 100
    issues = []
    
    buttons = soup.find_all("button")
    role_buttons = soup.find_all(["a", "div", "span"], attrs={"role": "button"})
    # Also check generic <a> tags that look like CTA buttons
    ctas = soup.find_all("a", class_=re.compile(r"btn|button|cta", re.I))
    all_btns = list(set(buttons + role_buttons + ctas))
    
    if not all_btns:
        return 0, ["No interactive buttons or CTAs detected."]

    vague_patterns = [
        "click here", "read more", "learn more", "submit", "go", 
        "clicca qui", "scopri di più", "invio", "continua"
    ]
    
    vague_count = 0
    total_checked = 0
    for btn in all_btns:
        text = btn.get_text(strip=True).lower()
        if not text:
            vague_count += 1
            issues.append(f"Empty button text detected: {str(btn)[:50]}...")
            continue
        
        total_checked += 1
        if any(p in text for p in vague_patterns) and len(text.split()) < 3:
            vague_count += 1
            issues.append(f"Vague button text detected: '{text}'")

    if total_checked > 0:
        penalty = int((vague_count / total_checked) * 60)
        score = max(0, 100 - penalty)
        
    return score, issues

def _audit_form_readability(soup: BeautifulSoup) -> tuple[int, list[str]]:
    """
    Verify presence of <label> tags and aria-labels for form inputs.
    """
    score = 100
    issues = []
    
    forms = soup.find_all("form")
    if not forms:
        return 100, [] # No forms is fine, unless the profile requires them

    inputs = soup.find_all(["input", "select", "textarea"])
    if not inputs:
        return 100, []

    unlabeled_count = 0
    for inp in inputs:
        # Ignore hidden inputs or buttons
        if inp.get("type") in ["hidden", "submit", "button", "image"]:
            continue
            
        inp_id = inp.get("id")
        has_label = False
        
        # Check for associated <label>
        if inp_id and soup.find("label", attrs={"for": inp_id}):
            has_label = True
        # Check for parent <label>
        elif inp.find_parent("label"):
            has_label = True
        # Check for aria-label or title
        elif inp.get("aria-label") or inp.get("title") or inp.get("placeholder"):
            has_label = True
            
        if not has_label:
            unlabeled_count += 1
            issues.append(f"Unlabeled input detected (type={inp.get('type','text')}, id={inp_id})")

    if inputs:
        penalty = int((unlabeled_count / len(inputs)) * 80)
        score = max(0, 100 - penalty)
        
    return score, issues

def _audit_cta_clarity(soup: BeautifulSoup, business_profile: dict) -> tuple[int, list[str]]:
    """
    Evaluate action-oriented verbs tied to the business profile.
    """
    score = 100
    issues = []
    
    profile_label = business_profile.get("label", "Generic")
    target_verbs = []
    
    # Define profile-specific semantic verbs
    if "SaaS" in profile_label:
        target_verbs = ["demo", "trial", "signup", "register", "price", "get started", "schedule", "demo", "book"]
    elif "Local" in profile_label or "Restaurant" in profile_label:
        target_verbs = ["book", "reserve", "order", "call", "visit", "direction", "menu", "contact", "appointment"]
    elif "Commerce" in profile_label:
        target_verbs = ["buy", "cart", "shop", "add", "checkout", "purchase", "pay"]
    else:
        target_verbs = ["contact", "send", "message", "inquiry", "quote", "apply"]

    text_content = soup.get_text().lower()
    
    found_verbs = [v for v in target_verbs if v in text_content]
    if not found_verbs:
        score = 40
        issues.append(f"Missing high-intent verbs for {profile_label} profile (expected: {target_verbs[:3]}...)")
    elif len(found_verbs) < 2:
        score = 70
        issues.append(f"Low intensity of action verbs for {profile_label} profile.")

    return score, issues

def process(state: dict) -> dict:
    """
    Audit site for autonomous agent operability.

    Args:
        state: Pipeline state dictionary.

    Returns:
        Updated state with ``state["agentic_readiness"]``.
    """
    console.print("[bold blue]Node: Agentic Readiness[/bold blue] | Auditing agent operability...")

    raw_html = state.get("client_content_raw", "")
    business_profile = state.get("business_profile_summary", {})
    
    if not raw_html:
        console.print("[yellow]No raw HTML available. Skipping audit.[/yellow]")
        state["agentic_readiness"] = {
            "button_semantics_score": 0,
            "form_readability_score": 0,
            "cta_clarity_score": 0,
            "issues": ["No raw HTML found for extraction."],
            "notes": "Audit failed: Source content missing."
        }
        return state

    soup = BeautifulSoup(raw_html, "html.parser")
    
    # Run Audits
    btn_score, btn_issues = _audit_button_semantics(soup)
    form_score, form_issues = _audit_form_readability(soup)
    cta_score, cta_issues = _audit_cta_clarity(soup, business_profile)
    
    all_issues = btn_issues + form_issues + cta_issues
    
    # Summary Notes
    if btn_score > 80 and form_score > 80 and cta_score > 80:
        notes = "Excellent agentic operability. Semantic markers and clear CTAs are present."
    elif btn_score < 50 or form_score < 50:
        notes = "Poor agentic operability. High risk of autonomous agents failing to execute flows."
    else:
        notes = "Moderate agentic operability. Some semantic markers are missing or vague."

    state["agentic_readiness"] = {
        "button_semantics_score": btn_score,
        "form_readability_score": form_score,
        "cta_clarity_score": cta_score,
        "issues": all_issues,
        "notes": notes
    }

    console.print(
        f"[bold green]Agentic Audit Complete[/bold green] | "
        f"Semantic: [cyan]{btn_score}[/cyan] | Forms: [yellow]{form_score}[/yellow] | CTA: [magenta]{cta_score}[/magenta]"
    )

    return state
