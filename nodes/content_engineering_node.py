"""Workflow node for evaluating content for LLM retrieval and generation."""
import re
from rich.console import Console
from nodes.business_profiles import DEFAULT_PROFILE_KEY

console = Console()

def _evaluate_answer_first(content: str) -> int:
    """Check if the first sentences after headings contain direct definitional answers."""
    score = 50
    # Split by headings
    sections = re.split(r'(?m)^#+\s+.*$', content)
    if not sections or len(sections) == 1:
        # No headings, check the very first paragraph
        sections = [content]
        
    direct_hits = 0
    total_sections = 0
    
    definitional_verbs = [r'\bis a\b', r'\bare\b', r'\brefers to\b', r'\bprovides\b', r'\bdesigned to\b', r'\bmeans\b']
    verb_pattern = re.compile('|'.join(definitional_verbs), re.IGNORECASE)
    
    for sec in sections:
        sec = sec.strip()
        if not sec: continue
        total_sections += 1
        
        # Get first paragraph
        first_para = sec.split('\n\n')[0].strip()
        sentences = re.split(r'[.!?]+', first_para)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if not sentences: continue
        
        # Check first 2 sentences for definitional structures and concise length
        first_two = " ".join(sentences[:2])
        if len(first_two.split()) < 40 and verb_pattern.search(first_two):
            direct_hits += 1
            
    if total_sections > 0:
        ratio = direct_hits / total_sections
        score += int(ratio * 50)  # Up to +50
        if ratio < 0.2:
            score -= 20
            
    return max(0, min(100, score))


def _evaluate_evidence_density(content: str, profile_key: str = DEFAULT_PROFILE_KEY) -> int:
    """Evaluate presence of numbers, stats, dates, entities, and certifications.
       If platform profile, reward transactional and UX tokens instead of pure text density.
    """
    PLATFORM_PROFILES = {"marketplace", "consumer_saas", "ecommerce_brand"}
    score = 30
    content_lower = content.lower()
    
    if profile_key in PLATFORM_PROFILES:
        # Platform/App evaluation: transactional UI, app stores, partner workflows
        app_links = len(re.findall(r'(?i)app\s*store|play\s*store|download|install|ios|android', content_lower))
        partner_ux = len(re.findall(r'(?i)partner|rider|driver|seller|vendor|merchant|become a', content_lower))
        cta_ux = len(re.findall(r'(?i)login|sign\s*up|register|get\s*started|order\s*now|checkout', content_lower))
        support_ux = len(re.findall(r'(?i)faq|help\s*center|support|terms|policies', content_lower))
        
        total_hits = (app_links * 3) + (partner_ux * 2) + (cta_ux * 2) + (support_ux * 2)
        words = max(1, len(content.split()))
        
        # Platforms have less words, so density is computed aggressively higher
        density = (total_hits / words) * 100 * 3
        
        if density > 10 or total_hits > 15:
            score += 70
        elif density > 5 or total_hits > 8:
            score += 40
        elif density > 2 or total_hits > 3:
            score += 15
        else:
            score -= 15
            
    else:
        # Standard Knowledge/B2B evaluation
        stats_count = len(re.findall(r'\d+(?:\.\d+)?%?', content))
        money_count = len(re.findall(r'[$€£]\d+', content))
        quote_count = len(re.findall(r'\"([^\"]*)\"', content))
        date_count  = len(re.findall(r'\b(19|20)\d{2}\b|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}\b', content, re.IGNORECASE))
        cert_count  = len(re.findall(r'\b(?:ISO|SOC\s?2|HIPAA|GDPR|PCI-DSS|PCI|IEEE|FDA)\b', content, re.IGNORECASE))
        
        entity_count = len(re.findall(r'[a-z][.,]?\s+([A-Z][a-z]+)', content))
        
        total_hits = stats_count + (money_count * 2) + (quote_count * 2) + (date_count * 1.5) + (cert_count * 2) + (entity_count * 0.5)
        
        words = max(1, len(content.split()))
        density = (total_hits / words) * 100
        
        if density > 8:
            score += 70
        elif density > 4:
            score += 40
        elif density > 1:
            score += 15
        else:
            score -= 15
        
    return max(0, min(100, int(score)))


def _evaluate_chunkability(content: str) -> int:
    """Evaluate if content is well-structured and penalize weak reference openings."""
    score = 50
    words = len(content.split())
    if words < 50:
        return 50 # Not enough text to judge
        
    paragraphs = [p.strip() for p in content.split('\n\n') if p.strip()]
    bullets = len(re.findall(r'(?m)^[-*•]\s', content))
    headers = len(re.findall(r'(?m)^#+\s', content))
    
    avg_words_per_para = words / max(1, len(paragraphs))
    
    if avg_words_per_para < 60:
        score += 20
    elif avg_words_per_para > 150:
        score -= 30
        
    if bullets > 0 or headers > 0:
        score += 30
        
    # Penalize weak pronoun openings (e.g. "This is", "It has") which break context when chunked
    weak_starts = 0
    weak_pattern = re.compile(r'^(This|That|It|They|These|Those|Such)\s+(is|are|was|were|will|has|have|can|could|would|should|makes|does|seems|appears)', re.IGNORECASE)
    
    for p in paragraphs:
        if weak_pattern.search(p):
            weak_starts += 1
            
    if weak_starts > 0:
        penalty = min(30, weak_starts * 5)
        score -= penalty
        
    return max(0, min(100, int(score)))


def _evaluate_llm_style(content: str) -> int:
    """Penalize fluff and generic marketing jargon using ratios."""
    score = 100
    words = max(1, len(content.split()))
    content_lower = content.lower()
    
    jargon = [
        "leading provider", "innovative", "synergy", "cutting-edge", 
        "best-in-class", "revolutionary", "game-changer", "world-class",
        "state-of-the-art", "premier", "seamless", "disruptive",
        "next-generation", "empower", "leverage", "paradigm shift"
    ]
    
    jargon_hits = sum(content_lower.count(j) for j in jargon)
    
    # Also check intro bloat / generic framing
    first_para = content.strip().split('\n\n')[0].lower()
    intro_jargon = sum(first_para.count(j) for j in jargon)
    if intro_jargon >= 2 or len(first_para.split()) > 60 and not any(v in first_para for v in ['is a', 'provides']):
        score -= 15 # Delaying the actual answer
        
    density = (jargon_hits / words) * 100
    
    if density > 3:
        score -= 50
    elif density > 1.5:
        score -= 30
    elif density > 0.5:
        score -= 10
        
    return max(0, min(100, int(score)))


def _check_profile_gaps(content: str, profile_summary: dict, schema_counts: dict) -> list:
    """Identify missing signals required by the business profile, alias-aware."""
    gaps = []
    content_lower = content.lower()
    
    must_haves = profile_summary.get("must_have_signals", [])
    
    # Map high-level requirements to common semantic instances
    aliases = {
        "pricing": ["pricing", "price", "cost", "fee", "plans", "subscription", "tariffe", "prezzi"],
        "case studies": ["case studies", "case study", "customer stories", "success stories", "casi studio"],
        "reviews": ["reviews", "review", "testimonial", "testimonials", "ratings", "recensioni", "opinioni"],
        "features": ["features", "capabilities", "functionality", "funzionalità"],
        "integrations": ["integrations", "plugins", "connects with", "integrazioni"],
        "author profiles": ["author", "about the author", "written by", "autore"],
        "doctor profiles": ["doctor", "dr.", "physician", "dott.", "dottore", "medico"],
        "service pages": ["services", "what we do", "our offerings", "servizi"],
        "course outcomes": ["outcomes", "what you will learn", "syllabus", "curriculum", "risultati"],
        "menu": ["menu", "dishes", "food", "drinks", "piatti"],
        "location": ["location", "address", "map", "directions", "indirizzo", "dove siamo"]
    }
    
    def _find_match(signal: str, text: str) -> bool:
        signal_lower = signal.lower()
        if signal_lower in text: return True
        for key, words in aliases.items():
            if key in signal_lower or signal_lower in key:
                if any(w in text for w in words):
                    return True
        return False
        
    for req in must_haves:
        if "schema" in req.lower():
            target_schema = req.lower().split()[0].capitalize() # e.g. "Product schema" -> "Product"
            if target_schema not in schema_counts and target_schema.lower() not in [k.lower() for k in schema_counts.keys()]:
                gaps.append(f"Missing schema: {target_schema}")
        else:
            if not _find_match(req, content_lower):
                gaps.append(f"Missing content signal: {req}")
                
    return gaps


def process(state: dict) -> dict:
    """
    Evaluates client content for LLM optimization readiness using heuristics.
    
    Args:
        state: workflow state dictionary.
        
    Returns:
        Updated state with 'content_engineering' namespace.
    """
    new_state = state.copy()
    
    console.print("[bold blue]Node: Content Engineering[/bold blue] | Evaluating LLM retrieval readiness...")
    
    content = state.get("client_content_clean", "")
    profile_summary = state.get("business_profile_summary", {})
    schema_counts = state.get("schema_type_counts", {})
    
    geo_behavior = profile_summary.get("geo_behavior", "standard_retrieval")
    risk_factors = profile_summary.get("risk_factors", [])
    
    # Initialize all locals safely at the top
    notes = "Deterministic heuristic evaluation completed successfully."
    is_extreme_degraded = False
    citation_readiness = "N/A"
    rewrite_tasks = []
    
    # Graceful fallback if content is missing
    if not content or len(content.strip()) < 50:
        console.print("[yellow]Warning: Content missing or too thin for engineering evaluation.[/yellow]")
        new_state["content_engineering"] = {
            "answer_first_score": 0,
            "evidence_density_score": 0,
            "chunkability_score": 0,
            "llm_style_score": 0,
            "citation_readiness_detail": "Not Ready (Insufficient Content)",
            "rewrite_tasks": ["Add substantial core content to the page."],
            "profile_specific_gaps": [],
            "notes": "Evaluation skipped due to insufficient content payload.",
            "is_extreme_degraded": True
        }
        return new_state
        
    # Run heuristics
    profile_key = state.get("business_profile_key", DEFAULT_PROFILE_KEY)
    
    answer_first_score = _evaluate_answer_first(content)
    evidence_density_score = _evaluate_evidence_density(content, profile_key)
    chunkability_score = _evaluate_chunkability(content)
    llm_style_score = _evaluate_llm_style(content)
    
    # Check gaps
    profile_gaps = _check_profile_gaps(content, profile_summary, schema_counts)
    
    # Synthesis & Strategy
    if answer_first_score < 50:
        rewrite_tasks.append(
            f"Invert content structure to provide direct definitional answers immediately below headers. "
            f"This is critical for '{geo_behavior}' profiles to win direct-answer snippets."
        )
    if evidence_density_score < 50:
        risk_str = (f"This mitigates common '{profile_summary.get('label', 'industry')}' risk factors like: {risk_factors[0]}." 
                    if risk_factors else "This provides essential anchoring for AI engines.")
        rewrite_tasks.append(
            f"Inject verifiable concrete data (statistics, dates, named entities, certifications). {risk_str}"
        )
    if chunkability_score < 50:
        rewrite_tasks.append(
            "Break up long text blocks and avoid weak, pronoun-led paragraph openings "
            "(e.g., 'This is') that lose context when chunked into vector databases."
        )
    if llm_style_score < 50:
        rewrite_tasks.append(
            "Strip away generic marketing fluff and 'intro-bloat'. Use neutral, factual language that states "
            "the core entity immediately upon section start."
        )
        
    avg_score = (answer_first_score + evidence_density_score + chunkability_score + llm_style_score) / 4
    
    if avg_score >= 80:
        citation_readiness = "Excellent (LLM-Native)"
    elif avg_score >= 50:
        citation_readiness = "Moderate (Needs Optimization)"
    else:
        citation_readiness = "Poor (Difficult for RAG to parse)"
        
    # v4.5 Recalibration: Semantic-Aware Thin Content Safeguard
    word_count = len(content.split())
    depth = state.get("client_content_depth", {})
    signals = depth.get("semantic_signals", {})
    has_rich_signals = signals.get("schema_signal_count", 0) >= 2 or signals.get("cta_count", 0) >= 2
    
    is_extreme_degraded = state.get("audit_integrity_status") == "degraded" and word_count < 150
    
    if is_extreme_degraded:
        if has_rich_signals:
            # Case 1: thin_but_semantic (Meaningful structure exists)
            citation_readiness = f"[PARTIAL-RICH] {citation_readiness}"
            notes = f"[RESCUE SUCCESS] Low word count ({word_count}) but high structural density. Evidence is usable but require manual verification."
            rewrite_tasks.insert(0, "[NOTE] Valid mapping from structural signals (CTAs/Schema) detected despite thin text extraction.")
        else:
            # Case 2: thin_and_weak (Truly low evidence)
            citation_readiness = f"[TENTATIVE] {citation_readiness}"
            notes = f"[CAUTION: Thin Content] Weak semantic markers detected. Audit findings are speculative."
            rewrite_tasks.insert(0, "[CRITICAL] Audit based on extremely thin extraction with no compensating structural signals.")

    new_state["content_engineering"] = {
        "answer_first_score": int(answer_first_score),
        "evidence_density_score": int(evidence_density_score),
        "chunkability_score": int(chunkability_score),
        "llm_style_score": int(llm_style_score),
        "citation_readiness_detail": citation_readiness,
        "rewrite_tasks": rewrite_tasks,
        "profile_specific_gaps": profile_gaps,
        "notes": notes,
        "is_extreme_degraded": is_extreme_degraded,
        "thin_but_semantic": is_extreme_degraded and has_rich_signals
    }
    
    console.print(f"[bold green]Evaluation Complete[/bold green] | Readiness: [cyan]{citation_readiness}[/cyan] (Avg Score: {avg_score:.1f})")
    
    return new_state
