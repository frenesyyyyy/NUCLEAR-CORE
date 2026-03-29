import os
import json
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()

def _render_profile_section(summary: dict) -> str:
    """Render Business Profile summary."""
    if not summary: return ""
    md = f"## 🏢 Business Intelligence Profile: {summary.get('label', 'General')}\n"
    md += f"- **Macro Industry:** {summary.get('macro_industry', 'N/A')}\n"
    md += f"- **Geo-Behavior:** {summary.get('geo_behavior', 'N/A')}\n"
    
    signals = summary.get("must_have_signals", [])
    if signals:
        md += "- **Required Signals:** " + ", ".join(signals[:5]) + ("..." if len(signals) > 5 else "") + "\n"
    
    personas = summary.get("persona_templates", [])
    if personas:
        md += "- **Target Personas:** " + ", ".join([p.get("persona") for p in personas]) + "\n"
    
    md += "\n---\n\n"
    return md

def _render_content_eng_section(eng: dict) -> str:
    """Render Content Engineering findings."""
    if not eng: return ""
    md = "## 🏗️ Content Engineering Analysis\n"
    
    if eng.get("is_extreme_degraded"):
        md += "> [!CAUTION]\n"
        md += "> **EXTREME DEGRADED DATA**: This analysis is derived from an extremely thin content payload (< 150 words). Recommendation confidence is tentative.  \n\n"

    md += f"- **LLM Readiness:** {eng.get('citation_readiness_detail', 'N/A')}\n"
    md += f"- **Answer-First Score:** {eng.get('answer_first_score', 0)}/100\n"
    md += f"- **Evidence Density:** {eng.get('evidence_density_score', 0)}/100\n"
    md += f"- **Chunkability:** {eng.get('chunkability_score', 0)}/100\n"
    
    tasks = eng.get("rewrite_tasks", [])
    if tasks:
        md += "### Key Optimization Tasks:\n"
        for t in tasks[:3]:
            md += f"- {t}\n"
    
    md += "\n---\n\n"
    return md

def _render_earned_media_section(earned_media: dict, taxonomy: dict) -> str:
    """Render Off-site Brand Trust and Citation context."""
    if not earned_media: return ""
    md = "## 🌐 Off-site Brand Trust & Source Taxonomy\n"
    md += f"- **Brand Strength Score:** {earned_media.get('strength_score', 0)}/100\n"
    md += f"- **Reputation Risk:** {earned_media.get('reputation_risk_score', 0)}/100\n"
    
    if taxonomy:
        md += f"- **Trust Mix Summary:** {taxonomy.get('trust_mix_summary', 'N/A')}\n"
        md += f"- **Citations Found:** Owned ({taxonomy.get('owned_count', 0)}), Earned ({taxonomy.get('earned_count', 0)}), Review ({taxonomy.get('review_count', 0)}), Forum ({taxonomy.get('forum_count', 0)})\n"
        risk = taxonomy.get("citation_source_risk", "") # This is a list in refined model, but handled as str/list
        if risk:
            if isinstance(risk, list): risk = ", ".join(risk)
            if risk != "Low":
                md += f"- **⚠️ Citation Risk:** {risk}\n"
            
    md += "\n---\n\n"
    return md

def _render_model_analytics_section(analytics: dict) -> str:
    """Render Model-Aware Visibility Analytics (Active Only)."""
    if not analytics: return ""
    md = "## 📊 Tiered Visibility Intelligence\n"
    
    diag = analytics.get("stress_test_diagnostics", {})
    if diag:
        md += f"### 🔍 Search Intelligence Diagnostics\n"
        md += f"- **Discovery Query Volume:** {diag.get('query_count', 0)} realistic search sets\n"
        md += f"- **Bucket Diversity:** {diag.get('bucket_diversity', 0)} intent categories detected (Best-of, Educational, etc.)\n"
        md += f"- **Point Conversion Rate:** {diag.get('point_conversion', 0):.1f} avg points/query\n"
        
        fallback_count = diag.get("fallback_count", 0)
        if fallback_count > 0:
            md += f"- **Profile-Aware Fallbacks:** {fallback_count} templates used (No taxonomical leakage)\n"
            if fallback_count > (diag.get('query_count', 0) * 0.5):
                md += "\n> [!WARNING]\n"
                md += "> **HIGH FALLBACK RATE**: Discovery queries are heavily reliant on profile templates due to low topic gap density on-site.  \n"

    md += f"\n- **Estimated AI Mention Volume:** ~{analytics.get('position_adjusted_word_count', 0)} words\n"
    
    tier_metrics = analytics.get("tier_metrics", {})
    if tier_metrics:
        md += "### 🎯 Discovery Hit Rate (by Tier):\n"
        for t_key, stats in tier_metrics.items():
            label = {
                "blind_discovery": "Blind Discovery (T1)",
                "contextual_discovery": "Contextual Discovery (T2)",
                "branded_validation": "Branded Validation (T3)"
            }.get(t_key, t_key)
            
            queries = stats.get("queries", 0)
            matches = stats.get("matches", 0)
            hit_rate = (matches / queries * 100) if queries > 0 else 0.0
            
            md += f"- **{label}:** {hit_rate:.1f}% ({matches}/{queries})\n"
        
        # Agency-Grade Warning Logic
        t12_total = tier_metrics.get("blind_discovery", {}).get("queries", 0) + tier_metrics.get("contextual_discovery", {}).get("queries", 0)
        t12_matches = tier_metrics.get("blind_discovery", {}).get("matches", 0) + tier_metrics.get("contextual_discovery", {}).get("matches", 0)
        t3_matches = tier_metrics.get("branded_validation", {}).get("matches", 0)
        t3_total = tier_metrics.get("branded_validation", {}).get("queries", 0)
        
        t12_rate = (t12_matches / t12_total * 100) if t12_total > 0 else 0.0
        t3_rate = (t3_matches / t3_total * 100) if t3_total > 0 else 0.0
        
        if t3_rate > 50 and t12_rate < 20:
             md += "\n> [!IMPORTANT]\n"
             md += "> **VISIBILITY GAP ALERT**: Brand is recognized when named directly, but remains largely absent from true discovery queries.\n"

    risks = analytics.get("engine_specific_risks", {})
    if risks.get("Perplexity"):
        md += "\n### Engine specific risks:\n"
        for r in risks["Perplexity"][:2]:
            md += f"- {r}\n"
            
    md += "\n---\n\n"
    return md

def _render_agency_action_plan(blueprint: dict) -> str:
    """Render the Phased Agency Strategic Roadmap (30/60/90 Day)."""
    if not blueprint: return ""
    
    md = "## 📈 Agency Strategic Action Plan\n"
    md += "> This roadmap prioritizes actions based on visibility impact and implementation complexity.\n\n"

    # Category Mapping
    phases = {
        "30-Day Quick Wins": [],
        "60-Day Structural Fixes": [],
        "90-Day Authority & Visibility Expansion": []
    }

    # Phasing Logic
    # 30-Day: Trust Actions + technical
    trust = blueprint.get("trust_actions", [])
    crawler = blueprint.get("crawler_actions", [])
    phases["30-Day Quick Wins"].extend(trust)
    phases["30-Day Quick Wins"].extend(crawler)

    # 60-Day: Content Priorities + Schema
    pages = blueprint.get("page_priorities", [])
    schema = blueprint.get("schema_actions", [])
    phases["60-Day Structural Fixes"].extend(pages[:3])
    phases["60-Day Structural Fixes"].extend(schema)

    # 90-Day: Discovery / Intent Gaps + remaining pages
    discovery = blueprint.get("discovery_gap_actions", [])
    phases["90-Day Authority & Visibility Expansion"].extend(discovery)
    phases["90-Day Authority & Visibility Expansion"].extend(pages[3:])

    for phase_name, actions in phases.items():
        if not actions: continue
        md += f"### {phase_name}\n"
        for act in actions:
            title = act.get("action_title", "Unnamed Action")
            why = act.get("why_it_matters", "Strategic improvement")
            evidence = act.get("evidence_basis", "Profile inference")
            priority = act.get("priority", "Medium")
            
            # Confidence logic based on evidence string
            confidence = "High" if "direct-evidence" in evidence.lower() else "Medium"
            if "partial-evidence" in evidence.lower(): confidence = "Low (Tentative)"

            expected_impact = act.get("expected_impact", "High visibility lift")

            md += f"#### **{title}**\n"
            md += f"- **Why this matters for visibility:** {why}\n"
            md += f"- **Expected Impact:** {expected_impact}\n"
            md += f"- **Evidence basis:** {evidence}\n"
            md += f"- **Priority:** {priority} | **Confidence level:** {confidence}\n\n"
            
    md += "---\n\n"
    return md

def _render_blueprint_section(blueprint: dict) -> str:
    if not blueprint: return ""
    md = "## 🛠️ Implementation Specs (Dev-Ready)\n"
    
    # Robots Patch
    patch = blueprint.get("robots_patch", "")
    if patch and "#" not in patch[:2]:
        md += "### Recommended robots.txt Patch:\n"
        md += f"```text\n{patch}\n```\n\n"
    
    md += "*Full technical JSON-LD blocks are available in the audit_data.json export.*\n"
    md += "\n---\n\n"
    return md

def _render_agentic_section(agentic: dict) -> str:
    if not agentic: return ""
    md = "## 🤖 Agentic Readiness (Future-Proofing Audit)\n"
    md += f"- **Button Semantics:** {agentic.get('button_semantics_score', 0)}/100\n"
    md += f"- **Form Readability:** {agentic.get('form_readability_score', 0)}/100\n"
    md += f"- **CTA Clarity:** {agentic.get('cta_clarity_score', 0)}/100\n"
    md += f"\n**Notes:** {agentic.get('notes', 'N/A')}\n"
    md += "\n---\n\n"
    return md

def process(state: dict) -> dict:
    console.print("[cyan]Finalizer Node[/cyan]: Exporting hardened agency audit artifacts...")
    
    exports_dir = "exports"
    os.makedirs(exports_dir, exist_ok=True)
    
    run_id = state.get("run_id", "Unknown")
    locale = state.get("locale", "en")
    
    integrity_status = state.get("audit_integrity_status", "valid")
    integ_reasons = state.get("audit_integrity_reasons", [])
    fetch_notes = state.get("content_fetch_notes", "")
    
    # 1. JSON Export
    json_export_path = os.path.join(exports_dir, f"geo_audit_{run_id}.json")
    try:
        with open(json_export_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
    except Exception as e:
        console.print(f"[bold red]NODE_FAILED[/bold red]: JSON Export: {e}")

    # 2. SQLite Record
    try:
        db_path = "geo_audit.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA table_info(audits)")
        columns = [col[1] for col in cursor.fetchall()]
        
        data_map = {
            "run_id": run_id,
            "started_at": state.get("started_at", ""),
            "url": state.get("url", ""),
            "locale": locale,
            "target_industry": state.get("target_industry", ""),
            "metrics_json": json.dumps(state.get("metrics", {})),
            "integrity_status": integrity_status,
            "integrity_score": state.get("audit_integrity_score", 0),
            "overall_pipeline_readiness": state.get("overall_pipeline_readiness", "standard"),
            "roi_verified": int(state.get("roi_verified", False)),
            "export_paths": json.dumps({"json": f"exports/geo_audit_{run_id}.json", "md": f"exports/geo_audit_{run_id}.md"})
        }
        
        active_map = {k: v for k, v in data_map.items() if k in columns}
        col_names = ", ".join(active_map.keys())
        placeholders = ", ".join(["?"] * len(active_map))
        cursor.execute(f"INSERT OR REPLACE INTO audits ({col_names}) VALUES ({placeholders})", list(active_map.values()))
        conn.commit()
    except Exception as e:
        console.print(f"[bold red]NODE_FAILED[/bold red]: SQLite Record: {e}")

    # 3. Agency Markdown Report
    try:
        md_file = os.path.join(exports_dir, f"geo_audit_{run_id}.md")
        report_title = "Generative Visibility Intelligence Report" if locale != "it" else "Report di Generative Visibility Intelligence"
        
        md_content = f"# ☢️ NUCLEAR AI: {report_title}\n\n"
        
        # 3a. Executive Summary & Agency Verdict (Requirement C/E)
        blueprint = state.get("implementation_blueprint", {})
        all_strategic = blueprint.get("all_strategic_actions", [])
        source_mode = state.get("source_of_truth_mode", "hybrid")
        
        # Verdict Logic (Requirement C/D)
        verdict = state.get("agency_verdict", "REQUIRES ANALYST REVIEW")
        verdict_reason = state.get("agency_verdict_reason", "Fallback: Unverified extraction.")

        # Action Quality Check (Backup safety check on actions)
        generic_only = all("Schema" in a.get("action_title", "") or "robots.txt" in a.get("action_title", "") for a in all_strategic)
        if verdict == "CLIENT READY" and (len(all_strategic) < 3 or generic_only):
            verdict = "REQUIRES ANALYST REVIEW"
            if len(all_strategic) < 3:
                verdict_reason = "Insufficient strategic depth (fewer than 3 actions generated)."
            else:
                verdict_reason = "Action plan is purely technical; lacks business-specific strategy."

        # Add Visual Verdict Banner
        v_color = "green" if "CLIENT READY" == verdict else ("yellow" if "REVIEW" in verdict else "red")
        md_content += f"## ⚖️ Audit Verdict: **{verdict}**\n"
        md_content += f"> **Verdict Basis:** {verdict_reason}\n\n"
        
        tier_stats = state.get("stress_test_tier_stats", {})
        blind_hits = tier_stats.get("blind_discovery", {}).get("matches", 0)
        
        if blind_hits == 0:
            md_content += "> [!WARNING]\n"
            md_content += "> **ZERO DISCOVERY VISIBILITY**: Brand has no visibility in blind category searches.\n"
            md_content += "> Current performance depends entirely on strictly branded or direct query matches.\n\n"
            
        md_content += "---\n\n"

        # 3b. Agency Decision Summary (Requirement E)
        md_content += "## 🎯 Agency Decision Summary\n"
        md_content += f"- **Verdict:** {verdict}\n"
        md_content += f"- **Rationale:** {verdict_reason}\n"
        md_content += f"- **Key Risks:** {'Thin extraction payload' if integrity_status != 'valid' else 'None detected'}, {'Off-site bias' if source_mode == 'offsite_only' else 'Standard market noise'}\n"
        
        next_step = "Proceed to client presentation."
        if verdict == "NOT CLIENT READY":
            next_step = "Retry extraction with custom proxies or manual site input."
        elif verdict == "REQUIRES ANALYST REVIEW":
            next_step = "Manually verify profile-inferred actions against on-site reality."
        md_content += f"- **Recommended Next Step:** {next_step}\n\n---\n\n"

        # 3c. Integrity Banners & Rescue Reporting
        if source_mode == "offsite_only":
            md_content += "> [!CAUTION]\n"
            md_content += "> **OFF-SITE INFERENCE ONLY**: Site extraction failed or was blocked. Findings are derived exclusively from off-site intelligence and corroborated market profiles.  \n"
            md_content += "> **Integrity Level:** UNVERIFIED (Off-site Grounding)  \n"
            md_content += "> **Action:** Recommendations below are strategic hypotheses and require manual verification on-site.\n\n"
        elif integrity_status == "invalid":
            md_content += "> [!CAUTION]\n"
            md_content += "> **INVALID AUDIT ALERT**: Site extraction failed entirely.  \n"
            
            # Refined Research Success Check: 
            # We have info if we have serper results OR competitors OR authority entities
            raw_data = state.get("raw_data_complete", {})
            has_serper = len(raw_data.get("serper_results", [])) > 0
            has_entities = len(raw_data.get("competitor_entities", [])) > 0 or len(raw_data.get("authority_entities", [])) > 0
            
            if has_serper or has_entities:
                md_content += "> **Note:** Site-native extraction failed. This report reflects off-site visibility intelligence and profile-inferred recommendations, not a verified on-site audit.  \n"
            
            md_content += f"> **Reason(s):** {', '.join(integ_reasons) if integ_reasons else fetch_notes or 'Access blocked.'}  \n"
            md_content += "> **Action:** Recommendations below are strictly profile-inferred and require manual validation.\n\n"
            
        elif integrity_status == "degraded":
            eng = state.get("content_engineering", {})
            if eng.get("thin_but_semantic"):
                md_content += "> [!IMPORTANT]\n"
                md_content += "> **RESCUED SITE EVIDENCE**: Partial extraction but high structural density (Headings/CTAs/Schema) detected.  \n"
                md_content += f"> **Extraction Report:** {', '.join(integ_reasons) if integ_reasons else 'Thin content rescued.'}  \n"
                md_content += "> **Action:** Strategic signals are usable; recommendations are evidence-grounded despite low text volume.\n\n"
            else:
                md_content += "> [!WARNING]\n"
                md_content += "> **DEGRADED AUDIT ALERT**: Thin site extraction with weak semantic signals.  \n"
                if "ScraperAPI" in fetch_notes:
                    md_content += f"> **Rescue Log:** {fetch_notes}  \n"
                md_content += f"> **Reason(s):** {', '.join(integ_reasons) if integ_reasons else 'Extraction issues.'}  \n"
                md_content += "> **Action:** On-site recommendations have limited reliability.\n\n"
            
        elif integrity_status == "valid":
            if "ScraperAPI" in fetch_notes:
                 md_content += "> [!NOTE]\n"
                 md_content += f"> **Anti-Block Rescue:** {fetch_notes}  \n"
                 md_content += "> The system successfully bypassed bot-protection to extract high-quality ground-truth content.\n\n"

        md_content += f"**Target URL:** {state.get('url', 'N/A')}  \n"
        md_content += f"**Brand:** {state.get('brand_name', 'N/A')} | **Industry:** {state.get('target_industry', 'N/A')}  \n\n---\n\n"
        
        # 3d. Executive Matrix
        metrics = state.get("metrics", {})
        md_content += "## 📊 Visibility Health Matrix\n"
        md_content += f"- **Visibility Context Index:** {metrics.get('Entity Consensus', 0)}/100\n"
        md_content += f"- **Defensible Evidence Depth:** {metrics.get('Defensible Evidence Depth', 0)}/100\n"
        md_content += f"- **Visibility Uncertainty Risk:** {metrics.get('Hallucination Risk', 100)}%  \n"
        md_content += f"- **Data Confidence Score:** {state.get('confidence_score', 0)}/100\n"
        md_content += f"- **Classification Mapping:** {state.get('business_profile_key', 'N/A')} ({state.get('classification_reliability', 'Standard')} Reliability)\n\n"

        if state.get("validator_notes"):
            md_content += f"> **Validation Verdict:** {state.get('validator_notes')}\n\n"
        
        md_content += "---\n\n"
        
        # 3e. Intelligence Layers
        md_content += _render_profile_section(state.get("business_profile_summary", {}))
        md_content += _render_content_eng_section(state.get("content_engineering", {}))
        md_content += _render_earned_media_section(state.get("earned_media", {}), state.get("source_taxonomy", {}))
        md_content += _render_model_analytics_section(state.get("model_analytics", {}))
        md_content += _render_agency_action_plan(state.get("implementation_blueprint", {}))
        md_content += _render_blueprint_section(state.get("implementation_blueprint", {}))
        md_content += _render_agentic_section(state.get("agentic_readiness", {}))
        
        md_content += f"\n*Report generated by Nuclear AI Platform on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n"

        with open(md_file, "w", encoding="utf-8") as f:
            f.write(md_content)
            
    except Exception as e:
        console.print(f"[bold red]NODE_FAILED[/bold red]: Markdown Export: {e}")

    state.update({"status": "Completed", "markdown_report_path": md_file if 'md_file' in locals() else ""})
    console.print(f"   [green]Finalizer Complete[/green] | Artifacts secured for {run_id}.")
    return state
