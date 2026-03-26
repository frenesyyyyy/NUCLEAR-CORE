import os
import json
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()

def process(state: dict) -> dict:
    console.print("[cyan]Finalizer Node[/cyan]: Exporting audit artifacts...")
    
    # Establish base paths
    exports_dir = "exports"
    os.makedirs(exports_dir, exist_ok=True)
    
    run_id = state.get("run_id", "Unknown")
    locale = state.get("locale", "en")
    
    # Variables for state tracking
    sqlite_saved = False
    json_export_path = ""
    markdown_report_path = ""
    streamlit_ready = False
    
    # 1. JSON Export
    try:
        json_file = os.path.join(exports_dir, f"geo_audit_{run_id}.json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        json_export_path = json_file
        console.print(f"[green]Finalizer Node[/green]: JSON export saved to {json_export_path}")
    except Exception as e:
        console.print(f"[bold red]NODE_FAILED[/bold red]: JSON Export: {e}")

    # 2. SQLite Record
    try:
        db_path = "geo_audit.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS audits (
            run_id TEXT PRIMARY KEY,
            started_at TEXT,
            url TEXT,
            locale TEXT,
            target_industry TEXT,
            metrics_json TEXT,
            citation_status TEXT,
            projected_traffic_lift TEXT,
            confidence_score INTEGER,
            validation TEXT,
            roi_verified BOOLEAN,
            export_paths TEXT
        )
        ''')
        
        # Upgrade existing DBs seamlessly
        new_columns = [
            ("metrics_json", "TEXT"), ("citation_status", "TEXT"), 
            ("projected_traffic_lift", "TEXT"), ("confidence_score", "INTEGER"), 
            ("export_paths", "TEXT")
        ]
        for col, col_type in new_columns:
            try:
                cursor.execute(f"ALTER TABLE audits ADD COLUMN {col} {col_type}")
            except sqlite3.OperationalError:
                pass
        
        # Need to determine Markdown path prior if possible, but it hasn't been written yet.
        # We'll just build a placeholder export_paths JSON and the real export paths are saved to state.
        export_paths = json.dumps({"json": f"exports/geo_audit_{run_id}.json", "md": f"exports/geo_audit_{run_id}.md"})
        
        cursor.execute('''
        INSERT OR REPLACE INTO audits 
        (run_id, started_at, url, locale, target_industry, metrics_json, citation_status, projected_traffic_lift, confidence_score, validation, roi_verified, export_paths)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            run_id,
            state.get("started_at", ""),
            state.get("url", ""),
            locale,
            state.get("target_industry", ""),
            json.dumps(state.get("metrics", {})),
            state.get("citation_status", ""),
            state.get("projected_traffic_lift", ""),
            state.get("confidence_score", 0),
            state.get("validation", ""),
            state.get("roi_verified", False),
            export_paths
        ))
        
        conn.commit()
        conn.close()
        sqlite_saved = True
        console.print(f"[green]Finalizer Node[/green]: SQLite record saved to {db_path}")
    except Exception as e:
        console.print(f"[bold red]NODE_FAILED[/bold red]: SQLite Export: {e}")

    # 3. Branded Markdown Report
    try:
        md_file = os.path.join(exports_dir, f"geo_audit_{run_id}.md")
        
        # Determine labels based on locale
        if locale == "it":
            title = "Report di Generative Visibility Intelligence"
            url_label = "URL Destinazione"
            ind_label = "Settore"
            aud_label = "Pubblico Principale"
            met_label = "Intelligence Matrix"
            ent_label = "Visibility Index (Entity Consensus)"
            info_label = "Information Gain (Mancanze/Upside)"
            risk_label = "Hallucination Risk (Rischio Errori AI)"
            conf_label = "Data Confidence Score"
            ev_label = "Sintesi Copertura Evidenze"
            cit_label = "Stato Citazioni"
            read_label = "Livello Readiness"
            roi_label = "Proiezioni & Validazione ROI"
            rec_label = "Agency Recommendation Pack"
            strat_label = "Evidenze Strutturate"
            lim_label = "Limitazioni nell'Analisi"
            based_on_ev = "Basato sulle evidenze disponibili..."
            lim_data = "I dati limitati indicano..."
        else:
            title = "Generative Visibility Intelligence Report"
            url_label = "Target URL"
            ind_label = "Industry"
            aud_label = "Target Audience"
            met_label = "Agency Intelligence Matrix"
            ent_label = "Visibility Index (Entity Consensus)"
            info_label = "Information Gain (Content Upside)"
            risk_label = "Hallucination Risk"
            conf_label = "Data Confidence Score"
            ev_label = "Evidence Coverage"
            cit_label = "Citation Status"
            read_label = "Readiness Level"
            roi_label = "Lift Projection & ROI"
            rec_label = "Agency Recommendation Pack"
            strat_label = "Structured Evidence"
            lim_label = "Limitations & Missing Data"
            based_on_ev = "Based on available evidence..."
            lim_data = "Limited data suggests..."
            
        metrics = state.get("metrics", {})
        confidence = state.get("confidence_score", 0)
        extraction_integrity = state.get("extraction_integrity", confidence)
        context_reliability  = state.get("context_reliability", confidence)
        
        class_notes = state.get("classification_notes", "")
        classification_notice = ""
        if class_notes and "confirmed" not in class_notes.lower():
            classification_notice = f"\n> **⚠️ Internal Context Correction:** L'AI iniziale ha mal interpretato il mercato a causa di segnali ambigui; il contesto è stato corretto d'ufficio tramite l'analisi semantica profonda.\n> *Note: {class_notes}*\n"
        
        limitations = ""
        if confidence < 60:
            limit_msg = state.get("evidence_limitations", "Low confidence due to parsing limitations or thin content.")
            limitations = f"\n> **⚠️ {lim_label}:** {limit_msg} Metrics may be volatile. {lim_data}\n"
        else:
            limitations = f"\n> **ℹ️ {lim_label}:** {based_on_ev}\n"
            
        schema_types = state.get("schema_type_counts", {})
        schema_str = ", ".join([f"{k} ({v})" for k,v in schema_types.items()]) if schema_types else "None detected"
        
        try:
            rec_data = json.loads(state.get("geo_recommendation_pack", "[]"))
            if isinstance(rec_data, list) and len(rec_data) > 0:
                rec_md = ""
                for r in rec_data:
                    r_title = r.get("title", "Azione Strategica")
                    r_rat = r.get("rationale", "")
                    r_pri = r.get("priority", "Medium")
                    r_type = r.get("implementation_type", "Content")
                    
                    pri_icon = "🔴" if "high" in r_pri.lower() else ("🟡" if "med" in r_pri.lower() else "🟢")
                    rec_md += f"### {pri_icon} {r_title}\n**Priority:** {r_pri} | **Type:** {r_type}\n{r_rat}\n\n"
            else:
                rec_md = str(state.get("geo_recommendation_pack", "N/A"))
        except:
            rec_md = str(state.get("geo_recommendation_pack", "N/A"))
            
        # Stress Test Rendering
        stress_tests = state.get("stress_test_log", [])
        stress_md = ""
        if stress_tests:
            stress_md = "## 🔍 AI Search Stress Test\n*Real-time queries executed against Perplexity to test brand presence.*\n\n| Tier | Search Query | Result |\n|---|---|---|\n"
            for test in stress_tests:
                t_tier = {
                    "blind_discovery": "Blind Discovery",
                    "contextual_discovery": "Contextual",
                    "branded_validation": "Branded Validation"
                }.get(test.get("tier", "Unknown"), test.get("tier", "Unknown"))
                t_query = test.get("query", "")
                t_res = "✅ Matched" if test.get("matched") else "❌ Missed"
                stress_md += f"| {t_tier} | *{t_query}* | {t_res} |\n"
            stress_md += "\n---\n\n"

        # Italian Trust Signals Rendering
        italian_trust_md = ""
        if locale == "it":
            it_sig = state.get("italian_trust_signals", {})
            if it_sig:
                italian_trust_md = "## 🇮🇹 Italian Market Trust Signals\n*AI engines look for these specific anchors to verify Italian entities.*\n"
                italian_trust_md += f"- **Partita IVA:** {'✅ Detected' if it_sig.get('piva_detected') else '❌ Missing'}\n"
                italian_trust_md += f"- **PEC (Posta Certificata):** {'✅ Detected' if it_sig.get('pec_detected') else '❌ Missing'}\n"
                italian_trust_md += f"- **Camera di Commercio / REA:** {'✅ Detected' if it_sig.get('rea_detected') or it_sig.get('camera_commercio_detected') else '❌ Missing'}\n\n---\n\n"

        md_content = f"""# ☢️ NUCLEAR AI: {title}

**{url_label}:** {state.get("url", "N/A")}  
**Brand:** {state.get("brand_name", "N/A")} ({state.get("scale_level", "National")})  
**{ind_label}:** {state.get("target_industry", "N/A")}  

---

## 🎯 Sintesi Esecutiva & Posizionamento (Executive Summary)
*Questo Audit mostra quanto il brand è visibile, raccomandato e considerato autorevole dai modelli di intelligenza artificiale (AIO e RAG).*

- **{ent_label}: {metrics.get("Entity Consensus", 0)}/100** 
  *(Misura quanto le entità del brand sono allineate con gli standard tecnici e i competitor di settore)*
- **{risk_label}: {metrics.get("Hallucination Risk", 100)}%** 
  *(Il grado di rischio che un'AI inventi, distorca o ometta informazioni proprietarie a causa di segnali deboli sul sito)*
- **{info_label}: {metrics.get("Information Gain", 0)}/100** 
  *(Il valore unico dei contenuti proprietari rispetto al rumore di mercato e ai topic gap)*
- **{conf_label}: {confidence}/100** 
  *(L'indice di integrità e completezza dei dati tecnici estratti dal sito durante questo audit)*
- **Stato AI Readiness:** {metrics.get("Citation Readiness", "N/A")}

---

## 🛡️ {ev_label}
{limitations}
- **Schema detected:** {schema_str}
- **E-E-A-T Gaps:** {", ".join(state.get("e_e_a_t_gaps", [])) if state.get("e_e_a_t_gaps") else "None detected"}
- **Original IP:** {", ".join(state.get("original_frameworks", [])) if state.get("original_frameworks") else "None detected"}

---

## 🧮 Confidence Breakdown
*The confidence score is now split into two independent dimensions.*

| Dimension | Score | What it Measures |
|---|---|---|
| **Extraction Integrity** | {extraction_integrity}/100 | How faithfully the site content was captured (JS fallback, robots, word count, extraction warnings) |
| **Context Reliability** | {context_reliability}/100 | How trustworthy the business frame is (schema completeness, OG quality, industry correction flag, market intelligence quality) |
| **Blended Confidence** | **{confidence}/100** | Equally-weighted average of both dimensions |

> *These scores are separate. A site can extract cleanly but still have a wrong or polluted business frame.*
{classification_notice}
---

{italian_trust_md}{stress_md}## 🚀 {roi_label}
- Conservative Lift Projection: {state.get("projected_traffic_lift", "N/A")}
- ROI Verified: {"✅" if state.get("roi_verified") else "❌"}
- Note: {state.get("validator_notes", "")}

---

## 📋 {rec_label}
{rec_md}

### Targeted Expansions:
{chr(10).join([f"- {item}" for item in state.get("recommended_content", [])]) if state.get("recommended_content") else "N/A"}

---
*Report generated by Nuclear AI Generative Visibility Intelligence Platform on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        with open(md_file, "w", encoding="utf-8") as f:
            f.write(md_content)
            
        markdown_report_path = md_file
        console.print(f"[green]Finalizer Node[/green]: Agency Markdown report saved to {markdown_report_path}")
    except Exception as e:
        console.print(f"[bold red]NODE_FAILED[/bold red]: Markdown Export: {e}")

    # Set finalize state contract variables
    state["sqlite_saved"] = sqlite_saved
    state["json_export_path"] = json_export_path
    state["markdown_report_path"] = markdown_report_path
    
    if json_export_path:
        streamlit_ready = True
        
    state["streamlit_ready"] = streamlit_ready
    state["status"] = "Completed"

    console.print("[bold cyan]Finalizer Node[/bold cyan]: Agency-Grade audit completed and artifacts secured.")

    return state
