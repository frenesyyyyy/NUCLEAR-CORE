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
            title = "Report Ufficiale Audit GEO v4.2 (Agency-Grade)"
            url_label = "URL Destinazione"
            ind_label = "Settore"
            aud_label = "Pubblico Principale"
            met_label = "Intelligence Matrix (70/20/10)"
            ent_label = "Entity Consensus"
            info_label = "Information Gain (Mancanze/Upside)"
            risk_label = "Hallucination Risk (Rischio Errori AI)"
            conf_label = "Affidabilità Dati d'Estrazione"
            cit_label = "Stato Citazioni"
            read_label = "Livello Readiness"
            roi_label = "Proiezioni Limitate & Validazione ROI"
            rec_label = "GEO Strategy: Recommendation Pack Dettagliato"
            strat_label = "Evidenze Strutturate & Content Strategy"
            lim_label = "Limitazioni nell'Analisi"
        else:
            title = "Official GEO Audit Report v4.2 (Agency-Grade)"
            url_label = "Target URL"
            ind_label = "Industry"
            aud_label = "Target Audience"
            met_label = "Agency Intelligence Matrix"
            ent_label = "Entity Consensus"
            info_label = "Information Gain (Content Upside)"
            risk_label = "Hallucination Risk"
            conf_label = "Extraction Confidence Score"
            cit_label = "Citation Status"
            read_label = "Readiness Level"
            roi_label = "Bounded Lift Projection & ROI"
            rec_label = "Agency Recommendation Pack"
            strat_label = "Structured Evidence & Content Strategist"
            lim_label = "Evidence Limitations"
            
        metrics = state.get("metrics", {})
        confidence = state.get("confidence_score", 0)
        
        limitations = ""
        if confidence < 60:
            limit_msg = state.get("evidence_limitations", "Low confidence due to parsing limitations or thin content.")
            limitations = f"\n> **⚠️ {lim_label}:** {limit_msg} Metrics may be volatile.\n"
            
        md_content = f"""# ☢️ NUCLEAR AI: {title}

**{url_label}:** {state.get("url", "N/A")}  
**Brand:** {state.get("brand_name", "N/A")} ({state.get("scale_level", "National")})  
**{ind_label}:** {state.get("target_industry", "N/A")}  
**{read_label}:** {metrics.get("Citation Readiness", "N/A")}  
**{conf_label}:** {confidence}/100
{limitations}
---

## 📊 {met_label}
- **{ent_label}:** {metrics.get("Entity Consensus", 0)}% (Brand entity detection)
- **{info_label}:** {metrics.get("Information Gain", 0)}% (Estimated missing topics)
- **{risk_label}:** {metrics.get("Hallucination Risk", 100)}% (Risk of LLMs fabricating brand info)
- **{cit_label}:** {state.get("citation_status", "N/A")}

---

## 🧠 {strat_label}
- **Schema detected:** {", ".join(state.get("schema_objects", [])) if state.get("schema_objects") else "None detected"}
- **E-E-A-T Gaps:** {", ".join(state.get("e_e_a_t_gaps", [])) if state.get("e_e_a_t_gaps") else "None detected"}
- **Original IP:** {", ".join(state.get("original_frameworks", [])) if state.get("original_frameworks") else "None detected"}

---

## 🚀 {roi_label}
- Conservative Lift Projection: {state.get("projected_traffic_lift", "N/A")}
- ROI Verified: {"✅" if state.get("roi_verified") else "❌"}
- Note: {state.get("validator_notes", "")}

---

## 📋 {rec_label}
{state.get("geo_recommendation_pack", "N/A")}

### Targeted Expansions:
{chr(10).join([f"- {item}" for item in state.get("recommended_content", [])]) if state.get("recommended_content") else "N/A"}

---
*Report generated by Nuclear AI GEO Optimizer v4.2 Agency-Grade on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
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
