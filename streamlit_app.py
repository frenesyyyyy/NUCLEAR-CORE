import streamlit as st
import json
import os
from datetime import datetime

st.set_page_config(page_title="Nuclear AI GEO Optimizer Dashboard", layout="wide", page_icon="☢️")

# Custom CSS for Agency Grade look
st.markdown("""
<style>
    .metric-box {
        background-color: #1e1e1e;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        border: 1px solid #333;
    }
    .metric-title {
        color: #888;
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .metric-value {
        color: #00ffcc;
        font-size: 32px;
        font-weight: bold;
        margin-top: 10px;
    }
    .recommendation-box {
        background-color: #161b22;
        border-left: 4px solid #00ffcc;
        padding: 15px;
        margin-bottom: 10px;
        border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)

def load_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_latest_export():
    exports_dir = "exports"
    if not os.path.exists(exports_dir):
        return None
    files = [os.path.join(exports_dir, f) for f in os.listdir(exports_dir) if f.endswith('.json')]
    if not files:
        return None
    latest_file = max(files, key=os.path.getctime)
    return latest_file

def main():
    st.title("☢️ Nuclear AI GEO Optimizer")
    st.markdown("### Client Audit Presentation Dashboard (v4.4 Agency-Grade)")
    
    st.sidebar.header("Data Source")
    
    uploaded_file = st.sidebar.file_uploader("Upload JSON Export", type=['json'])
    
    data = None
    if uploaded_file is not None:
        data = json.load(uploaded_file)
    else:
        latest = get_latest_export()
        if latest:
            st.sidebar.success(f"Loaded latest export: {os.path.basename(latest)}")
            data = load_data(latest)
        else:
            st.warning("No JSON export found. Please upload a JSON file or run the optimizer pipeline to generate one.")
    
    if data:
        locale = data.get("locale", "en")
        
        # Localization Dictionary
        labels = {
            "en": {
                "overview": "Audit Overview",
                "metrics": "Key Performance Metrics",
                "recommendation": "GEO Recommendation Pack",
                "target_url": "Target URL",
                "industry": "Industry",
                "status": "Audit Status",
                "entity_consensus": "Entity Consensus",
                "info_gain": "Information Gain",
                "hallucination_risk": "Hallucination Risk",
                "confidence_score": "Extraction Confidence",
                "citation_status": "Citation Status",
                "projected_lift": "Projected Traffic Lift",
                "roi_verified": "ROI Viability",
                "raw_data": "Raw Data JSON"
            },
            "it": {
                "overview": "Panoramica dell'Audit",
                "metrics": "Metriche di Performance (KPI)",
                "recommendation": "Pacchetto di Raccomandazioni GEO",
                "target_url": "URL Destinazione",
                "industry": "Settore",
                "status": "Stato Audit",
                "entity_consensus": "Consenso Entità",
                "info_gain": "Guadagno Informativo",
                "hallucination_risk": "Rischio Allucinazione",
                "confidence_score": "Affidabilità Estrazione",
                "citation_status": "Stato Citazioni",
                "projected_lift": "Aumento Traffico Previsto",
                "roi_verified": "Sostenibilità ROI",
                "raw_data": "Dati Grezzi JSON"
            }
        }
        
        l = labels.get(locale, labels["en"])
        
        metrics = data.get("metrics", {})
        confidence = data.get("confidence_score", 0)
        
        # Display warning banner if confidence is low
        if confidence < 60:
            limit_msg = data.get("evidence_limitations", "Low confidence due to parsing limitations or thin content.")
            st.warning(f"⚠️ **{l['confidence_score']} < 60:** {limit_msg} Metrics may be volatile.")
        
        st.header(l["overview"])
        col1, col2, col3, col4 = st.columns(4)
        col1.write(f"**{l['target_url']}:** {data.get('url', 'N/A')}")
        col2.write(f"**{l['industry']}:** {data.get('target_industry', 'N/A')}")
        col3.write(f"**{l['status']}:** {data.get('status', 'Completed')}")
        col4.write(f"**Readiness:** {metrics.get('Citation Readiness', 'N/A')}")
        
        st.markdown("---")
        
        tab_overview, tab_evidence, tab_recs, tab_val, tab_qa = st.tabs(["Overview & Metrics", "Evidence Tracking", "Recommendations", "Validation & ROI", "🔬 QA / Debug Cockpit"])
        
        with tab_overview:
            st.header(l["metrics"])
            m1, m2, m3, m4, m5, m6 = st.columns(6)
            with m1:
                st.markdown(f'<div class="metric-box"><div class="metric-title">{l["entity_consensus"]}</div><div class="metric-value">{metrics.get("Entity Consensus", "0")}%</div></div>', unsafe_allow_html=True)
            with m2:
                st.markdown(f'<div class="metric-box"><div class="metric-title">{l["info_gain"]}</div><div class="metric-value">{metrics.get("Information Gain", "0")}%</div></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div class="metric-box"><div class="metric-title">{l["confidence_score"]}</div><div class="metric-value">{confidence}/100</div></div>', unsafe_allow_html=True)
            with m4:
                st.markdown(f'<div class="metric-box"><div class="metric-title">{l["hallucination_risk"]}</div><div class="metric-value">{metrics.get("Hallucination Risk", "0")}%</div></div>', unsafe_allow_html=True)
            with m5:
                st.markdown(f'<div class="metric-box"><div class="metric-title">{l["citation_status"]}</div><div class="metric-value" style="font-size:20px;">{data.get("citation_status", "N/A")}</div></div>', unsafe_allow_html=True)
            with m6:
                st.markdown(f'<div class="metric-box"><div class="metric-title">{l["projected_lift"]}</div><div class="metric-value" style="font-size:20px;">{data.get("projected_traffic_lift", "N/A")}</div></div>', unsafe_allow_html=True)
        
        with tab_evidence:
            st.header("Structured Evidence" if locale == "en" else "Evidenze Strutturate")
            
            schema_types = data.get("schema_type_counts", {})
            st.subheader("Detected Schema" if locale == "en" else "Schema Rilevati")
            if schema_types:
                st.markdown(", ".join([f"{k} ({v})" for k,v in schema_types.items()]))
            else:
                st.markdown("None detected")
                
            auth_signals = data.get("brand_authority_signals", {})
            if auth_signals:
                st.subheader("Brand Authority Signals" if locale == "en" else "Segnali Autorità Brand")
                st.json(auth_signals)
            
            # Content depth
            depth = data.get("client_content_depth", {})
            st.subheader("Content Extraction Depth")
            st.json(depth)
            
            with st.expander(l["raw_data"]):
                st.json(data)
                
        with tab_recs:
            st.header(l["recommendation"])
            
            # ── Evidence-Traceable Blueprint Actions ──
            blueprint = data.get("implementation_blueprint", {})
            all_actions = blueprint.get("all_strategic_actions", [])
            
            if all_actions:
                for a in all_actions:
                    title = a.get("action_title", "Unnamed")
                    priority = a.get("priority", "Medium")
                    origin = a.get("evidence_origin", "profile_inference")
                    conf = a.get("evidence_confidence", "low")
                    signals = a.get("supporting_signals", [])
                    why = a.get("why_it_matters", "")
                    
                    pri_icon = "🔴" if priority in ("Critical", "High") else ("🟡" if priority == "Medium" else "🟢")
                    conf_color = "#00ffcc" if conf == "high" else ("#ffcc00" if conf == "medium" else "#ff6666")
                    
                    st.markdown(f'''
                    <div class="recommendation-box">
                        <h4>{pri_icon} {title}</h4>
                        <p><strong>Priority:</strong> {priority} | <strong>Origin:</strong> {origin.replace("_"," ").title()} | <span style="color:{conf_color}"><strong>Confidence:</strong> {conf.title()}</span></p>
                        <p>{why}</p>
                        <p style="color:#888;font-size:12px;"><strong>Signals:</strong> {", ".join(signals) if signals else "N/A"}</p>
                    </div>
                    ''', unsafe_allow_html=True)
            else:
                # Fallback to legacy researcher pack
                recommendations_raw = data.get("geo_recommendation_pack", "[]")
                try:
                    rec_data = json.loads(recommendations_raw)
                    if isinstance(rec_data, list) and rec_data:
                        for r in rec_data:
                            r_title = r.get("title", "Azione Strategica")
                            r_rat = r.get("rationale", "")
                            r_pri = r.get("priority", "Medium")
                            r_type = r.get("implementation_type", "Content")
                            pri_icon = "🔴" if "high" in r_pri.lower() else ("🟡" if "med" in r_pri.lower() else "🟢")
                            st.markdown(f'''
                            <div class="recommendation-box">
                                <h4>{pri_icon} {r_title}</h4>
                                <p><strong>Priority:</strong> {r_pri} | <strong>Type:</strong> {r_type}</p>
                                <p>{r_rat}</p>
                            </div>
                            ''', unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="recommendation-box">{recommendations_raw}</div>', unsafe_allow_html=True)
                except:
                    st.markdown(f'<div class="recommendation-box">{recommendations_raw}</div>', unsafe_allow_html=True)
            
            if data.get("recommended_content"):
                st.subheader("Agency Content Strategy" if locale == "en" else "Strategia Contenuti Agency")
                for item in data.get("recommended_content", []):
                    st.markdown(f"- {item}")
                    
        with tab_val:
            st.header(l["roi_verified"])
            if data.get("roi_verified"):
                st.success("✅ ROI Positive / Verified" if locale == "en" else "✅ ROI Positivo / Verificato")
            else:
                st.error("❌ High Risk / Unverified ROI" if locale == "en" else "❌ Alto Rischio / ROI Non Verificato")
            
            val_notes = data.get("validator_notes", "")
            if val_notes:
                st.info(val_notes)

        # ═══════════════════════════════════════════════════════════════════════
        # QA / DEBUG COCKPIT TAB
        # ═══════════════════════════════════════════════════════════════════════
        with tab_qa:
            st.header("🔬 QA / Debug Cockpit")
            st.caption("Internal provenance and diagnostic surfaces for audit reliability debugging.")

            # ── 1. Profile & Classification ──────────────────────────────────
            with st.expander("🏷️ Profile & Classification Provenance", expanded=True):
                raw_key = data.get("business_profile_key", "N/A")
                norm_key = data.get("business_profile_summary", {}).get("canonical_key", raw_key)
                
                pc1, pc2, pc3 = st.columns(3)
                pc1.metric("Raw Profile Key", raw_key)
                pc2.metric("Normalized Key", norm_key)
                pc3.metric("Classification Reliability", data.get("classification_reliability", "N/A"))
                
                taxonomy = data.get("source_taxonomy", {})
                st.markdown(f"**Source Pack Used:** `{taxonomy.get('source_pack_used', 'N/A')}`")
                st.markdown(f"**Source Detection Notes:** {taxonomy.get('source_detection_notes', 'N/A')}")
                
                integrity = data.get("audit_integrity_status", "N/A")
                integ_score = data.get("audit_integrity_score", 0)
                mode = data.get("source_of_truth_mode", "hybrid")
                st.markdown(f"**Integrity:** `{integrity}` (Score: {integ_score}/100) | **Mode:** `{mode}`")
                
                reasons = data.get("audit_integrity_reasons", [])
                if reasons:
                    st.warning("**Integrity Issues:** " + ", ".join(reasons))

            # ── 2. Verdict Provenance — "Why Not Client Ready?" ──────────────
            with st.expander("⚖️ Verdict Provenance — Why Not Client Ready?", expanded=True):
                verdict = data.get("agency_verdict", "N/A")
                verdict_reason = data.get("agency_verdict_reason", "N/A")
                
                if verdict == "CLIENT READY":
                    st.success(f"✅ **{verdict}** — {verdict_reason}")
                elif verdict == "NOT CLIENT READY":
                    st.error(f"❌ **{verdict}** — {verdict_reason}")
                else:
                    st.warning(f"⚠️ **{verdict}** — {verdict_reason}")
                
                # Threshold Matrix
                st.markdown("##### Threshold Pass/Fail Matrix")
                tier_stats = data.get("stress_test_tier_stats", {})
                blind_q = tier_stats.get("blind_discovery", {}).get("queries", 0)
                blind_m = tier_stats.get("blind_discovery", {}).get("matches", 0)
                ctx_q = tier_stats.get("contextual_discovery", {}).get("queries", 0)
                ctx_m = tier_stats.get("contextual_discovery", {}).get("matches", 0)
                blind_rate = (blind_m / blind_q * 100) if blind_q > 0 else 0
                ctx_rate = (ctx_m / ctx_q * 100) if ctx_q > 0 else 0
                
                pen_gaps = taxonomy.get("penalized_relevant_gaps", [])
                ev_depth = metrics.get("Defensible Evidence Depth", 0)
                consensus_val = metrics.get("Entity Consensus", 0)
                
                checks = [
                    ("Confidence ≥ 40 (Hard Veto)", confidence >= 40, f"{confidence}%"),
                    ("Confidence ≥ 70 (Soft Veto)", confidence >= 70, f"{confidence}%"),
                    ("Blind Discovery > 0%", blind_rate > 0, f"{blind_rate:.1f}%"),
                    ("T1+T2 Combined > 0%", (blind_rate + ctx_rate) > 0, f"{(blind_rate + ctx_rate)/2:.1f}%"),
                    ("Penalized Gaps < 2", len(pen_gaps) < 2, f"{len(pen_gaps)} gaps"),
                    ("Evidence Depth ≥ 30", ev_depth >= 30, f"{ev_depth}"),
                    ("Consensus ≥ 30", consensus_val >= 30, f"{consensus_val}"),
                    ("Integrity Valid", integrity == "valid", integrity),
                ]
                
                for label, passed, value in checks:
                    icon = "✅" if passed else "❌"
                    st.markdown(f"{icon} **{label}** → `{value}`")
                
                # Contradiction Flags
                flags = data.get("contradiction_flags", [])
                c_reasons = data.get("contradiction_reasons", [])
                if flags:
                    st.markdown("##### ⚠️ Contradiction Flags Detected")
                    for f, r in zip(flags, c_reasons):
                        st.markdown(f"- **{f}**: {r}")
                else:
                    st.markdown("##### ✅ No Contradictions Detected")
                
                # Decision Risks
                risks = data.get("decision_risks", [])
                if risks and risks != ["No critical risks identified."]:
                    st.markdown("##### Decision Risks")
                    for r in risks:
                        st.markdown(f"- {r}")

            # ── 3. Source Quality Debug ──────────────────────────────────────
            with st.expander("📡 Source Quality Debug Table"):
                if taxonomy:
                    # Family Breakdown
                    family_bd = taxonomy.get("source_family_breakdown", {})
                    if family_bd:
                        st.markdown("##### Source Family Breakdown")
                        import pandas as pd
                        rows = [{"Family": k, "Count": v} for k, v in family_bd.items()]
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                    
                    # Counts Summary
                    sc1, sc2, sc3, sc4 = st.columns(4)
                    sc1.metric("Total Sources", taxonomy.get("total_sources_detected", 0))
                    sc2.metric("Unclassified", taxonomy.get("unclassified_count", 0))
                    sc3.metric("Ignored Noise", taxonomy.get("ignored_noise_count", 0))
                    sc4.metric("Relevant Gaps", taxonomy.get("relevant_gap_count", 0))
                    
                    st.markdown(f"**Trust Mix:** {taxonomy.get('trust_mix_summary', 'N/A')}")
                    st.markdown(f"**Trust Label (Validator):** `{taxonomy.get('trust_mix', 'N/A')}`")
                    
                    # Trust Anchors
                    anchors = taxonomy.get("trust_anchors_found", [])
                    if anchors:
                        st.markdown(f"**Trust Anchors Found:** {', '.join(anchors)}")
                    
                    # Penalized Gaps
                    if pen_gaps:
                        st.error(f"**Penalized Relevant Gaps:** {', '.join(pen_gaps)}")
                    
                    # Ignored Irrelevant
                    ignored = taxonomy.get("ignored_irrelevant_gaps", [])
                    if ignored:
                        st.markdown(f"**Ignored Irrelevant Gaps:** {', '.join(ignored)}")
                    
                    # Citation Risks
                    cit_risk = taxonomy.get("citation_source_risk", [])
                    if cit_risk:
                        risk_str = ", ".join(cit_risk) if isinstance(cit_risk, list) else str(cit_risk)
                        st.warning(f"**Citation Source Risk:** {risk_str}")
                    
                    st.caption(taxonomy.get("notes", ""))
                else:
                    st.info("No source taxonomy data available.")

            # ── 4. Confidence Subcomponents ──────────────────────────────────
            with st.expander("🧩 Confidence Subcomponents"):
                cc1, cc2, cc3, cc4 = st.columns(4)
                cc1.metric("Data Confidence", f"{confidence}/100")
                cc2.metric("Evidence Confidence", data.get("evidence_confidence", "N/A"))
                cc3.metric("Trust Confidence", data.get("trust_confidence", "N/A"))
                cc4.metric("Schema Confidence", data.get("schema_confidence", "N/A"))
                
                st.markdown(f"**Pipeline Readiness:** `{data.get('overall_pipeline_readiness', 'N/A')}`")
                
                # Fallback / Rescue
                fetch_notes = data.get("content_fetch_notes", "")
                if fetch_notes:
                    st.markdown(f"**Fetch Notes:** {fetch_notes}")
                js_fallback = data.get("js_fallback_used", False)
                if js_fallback:
                    st.warning("⚡ JS Rendering Fallback was activated for this audit.")
                    
                content_eng = data.get("content_engineering", {})
                if content_eng.get("is_extreme_degraded"):
                    st.error("🚨 EXTREME DEGRADED: Content payload < 150 words.")
                if content_eng.get("thin_but_semantic"):
                    st.info("🔄 RESCUED: Thin content but high semantic density detected.")

            # ── 5. Telemetry by Node ─────────────────────────────────────────
            with st.expander("⏱️ Telemetry by Node"):
                telem = data.get("execution_telemetry", [])
                if telem:
                    import pandas as pd
                    rows = []
                    for t in telem:
                        rows.append({
                            "Node": t.get("node_name", "?"),
                            "Bundle": t.get("bundle", "?"),
                            "Duration (ms)": t.get("duration_ms", 0),
                            "Status": t.get("status", "?"),
                            "Retries": t.get("retries", 0),
                            "Error": t.get("error", "") or "—"
                        })
                    df = pd.DataFrame(rows)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.metric("Total Pipeline Duration", f"{sum(r['Duration (ms)'] for r in rows):,} ms")
                else:
                    st.info("No telemetry data found in this export. Ensure telemetry is exported to state.")

if __name__ == "__main__":
    main()
