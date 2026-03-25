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
    st.markdown("### Client Audit Presentation Dashboard")
    
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
                "citation_status": "Stato Citazioni",
                "projected_lift": "Aumento Traffico Previsto",
                "roi_verified": "Sostenibilità ROI",
                "raw_data": "Dati Grezzi JSON"
            }
        }
        
        l = labels.get(locale, labels["en"])
        
        st.header(l["overview"])
        col1, col2, col3 = st.columns(3)
        col1.write(f"**{l['target_url']}:** {data.get('url', 'N/A')}")
        col2.write(f"**{l['industry']}:** {data.get('target_industry', 'N/A')}")
        col3.write(f"**{l['status']}:** {data.get('status', 'Completed')}")
        
        st.markdown("---")
        st.header(l["metrics"])
        
        metrics = data.get("metrics", {})
        
        m1, m2, m3, m4, m5 = st.columns(5)
        
        with m1:
            st.markdown(f'<div class="metric-box"><div class="metric-title">{l["entity_consensus"]}</div><div class="metric-value">{metrics.get("Entity Consensus", "0")}%</div></div>', unsafe_allow_html=True)
        with m2:
            st.markdown(f'<div class="metric-box"><div class="metric-title">{l["info_gain"]}</div><div class="metric-value">{metrics.get("Information Gain", "0")}%</div></div>', unsafe_allow_html=True)
        with m3:
            st.markdown(f'<div class="metric-box"><div class="metric-title">{l["hallucination_risk"]}</div><div class="metric-value">{metrics.get("Hallucination Risk", "0")}%</div></div>', unsafe_allow_html=True)
        with m4:
            st.markdown(f'<div class="metric-box"><div class="metric-title">{l["citation_status"]}</div><div class="metric-value" style="font-size:24px;">{metrics.get("Citation Status", "N/A")}</div></div>', unsafe_allow_html=True)
        with m5:
            st.markdown(f'<div class="metric-box"><div class="metric-title">{l["projected_lift"]}</div><div class="metric-value">{data.get("projected_traffic_lift", "N/A")}</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        
        col_rec, col_val = st.columns([2, 1])
        
        with col_rec:
            st.header(l["recommendation"])
            recommendations = data.get("geo_recommendation_pack", "No recommendations available.")
            st.markdown(f'<div class="recommendation-box">{recommendations}</div>', unsafe_allow_html=True)
            
        with col_val:
            st.header(l["roi_verified"])
            if data.get("roi_verified"):
                st.success("✅ ROI Positive / Verified" if locale == "en" else "✅ ROI Positivo / Verificato")
            else:
                st.error("❌ High Risk / Unverified ROI" if locale == "en" else "❌ Alto Rischio / ROI Non Verificato")
            
            val_notes = data.get("validator_notes", "")
            if val_notes:
                st.info(val_notes)

        with st.expander(l["raw_data"]):
            st.json(data)

if __name__ == "__main__":
    main()
