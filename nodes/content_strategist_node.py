import os
import json
import time
from rich.console import Console
from google import genai

console = Console()

def process(state: dict) -> dict:
    console.print("[cyan]Content Strategist Node[/cyan]: Extracting structured data and E-E-A-T anchors...")
    
    # Defaults for new fields - only adding to state
    schema_objects = []
    original_frameworks = []
    e_e_a_t_gaps = []
    recommended_content = []
    structured_data_extract = []
    evidence_summary = ""
    extraction_warnings = []
    
    url = state.get("url", "")
    client_content = state.get("client_content_clean", "")
    gemini_key = os.getenv("GEMINI_API_KEY")
    
    # Deterministic Schema Extraction
    json_ld_blocks = state.get("json_ld_blocks", [])
    for block in json_ld_blocks:
        try:
            parsed = json.loads(block)
            if isinstance(parsed, dict):
                stype = parsed.get("@type")
                if stype:
                    structured_data_extract.append(str(stype))
                if "@graph" in parsed and isinstance(parsed["@graph"], list):
                    for item in parsed["@graph"]:
                        if isinstance(item, dict) and "@type" in item:
                            structured_data_extract.append(str(item["@type"]))
            elif isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        stype = item.get("@type")
                        if stype:
                            structured_data_extract.append(str(stype))
        except:
            extraction_warnings.append("Failed to parse a JSON-LD block")
            pass
            
    schema_objects = list(set(structured_data_extract))
    evidence_summary = f"Detected {len(schema_objects)} schema types. Page Title: {state.get('page_title', 'Missing')}."
    
    if not gemini_key or not client_content:
        console.print("[yellow]Content Strategist Node: Skipping due to missing API key or content.[/yellow]")
        state["schema_objects"] = schema_objects
        state["original_frameworks"] = original_frameworks
        state["e_e_a_t_gaps"] = e_e_a_t_gaps
        state["recommended_content"] = recommended_content
        state["structured_data_extract"] = structured_data_extract
        state["evidence_summary"] = evidence_summary
        state["extraction_warnings"] = extraction_warnings
        return state

    client = genai.Client(api_key=gemini_key)
    
    # 5-second sleep for rate limits
    time.sleep(5)
    
    prompt = f"""
    You are a Senior GEO Content Auditor. 
    Analyze the following raw website content and the provided evidence summary to identify proprietary intellectual property and E-E-A-T signals.
    
    URL: {url}
    Evidence Summary: {evidence_summary}
    Content Snippet: {client_content[:15000]}
    
    Return STRICT JSON with exactly these keys:
    "original_frameworks": list of proprietary methodologies or unique service names detected
    "e_e_a_t_gaps": list of missing authority anchors (e.g., "Missing author credentials", "No trust badges")
    "recommended_content": list of 3-5 specific new pages or sections to improve GEO standing
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash-lite',
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        output = json.loads(response.text)
        
        original_frameworks = output.get("original_frameworks", [])
        e_e_a_t_gaps = output.get("e_e_a_t_gaps", [])
        recommended_content = output.get("recommended_content", [])
        
        console.print(f"[green]Content Strategist Node[/green]: Successfully extracted intelligence for {len(schema_objects)} schema types and mapped {len(e_e_a_t_gaps)} E-E-A-T gaps.")
    except Exception as e:
        console.print(f"[yellow]Content Strategist Node Failure: {e}[/yellow]")
        extraction_warnings.append(str(e))

    # Safe additions to state (no overwriting existing required fields)
    state["schema_objects"] = schema_objects
    state["original_frameworks"] = original_frameworks
    state["e_e_a_t_gaps"] = e_e_a_t_gaps
    state["recommended_content"] = recommended_content
    state["structured_data_extract"] = structured_data_extract
    state["evidence_summary"] = evidence_summary
    state["extraction_warnings"] = extraction_warnings
    
    return state
