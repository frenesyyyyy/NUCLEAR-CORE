[1mdiff --git a/nodes/prospector_node.py b/nodes/prospector_node.py[m
[1mindex e895feb..03c0943 100644[m
[1m--- a/nodes/prospector_node.py[m
[1m+++ b/nodes/prospector_node.py[m
[36m@@ -190,9 +190,9 @@[m [mdef process(state: dict) -> dict:[m
     source_mode = state.get("source_of_truth_mode", "hybrid")[m
     [m
     serper_api_key = os.getenv("SERPER_API_KEY")[m
[31m-    perplexity_api_key = os.getenv("PERPLEXITY_API_KEY")[m
[32m+[m[32m    gemini_key = os.getenv("GEMINI_API_KEY")[m
     [m
[31m-    if not serper_api_key or not perplexity_api_key:[m
[32m+[m[32m    if not serper_api_key or not gemini_key:[m
         console.print("[bold red]NODE_FAILED[/bold red]: Prospector (API Keys missing). Using fallbacks.")[m
         state["raw_data_complete"] = raw_data_complete[m
         state["external_sources"] = external_sources[m
[36m@@ -310,7 +310,28 @@[m [mdef process(state: dict) -> dict:[m
     """[m
 [m
     console.print(f"Executing Tier-1 Intelligence Retrieval (Geo: {final_loc} | Conf: {loc_conf})...")[m
[31m-    raw_intelligence = call_perplexity(system_role, agency_query, str(perplexity_api_key))[m
[32m+[m[41m    [m
[32m+[m[32m    # LEGACY PATH: Perplexity implementation retained intentionally for optional future reactivation.[m
[32m+[m[32m    # raw_intelligence = call_perplexity(system_role, agency_query, str(perplexity_api_key))[m
[32m+[m[41m    [m
[32m+[m[32m    # ACTIVE PATH: Serper + Gemini 2.5 Flash Lite[m
[32m+[m[32m    try:[m
[32m+[m[32m        gemini_client = genai.Client(api_key=gemini_key)[m
[32m+[m[32m        serper_context = json.dumps([{"title": r.get("title"), "snippet": r.get("snippet")} for r in s_results])[m
[32m+[m[41m        [m
[32m+[m[32m        full_query = f"{system_role}\n\n{agency_query}\n\nContext from live search:\n{serper_context}"[m
[32m+[m[41m        [m
[32m+[m[32m        def _intel_req():[m
[32m+[m[32m            return gemini_client.models.generate_content([m
[32m+[m[32m                model="gemini-2.5-flash-lite",[m
[32m+[m[32m                contents=full_query,[m
[32m+[m[32m                config={"response_mime_type": "application/json"}[m
[32m+[m[32m            )[m
[32m+[m[32m        res = execute_with_backoff(_intel_req, max_retries=3, initial_delay=3.0)[m
[32m+[m[32m        raw_intelligence = res.text[m
[32m+[m[32m    except Exception as e:[m
[32m+[m[32m        console.print(f"[yellow]Gemini Intelligence Error[/yellow]: {e}")[m
[32m+[m[32m        raw_intelligence = "{}"[m
     [m
     try:[m
         match = re.search(r'```(?:json)?(.*?)```', raw_intelligence, re.DOTALL | re.IGNORECASE)[m
[1mdiff --git a/nodes/researcher_node.py b/nodes/researcher_node.py[m
[1mindex 1ab3752..5029823 100644[m
[1m--- a/nodes/researcher_node.py[m
[1m+++ b/nodes/researcher_node.py[m
[36m@@ -371,7 +371,7 @@[m [mdef _brand_mentioned(brand_name: str, url: str, og_title: str, answer_text: str,[m
 [m
 def _run_stress_test([m
     all_queries: list,[m
[31m-    perplexity_key: str,[m
[32m+[m[32m    serper_key: str,[m
     brand_name: str,[m
     url: str,[m
     og_title: str,[m
[36m@@ -409,24 +409,49 @@[m [mdef _run_stress_test([m
 [m
         matched = False[m
         try:[m
[31m-            payload = {[m
[31m-                "model": "sonar-pro",[m
[31m-                "messages": [[m
[31m-                    {"role": "system", "content": "You are a helpful search assistant."},[m
[31m-                    {"role": "user",   "content": q_text}[m
[31m-                ][m
[31m-            }[m
[31m-            def _perp_req():[m
[31m-                response = requests.post([m
[31m-                    "https://api.perplexity.ai/chat/completions",[m
[31m-                    json=payload,[m
[31m-                    headers={"Authorization": f"Bearer {perplexity_key}"},[m
[31m-                    timeout=30[m
[32m+[m[32m            # LEGACY PATH: Perplexity implementation retained intentionally for optional future reactivation.[m
[32m+[m[32m            # payload = {[m
[32m+[m[32m            #     "model": "sonar-pro",[m
[32m+[m[32m            #     "messages": [[m
[32m+[m[32m            #         {"role": "system", "content": "You are a helpful search assistant."},[m
[32m+[m[32m            #         {"role": "user",   "content": q_text}[m
[32m+[m[32m            #     ][m
[32m+[m[32m            # }[m
[32m+[m[32m            # def _perp_req():[m
[32m+[m[32m            #     response = requests.post([m
[32m+[m[32m            #         "https://api.perplexity.ai/chat/completions",[m
[32m+[m[32m            #         json=payload,[m
[32m+[m[32m            #         headers={"Authorization": f"Bearer {os.getenv('PERPLEXITY_API_KEY', 'legacy_key')}"},[m
[32m+[m[32m            #         timeout=30[m
[32m+[m[32m            #     )[m
[32m+[m[32m            #     response.raise_for_status()[m
[32m+[m[32m            #     return response.json()["choices"][0]["message"]["content"][m
[32m+[m[32m            #[m
[32m+[m[32m            # answer = execute_with_backoff(_perp_req, max_retries=3, initial_delay=3.0)[m
[32m+[m[41m            [m
[32m+[m[32m            # ACTIVE PATH: Serper + Gemini 2.5 Flash Lite[m
[32m+[m[32m            def _search_and_reason():[m
[32m+[m[32m                # 1. Retrieval[m
[32m+[m[32m                s_url = "https://google.serper.dev/search"[m
[32m+[m[32m                s_payload = json.dumps({"q": q_text, "num": 10})[m
[32m+[m[32m                s_headers = {'X-API-KEY': serper_key, 'Content-Type': 'application/json'}[m
[32m+[m[32m                s_res = requests.post(s_url, headers=s_headers, data=s_payload, timeout=15)[m
[32m+[m[32m                s_res.raise_for_status()[m
[32m+[m[32m                s_data = s_res.json().get("organic", [])[m
[32m+[m[41m                [m
[32m+[m[32m                context = json.dumps([{"title": r.get("title"), "snippet": r.get("snippet")} for r in s_data[:8]])[m
[32m+[m[41m                [m
[32m+[m[32m                # 2. Reasoning[m
[32m+[m[32m                prompt = f"Based on these search results for the query '{q_text}', summarize the top recommendations and specific brands/solutions mentioned in the snippets. Provide a direct, factual answer.\n\nSearch context:\n{context}"[m
[32m+[m[41m                [m
[32m+[m[32m                res = gemini_client.models.generate_content([m
[32m+[m[32m                    model="gemini-2.5-flash-lite",[m
[32m+[m[32m                    contents=prompt[m
                 )[m
[31m-                response.raise_for_status()[m
[31m-                return response.json()["choices"][0]["message"]["content"][m
[31m-[m
[31m-            answer = execute_with_backoff(_perp_req, max_retries=3, initial_delay=3.0)[m
[32m+[m[32m                return res.text[m
[32m+[m[41m            [m
[32m+[m[32m            answer = execute_with_backoff(_search_and_reason, max_retries=3, initial_delay=3.0)[m
[32m+[m[41m            [m
             matched = _brand_mentioned(brand_name, url, og_title, answer, gemini_client)[m
             if matched:[m
                 visibility_points += pts[m
[36m@@ -581,9 +606,9 @@[m [mdef process(state: dict) -> dict:[m
     authority_match_score = 0[m
     [m
     gemini_key = os.getenv("GEMINI_API_KEY")[m
[31m-    perplexity_key = os.getenv("PERPLEXITY_API_KEY")[m
[32m+[m[32m    serper_key = os.getenv("SERPER_API_KEY")[m
 [m
[31m-    if not gemini_key or not perplexity_key:[m
[32m+[m[32m    if not gemini_key or not serper_key:[m
         console.print("[bold red]NODE_FAILED[/bold red]: Researcher (API Keys missing). Using fallbacks.")[m
         state["metrics"] = metrics[m
         return state[m
[36m@@ -728,7 +753,7 @@[m [mdef process(state: dict) -> dict:[m
         }[m
         [m
         visibility_points, total_possible, stress_test_log, tier_stats = _run_stress_test([m
[31m-            all_queries, str(perplexity_key), bran