import sqlite3
import json

def check():
    run_id = "e1c7ffe0-0df4-475d-9871-abbfa59754fb"
    try:
        conn = sqlite3.connect("geo_audit.db")
        cursor = conn.cursor()
        
        cursor.execute("SELECT run_id, target_industry, integrity_status, overall_pipeline_readiness FROM audits WHERE run_id=?", (run_id,))
        row = cursor.fetchone()
        if row:
            print(f"Run ID: {row[0]}")
            print(f"Industry: {row[1]}")
            print(f"Integrity: {row[2]}")
            print(f"Readiness: {row[3]}")
        else:
            print(f"Run {run_id} not found in DB.")
            
        conn.close()
    except Exception as e:
        print(f"Error checking DB: {e}")

if __name__ == "__main__":
    check()
