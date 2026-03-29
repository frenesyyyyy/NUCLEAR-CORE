import sqlite3
import json

def check():
    try:
        conn = sqlite3.connect("geo_audit.db")
        cursor = conn.cursor()
        
        # Check last row
        cursor.execute("SELECT run_id, target_industry, integrity_status, overall_pipeline_readiness FROM audits ORDER BY started_at DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            print(f"Last Audit: ID={row[0]}")
            print(f"Industry: {row[1]}")
            print(f"Integrity: {row[2]}")
            print(f"Readiness: {row[3]}")
        else:
            print("No audits found.")
            
        conn.close()
    except Exception as e:
        print(f"Error checking DB: {e}")

if __name__ == "__main__":
    check()
