import sqlite3
import json

def check():
    try:
        conn = sqlite3.connect("geo_audit.db")
        cursor = conn.cursor()
        
        # Check column names
        cursor.execute("PRAGMA table_info(audits)")
        cols = [c[1] for c in cursor.fetchall()]
        print(f"Columns in 'audits': {cols}")
        
        # Check last row
        cursor.execute("SELECT run_id, integrity_status FROM audits ORDER BY started_at DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            print(f"Last Audit: ID={row[0]}, Integrity={row[1]}")
        else:
            print("No audits found.")
            
        conn.close()
    except Exception as e:
        print(f"Error checking DB: {e}")

if __name__ == "__main__":
    check()
