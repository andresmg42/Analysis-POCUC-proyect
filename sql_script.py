import re
import sys
import pymysql
import pymysql.cursors
import os
import yaml

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

config_file_path = os.path.join(script_dir, "config_fill.yml")

script_file_path=os.path.join(script_dir,"sql_script.sql")

with open(config_file_path, "r") as f:
    config = yaml.safe_load(f)
    config_co = config["POCUCDB"]

# ══════════════════════════════════════════════════════════════
# CONFIGURATION  ← edit these values
# ══════════════════════════════════════════════════════════════
DB_CONFIG = {
    "host":     config_co['host'],       # e.g. "127.0.0.1"
    "port":     config_co['port'],
    "user":     config_co['user'],            # your MySQL user
    "password": config_co['password'],    # your MySQL password
    "database": config_co['dbname']# your database name
   
}



SQL_FILE = script_file_path         # path to the SQL file

# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def load_statements(path: str) -> list[str]:
    """
    Read the SQL file and split it into executable statements.

    Rules:
    - Lines starting with '--' are comments → skip.
    - Statements are separated by ';'.
    - SET @variable lines are kept as statements.
    - Empty statements are discarded.
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    # Remove full-line comments
    lines = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped == "":
            continue
        lines.append(line)

    # Re-join and split on ';'
    joined = "\n".join(lines)
    parts  = joined.split(";")

    statements = []
    for part in parts:
        stmt = part.strip()
        if stmt:
            statements.append(stmt)

    return statements


def categorise(stmt: str) -> str:
    """Return a short label for progress reporting."""
    upper = stmt.upper().lstrip()
    if upper.startswith("START TRANSACTION"):  return "TRANSACTION START"
    if upper.startswith("COMMIT"):             return "COMMIT"
    if upper.startswith("ROLLBACK"):           return "ROLLBACK"
    if upper.startswith("SET"):                return "SET variable"
    if "SURVEYSESSION_SURVEYSESSION" in upper: return "surveysession INSERT"
    if "VISIT_VISIT" in upper:                 return "visit INSERT"
    if "RESPONSE_RESPONSE" in upper:           return "response INSERT"
    if upper.startswith("SELECT"):             return "SELECT (verify)"
    return "other"


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    print(f"Reading {SQL_FILE} ...")
    try:
        statements = load_statements(SQL_FILE)
    except FileNotFoundError:
        print(f"ERROR: '{SQL_FILE}' not found.")
        print("Make sure seed_data.sql is in the same folder as this script.")
        sys.exit(1)

    print(f"Loaded {len(statements)} statements.\n")

    # ── Connect ───────────────────────────────────────────────
    print("Connecting to MySQL ...")
    try:
        conn = pymysql.connect(
            **DB_CONFIG,
            autocommit=False,          # we control the transaction
            cursorclass=pymysql.cursors.DictCursor,
        )
    except pymysql.err.OperationalError as e:
        print(f"\nConnection FAILED: {e}")
        print("Check host, port, user, password and database name in DB_CONFIG.")
        sys.exit(1)

    print(f"Connected to {DB_CONFIG['host']}:{DB_CONFIG['port']}"
          f" / {DB_CONFIG['database']}\n")

    # ── Execute ───────────────────────────────────────────────
    counts = {}
    cursor = conn.cursor()

    try:
        for i, stmt in enumerate(statements, start=1):
            label = categorise(stmt)
            counts[label] = counts.get(label, 0) + 1

            # Progress indicator every 20 statements
            if i % 20 == 0 or i == len(statements):
                print(f"  [{i:>4}/{len(statements)}]  {label}", flush=True)

            cursor.execute(stmt)

            # After SELECT statements print the result rows
            if label == "SELECT (verify)":
                rows = cursor.fetchall()
                print("\n  ── Verification results ──────────────────")
                for row in rows:
                    print(f"     {row}")
                print("")

        conn.commit()
        print("\n✓  Transaction COMMITTED successfully.\n")

    except Exception as e:
        conn.rollback()
        print(f"\n✗  ERROR on statement #{i}:\n")
        # Print the first 300 chars of the failing statement
        print("   " + stmt[:300].replace("\n", "\n   "))
        print(f"\n   MySQL error: {e}")
        print("\n   Transaction ROLLED BACK — no changes were saved.")
        sys.exit(1)

    finally:
        cursor.close()
        conn.close()

    # ── Summary ───────────────────────────────────────────────
    print("── Execution summary ─────────────────────────────────")
    for label, n in sorted(counts.items()):
        print(f"   {label:<30} {n:>5} statement(s)")
    print("")
    print("Next step: run the verification query at the bottom of")
    print("seed_data.sql to confirm row counts in each table.")


if __name__ == "__main__":
    main()