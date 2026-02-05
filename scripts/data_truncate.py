import oracledb
import os
import sys

# 1. Config
DB_USER = 'RETAIL_DW'
DB_PASS = 'RetailPass123'
DB_DSN = 'oracle-db:1521/xepdb1' 

if len(sys.argv) < 2:
    print("❌ Usage: python data_truncate.py <path_to_sql_file>")
    exit(1)

sql_file_path = sys.argv[1]

print(f"--- Running SQL Script: {sql_file_path} ---")

try:
    # 2. Connect
    connection = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
    cursor = connection.cursor()

    # 3. Read File
    with open(sql_file_path, 'r') as f:
        full_sql = f.read()

    # 4. Split by '/' (The standard SQL*Plus delimiter)
    commands = full_sql.split('/')

    # 5. Execute
    for cmd in commands:
        clean_cmd = cmd.strip()
        
        # Skip empty strings
        if not clean_cmd:
            continue
            
        # FIX: "Smart Detection" for PL/SQL
        # We look at the first real word, ignoring comments
        lines = clean_cmd.splitlines()
        first_token = ""
        for line in lines:
            s_line = line.strip()
            # Ignore empty lines or comment lines
            if not s_line or s_line.startswith('--'):
                continue
            # Found the first code line, grab the first word
            first_token = s_line.split()[0].upper()
            break
            
        is_plsql = (first_token == 'BEGIN' or first_token == 'DECLARE')
        
        if is_plsql:
            # PL/SQL: MUST end with ';'
            if not clean_cmd.endswith(';'):
                clean_cmd += ';'
        else:
            # Standard SQL: MUST NOT end with ';' for oracledb
            if clean_cmd.endswith(';'):
                clean_cmd = clean_cmd[:-1]

        try:
            cursor.execute(clean_cmd)
        except oracledb.Error as e:
            # Ignore "Table/Seq does not exist" errors (Clean start)
            error_str = str(e)
            if 'ORA-00942' in error_str or 'ORA-02289' in error_str:
                pass 
            else:
                print(f"⚠️ Warning: {e}")

    print("✅ SQL Script Executed Successfully.")

except Exception as e:
    print(f"❌ Fatal Error: {e}")
    exit(1)

finally:
    if 'connection' in locals():
        connection.close()