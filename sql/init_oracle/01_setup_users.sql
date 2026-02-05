-- 01_setup_users.sql
-- Run this as SYSTEM

-- 1. Create the dedicated user
CREATE USER retail_dw IDENTIFIED BY RetailPass123;

-- 2. Grant standard permissions
GRANT CONNECT, RESOURCE TO retail_dw;
GRANT CREATE VIEW TO retail_dw;
GRANT UNLIMITED TABLESPACE TO retail_dw;

-- 3. CRITICAL: Grant permission to read/write external files
-- This allows the user to access the mapped Docker volume
GRANT CREATE ANY DIRECTORY TO retail_dw;

-- 4. Verify
SELECT username, account_status FROM dba_users WHERE username = 'RETAIL_DW';