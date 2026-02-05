--------------------------------------------------------
-- PERMANENT ARCHIVE SETUP (Run Once)
-- Purpose: Creates "Forever Storage" for Archives & Errors.
--------------------------------------------------------

-- 1. Batch ID Sequence
DECLARE
    v_count NUMBER;
BEGIN
    SELECT count(*) INTO v_count FROM user_sequences WHERE sequence_name = 'SEQ_BATCH_ID';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE seq_batch_id START WITH 1000 INCREMENT BY 1';
    END IF;
END;
/

-- 2. The Archive Table (The Raw Vault)
DECLARE
    v_count NUMBER;
BEGIN
    SELECT count(*) INTO v_count FROM user_tables WHERE table_name = 'RAW_SALES_ARCHIVE';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE raw_sales_archive (
                trans_id    VARCHAR2(50),
                cust_id     VARCHAR2(50),
                cust_name   VARCHAR2(100),
                prod_id     VARCHAR2(50),
                prod_name   VARCHAR2(100),
                category    VARCHAR2(50),
                price       VARCHAR2(50),
                quantity    VARCHAR2(50),
                txn_date    VARCHAR2(50),
                -- Audit Columns
                batch_id    NUMBER,
                archived_at TIMESTAMP DEFAULT SYSTIMESTAMP,
                source_file VARCHAR2(100),
                CONSTRAINT pk_raw_archive PRIMARY KEY (batch_id, trans_id)
            )';
    END IF;
END;
/

-- 3. The Reject Table (Error History) - MOVED HERE
DECLARE
    v_count NUMBER;
BEGIN
    SELECT count(*) INTO v_count FROM user_tables WHERE table_name = 'ERR_SALES_REJECTS';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE err_sales_rejects (
                reject_id   NUMBER GENERATED ALWAYS AS IDENTITY,
                batch_id    NUMBER,
                trans_id    VARCHAR2(50),
                amount      NUMBER,
                reason      VARCHAR2(255),
                rejected_at TIMESTAMP DEFAULT SYSTIMESTAMP
            )';
    END IF;
END;
/