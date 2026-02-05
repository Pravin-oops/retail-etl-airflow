--------------------------------------------------------
-- PACKAGE SPECIFICATION
--------------------------------------------------------
CREATE OR REPLACE PACKAGE pkg_etl_retail AS
    PROCEDURE load_daily_sales;
END pkg_etl_retail;
/

--------------------------------------------------------
-- PACKAGE BODY
--------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY pkg_etl_retail AS

    -- ===============================================================
    -- PRIVATE FUNCTION: ARCHIVE_RAW_DATA
    -- ===============================================================
    FUNCTION archive_raw_data RETURN NUMBER IS
        v_batch_id     NUMBER;
        v_dynamic_file VARCHAR2(100);
        v_count        NUMBER;
    BEGIN
        v_batch_id := seq_batch_id.NEXTVAL;
        v_dynamic_file := 'sales_data_' || TO_CHAR(SYSDATE, 'DDMMYYYY') || '.csv';
        
        DBMS_OUTPUT.PUT_LINE('--- STEP 1: ARCHIVING (Batch ' || v_batch_id || ') ---');
        DBMS_OUTPUT.PUT_LINE('Target File: ' || v_dynamic_file);

        BEGIN
            EXECUTE IMMEDIATE 'ALTER TABLE ext_sales_data LOCATION (''' || v_dynamic_file || ''')';
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('❌ Error: Could not find file ' || v_dynamic_file);
                RAISE;
        END;

        INSERT INTO raw_sales_archive (
            trans_id, cust_id, cust_name, prod_id, prod_name, 
            category, price, quantity, txn_date, 
            batch_id, archived_at, source_file
        )
        SELECT 
            trans_id, cust_id, cust_name, prod_id, prod_name, 
            category, price, quantity, txn_date,
            v_batch_id, SYSTIMESTAMP, v_dynamic_file
        FROM ext_sales_data;
        
        v_count := SQL%ROWCOUNT;
        DBMS_OUTPUT.PUT_LINE('✅ Archived ' || v_count || ' rows safely.');
        COMMIT;
        
        RETURN v_batch_id;
    END archive_raw_data;


    -- ===============================================================
    -- PRIVATE PROCEDURE: LOAD_STAR_SCHEMA
    -- ===============================================================
    PROCEDURE load_star_schema(p_batch_id NUMBER) IS
        CURSOR c_raw_data IS
            SELECT trans_id, cust_id, cust_name, prod_id, prod_name, category, 
                   TO_NUMBER(price) as price, 
                   TO_NUMBER(quantity) as quantity, 
                   TO_DATE(txn_date, 'YYYY-MM-DD') as txn_date
            FROM raw_sales_archive
            WHERE batch_id = p_batch_id;
            -- REMOVED "AND category IS NOT NULL" so we can catch them manually!
              
        v_cust_key     NUMBER;
        v_prod_key     NUMBER;
        v_time_id      DATE;
        v_amount       NUMBER;
        v_loaded_cnt   NUMBER := 0;
        v_rejected_cnt NUMBER := 0;
        v_err_msg      VARCHAR2(500);
    BEGIN
        DBMS_OUTPUT.PUT_LINE('--- STEP 2: TRANSFORM and LOAD ---');
        
        FOR r IN c_raw_data LOOP
            BEGIN
                -- 1. BUSINESS LOGIC CHECK: Missing Category
                IF r.category IS NULL THEN
                     -- A. Log to Console
                    DBMS_OUTPUT.PUT_LINE('⚠️ REJECT: Trans ' || r.trans_id || ' (Missing Category)');
                    
                    -- B. Insert into Reject Table
                    INSERT INTO err_sales_rejects (batch_id, trans_id, amount, reason)
                    VALUES (p_batch_id, r.trans_id, (r.price * r.quantity), 'Data Quality: Missing Category');
                    
                    v_rejected_cnt := v_rejected_cnt + 1;
                    CONTINUE; -- Skip this row
                END IF;

                -- 2. CALCULATE AMOUNT
                v_amount := r.price * r.quantity;

                -- ---------------------------------------------------
                -- A. DIMENSION HANDLING
                -- ---------------------------------------------------
                
                -- Customer
                BEGIN
                    SELECT cust_surrogate_key INTO v_cust_key FROM dim_customer
                    WHERE cust_original_id = r.cust_id;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_cust_key := seq_cust_id.NEXTVAL;
                        INSERT INTO dim_customer (cust_surrogate_key, cust_original_id, cust_name)
                        VALUES (v_cust_key, r.cust_id, r.cust_name);
                END;

                -- Product
                BEGIN
                    SELECT prod_surrogate_key INTO v_prod_key FROM dim_product
                    WHERE prod_original_id = r.prod_id;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_prod_key := seq_prod_id.NEXTVAL;
                        INSERT INTO dim_product (prod_surrogate_key, prod_original_id, prod_name, category)
                        VALUES (v_prod_key, r.prod_id, r.prod_name, r.category);
                END;

                -- Time
                v_time_id := r.txn_date;
                MERGE INTO dim_time d USING (SELECT v_time_id AS t_date FROM dual) s
                ON (d.time_id = s.t_date)
                WHEN NOT MATCHED THEN
                    INSERT (time_id, day_name, month_name, year_num, quarter)
                    VALUES (v_time_id, TO_CHAR(v_time_id, 'DAY'), TO_CHAR(v_time_id, 'MONTH'), 
                            TO_NUMBER(TO_CHAR(v_time_id, 'YYYY')), TO_NUMBER(TO_CHAR(v_time_id, 'Q')));

                -- ---------------------------------------------------
                -- B. FACT LOAD
                -- ---------------------------------------------------
                INSERT INTO fact_sales (
                    sales_id, cust_surrogate_key, prod_surrogate_key, time_id, quantity, amount, txn_date
                ) VALUES (
                    seq_sales_id.NEXTVAL, v_cust_key, v_prod_key, v_time_id, 
                    r.quantity, v_amount, r.txn_date
                );
                
                v_loaded_cnt := v_loaded_cnt + 1;

            EXCEPTION
                -- ---------------------------------------------------
                -- C. SYSTEM ERROR LOGGING
                -- ---------------------------------------------------
                WHEN OTHERS THEN
                    v_err_msg := SUBSTR(SQLERRM, 1, 200);
                    DBMS_OUTPUT.PUT_LINE('❌ SYSTEM ERROR on Trans ' || r.trans_id || ': ' || v_err_msg);
                    
                    INSERT INTO err_sales_rejects (batch_id, trans_id, amount, reason)
                    VALUES (p_batch_id, r.trans_id, v_amount, 'System Error: ' || v_err_msg);
                    
                    v_rejected_cnt := v_rejected_cnt + 1;
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('📊 SUMMARY: Loaded ' || v_loaded_cnt || ' | Rejected ' || v_rejected_cnt);
        COMMIT;
    END load_star_schema;


    -- ===============================================================
    -- PUBLIC MAIN
    -- ===============================================================
    PROCEDURE load_daily_sales IS
        v_current_batch NUMBER;
    BEGIN
        v_current_batch := archive_raw_data();
        load_star_schema(v_current_batch);
        DBMS_OUTPUT.PUT_LINE('--- JOB COMPLETE ---');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('❌ Fatal Job Failure: ' || SQLERRM);
            ROLLBACK;
            RAISE;
    END load_daily_sales;

END pkg_etl_retail;
/