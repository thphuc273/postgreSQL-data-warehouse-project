-- crm_cust_info table
-- CHECK FOR DUPLICATES OR NULL IN PRIMARY KEY
-- EXPECTATION: NO RESULTS
SELECT 
cst_id,
count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;

-- CHECK FOR UNWANTED SPACE
-- EXPECTATION: NO RESULTS
SELECT cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

-- DATA STANDARDIZATION & CONSISTENT
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

-- crm_prd_info table
-- CHECK FOR DUPLICATES OR NULL IN PRIMARY KEY
-- EXPECTATION: NO RESULTS
SELECT 
prd_id,
count(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL;

-- CHECK FOR UNWANTED SPACE
-- EXPECTATION: NO RESULTS
SELECT prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- CHECK FOR NEGATIVE NUMBER OR NULL
-- EXPECTATION: NO RESULTS
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- CHECK FOR DATE LOGIC (START DATE < END DATE)
select * from silver.crm_prd_info
where prd_start_dt > prd_end_dt;

-- crm_sales_details table
-- CHECK VALID DATE: ORDER DATE, SHIP DATE, DUE DATE
SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LENGTH(sls_due_dt::text) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101;

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt > sls_due_dt OR sls_order_dt > sls_due_dt

-- CHECK DATA CONSISTENT: BETWEEN SALES, QUANTITY, AND PRICE
-- >> SALES = QUANTITY * PRICE
-- >> VALUE MUST NOT BE NULL, ZERO OR NEGATIVE
-- >>> SOLUTION: TELL DATA EXPERT TO FIX IT IN SOURCE SYSTEM OR FIX IN DATA WAREHOUSE

-- CHECK DATA CONSISTENT: BETWEEN SALES, QUANTITY, AND PRICE
select distinct
sls_sales as old_sales,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0  OR ABS(sls_price) * sls_quantity != sls_sales
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
sls_price as old_price,
CASE WHEN sls_price <= 0 OR sls_price IS NULL 
    THEN sls_sales / NULLIF(sls_quantity, 0) 
    ELSE sls_price
END AS sls_price,
sls_quantity
from silver.crm_sales_details
where sls_sales IS NULL 
OR sls_quantity IS NULL 
OR sls_price IS NULL
OR sls_price * sls_quantity != sls_sales
ORDER BY sls_sales, sls_quantity, sls_price;

-- erp_cust_az12 table
-- CHECK cid column
-- CHECK bdate column
select distinct bdate
from silver.erp_cust_az12
where bdate > CURRENT_DATE or bdate < '1924-01-01';
-- CHECK gen column
select distinct gen
from silver.erp_cust_az12

-- erp_loc_a101 table
-- CHECK cntry column
SELECT distinct cntry 
FROM silver.erp_loc_a101
ORDER BY cntry

-- erp px_cat_g1v2 table
--CHECK UNWANTED SPACE
select * from silver.erp_px_cat_g1v2
where cat != TRIM(cat)
or subcat != TRIM(subcat)
or maintenance != TRIM(maintenance);
-- CHECK DATA STANDARDIZATION
select distinct maintenance from silver.erp_px_cat_g1v2



-- >>>>> FINAL PROCEDURE LOAD_SILVER()
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
BEGIN
    RAISE NOTICE 'LOADING SILVER LAYER ...';
    batch_start_time := clock_timestamp();

    -- >> INSERT FINAL DATA INTO silver.crm_cust_info
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE 'Loading data into silver.crm_cust_info...';
    start_time := clock_timestamp();
    INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE UPPER(TRIM(cst_marital_status))
            WHEN 'M' THEN 'Married'
            WHEN 'S' THEN 'Single'
            ELSE 'n/a'
        END,
        CASE UPPER(TRIM(cst_gndr))
            WHEN 'F' THEN 'Female'
            WHEN 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;
    end_time := clock_timestamp();
    RAISE NOTICE 'Data loaded into silver.crm_cust_info in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- >> INSERT FINAL DATA INTO silver.crm_prd_info
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE 'Loading data into silver.crm_prd_info...';
    start_time := clock_timestamp();
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt)
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id,
        SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            WHEN 'R' THEN 'Road'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info;
    end_time := clock_timestamp();
    RAISE NOTICE 'Data loaded into silver.crm_prd_info in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- >> INSERT FINAL DATA INTO silver.crm_sales_details
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE 'Loading data into silver.crm_sales_details...';
    start_time := clock_timestamp();
    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price)
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD') 
        END,
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD') 
        END,
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD') 
        END,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR ABS(sls_price) * sls_quantity != sls_sales
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price <= 0 OR sls_price IS NULL 
            THEN sls_sales / NULLIF(sls_quantity, 0) 
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;
    end_time := clock_timestamp();
    RAISE NOTICE 'Data loaded into silver.crm_sales_details in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- >> INSERT FINAL DATA INTO silver.erp_cust_az12
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE 'Loading data into silver.erp_cust_az12...';
    start_time := clock_timestamp();
    INSERT INTO silver.erp_cust_az12(
        cid,
        bdate,
        gen)
    SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
             ELSE cid
        END AS cid,
        CASE WHEN bdate > CURRENT_DATE THEN NULL
             ELSE bdate
        END AS bdate,
        CASE WHEN TRIM(UPPER(gen)) IN ('F', 'FEMALE') THEN 'Female'
             WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male'
             ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;
    end_time := clock_timestamp();
    RAISE NOTICE 'Data loaded into silver.erp_cust_az12 in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- >> INSERT FINAL DATA INTO silver.erp_loc_a101
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE 'Loading data into silver.erp_loc_a101...';
    start_time := clock_timestamp();
    INSERT INTO silver.erp_loc_a101(
        cid,
        cntry)
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
             WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
             WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
             ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;
    end_time := clock_timestamp();
    RAISE NOTICE 'Data loaded into silver.erp_loc_a101 in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- >> INSERT FINAL DATA INTO silver.erp_px_cat_g1v2
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE 'Loading data into silver.erp_px_cat_g1v2...';
    start_time := clock_timestamp();
    INSERT INTO silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance)
    SELECT 
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
    end_time := clock_timestamp();
    RAISE NOTICE 'Data loaded into silver.erp_px_cat_g1v2 in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    batch_end_time := clock_timestamp();
    RAISE NOTICE 'Silver layer load completed successfully.';
    RAISE NOTICE 'Total time taken for loading silver layer: % seconds.', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
END;
$$;

CALL silver.load_silver()
