/*
=====================================================
Quality checks
=====================================================
Script purpose:
    This script performs quality checks on the Silver layer tables in the data warehouse.
    It checks for duplicates, null values, unwanted spaces, logical inconsistencies and standardization in the data.
    It includes checks for:
        - Null or duplicate primary keys
        - Unwanted leading/trailing spaces in text fields
        - Data standardization and consistency
        - Invalid date range and order
        - Data consistency between related fields
Usage note:
    - Run these checks after loading data into the Silver layer.
    - Investigate any issues found and take corrective actions as needed.
*/

-- --------------------------------------------------
-- 1) silver.crm_cust_info
-- --------------------------------------------------
SELECT cst_id, COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

--- crm_cust_info: null primary key count (expect 0) ---
SELECT COUNT(*) AS null_cst_id_count
FROM silver.crm_cust_info
WHERE cst_id IS NULL;

--- crm_cust_info: unwanted leading/trailing spaces in first name (expect 0 rows) ---'
SELECT cst_id, cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname IS NOT NULL
  AND cst_firstname != TRIM(cst_firstname)
LIMIT 100;

--- crm_cust_info: unwanted leading/trailing spaces in last name (expect 0 rows) ---'
SELECT cst_id, cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname IS NOT NULL
  AND cst_lastname != TRIM(cst_lastname)
LIMIT 100;

--- crm_cust_info: unwanted spaces in gender (expect 0 rows) ---'
SELECT cst_id, cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr IS NOT NULL
  AND cst_gndr != TRIM(cst_gndr)
LIMIT 100;

--- crm_cust_info: distinct gender values ---'
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info ORDER BY cst_gndr;

--- crm_cust_info: distinct marital_status values ---'
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info ORDER BY cst_marital_status;


-- --------------------------------------------------
-- 2) silver.crm_prd_info
-- --------------------------------------------------
--- crm_prd_info: duplicates or null prd_id (expect none) ---'
SELECT prd_id, COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

--- crm_prd_info: null prd_id count ---'
SELECT COUNT(*) AS null_prd_id_count FROM silver.crm_prd_info WHERE prd_id IS NULL;

--- crm_prd_info: unwanted spaces in prd_nm (expect 0 rows) ---'
SELECT prd_id, prd_nm
FROM silver.crm_prd_info
WHERE prd_nm IS NOT NULL
  AND prd_nm != TRIM(prd_nm)
LIMIT 100;

--- crm_prd_info: negative or null prd_cost (expect 0 rows) ---'
SELECT prd_id, prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0
LIMIT 100;

--- crm_prd_info: distinct prd_line values ---'
SELECT DISTINCT prd_line FROM silver.crm_prd_info ORDER BY prd_line;

--- crm_prd_info: start date after end date (expect 0 rows) ---'
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt IS NOT NULL
  AND prd_end_dt IS NOT NULL
  AND prd_start_dt > prd_end_dt
LIMIT 100;


-- --------------------------------------------------
-- 3) silver.crm_sales_details
-- --------------------------------------------------
--- crm_sales_details: invalid date fields (format YYYYMMDD integer checks) ---'
SELECT sls_ord_num, sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt IS NULL
   OR sls_due_dt <= 0
   OR LENGTH(sls_due_dt::text) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101
LIMIT 100;

--- crm_sales_details: order/ship/due date logic issues ---'
SELECT *
FROM silver.crm_sales_details
WHERE (sls_ship_dt IS NOT NULL AND sls_due_dt IS NOT NULL AND sls_ship_dt > sls_due_dt)
   OR (sls_order_dt IS NOT NULL AND sls_due_dt IS NOT NULL AND sls_order_dt > sls_due_dt)
LIMIT 100;

--- crm_sales_details: inconsistent sales vs price*quantity ---'
SELECT DISTINCT
    sls_ord_num,
    sls_sales AS old_sales,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR ABS(sls_price) * sls_quantity != sls_sales
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END AS expected_sales,
    sls_price AS old_price,
    CASE WHEN sls_price <= 0 OR sls_price IS NULL
         THEN CASE WHEN sls_quantity = 0 THEN NULL ELSE sls_sales / NULLIF(sls_quantity,0) END
         ELSE sls_price
    END AS derived_price,
    sls_quantity
FROM silver.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR (sls_price * sls_quantity) != sls_sales
ORDER BY old_sales NULLS FIRST, sls_quantity, old_price
LIMIT 200;


-- --------------------------------------------------
-- 4) silver.erp_cust_az12
-- --------------------------------------------------
--- erp_cust_az12: birthdate out-of-range ---'
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate IS NOT NULL AND (bdate > CURRENT_DATE OR bdate < DATE '1924-01-01')
LIMIT 100;

\echo '--- erp_cust_az12: distinct gen values ---'
SELECT DISTINCT gen FROM silver.erp_cust_az12 ORDER BY gen;


-- --------------------------------------------------
-- 5) silver.erp_loc_a101
-- --------------------------------------------------
--- erp_loc_a101: distinct countries ---'
SELECT DISTINCT cntry FROM silver.erp_loc_a101 ORDER BY cntry;


-- --------------------------------------------------
-- 6) silver.erp_px_cat_g1v2
-- --------------------------------------------------
--- erp_px_cat_g1v2: unwanted spaces in text columns ---'
SELECT * FROM silver.erp_px_cat_g1v2
WHERE (cat IS NOT NULL AND cat != TRIM(cat))
   OR (subcat IS NOT NULL AND subcat != TRIM(subcat))
   OR (maintenance IS NOT NULL AND maintenance != TRIM(maintenance))
LIMIT 200;

--- erp_px_cat_g1v2: distinct maintenance values ---'
SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2 ORDER BY maintenance;

