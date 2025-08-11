
/*
The script below is used to load data from CSV files into the PostgreSQL database.
This is from client-side loading using the \copy command, loading to PostgreSQL database.
It is efficient if we conduct on local machine and postgreSQL server does not have access to the file system.
*/

-- TRUNCATE TABLE bronze.crm_cust_info;
-- \copy bronze.crm_cust_info
-- FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_crm/cust_info.csv'
-- DELIMITER ','
-- CSV HEADER;

-- TRUNCATE TABLE bronze.crm_prd_info
-- \copy bronze.crm_prd_info
-- FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_crm/prd_info.csv'
-- DELIMITER ','
-- CSV HEADER;

-- TRUNCATE TABLE bronze.crm_sales_details
-- \copy bronze.crm_sales_details
-- FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_crm/sales_details.csv'
-- DELIMITER ','
-- CSV HEADER;

-- TRUNCATE TABLE bronze.erp_cust_az12
-- \copy bronze.erp_cust_az12
-- FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
-- DELIMITER ','
-- CSV HEADER;

-- TRUNCATE TABLE bronze.erp_loc_a101
-- \copy bronze.erp_loc_a101
-- FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
-- DELIMITER ','
-- CSV HEADER;

-- TRUNCATE TABLE bronze.erp_px_cat_g1v2
-- \copy bronze.erp_px_cat_g1v2
-- FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
-- DELIMITER ','
-- CSV HEADER;



/*
================================
Stored procedure: Load Bronze layer (source --> bronze)
================================
Script purpose:
    This stored procedure loads data from CSV files into the Bronze layer of the data warehouse.
    It performs the following actions:
        - Truncates existing tables in the Bronze schema.
        - Loads data from CSV files into the corresponding Bronze tables.
Parameters:
    None
Usage:
    CALL bronze.load_bronze();
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
BEGIN
    RAISE NOTICE 'LOADING BRONZE LAYER ...';
    batch_start_time := clock_timestamp();
    -- CRM Customer Info
    RAISE NOTICE 'Loading crm_cust_info...';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE $cmd$
        COPY bronze.crm_cust_info
        FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER
    $cmd$;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_cust_info loaded successfully in % seconds.', EXTRACT(EPOCH FROM (end_time - start_time));
    
    -- CRM Product Info
    RAISE NOTICE 'Loading crm_prd_info...';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE $cmd$
        COPY bronze.crm_prd_info
        FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_crm/prd_info.csv'
        DELIMITER ','
        CSV HEADER
    $cmd$;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_prd_info loaded successfully in % seconds.', EXTRACT(EPOCH FROM (end_time - start_time));

    -- CRM Sales Details
    RAISE NOTICE 'Loading crm_sales_details...';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE $cmd$
        COPY bronze.crm_sales_details
        FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER
    $cmd$;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_sales_details loaded successfully in % seconds.', EXTRACT(EPOCH FROM (end_time - start_time));

    -- ERP Customer AZ12
    RAISE NOTICE 'Loading erp_cust_az12...';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE $cmd$
        COPY bronze.erp_cust_az12
        FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER
    $cmd$;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_cust_az12 loaded successfully in % seconds.', EXTRACT(EPOCH FROM (end_time - start_time));


    -- ERP Location A101
    RAISE NOTICE 'Loading erp_loc_a101...';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE $cmd$
        COPY bronze.erp_loc_a101
        FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        DELIMITER ','
        CSV HEADER
    $cmd$;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_loc_a101 loaded successfully in % seconds.', EXTRACT(EPOCH FROM (end_time - start_time));


    -- ERP PX_CAT_G1V2
    RAISE NOTICE 'Loading erp_px_cat_g1v2...';
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE $cmd$
        COPY bronze.erp_px_cat_g1v2
        FROM '/Users/phucnguyen/Desktop/Hands-on-project/PostgreSQL-project/postgreSQL-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER
    $cmd$;
    end_time := clock_timestamp();
    batch_end_time := clock_timestamp();
    RAISE NOTICE 'Bronze layer load completed successfully.';
    RAISE NOTICE 'Total time taken for loading bronze layer: % seconds.', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));

EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading bronze layer: %', SQLERRM;
END;
$$;

CALL bronze.load_bronze();
