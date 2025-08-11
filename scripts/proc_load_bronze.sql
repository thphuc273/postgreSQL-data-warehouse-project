/*
================================
DDL script: Create Bronze Layer
================================
Script purpose:
    This script creates tables in Bronze schema, drop existing tables if they exist, 
    and prepares the database for loading data.
    Run this script to re-define the DDL structure of Bronze tables
*/

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status CHAR(1),
    cst_gndr CHAR(1),
    cst_create_date DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(255),
    prd_cost NUMERIC(10,2),
    prd_line CHAR(1),
    prd_start_dt DATE,
    prd_end_dt DATE
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
    cid VARCHAR(50),
    country VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
    id VARCHAR(50), 
    cat VARCHAR(50),
    subcat VARCHAR(50), 
    maintenance VARCHAR(50)
);



