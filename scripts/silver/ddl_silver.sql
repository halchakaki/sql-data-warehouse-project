/*
--==================================================================================--
DDL Script: Create Silver Tables
--==================================================================================--
Script Purpose:
	This script creates tables in the 'sivler' schema, dropping existing tables if
	they already exist.
	Run this script to re-deifne the DDL structure of 'bronze' tables.
	See below for script to Truncate and Insert transformed/cleansed data.
*/


	---- 1. DROP + CREATE TABLES & ADD create_date COLUMN ----
	
-------------------------------------------------------	
	-- Create cust_info table from crm source.
DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP  --Auto timestamp when row values are inserted
);
-------------------------------------------------------
	-- Create prd_info table from crm source.
DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP  --Auto timestamp when row values are inserted
);
-------------------------------------------------------
	-- Create sales_details table from crm source.
DROP TABLE IF EXISTS silver.crm_sales_info;

CREATE TABLE silver.crm_sales_info (
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP  --Auto timestamp when row values are inserted
);
-------------------------------------------------------
	-- Create LOC_A101 table from erp source.
DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
	cid VARCHAR(50),
	cntry VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP  --Auto timestamp when row values are inserted
);
-------------------------------------------------------
	-- Create CUST_AZ12 table from erp source.
DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP  --Auto timestamp when row values are inserted
);
-------------------------------------------------------
	-- Create PX_CAT_G1V2 table from erp source.
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP  --Auto timestamp when row values are inserted
);
-------------------------------------------------------



--==================================================================================--

/*
--==================================================================================--
DDL Script: Load Silver Layer (Bronze -> Silver)
--==================================================================================--
Script Purpose:
	This script performs the ETL (Extract, Transform, Load) process to populate
	the 'silver' schema tables from the 'bronze' schema.
Action Performed:
	-Truncate Silver Tables.
	-Insert transformed and cleansed data from Bronze into Silver tables.
*/


	---- 2. TRANSFORM DATA ----
	
-------------------------------------------------------	
	-- Clear data from table silver.crm_cust_info
TRUNCATE TABLE silver.crm_cust_info; 

	-- Inserting Transformed Data.
INSERT INTO silver.crm_cust_info (
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
	TRIM(cst_firstname) AS cst_firstname, 
	TRIM(cst_lastname) AS cst_lastname,	
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 	 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 	 ELSE 'n/a'
	END cst_gndr,
	cst_create_date  
	FROM (
		SELECT			
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id  ORDER BY cst_create_date DESC) AS flag_last    
		FROM bronze.crm_cust_info
	)t WHERE flag_last = 1;
-------------------------------------------------------
	-- Clear data from table silver.crm_prd_info
TRUNCATE TABLE silver.crm_prd_info;

	-- Inserting Transformed Data.
INSERT INTO silver.crm_prd_info (
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
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key FROM 7 FOR LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST (
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE)
		AS prd_end_dt
FROM bronze.crm_prd_info;
-------------------------------------------------------
	-- Clear data from table silver.crm_sales_info
TRUNCATE TABLE silver.crm_sales_info;

	-- Inserting Transformed Data.
INSERT INTO silver.crm_sales_info (
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
	CASE WHEN sls_order_dt <= 0 
		OR LENGTH(sls_order_dt::text) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt <= 0 
		OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt <= 0 
		OR LENGTH(sls_due_dt::text) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,	
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_info;
-------------------------------------------------------
	-- Clear data from table silver.erp_loc_a101
TRUNCATE TABLE silver.erp_loc_a101;

	-- Inserting Transformed Data.
INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry)

SELECT
	REPLACE (cid, '-', '') cid,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;
-------------------------------------------------------
	-- Clear data from table silver.erp_cust_az12
TRUNCATE TABLE silver.erp_cust_az12;

	-- Inserting Transformed Data.
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen)

SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
		ELSE cid
	END cid,
	CASE
		WHEN bdate > CURRENT_DATE THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;
-------------------------------------------------------
	-- Clear data from table silver.erp_px_cat_g1v2
TRUNCATE TABLE silver.erp_px_cat_g1v2;

	-- Inserting Transformed Data.
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance)

	
SELECT  -- Data looks clean 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;   
-------------------------------------------------------

SELECT 'Silver_Layer Transformed/Cleansed Data Inserted Successfully';


--==========================================================--

