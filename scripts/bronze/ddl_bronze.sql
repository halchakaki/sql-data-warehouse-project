--BRONZE LAYER SCRIPT STILL IN PROGRESS--

/*
=============================================================================
DDL Script: Create Bronze Tables
=============================================================================
Script Purpose:
	This script creates tables in the 'bronze' schema, dropping existing 
	tables if they already exist.
	Run this script to re-define the DDL structure of 'bronze' Tables
=============================================================================
*/


	---- 1. DROP + CREATE TABLES.
	
	-- Create cust_info table from crm source.
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);
----------------------------------------------------
	-- Create prd_info table from crm source.
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt TIMESTAMP,
	prd_end_dt TIMESTAMP
);
----------------------------------------------------
	-- Create sales_details table from crm source.
DROP TABLE IF EXISTS bronze.crm_sales_info;
CREATE TABLE bronze.crm_sales_info (
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
----------------------------------------------------
	-- Create LOC_A101 table from erp source.
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid VARCHAR(50),
	cntry VARCHAR(50)
);
----------------------------------------------------
	-- Create CUST_AZ12 table from erp source.
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);
----------------------------------------------------
	-- Create PX_CAT_G1V2 table from erp source.
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);
----------------------------------------------------


--==================================================================================--



	---- 2. LOAD DATA.
	
	-- Clear data from table.
TRUNCATE TABLE bronze.crm_cust_info; 

	-- Load cust_info from CSV to bronze.crm_cust_info table.
	-- if \copy gets permission errors, right click on crm_cust_info table on the left and import data - from file path. (Header = Yes, Delimiter = ',').
\COPY bronze.crm_cust_info
FROM 'C:/Users/halch/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
DELIMITER ','
CSV HEADER;
----------------------------------------------------

	-- Clear data from table.
TRUNCATE TABLE bronze.crm_prd_info;

	-- Load prd_info from CSV to bronze.crm_prd_info table.
	-- if \copy gets permission errors, right click on crm_prd_info table on the left and import data - from file path. (Header = Yes, Delimiter = ',').
\COPY bronze.crm_prd_info
FROM 'C:/Users/halch/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
DELIMITER ','
CSV HEADER;
----------------------------------------------------

	-- Clear data from table.
TRUNCATE TABLE bronze.crm_sales_info;

	-- Load sales_info from CSV to bronze.crm_sales_info table.
	-- if \copy gets permission errors, right click on crm_sales_info table on the left and import data - from file path. (Header = Yes, Delimiter = ',').
\COPY bronze.crm_sales_info
FROM 'C:/Users/halch/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
DELIMITER ','
CSV HEADER;
----------------------------------------------------

	-- Clear data from table.
TRUNCATE TABLE bronze.erp_loc_a101; 

	-- Load lOC_A101 from CSV to bronze.erp_loc_a101 table.
	-- if \copy gets permission errors, right click on erp_loc_a101 table on the left and import data - from file path. (Header = Yes, Delimiter = ',').
\COPY bronze.erp_loc_a101
FROM 'C:/Users/halch/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
DELIMITER ','
CSV HEADER;
----------------------------------------------------

	-- Clear data from table.
TRUNCATE TABLE bronze.erp_cust_az12;

	-- Load prd_info from CSV to bronze.erp_cust_az12 table.
	-- if \copy gets permission errors, right click on erp_cust_az12 table on the left and import data - from file path. (Header = Yes, Delimiter = ',').
\COPY bronze.erp_cust_az12
FROM 'C:/Users/halch/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
DELIMITER ','
CSV HEADER;
----------------------------------------------------

	-- Clear data from table.
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	-- Load sales_info from CSV to bronze.erp_px_cat_g1v2 table.
	-- if \copy gets permission errors, right click on erp_px_cat_g1v2 table on the left and import data - from file path. (Header = Yes, Delimiter = ',').
\COPY bronze.erp_px_cat_g1v2
FROM 'C:/Users/halch/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
DELIMITER ','
CSV HEADER;
----------------------------------------------------


--==================================================================================--



	---- 3. TEST TABLES & DATA.


SELECT * FROM bronze.crm_cust_info;

SELECT * FROM bronze.crm_prd_info;

SELECT * FROM bronze.crm_sales_info;

SELECT * FROM bronze.erp_loc_a101;

SELECT * FROM bronze.erp_cust_az12;

SELECT * FROM bronze.erp_px_cat_g1v2;

