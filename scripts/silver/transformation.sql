
/* 
THIS SCRIPT IS TO SHOW THE TRANSFORMATION FOR EACH TABLE AND NOT TO RUN AS A WHOLE
Please see 'ddl_silver.sql' for full script to run 
*/



--==============================================--
	--TRANSFORM crm_cust_info.

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
	)t WHERE flag_last = 1



--==============================================--

	--TRANSFORM crm_prd_info.

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
		ELSE 'n/n'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST (
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE)
		AS prd_end_dt
FROM bronze.crm_prd_info


--==============================================--

	--TRANSFORM crm_sales_info.

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
FROM bronze.crm_sales_info



--==============================================--

	--TRANSFORM erp_loc_a101.

SELECT
	REPLACE (cid, '-', '') cid,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101


--==============================================--

	--TRANSFORM erp_cust_az12.

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
FROM bronze.erp_cust_az12


--==============================================--

	--TRANSFORM erp.px_cat_g1v2.
	--All data in this table are clean.

SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2


--==============================================--
