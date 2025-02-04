
--checking quality of data 
--=========================
--presence of duplicates 
SELECT cst_id,
       count(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id 
HAVING count(*)>1 OR cst_id IS NULL;
--check for unwanted spaces
--Expectation:no result
SELECT cst_lastname
FROM bronze.crm_cust_info WHERE cst_lastname!=TRIM(cst_lastname);
--checking consistency in low cardinality columns
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;
--checking consistency in low cardinality columns
SELECT  DISTINCT cst_martial_status
FROM bronze.crm_cust_info;
--=================================
--splitting data
  SELECT REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id
	  ,SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key
FROM [bronze].[crm_prd_info]
--check for unwanted spaces
--Expectation:no result
SELECT prd_nm
FROM bronze.crm_prd_info WHERE prd_nm!=TRIM(prd_nm);
--checking for negative numbers or NULLs
--Expectation:No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost<0 OR prd_cost IS NULL;
--checking consistency in low cardinality columns
SELECT DISTINCT prd_line
FROM [bronze].[crm_prd_info]
--checking consistency in low cardinality columns
SELECT DISTINCT prd_cost
FROM [bronze].[crm_prd_info]
 --checking start and end date quality 
 SELECT * FROM
 [bronze].[crm_prd_info] 
 WHERE prd_end_dt<prd_start_5dt;
 -- Expectation:no reuslt but we got some records
 SELECT prd_id
      ,prd_key
	  ,prd_nm
      ,prd_start_5dt
      ,prd_end_dt
	  ,LEAD(prd_start_5dt) OVER(PARTITION BY prd_key ORDER BY prd_start_5dt)-1 AS prd_end_dt_test
  FROM [Datawarehouse].[bronze].[crm_prd_info]
WHERE prd_key IN('AC-HE-HL-U509-R','AC-HE-HL-U509')
   --WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN 
  --(SELECT DISTINCT id FROM bronze.erp_cat_giv2)
--========================================================  
 --Checking unwanted spaces at sls_old_num
 --Exprecations:no result(we got empty means nounwanted spaces )
 SELECT sls_ord_num
  FROM [bronze].[crm_sales_details]
  WHERE sls_ord_num!=TRIM(sls_ord_num)

--sls_prd_key and sls_cust_id are present in 
--[silver].[crm_cust_info][silver].[crm_prd_info]
--so these columns have good data no need any transformations.


--orderdate,duedate ship date are not in date format 
--formatit reomve negatives, nulls zeros as theyc ant be casted to DATE
--Also check for outliers by validating the 
--boundaries of the date range
SELECT NULLIF(sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NULL OR sls_order_dt<=0 OR LEN(sls_order_dt)!=8 OR
sls_order_Dt>20500101 OR sls_order_Dt<19000101;

--no resultset hence there is quality data in sls_ship_dt
SELECT NULLIF(sls_ship_dt,0)
FROM bronze.crm_sales_details
WHERE sls_ship_dt IS NULL OR sls_ship_dt<=0 OR LEN(sls_ship_dt)!=8 OR
sls_ship_dt>20500101 OR sls_ship_dt<19000101;
--no resultset hence there is quality data in sls_ship_dt
SELECT NULLIF(sls_due_dt,0)
FROM bronze.crm_sales_details
WHERE sls_due_dt IS NULL OR sls_due_dt<=0 OR LEN(sls_due_dt)!=8 OR
sls_due_dt>20500101 OR sls_due_dt<19000101;
--no resultset hence there is quality data in sls_ship_dt
SELECT *
FROM [bronze].[crm_sales_details]
WHERE sls_order_dt>sls_due_dt OR sls_order_dt>sls_ship_dt;
---Checking quality of sales,quantity,prcie
--quality has good data
--so tranform the sales,price
SELECT DISTINCT
     sls_sales,
    sls_quantity,
	sls_price
FROM [bronze].[crm_sales_details]
WHERE sls_sales!=sls_price*sls_quantity
OR sls_price IS NULL 
OR sls_quantity IS NULL 
OR sls_sales IS NULL 
OR sls_price<=0
OR sls_quantity<=0
OR sls_sales<=0
order by sls_sales,
    sls_quantity,
	sls_price;
--===================================================================


 --remvoe NAS in erp_cust_az12 in bronze.erp_cust_az12 as 
 --NAS is not present in  [silver].[crm_cust_info]

  SELECT 
	CASE 
	    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
	END cid
  FROM [bronze].[erp_cust_az12]
--check bdate has corretc date format 
--and also check if there is  any future bday then
SELECT DISTINCT 
 bdate
 FROM [bronze].[erp_cust_az12]
 WHERE bdate<'1924-01-01' OR bdate>GETDATE();


 --Data standardization and consistency
 SELECT DISTINCT
 gen
 FROM  [silver].[erp_cust_az12] WHERE gen!=TRIM(gen);



 --========================================
 
 --we cid incrm_cust_info without any sysmbol but in this er_loc_a101 has '-'
  --so now we need to remove it
 SELECT 
      REPLACE(cid,'-','') AS cid
      ,cntry
  FROM [Datawarehouse].[bronze].[erp_loc_a101]

  --data standardiazation & consistency
  --handling missing values and normalize or blank country codes
  SELECT DISTINCT cntry
  FROM bronze.erp_loc_a101
  ORDER BY cntry
  --============================================================
 
  --check unwanted spaces in cat
  SELECT  cat
  FROM [bronze].[erp_cat_giv2]
  WHERE TRIM(cat)!=cat;

--check unwanted spaces in cat
  SELECT  subcat
  FROM [bronze].[erp_cat_giv2]
  WHERE TRIM(subcat)!=subcat;

  --check unwanted spaces in cat
  SELECT  maintenance
  FROM [bronze].[erp_cat_giv2]
  WHERE TRIM(maintenance)!=maintenance;
