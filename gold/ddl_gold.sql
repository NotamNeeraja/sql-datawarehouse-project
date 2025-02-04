
 CREATE VIEW gold.dim_customers AS(
 SELECT 
     ROW_NUMBER() OVER(ORDER BY ci.[cst_id])customer_key,
     ci.[cst_id] AS customer_id,
      ci.[cst_key] AS customer_number,
      ci.[cst_firstname] AS first_name,
      ci.[cst_lastname] AS last_name,
	  la.[cntry]AS country,
	   CASE WHEN  cst_gndr!='n/a' THEN cst_gndr
	   ELSE COALESCE(gen,'n/a')
	   END gender,
      ci.[cst_martial_status] AS martial_status,
	  ca.[bdate] AS birthday,
      ci.[cst_create_Date] AS create_date
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=La.cid
)
   
--data quality check 
SELECT * FROM [gold].[dim_customers]
--====================================================

   
 CREATE VIEW gold.dim_products AS(
 SELECT
      ROW_NUMBER() OVER(ORDER BY pn.prd_key,pn.prd_start_5dt) AS product_key,
      pn.[prd_id] AS product_id,
	  pn.[prd_key] AS product_number,
      pn.[prd_nm] AS product_name,
      pn.[cat_id] AS category_id,
	  pc.cat AS category,
	  pc.subcat AS subcategory,
	  pc.maintenance ,
      pn.[prd_cost] AS cost,
      pn.[prd_line] AS product_line,
      pn.[prd_start_5dt] AS start_Date
FROM [silver].[crm_prd_info] pn
LEFT JOIN   [silver].[erp_cat_giv2] pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL
)

SELECT * FROM [gold].[dim_products]
--======================================================

   
CREATE VIEW gold.fact_sales AS(
SELECT
       sls_ord_num AS order_num
	   ,pr.product_key 
	   ,cu.customer_key
      ,sls_order_dt AS order_date
      ,sls_ship_dt AS shipping_date
      ,sls_due_dt AS due_date
      ,sls_sales AS sales_Amount
      ,sls_quantity AS quantity
      ,sls_price AS price
FROM [silver].[crm_sales_details] sd
LEFT JOIN [gold].[dim_products] pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN [gold].[dim_customers] cu
ON sd.sls_cust_id=cu.customer_id)
