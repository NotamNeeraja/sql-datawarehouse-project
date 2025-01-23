 
 

CREATE DATABASE Datawarehouse;

USE Datawarehouse;

## creating schema for bronze silver and gold layer 
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL 
     DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info(
            cst_id INT,
			cst_key NVARCHAR(50),
			cst_firstname NVARCHAR(50),
			cst_lastname NVARCHAR(50),
			cst_martial_status NVARCHAR(50),
			cst_gndr NVARCHAR(50),
			cst_create_Date DATE
                );
IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL 
     DROP TABLE bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info(
                  prd_id INT,
				  prd_key NVARCHAR(50),
				  prd_nm NVARCHAR(50),
				  prd_cost INT,
				  prd_line NVARCHAR(50),
				  prd_start_5dt DATETIME,
				  prd_end_dt DATETIME
                 )
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL 
     DROP TABLE bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details(
                     sls_ord_num NVARCHAR(50),
					 sls_prd_key NVARCHAR(50),
					 sls_cust_id INT,
					 sls_order_dt INT,
					 sls_ship_dt INT,
					 sls_due_dt INT,
					 sls_sales INT,
					 sls_quantity INT,
					 sls_price INT) 
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL 
     DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101(
                     cid NVARCHAR(50),
					 cntry NVARCHAR(50)
					 );
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL 
     DROP TABLE bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12(
                  cid NVARCHAR(50),
				  bdate DATE,
				   gen NVARCHAR(50)
				   )
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL 
     DROP TABLE bronze.erp_cat_giv2
 CREATE TABLE bronze.erp_cat_giv2(
                     id NVARCHAR(50),
					 cat NVARCHAR(50),
					 subcat NVARCHAR(50),
					 maintenance NVARCHAR(50)
                    )
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
   DECLARE @start_time DATETIME,@end_time DATETIME
   BEGIN TRY
	 PRINT 'Loading bronze layer';
	 PRINT '========================';
	--inserting data in bulk(at a time all the records)
	--so we use BULK keyword
	PRINT '----------------------------';
	PRINT 'Loading tables';
	SET @start_time=GETDATE()
	TRUNCATE TABLE [bronze].[crm_cust_info]--to add fresh data every time 
	BULK INSERT	[bronze].[crm_cust_info]
	FROM 'C:\data\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
   
	TRUNCATE TABLE [bronze].[crm_prd_info]
	 BULk INSERT [bronze].[crm_prd_info]
	 FROM 'C:\data\datasets\source_crm\prd_info.csv'
	 WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	)
 

	TRUNCATE TABLE [bronze].[crm_sales_details] 
	BULk INSERT  [bronze].[crm_sales_details] 
	 FROM 'C:\data\datasets\source_crm\sales_details.csv'
	 WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	)

	TRUNCATE TABLE [bronze].[erp_cust_az12]
	BULK INSERT  [bronze].[erp_cust_az12]
	 FROM 'C:\data\datasets\source_erp\CUST_AZ12.csv'
	 WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	)

	TRUNCATE TABLE [bronze].[erp_loc_a101]
	BULK INSERT  [bronze].[erp_loc_a101]
	 FROM 'C:\data\datasets\source_erp\LOC_A101.csv'
	 WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	) 

	TRUNCATE TABLE [bronze].[erp_cat_giv2]
	BULK INSERT [bronze].[erp_cat_giv2]   
	 FROM 'C:\data\datasets\source_erp\PX_CAT_G1V2.csv'
	 WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	)
	
    END TRY
	BEGIN CATCH
	   PRINT 'ERROR OCCURED DURING BRONZE LAYER LOADING'+ERROR_MESSAGE();
	END CATCH
	 SET @end_time=GETDATE()
	PRINT 'time taken to load table '+CAST((DATEDIFF(second,@start_time,@end_time)) AS NVARCHAR)+'seconds'
END
