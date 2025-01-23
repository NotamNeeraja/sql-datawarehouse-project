
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
---------------EXEC [bronze].[load_bronze];
