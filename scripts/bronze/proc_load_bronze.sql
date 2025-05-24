CREATE PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
	SET @batch_start_time = getdate()
		-- Truncate and bulk insert for crm_cust_info
		print'=============================='
		print'loading the bronze layer'
		print'============================== success'

		print'=============================='
		print'loading the crm tables'
		print'============================== success'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Admin\Desktop\Data Academy\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> cust_info table Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>----------------'
		-- Truncate and bulk insert for crm_prd_info

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Admin\Desktop\Data Academy\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>prd_info table Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>----------------'
		-- Truncate and bulk insert for crm_sales_details
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Admin\Desktop\Data Academy\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>sales_details table Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>----------------'
		print'=============================='
		print'loading the erp tables'
		print'============================== success'
		-- Truncate and bulk insert for erp_loc_a101

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Admin\Desktop\Data Academy\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>loc_a101 table Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>----------------'
		-- Truncate and bulk insert for erp_cust_az12
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Admin\Desktop\Data Academy\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>cust_az12 table Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>----------------'
		-- Truncate and bulk insert for erp_px_cat_g1v2

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Admin\Desktop\Data Academy\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>>px_cat_g1v2 table Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>----------------'
		SET @batch_end_time = getdate()
		print'Bronze Layer Load time: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
	END TRY
	BEGIN CATCH
		print'===================================================='
		print'An error occured during loading bronze Layer'
		print'Error message' + ERROR_MESSAGE();
		print'Error message' + CAST (ERROR_NUMBER() as nvarchar);
		print'Error message' + CAST (ERROR_STATE() as nvarchar);
	print'===================================================='
	END CATCH
END; -- It's good practice to end the BEGIN...END block with a semicolon.
GO  -- This 'GO' is correct and separates this procedure definition from any subsequent statements.



