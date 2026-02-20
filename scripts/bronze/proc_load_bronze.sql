--- stored procedure ---- source---> schema


--- this script is used for transferring the data from source into schemas through stored proedure
-----Performs the folllowing action
----- truncated the bronze table before loading data
---- uses the bulk insert command to load the CSV files


--- parameters it doesnot accept any parameters or return any values
----Usage example : EXEC bronze.load_bronze

create or alter procedure bronze.load_bronze as 
	begin
		BEGIN try
		declare @start_time datetime , @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
		print '--------------------------'
		print 'loading bronze layer'
		print '--------------------------'
		print '--------------------------------------'
		print '---- loading data from source CRM---'
		print '--------------------------------------'

		set @batch_start_time=getdate()
		set @start_time=getdate()
		print '>>> Truncating table ---> Bronze.crm_cust_info to make it empty>>>>'
		truncate table Bronze.crm_cust_info
		print '>>> Inserting table ---> Bronze.crm_cust_info>>>>'
		bulk insert Bronze.crm_cust_info
		from 'C:\Users\hp\Downloads\warehouse proj\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		--- since the first row contains the tablecolumn
		FIRSTROW = 2,
		FIELDTERMINATOR = ',', --- seperator
		TABLOCK
		);
		set @end_time=getdate()
		print 'loading time for Bronze.crm_cust_info :' + cast(datediff(second, @start_time, @end_time) as nvarchar)+ 'seconds';

		set @start_time=getdate()
		print '>>> Truncating table Bronze.crm_prd_info to make it empty>>>>'
		truncate table Bronze.crm_prd_info
		print '>>> Inserting table Bronze.crm_prd_info>>>>'
		bulk insert Bronze.crm_prd_info
		from 'C:\Users\hp\Downloads\warehouse proj\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		--- since the first row contains the tablecolumn
		FIRSTROW = 2,
		FIELDTERMINATOR = ',', --- seperator
		TABLOCK
		); set @end_time=getdate()
		print 'loading time for Bronze.crm_prd_info :' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';

		set @start_time=getdate()
		print '>>> Truncating table Bronze.crm_sales_details to make it empty>>>>'
		truncate table Bronze.crm_sales_details
		print '>>> Inserting table Bronze.crm_sales_details >>>>'
		bulk insert Bronze.crm_sales_details
		from 'C:\Users\hp\Downloads\warehouse proj\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		--- since the first row contains the tablecolumn
		FIRSTROW = 2,
		FIELDTERMINATOR = ',', --- seperator
		TABLOCK
		);set @end_time=getdate()
		print 'loading time for Bronze.crm_sales_details  :' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';



		print '--------------------------------------'
		print '---Loading data from Source ERP--------'
		print '---------------------------------------'

		set @start_time=getdate()
		print '>>> Truncating table Bronze.erp_cust_az12 to make it empty>>>>'
		truncate table Bronze.erp_cust_az12
		print '>>> Inserting table Bronze.erp_cust_az12 >>>>'
		bulk insert Bronze.erp_cust_az12
		from 'C:\Users\hp\Downloads\warehouse proj\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		--- since the first row contains the tablecolumn
		FIRSTROW = 2,
		FIELDTERMINATOR = ',', --- seperator
		TABLOCK
		);set @end_time=getdate()
		print 'loading time for Bronze.erp_cust_az12  :' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';

		set @start_time=getdate()
		print '>>> Truncating table erp_loc_a101 to make it empty>>>>'
		truncate table Bronze.erp_loc_a101
		print '>>> Inserting table erp_loc_a101 >>>>'
		bulk insert Bronze.erp_loc_a101
		from 'C:\Users\hp\Downloads\warehouse proj\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		--- since the first row contains the tablecolumn
		FIRSTROW = 2,
		FIELDTERMINATOR = ',', --- seperator
		TABLOCK
		);set @end_time=getdate()
		 print 'loading time for erp_loc_a101:' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';

		set @start_time=GETDATE()
		print '>>> Truncating table erp_px_cat_g1v2 to make it empty>>>>'
		truncate table Bronze.erp_px_cat_g1v2
		print '>>> Inserting tableerp_px_cat_g1v2 >>>>'
		bulk insert Bronze.erp_px_cat_g1v2
		from 'C:\Users\hp\Downloads\warehouse proj\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		--- since the first row contains the tablecolumn
		FIRSTROW = 2,
		FIELDTERMINATOR = ',', --- seperator
		TABLOCK
		);set @end_time=getdate()
		 print 'loading time for erp_px_cat_g1v2  :' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';
		set @batch_end_time=getdate()

		print '--- Bronze layer end ------'
		print 'loading time for bronze layer :' + cast(datediff(second, @batch_start_time, @batch_end_time)as nvarchar)+ 'seconds';

		end try
	begin catch
		print '-------------------------------'
		print 'Error occured during load bronze layer'
		print 'Error message' + Error_message();
		print 'Error number' + Error_number();
		print 'Error state' + Error_state();
		print '-------------------------------'
	end catch
end







  
