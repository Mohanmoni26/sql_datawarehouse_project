--- stored procedure ---- bronze schema---> silver schema


--- this script is used for transferring the data from source into schemas through stored proedure
-----Performs the folllowing action
----- truncated the silver table before loading data
---- inserts transformed and cleansed data into the silver from bronze table 


--- parameters it doesnot accept any parameters or return any values
----Usage example : EXEC silver.load.silver


create or alter procedure silver.load_silver as 
begin
BEGIN try
		declare @start_time datetime , @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
		print '--------------------------'
		print 'loading silver layer'
		print '--------------------------'
		print '--------------------------------------'
		print '---- loading data from source CRM---'
		print '--------------------------------------'

		set @batch_start_time=getdate()
		set @start_time=getdate()
		print '>>> Truncating table ---> silver.crm_cust_info to make it empty>>>>'
    
    truncate table silver.crm_cust_info
    print '>>> inserting table ---> silver.crm_cust_info>>>>'
    insert into silver.crm_cust_info(
    cst_id,cst_key,
    cst_firstname, cst_lastname,
    cst_marital_status, cst_gndr,
    cst_create_date
    )

    select	
    cst_id, cst_key,
    trim (cst_firstname) as cst_firstname,
    trim (cst_lastname) as cst_lastname,
    case when trim (upper (cst_marital_status)) = 'M' then 'Married'
	    when trim (upper (cst_marital_status)) = 'S' then 'Single'
	    else 'N/A'
    end cst_marital_status, --- normalize marital status to readable format
    case when trim (upper (cst_gndr)) = 'F' then 'Female'
	    when trim (upper (cst_gndr)) = 'M' then 'Male'
	    else 'N/A'
    end cst_gndr, --- normalize gender status to readable format
    cst_create_date
    from (
    select *, 
    row_number () over (partition by cst_id order by cst_create_date desc) as flag
    from bronze.crm_cust_info)t
    where flag = 1 -- select the most recent transacted customer
    set @end_time=getdate()
    print 'loading time for silver.crm_cust_info :' + cast(datediff(second, @start_time, @end_time) as nvarchar)+ 'seconds';
   

   set @start_time=getdate()
    print '>>> Truncating table silver.crm_prd_info to make it empty>>>>'
    truncate table silver.crm_prd_info
    print '>>> Inserting table silver.crm_prd_info>>>>'
    insert into silver.crm_prd_info(
        prd_id,cat_id,prd_key,
        prd_nm, prd_cost,
        prd_line, prd_start_dt, prd_end_dt
        )


    select 
        prd_id,
        replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
        substring(prd_key, 7, len(prd_key)) as prd_key, -- renamed to avoid duplicate alias
        prd_nm,
        coalesce(prd_cost, 0) as prd_cost,
        case upper(trim(prd_line))
            when 'M' then 'Mountain'
            when 'S' then 'Other sales'
            when 'R' then 'Road'
            when 'T' then 'Touring'
            else 'N/A'
        end as prd_line,
        cast(prd_start_dt as date) as prd_start_dt,
        dateadd(day, -1, cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) as date)) as prd_end_dt
    from bronze.crm_prd_info
    set @end_time=getdate()
    print 'loading time for silver.crm_prd_info :' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';


    
    set @start_time=getdate()
	print '>>> Truncating table silver.crm_sales_details to make it empty>>>>'
    truncate table silver.crm_sales_details
    print' Inserting silver.crm_sales_details'
    insert into silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,sls_ship_dt, sls_due_dt,
    sls_sales, sls_quantity, sls_price
    )
    select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or len (sls_order_dt) != 8 then Null
    else cast (cast (sls_order_dt as varchar) as date)
    end as sls_order_dt,
    case when sls_ship_dt = 0 or len (sls_ship_dt) != 8 then Null
    else cast (cast (sls_ship_dt as varchar) as date)
    end as sls_ship_dt,
    case when sls_due_dt = 0 or len (sls_due_dt) != 8 then Null
    else cast (cast (sls_due_dt as varchar) as date)
    end as sls_due_dt,
    case when sls_sales is null or sls_sales>=0 or sls_sales!= sls_quantity * abs(sls_price)
         then sls_quantity * abs(sls_price)
         else sls_sales 
         end sls_sales,
         sls_quantity,
    case when sls_price is null or sls_price >=0 
         then sls_price/ nullif (sls_quantity,0)
         else sls_price 
         end sls_price
    from Bronze.crm_sales_details
    set @end_time=getdate()
    print 'loading time for silver.crm_sales_details  :' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';


   
    print '--------------------------------------'
		print '---Loading data from Source ERP--------'
		print '---------------------------------------'

		set @start_time=getdate()
		print '>>> Truncating table silver.erp_cust_az12 to make it empty>>>>'
    truncate table silver.erp_cust_az12
    print' inserting silver.erp_cust_az12'
    insert into silver.erp_cust_az12(
    CID, BDATE, GEN
    )

    select 
    case when CID LIKE 'NAS%' then SUBSTRING (CID,4,len(CID))
          else CID
           end CID, --- remove nas as prefix
    case when BDATE> getdate() then null
        else BDATE
        end BDATE, --- set future birthdates to null
    case when upper (trim (GEN)) in ('F', 'Female') then 'Female'
          when upper(trim (GEN)) in ('M', 'Male') then 'Male'
         else 'N/A'
         end GEN --- cleansed the gender 
    from bronze.erp_cust_az12
    set @end_time=getdate()
    print 'loading time for silver.erp_cust_az12  :' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';
 

     
     set @start_time=getdate()
		print '>>> Truncating table silver.erp_loc_a101 to make it empty>>>>'
     truncate table silver.erp_loc_a101
     print' inserting silver.erp_loc_a101 '
     insert into silver.erp_loc_a101
     (CID, CNTRY)

     select 
     replace (CID,'-', '') as CID,
     case when trim (CNTRY) = 'DE' then 'Germany'
        when trim (CNTRY) in  ('US', 'USA') then 'United States'
        when trim (CNTRY) in  ('UK') then  'United Kingdom'
        when trim (CNTRY) = '' OR CNTRY is null then  'n/a'
        else CNTRY
        end CNTRY
     from Bronze.erp_loc_a101
     set @end_time=GETDATE()
     print 'loading time for erp_loc_a101:' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';
 
     
     set @start_time=GETDATE()
		print '>>> Truncating table erp_px_cat_g1v2 to make it empty>>>>'
     truncate table silver.erp_px_cat_g1v2
     print ' inserting silver.erp_px_cat_g1v2'
    insert into silver.erp_px_cat_g1v2(
        ID, CAT, SUBCAT, MAINTENANCE
    )
     select ID,
     CAT,
     SUBCAT,
     MAINTENANCE
     from bronze.erp_px_cat_g1v2
     set @end_time=getdate()
		 print 'loading time for silver.erp_px_cat_g1v2  :' + cast(datediff(second, @start_time, @end_time)as nvarchar)+ 'seconds';
		set @batch_end_time=getdate()

        print '--- silver layer end ------'
		print 'loading time for silver layer :' + cast(datediff(second, @batch_start_time, @batch_end_time)as nvarchar)+ 'seconds';
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
