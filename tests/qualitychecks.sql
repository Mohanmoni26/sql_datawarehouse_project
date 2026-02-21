--- check for nulls/duplicates in the primary key

select CID, count(*)
from Bronze.erp_cust_az12
group by CID
having count (*) > 1


--- data cleansing
select * from (
select *, 
row_number () over (partition by cst_id order by cst_create_date desc) as flag
from silver.crm_cust_info)t
where flag = 1

--- check for unwanted spaces

select sls_ord_num
from Bronze.crm_sales_details
where sls_ord_num != trim (sls_ord_num)

--- check for nulls/negatives
select  prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null


--- data standardization and consistency

select distinct CNTRY,
case when trim (CNTRY) = 'DE' then 'Germany'
    when trim (CNTRY) in  ('US', 'USA') then 'United States'
    when trim (CNTRY) in  ('UK') then  'United Kingdom'
    when trim (CNTRY) = '' OR CNTRY is null then  'n/a'
    else CNTRY
    end CNTRY
from Bronze.erp_loc_a101

--- select for valid date 
select BDATE from Bronze.erp_cust_az12
where BDATE < '1900-01-12' or BDATE > getdate()


--- check for invalid end date 
select * from bronze.crm_prd_info
where  prd_end_dt<prd_start_dt

---check for invalid dates
select nullif (sls_due_dt,0) as sls_due_dt
from silver.crm_sales_details
where sls_due_dt <= 0
or len(sls_due_dt) != 8
or sls_due_dt > 20500101
or sls_due_dt > 19000101


-- check for invalid order dates
select * from silver.crm_sales_details
where sls_order_dt> sls_ship_dt or  sls_order_dt> sls_due_dt
select * from silver.crm_cust_info

 
