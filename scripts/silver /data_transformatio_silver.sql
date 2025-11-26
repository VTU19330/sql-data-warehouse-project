/*
===============================================================================
Data Quality & Validation Script: CLEAN and LOAD Checks
===============================================================================
Purpose:
    This script performs comprehensive data quality validations across 
    multiple Bronze and Silver layer tables before promoting data from 
    Bronze -> Silver -> Gold layers.

    It validates:
      ✔ Primary Key Integrity (Uniqueness & Not Null)
      ✔ Data Cleanliness (Trimming, Invalid Formats, Spaces)
      ✔ Data Accuracy (Business Rule Validation)
      ✔ Data Standardization & Consistency (Low Cardinality Checks)
      ✔ Null / Negative / Out-of-Range Value Detection
      ✔ Temporal Logic Validations (Start-End Dates, Shipping, Due Dates)
      ✔ Data Integrity Across Layers (Bronze vs Silver Comparison)

Usage Instructions:
    1. Run Bronze Layer validation checks after initial ingestion.
    2. Apply cleansing/transformation logic to load data into Silver.
    3. Re-run same validations on Silver Layer to confirm data quality improvement.
    4. If all checks pass → Promote to Gold Layer.

Expected Outcome:
    - All validation queries should return ZERO records.
    - Any returned rows indicate data quality issues to be addressed.

Author: <Sravanth Kumar>
Environment: SQL / Data Warehouse (Bronze-Silver-Gold Architecture)
Date: <Insert Date>

===============================================================================
*/

----------------------------------------CLEAN AND LOAD FOR bronze.crm_cust_info--------------------------------
--CHEK FOR NULLs OR DUPLICATES IN PRIMARY KEY 
--EXPECTATION: NO RESULT
--QUALITY CHECK:A PRIMARY KEY MUST BE UNIQUE AND NOT NULL
select cst_id,count(*) as duplicate_number_of_IDs 
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;
--CHECK FOR UNWANTED SPACES IN STRING VALUES
--EXPECTATION:NO RESULT
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)--IF THE ORIGINAL VALUE IS NOT EQUAL TO SAME VALUE AFTER TRIMMING,IT MEANS THERE ARE SPACES!

select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)--IF THE ORIGINAL VALUE IS NOT EQUAL TO SAME VALUE AFTER TRIMMING,IT MEANS THERE ARE SPACES!
--DATA STANDARIZATION & CONSISTENCY
--CHECK THE CONSISTENCY OF VALUES IN LOW CARDINALITY COLUMNS
--IN OUR DATA WAREHOUSE,WE AIM TO STORE CLEAR AND MEANINGFUL VALUES RATHER THAN USING ABBREVIATED TERMS
select distinct(cst_gndr) from bronze.crm_cust_info

select distinct(cst_marital_status) from bronze.crm_cust_info

----------------------------QUALITY CHECK FOR SILVER LAYER silver.crm_cust_info-------------------------------
--RE-RUN THE QUERIES CHECK QUERIES FROM THS BRONZE LAYER TO VERIFY THE QUALITY OF DATA IN THE SILVER LAYER

--CHEK FOR NULLs OR DUPLICATES IN PRIMARY KEY 
--EXPECTATION: NO RESULT
--QUALITY CHECK:A PRIMARY KEY MUST BE UNIQUE AND NOT NULL
select cst_id,count(*) as duplicate_number_of_IDs 
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;
--CHECK FOR UNWANTED SPACES IN STRING VALUES
--EXPECTATION:NO RESULT
select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)--IF THE ORIGINAL VALUE IS NOT EQUAL TO SAME VALUE AFTER TRIMMING,IT MEANS THERE ARE SPACES!

select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)--IF THE ORIGINAL VALUE IS NOT EQUAL TO SAME VALUE AFTER TRIMMING,IT MEANS THERE ARE SPACES!
--DATA STANDARIZATION & CONSISTENCY
--CHECK THE CONSISTENCY OF VALUES IN LOW CARDINALITY COLUMNS
--IN OUR DATA WAREHOUSE,WE AIM TO STORE CLEAR AND MEANINGFUL VALUES RATHER THAN USING ABBREVIATED TERMS
select distinct(cst_gndr) from silver.crm_cust_info
select distinct(cst_marital_status) from silver.crm_cust_info





-------------------------------------------------CLEAN AND LOAD FOR bronze.crm_prd_info-------------------------------
--CHEK FOR NULLs OR DUPLICATES IN PRIMARY KEY 
--EXPECTATION: NO RESULT
--QUALITY CHECK:A PRIMARY KEY MUST BE UNIQUE AND NOT NULL
select prd_id,count(*) as duplicate_number_of_IDs 
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;
---CHECK FOR ANY DERIVED COLUMNS POSSIBILITY FOR JOINING WITH OTHER TABLES
---EXPECTATIONS:NOT AVAILABLE
select prd_key from bronze.crm_prd_info
--CHECK FOR UNWANTED SPACES
--EXPECTATION: NO RESULT
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm);
--CHECK FOR NULLs OR NEGATIVE NUMBERS
--EXPECTATION: NO RESULT
select prd_cost 
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;
--DATA STANDARIZATION AND CONSISTENCY
select distinct prd_line from bronze.crm_prd_info
--CHECK FOR INVALID DATA ORDERS (end date must not be earlier than the start date)
select * from bronze.crm_prd_info where prd_start_dt > prd_end_dt

-------------------------------QUALITY CHECK FOR SILVER LAYER silver.crm_prd_info-------------------
--RE-RUN THE QUERIES CHECK QUERIES FROM THS BRONZE LAYER TO VERIFY THE QUALITY OF DATA IN THE SILVER LAYER
select prd_id,count(*) as duplicate_number_of_IDs 
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;
---CHECK FOR ANY REQUIRED COLUMN HAS TO BE DIVIDED FOR JOINING WITH OTHER TABLES
---EXPECTATIONS:NOT AVAILABLE
select prd_key from silver.crm_prd_info
--CHECK FOR UNWANTED SPACES
--EXPECTATION: NO RESULT
select prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm);
--CHECK FOR NULLs OR NEGATIVE NUMBERS
--EXPECTATION: NO RESULT
select prd_cost 
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;
--DATA STANDARIZATION AND CONSISTENCY
select distinct prd_line from silver.crm_prd_info
--CHECK FOR INVALID DATA ORDERS (end date must not be earlier than the start date)
select * from silver.crm_prd_info where prd_start_dt > prd_end_dt






-----------------------CLEAN AND LOAD FOR bronze.crm_sales_details--------------------
---CHECK FOR INVALID DATES
select 
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 or len(sls_order_dt) != 8 
select 
nullif(sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 or len(sls_ship_dt) != 8 
select 
nullif(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 or len(sls_due_dt) != 8 
---CHECK FOR DATES VALIDALITY 
---ORDER DATE > SHIP DATE OR DUE DATE
select * from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt
---CHECK FOR DATA CONSISTENCY:BETWEEN SALES,QUANTITY AND PRICE
-->>>SALES = QUANTITY * PRICE
-->>>VALUES MUST NOT BE NULL,ZERO OR NEGATIVE
select 
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity* abs(sls_price) then sls_quantity* abs(sls_price)
     else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price<0 then sls_sales/nullif(sls_quantity,0)
     else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*sls_price or
sls_quantity is null or sls_price is null or sls_price <=0 or sls_quantity <=0

-----------------QUALITY CHECK FOR SILVER LAYER silver.crm_sales_details------------------------
--RE-RUN THE QUERIES CHECK QUERIES FROM THS BRONZE LAYER TO VERIFY THE QUALITY OF DATA IN THE SILVER LAYER
---CHECK FOR DATES VALIDALITY 
---ORDER DATE > SHIP DATE OR DUE DATE
select * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt
---CHECK FOR DATA CONSISTENCY:BETWEEN SALES,QUANTITY AND PRICE
-->>>SALES = QUANTITY * PRICE
-->>>VALUES MUST NOT BE NULL,ZERO OR NEGATIVE
select 
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity* abs(sls_price) then sls_quantity* abs(sls_price)
     else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price<0 then sls_sales/nullif(sls_quantity,0)
     else sls_price
end as sls_price
from silver.crm_sales_details
where sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*sls_price or
sls_quantity is null or sls_price is null or sls_price <=0 or sls_quantity <=0



-----------------------CLEAN AND LOAD FOR bronze.erp_cust_az12--------------------
---CHECK FOR POSSIBILITY FOR DERIVED COLUMNS
select cid from bronze.erp_cust_az12
---CHECK FOR BDATE FOR INDEX OUT OF RANGE
select bdate from bronze.erp_cust_az12 where bdate>getdate();
--DATA STANDARDIZATION AND CONSISTENCY
select distinct gen from bronze.erp_cust_az12
-----------------QUALITY CHECK FOR SILVER LAYER silver.erp_cust_az12 ------------------------
--RE-RUN THE QUERIES CHECK QUERIES FROM THS BRONZE LAYER TO VERIFY THE QUALITY OF DATA IN THE SILVER LAYER
---CHECK FOR BDATE FOR INDEX OUT OF RANGE
select bdate from silver.erp_cust_az12 where bdate>getdate();
--DATA STANDARDIZATION AND CONSISTENCY
select distinct gen from silver.erp_cust_az12




-----------------------CLEAN AND LOAD FOR bronze.erp_loc_a101--------------------
--CHECK FOR POSSIBILITY OF DERIVERD COLUMNS
select cid from bronze.erp_loc_a101;
--DATA STANDARDIZATION AND CONSISTENCY
select distinct cntr from bronze.erp_loc_a101 order by cntr;
-----------------QUALITY CHECK FOR SILVER LAYER silver.erp_loc_a101------------------------
--RE-RUN THE QUERIES CHECK QUERIES FROM THS BRONZE LAYER TO VERIFY THE QUALITY OF DATA IN THE SILVER LAYER
--DATA STANDARDIZATION AND CONSISTENCY
select distinct cntry from silver.erp_loc_a101 order by cntry;




---------------------------------------CLEAN AND LOAD FOR bronze.erp_px_cat_g1v2---------------------------------
---CHECK FOR UNWANTED SPACES
select * from bronze.erp_px_cat_g1v2 
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)
---DATA STANDARDIZATION AND CONSISTENCY
select distinct cat from bronze.erp_px_cat_g1v2
select distinct subcat from bronze.erp_px_cat_g1v2
select distinct maintenance from bronze.erp_px_cat_g1v2
------------------------QUALITY CHECK FOR SILVER LAYER silver.erp_px_cat_g1v2------------------------------------
--RE-RUN THE QUERIES CHECK QUERIES FROM THS BRONZE LAYER TO VERIFY THE QUALITY OF DATA IN THE SILVER LAYER
---CHECK FOR UNWANTED SPACES
select * from silver.erp_px_cat_g1v2 
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)
---DATA STANDARDIZATION AND CONSISTENCY
select distinct cat from silver.erp_px_cat_g1v2
select distinct subcat from silver.erp_px_cat_g1v2
select distinct maintenance from silver.erp_px_cat_g1v2






