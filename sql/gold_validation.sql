

-- GOLD LAYER: DATA VALIDATION


-- Check total customer count across product categories

SELECT
    SUM(total_customers) AS total_category_customers
FROM gold_product_category_analysis;


-- Check total customer count across loyalty segments

SELECT
    SUM(total_customers) AS total_loyalty_customers
FROM gold_loyalty_analysis;


-- Check regional customer count

SELECT
    SUM(total_customers) AS total_regional_customers
FROM customer_data_gold;
