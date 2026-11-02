

-- GOLD LAYER: PRODUCT CATEGORY ANALYSIS

-- Create product category analytics table

CREATE TABLE gold_product_category_analysis
WITH (
    format = 'PARQUET',
    external_location = 's3://aman-customer-data-lake-2026/gold/gold_product_category_analysis/',
    write_compression = 'SNAPPY'
)
AS
SELECT
    product_category,
    COUNT(*) AS total_customers,
    ROUND(AVG(purchase_amount), 2) AS average_purchase_amount,
    SUM(purchase_amount) AS total_purchase_amount,
    COUNT_IF(promotion_usage = 1) AS promotion_users
FROM customer_data_silver
GROUP BY product_category;


-- View product category analysis results

SELECT *
FROM gold_product_category_analysis;
