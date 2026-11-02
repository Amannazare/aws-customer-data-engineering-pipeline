

-- GOLD LAYER: REGIONAL BUSINESS ANALYSIS

-- Create the Gold regional analytics table

CREATE TABLE customer_data_gold
WITH (
    format = 'PARQUET',
    external_location = 's3://aman-customer-data-lake-2026/gold/customer-data/',
    write_compression = 'SNAPPY'
) AS
SELECT
    region,
    COUNT(*) AS total_customers,
    ROUND(AVG(age), 2) AS average_age,
    ROUND(AVG(income), 2) AS average_income,
    ROUND(AVG(purchase_amount), 2) AS average_purchase_amount,
    SUM(purchase_amount) AS total_purchase_amount
FROM customer_data_silver
GROUP BY region;


-- View regional business analysis results

SELECT *
FROM customer_data_gold;
