

-- GOLD LAYER: LOYALTY ANALYSIS


-- Create loyalty analytics table

CREATE TABLE gold_loyalty_analysis
WITH (
    format = 'PARQUET',
    external_location = 's3://aman-customer-data-lake-2026/gold/gold_loyalty_analysis/',
    write_compression = 'SNAPPY'
)
AS
SELECT
    loyalty_status,
    COUNT(*) AS total_customers,
    ROUND(AVG(age), 2) AS average_age,
    ROUND(AVG(income), 2) AS average_income,
    ROUND(AVG(purchase_amount), 2) AS average_purchase_amount,
    SUM(purchase_amount) AS total_purchase_amount
FROM customer_data_silver
GROUP BY loyalty_status;


-- View loyalty analysis results

SELECT *
FROM gold_loyalty_analysis;
