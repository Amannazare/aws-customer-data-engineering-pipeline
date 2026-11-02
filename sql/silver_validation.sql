

-- SILVER LAYER: RECORD COUNT VALIDATION
SELECT COUNT(*) AS total_silver_records
FROM customer_data_silver;


-- SILVER LAYER: DATA QUALITY VALIDATION
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT id) AS unique_customers,
    COUNT_IF(id IS NULL) AS missing_ids,
    COUNT_IF(age < 0) AS negative_age,
    COUNT_IF(income < 0) AS negative_income,
    COUNT_IF(purchase_amount < 0) AS negative_purchase_amount
FROM customer_data_silver;
