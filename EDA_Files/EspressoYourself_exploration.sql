-- Create main table for all collected transactions
CREATE TABLE transactions (
	transaction_id SERIAL PRIMARY KEY, -- Transaction index +1 increments
	transaction_date DATE NOT NULL, -- Date in format dd/mm/yyyy
	transaction_time TIME NOT NULL, -- Time in format hh:mm:ss
	transaction_qty SMALLINT NOT NULL, -- Quantity of ordered product
	store_id SMALLINT NOT NULL, -- Store id
	store_location VARCHAR(50) NOT NULL, -- Store location
	product_id SMALLINT NOT NULL, -- Purchased product id
	unit_price NUMERIC DEFAULT 0.0 NOT NULL, -- Unit price, default is 0
    product_category VARCHAR(50) NOT NULL, -- Product category
    product_type VARCHAR(50) NOT NULL, -- Product type
    product_detail VARCHAR(50) NOT NULL -- Product detail
	);

-- Import data from an '.csv' file, inside PostgreSQL folder so Server has access
COPY transactions
FROM 'C:/Program Files/PostgreSQL/CoffeeShopSales.csv'
WITH (FORMAT csv, HEADER);

-- Verify all data was succesfully imported
SELECT *
FROM transactions
LIMIT 10;

/*  ========================================
	=== START STAR SCHEMA IMPLEMENTATION ===
    ========================================
*/

-- CREATE DIMENSION TABLES
-- Create the store dimension table
CREATE TABLE stores (
	store_id SMALLINT PRIMARY KEY, -- Store id
	store_location VARCHAR(50) -- Store location
	);

-- Create the product_category dimension table
CREATE TABLE product_categories (
	product_category_id SERIAL PRIMARY KEY, -- Index increments +1 on each creation
	product_category VARCHAR(50) NOT NULL -- Product category name
	);

-- Create the product_type dimension table
CREATE TABLE product_types (
	product_type_id SERIAL PRIMARY KEY, -- Index imcrements +1 on each creation
	product_type VARCHAR(50) NOT NULL, -- Product type name
	product_category_id SMALLINT NOT NULL REFERENCES product_categories(product_category_id) -- Foreign key to product_categories
	);

-- Create the product dimension table
CREATE TABLE products (
	product_id SMALLINT PRIMARY KEY, -- Product id
	product_detail VARCHAR(50) NOT NULL, -- Product detail
	unit_price NUMERIC DEFAULT 0.0 NOT NULL, -- Unit price, default is 0.0
	product_type_id SMALLINT NOT NULL REFERENCES product_types(product_type_id) -- Foreign key to product_types
	);

-- CREATE FACT TABLE
-- Create transaction table
CREATE TABLE transaction (
	transaction_id SERIAL PRIMARY KEY, -- Unique transaction ID
	transaction_date DATE NOT NULL, -- Transaction date
	transaction_time TIME NOT NULL, -- Transaction time
	transaction_qty SMALLINT NOT NULL, -- Unit/s of sold product
	product_id SMALLINT NOT NULL REFERENCES products(product_id), -- Foreign key to products
	store_id SMALLINT NOT NULL REFERENCES stores(store_id) -- Foreign key to stores
	);

-- POPULATE DIMENSION TABLES
-- Populate stores table
INSERT INTO stores (
	store_id,
	store_location
	)
SELECT DISTINCT 
	store_id,
	store_location
FROM transactions;

-- Populate product_categories table
INSERT INTO product_categories (
	product_category
	)
SELECT DISTINCT product_category
FROM transactions;

-- Populate product_types table
INSERT INTO product_types (
	product_type,
	product_category_id
	)
SELECT DISTINCT 
	t.product_type,
	pc.product_category_id
FROM transactions AS t
JOIN product_categories AS pc
ON t.product_category = pc.product_category;

-- Populate products table
INSERT INTO products (
	product_id,
	product_detail,
	unit_price,
	product_type_id
	)
SELECT DISTINCT ON (t.product_id)
	t.product_id,
	t.product_detail,
	t.unit_price,
	pt.product_type_id
FROM transactions AS t
JOIN product_types AS pt
ON t.product_type = pt.product_type;

-- POPULATE FACT TABLE
INSERT INTO transaction (
	transaction_id,
	transaction_date,
	transaction_time,
	transaction_qty,
	store_id,
	product_id
	)
SELECT
	transaction_id,
	transaction_date,
	transaction_time,
	transaction_qty,
	t.store_id,
	t.product_id
FROM transactions AS t;

-- Verify data from dimensions table
SELECT * FROM stores;
SELECT * FROM product_categories;
SELECT * FROM product_types;
SELECT * FROM products;

-- Verify the fact table
SELECT *
FROM transaction;

/*  =======================================
	=== END STAR SCHEMA IMPLEMENTATION	 ===
	===									 ===
	=== START EXPLORATORY DATA ANALYSIS === 
    =======================================
*/

-- Count and understand content quantities
SELECT
	COUNT(*) AS total_transactions,
	COUNT(DISTINCT store_id) AS total_stores,
	COUNT(DISTINCT p.product_id) AS total_products,
	AVG(transaction_qty) AS avg_transaction_qty,
	AVG(p.unit_price) AS avg_product_price
FROM transaction AS t
JOIN products AS p
	ON t.product_id = p.product_id;

-- Stadistics for columns "transaction_qty" and "unit_price"
SELECT
    MIN(transaction_qty) AS min_qty,
    MAX(transaction_qty) AS max_qty,
    AVG(transaction_qty) AS avg_qty,
    STDDEV(transaction_qty) AS stddev_qty,
    MIN(unit_price) AS min_price,
    MAX(unit_price) AS max_price,
    AVG(unit_price) AS avg_price,
    STDDEV(unit_price) AS stddev_price
FROM transaction AS t
JOIN products AS p
	ON t.product_id = p.product_id;

-- Calculate revenue by product category
SELECT
    pc.product_category,
    COUNT(*) AS total_sales,
    SUM(t.transaction_qty) AS total_qty_sold,
    ROUND(SUM(p.unit_price * t.transaction_qty),0) AS total_revenue,
	ROUND(SUM(p.unit_price * t.transaction_qty) * 100.0 / total_revenue.total, 2) AS percentage_contribution
FROM transaction AS t
JOIN products AS p
	ON t.product_id = p.product_id
JOIN product_types AS pt
	ON p.product_type_id = pt.product_type_id
JOIN product_categories AS pc
	ON pt.product_category_id = pc.product_category_id
CROSS JOIN  
    (SELECT 
		SUM(p.unit_price * t.transaction_qty) AS total
     FROM transaction AS t
     JOIN products AS p
	 	ON t.product_id = p.product_id
	) AS total_revenue
GROUP BY
	pc.product_category,
	total_revenue.total
ORDER BY total_revenue DESC;

-- Calculate revenue by store location
SELECT
    s.store_location,
    COUNT(*) AS total_sales,
    SUM(t.transaction_qty) AS total_qty_sold,
    ROUND(SUM(t.transaction_qty * p.unit_price),0) AS total_revenue,
	ROUND(SUM(p.unit_price * t.transaction_qty) * 100.0 / total_revenue.total, 2) AS percentage_contribution
FROM transaction AS t
JOIN stores AS s
	ON t.store_id = s.store_id
JOIN products AS p
	ON t.product_id = p.product_id
CROSS JOIN  
    (SELECT 
		SUM(p.unit_price * t.transaction_qty) AS total
     FROM transaction AS t
     JOIN products AS p
	 	ON t.product_id = p.product_id
	) AS total_revenue
GROUP BY
	s.store_location,
	total_revenue.total
ORDER BY percentage_contribution DESC;

--Quantity of products sold and revenue by date
SELECT
    DATE(t.transaction_date) AS date,
    SUM(t.transaction_qty) AS total_qty_sold,
    SUM(t.transaction_qty * p.unit_price) AS total_revenue
FROM transaction AS t
JOIN products AS p
	ON t.product_id = p.product_id
GROUP BY DATE(t.transaction_date)
ORDER BY date;

-- Revenue by hour and day of week
SELECT
    EXTRACT(DOW FROM transaction_date) AS day_of_week,
    EXTRACT(HOUR FROM transaction_time) AS hour_of_day,
    SUM(transaction_qty) AS total_qty_sold,
    SUM(transaction_qty * unit_price) AS total_revenue
FROM transaction AS t
JOIN products AS p
	ON t.product_id = p.product_id
GROUP BY
    EXTRACT(DOW FROM transaction_date),
    EXTRACT(HOUR FROM transaction_time)
ORDER BY
    day_of_week,
	hour_of_day;

-- Monthly revenue by product category
SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    product_category,
    ROUND(AVG(unit_price),2) AS avg_price,
    SUM(transaction_qty) AS total_qty_sold
FROM transaction AS t
JOIN products AS p
	ON t.product_id = p.product_id
JOIN product_types AS pt
	ON p.product_type_id = pt.product_type_id
JOIN product_categories AS pc
	ON pt.product_category_id = pc.product_category_id
GROUP BY
    DATE_TRUNC('month', transaction_date),
    product_category
ORDER BY
    month,
	product_category ASC;

-- Revenue by store location and product category
SELECT
    s.store_location,
    pc.product_category,
    COUNT(*) AS total_transactions,
    SUM(t.transaction_qty) AS total_qty_sold,
    SUM(t.transaction_qty * p.unit_price) AS total_revenue
FROM transaction AS t
JOIN stores AS s
	ON t.store_id = s.store_id
JOIN products AS p
	ON t.product_id = p.product_id
JOIN product_types AS pt
	ON p.product_type_id = pt.product_type_id
JOIN product_categories AS pc
	ON pt.product_category_id = pc.product_category_id
GROUP BY
    s.store_location,
	pc.product_category
ORDER BY
    s.store_location,
	total_revenue DESC;

-- TOP 5 products by revenue
SELECT
    p.product_id,
    p.product_detail,
    pc.product_category,
    SUM(t.transaction_qty) AS total_qty_sold,
    ROUND(SUM(t.transaction_qty * p.unit_price)) AS total_revenue
FROM transaction AS t
JOIN products AS p
	ON t.product_id = p.product_id
JOIN product_types AS pt
	ON p.product_type_id = pt.product_type_id
JOIN product_categories AS pc
	ON pt.product_category_id = pc.product_category_id
GROUP BY
    p.product_id,
	p.product_detail,
	pc.product_category
ORDER BY
    total_revenue DESC
LIMIT 5;

-- Products revenue by month
SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    p.product_detail,
    SUM(t.transaction_qty) AS total_qty_sold,
    SUM(t.transaction_qty * p.unit_price) AS total_revenue
FROM transaction AS t
JOIN products AS p
	ON t.product_id = p.product_id
GROUP BY
    DATE_TRUNC('month',
	transaction_date),
	p.product_detail
ORDER BY
    month,
	total_revenue DESC;


-- Monthly growth rate
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', transaction_date) AS month,
        SUM(transaction_qty * unit_price) AS total_revenue
    FROM transaction AS t
    JOIN products AS p
		ON t.product_id = p.product_id
    GROUP BY
        DATE_TRUNC('month', transaction_date)
)
SELECT
    month,
    total_revenue,
    LAG(total_revenue) OVER (ORDER BY month) AS previous_month_revenue,
    ROUND(((total_revenue - LAG(total_revenue) OVER (ORDER BY month)) / LAG(total_revenue) OVER (ORDER BY month)) * 100, 2) AS revenue_growth_rate
FROM monthly_revenue
ORDER BY month;

