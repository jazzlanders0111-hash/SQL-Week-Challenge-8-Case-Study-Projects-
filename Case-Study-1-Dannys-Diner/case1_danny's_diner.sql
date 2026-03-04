DROP SCHEMA IF EXISTS dannys_diner CASCADE;

CREATE SCHEMA dannys_diner;
SET search_path = dannys_diner;

CREATE TABLE sales (
  "customer_id" VARCHAR(1),
  "order_date" DATE,
  "product_id" INTEGER
);

INSERT INTO sales
  ("customer_id", "order_date", "product_id")
VALUES
  ('A', '2021-01-01', '1'),
  ('A', '2021-01-01', '2'),
  ('A', '2021-01-07', '2'),
  ('A', '2021-01-10', '3'),
  ('A', '2021-01-11', '3'),
  ('A', '2021-01-11', '3'),
  ('B', '2021-01-01', '2'),
  ('B', '2021-01-02', '2'),
  ('B', '2021-01-04', '1'),
  ('B', '2021-01-11', '1'),
  ('B', '2021-01-16', '3'),
  ('B', '2021-02-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-07', '3');
 

CREATE TABLE menu (
  "product_id" INTEGER,
  "product_name" VARCHAR(5),
  "price" INTEGER
);

INSERT INTO menu
  ("product_id", "product_name", "price")
VALUES
  ('1', 'sushi', '10'),
  ('2', 'curry', '15'),
  ('3', 'ramen', '12');
  

CREATE TABLE members (
  "customer_id" VARCHAR(1),
  "join_date" DATE
);

INSERT INTO members
  ("customer_id", "join_date")
VALUES
  ('A', '2021-01-07'),
  ('B', '2021-01-09');

-- CHECKING ALL THE DATA IF THERE ARE ISSUES(NULLS, DUPLICATED ENTRIES, MISSING VALUES, ANOMALY ENTRY, ETC.)

SELECT * FROM sales;
SELECT * FROM menu;
SELECT * FROM members;

-- Pre_Query Table: I need to combine some tables for better view of some/all datasets

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
)
SELECT *
FROM full_table

-- Q1: What is the total amount each customer spent at the restaurant?

SELECT customer_id, SUM(price) AS total_amount
FROM sales
LEFT JOIN menu ON sales.product_id = menu.product_id
GROUP BY customer_id
ORDER BY customer_id ASC;

-- Q2: How many days has each customer visited the restaurant?

SELECT customer_id, COUNT(DISTINCT order_date) AS total_visit
FROM sales
GROUP BY customer_id
ORDER BY customer_id ASC;
-- or ORDER BY total_visit DESC; (if we choose to look at who visits the most)

-- Q3: What was the first item from the menu purchased by each customer?

WITH first_item_purchased AS (
	SELECT customer_id, product_name, order_date,
		DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY order_date ASC) AS rank
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
)
SELECT customer_id, product_name
FROM first_item_purchased
WHERE rank = 1
GROUP BY customer_id, product_name;

-- Q4: What is the most purchased item on the menu and how many times was it purchased by all customers?
-- (Pre-Query CTE Table usage for downstream)

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
)
SELECT product_name AS most_purchased_product, COUNT(product_name) AS total_purchased
FROM full_table
GROUP BY product_name
ORDER BY total_purchased DESC
LIMIT 1; -- change limit if you want top 5 or 10

-- Q5: Which item was the most popular for each customer?

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
),
most_pop_dish AS (
	SELECT customer_id, product_name, COUNT(product_name) AS total_orders,
		DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY COUNT(product_name) DESC) AS rank_pop
	FROM full_table
	GROUP BY customer_id, product_name
)
SELECT customer_id, product_name, total_orders
FROM most_pop_dish
WHERE rank_pop = 1;

-- Q6: Which item was purchased first by the customer after they became a member?

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
),
join_in_membership AS (
	SELECT customer_id, product_name AS membership_first_purchased,
		DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY order_date ASC) AS rank
	FROM full_table
	WHERE order_date >= join_date
)
SELECT customer_id, membership_first_purchased
FROM join_in_membership
WHERE rank = 1;

-- Q7: Which item was purchased just before the customer became a member?

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
),
before_membership AS (
	SELECT customer_id, product_name AS before_membership_last_purchased,
		DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rank
	FROM full_table
	WHERE order_date < join_date
)
SELECT customer_id, before_membership_last_purchased
FROM before_membership
WHERE rank = 1;

-- Q8: What is the total items and amount spent for each member before they became a member?

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
)
SELECT customer_id, COUNT(product_name) AS total_item, SUM(price) AS total_amount
FROM full_table
WHERE order_date < join_date
GROUP BY customer_id
ORDER BY customer_id ASC;

-- Q9: If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
)
SELECT customer_id,
	SUM(
		CASE
			WHEN product_name = 'sushi' THEN price * 20
			ELSE price * 10
		END
	) AS total_points
FROM full_table
GROUP BY customer_id
ORDER BY customer_id ASC;

-- Q10: In the first week after a customer joins the program (including their join date) 
-- they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
)
SELECT customer_id,
	SUM(
		CASE
			WHEN order_date BETWEEN join_date AND join_date + INTERVAL '6 days'
				THEN price * 20
			WHEN product_name = 'sushi' 
				THEN price * 20
			ELSE price * 10
		END
	) AS end_of_january_total_points
FROM full_table
WHERE order_date <= '2021-01-31' AND join_date IS NOT NULL
GROUP BY customer_id
ORDER BY customer_id ASC;

-- BONUS Q1: Joining all the items but adding membership confirmation

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
)
SELECT customer_id, order_date, product_name, price,
	CASE 
		WHEN order_date > join_date THEN 'Y'
		ELSE 'N'
	END AS membership_status
FROM full_table
ORDER BY customer_id, order_date ASC;


-- BONUS Q2: Ranking of all customer products after membership

WITH full_table AS (
	SELECT sales.customer_id, sales.order_date, sales.product_id,
	menu.product_name, menu.price, members.join_date
	FROM sales
	LEFT JOIN menu ON sales.product_id = menu.product_id
	LEFT JOIN members ON sales.customer_id = members.customer_id
),
membership_table AS (
	SELECT customer_id, order_date, product_name, price,
		CASE 
			WHEN order_date >= join_date THEN 'Y'
			ELSE 'N'
	END AS membership_status
FROM full_table
)
SELECT *,
	CASE 
		WHEN membership_status = 'N' THEN NULL
		ELSE DENSE_RANK() OVER
		(PARTITION BY customer_id ORDER BY order_date)
	END AS ranking
FROM membership_table;


	