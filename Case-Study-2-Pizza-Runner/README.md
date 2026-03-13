# Case Study #2 — Pizza Runner

## 📚 Table of Contents
- [Business Task](#business-task)
- [Entity Relationship Diagram](#entity-relationship-diagram)
- [Data Cleaning](#data-cleaning)
- [A. Pizza Metrics](#a-pizza-metrics)
- [B. Runner and Customer Experience](#b-runner-and-customer-experience)
- [C. Ingredient Optimisation](#c-ingredient-optimisation)
- [D. Pricing and Ratings](#d-pricing-and-ratings)
- [E. Bonus Questions](#e-bonus-questions)

> All case study details sourced from: [8 Week SQL Challenge — Case Study #2](https://8weeksqlchallenge.com/case-study-2/)

---

## Business Task

Danny launched Pizza Runner — an Uber-style pizza delivery service — by recruiting runners to deliver fresh pizzas from his house (Pizza Runner HQ) and building a mobile app to accept customer orders. He needs help cleaning his data and answering key operational questions to better manage his runners, understand customer behaviour, and optimise overall delivery performance.

---

## Entity Relationship Diagram

![ERD](https://github.com/user-attachments/assets/your-erd-image-here)

---

## Data Cleaning

> 💡 Before answering any questions, I inspected all six tables and found significant data quality issues in `customer_orders` and `runner_orders` — mixed `null` strings, empty strings, inconsistent units in distance and duration, and a year mismatch between order timestamps (2020) and runner registration dates (2021).
>
> Instead of CTEs, I used `TEMP TABLE`s so the cleaned data is reusable downstream across all sections without repeating the cleaning logic.

```sql
-- Cleans null strings, empty strings, and shifts order timestamps from 2020 → 2021
-- to align with runner registration dates for accurate time-gap calculations
DROP TABLE IF EXISTS cleaned_customer_orders CASCADE;
CREATE TEMP TABLE cleaned_customer_orders AS
SELECT
    ROW_NUMBER() OVER ()                       AS row_id,
    order_id,
    customer_id,
    pizza_id,
    NULLIF(NULLIF(exclusions, 'null'), '')     AS exclusions,
    NULLIF(NULLIF(extras,     'null'), '')     AS extras,
    REPLACE(order_time::TEXT, '2020', '2021')::TIMESTAMP AS order_time
FROM customer_orders;

-- Strips unit labels (km, mins, minutes) so distance and duration can be cast to numerics
DROP TABLE IF EXISTS cleaned_runner_orders CASCADE;
CREATE TEMP TABLE cleaned_runner_orders AS
SELECT
    order_id,
    runner_id,
    NULLIF(REPLACE(pickup_time::TEXT, '2020', '2021'), 'null')::TIMESTAMP AS pickup_time,
    NULLIF(REGEXP_REPLACE(distance, '[^0-9.]', '', 'g'), '')              AS distance_km,
    NULLIF(REGEXP_REPLACE(duration, '[^0-9.]', '', 'g'), '')              AS duration_mins,
    NULLIF(NULLIF(cancellation, 'null'), '')                               AS cancellation
FROM runner_orders;

ALTER TABLE cleaned_runner_orders
    ALTER COLUMN pickup_time   TYPE TIMESTAMP,
    ALTER COLUMN distance_km   TYPE FLOAT USING distance_km::FLOAT,
    ALTER COLUMN duration_mins TYPE INT   USING duration_mins::INT;

-- Normalises pizza_recipes from a comma-separated string into one row per topping
-- Stored as a TEMP TABLE so Section C queries can join directly without repeating UNNEST
DROP TABLE IF EXISTS pizza_recipes_expanded;
CREATE TEMP TABLE pizza_recipes_expanded AS
SELECT
    pizza_id,
    TRIM(topping)::INT AS topping_id
FROM pizza_recipes
CROSS JOIN LATERAL UNNEST(string_to_array(toppings, ',')) AS topping;
```

**Key cleaning decisions:**
- `NULLIF(NULLIF(col, 'null'), '')` handles both the string `'null'` and empty strings in one pass
- `REGEXP_REPLACE(col, '[^0-9.]', '', 'g')` strips all non-numeric characters — covers `km`, `mins`, `minutes`, and trailing spaces in one expression
- `row_id` via `ROW_NUMBER()` is added to `cleaned_customer_orders` to uniquely identify each pizza line item, since `order_id + pizza_id` alone is not unique (order 10 has two Meatlovers rows with different customisations)
- Year shifted from 2020 → 2021 to align with runner registration dates — this is important for any time-gap calculations in Section B

---

## A. Pizza Metrics

> This section focuses on core order volume metrics — total pizzas, unique orders, delivery counts, and order distribution across time and customers. Most queries run directly against `cleaned_customer_orders` and `cleaned_runner_orders`. Cancellation filtering is handled consistently using `cancellation IS NULL` in the `ON` clause rather than a `WHERE` filter after a `LEFT JOIN`, which is more explicit about intent.

---

### AQ1 — How many pizzas were ordered?

```sql
SELECT COUNT(order_id) AS total_ordered
FROM cleaned_customer_orders;
```

**Approach:**
- Simple `COUNT` on `order_id` — no filter needed since the question asks for all orders including cancelled ones
- Each row in `customer_orders` represents one pizza, so counting rows gives the total pizza count

**Result:**
| total_ordered |
|---|
| 14 |

- 14 pizzas were ordered in total across all 10 unique orders.

---

### AQ2 — How many unique customer orders were made?

```sql
SELECT COUNT(DISTINCT order_id) AS customer_orders
FROM cleaned_customer_orders;
```

**Approach:**
- `COUNT(DISTINCT order_id)` is the key — one order can contain multiple pizzas (multiple rows), so `DISTINCT` collapses those into a single order count
- Without `DISTINCT`, this would return 14 instead of the correct 10

**Result:**
| customer_orders |
|---|
| 10 |

---

### AQ3 — How many successful orders were delivered by each runner?

```sql
SELECT runner_id, COUNT(order_id) AS successful_orders
FROM cleaned_runner_orders
WHERE cancellation IS NULL
GROUP BY runner_id
ORDER BY runner_id;
```

**Approach:**
- After cleaning, cancelled orders have a proper `NULL` in `cancellation` — so `WHERE cancellation IS NULL` reliably filters them out
- Orders 6 and 9 are excluded as they were cancelled

**Result:**
| runner_id | successful_orders |
|---|---|
| 1 | 4 |
| 2 | 3 |
| 3 | 1 |

---

### AQ4 — How many of each type of pizza was delivered?

```sql
SELECT
    pn.pizza_name,
    COUNT(cot.order_id) AS total_delivered
FROM cleaned_customer_orders AS cot
INNER JOIN cleaned_runner_orders AS rot
    ON  cot.order_id = rot.order_id
    AND rot.cancellation IS NULL
INNER JOIN pizza_names AS pn ON cot.pizza_id = pn.pizza_id
GROUP BY pn.pizza_name
ORDER BY pn.pizza_name;
```

**Approach:**
- Putting `cancellation IS NULL` in the `ON` clause of the `INNER JOIN` is intentional — it's cleaner and more explicit about intent than a `LEFT JOIN` + `WHERE` filter which implicitly converts to an inner join anyway
- `INNER JOIN` to `pizza_names` maps pizza IDs to their readable names

**Result:**
| pizza_name | total_delivered |
|---|---|
| Meatlovers | 9 |
| Vegetarian | 3 |

- Meatlovers dominates with 9 of 12 delivered pizzas.

---

### AQ5 — How many Vegetarian and Meatlovers were ordered by each customer?

```sql
SELECT
    cot.customer_id,
    pn.pizza_name,
    COUNT(cot.order_id) AS total_orders
FROM cleaned_customer_orders AS cot
LEFT JOIN pizza_names AS pn ON cot.pizza_id = pn.pizza_id
GROUP BY cot.customer_id, pn.pizza_name
ORDER BY cot.customer_id;
```

**Approach:**
- This question asks for all *ordered* pizzas, not just delivered — so no cancellation filter here
- `LEFT JOIN` to `pizza_names` ensures no orders are lost even if a pizza_id somehow has no matching name

**Result:**
| customer_id | pizza_name | total_orders |
|---|---|---|
| 101 | Meatlovers | 2 |
| 101 | Vegetarian | 1 |
| 102 | Meatlovers | 2 |
| 102 | Vegetarian | 1 |
| 103 | Meatlovers | 3 |
| 103 | Vegetarian | 1 |
| 104 | Meatlovers | 3 |
| 105 | Vegetarian | 1 |

- Customer 104 is the only one who exclusively ordered Meatlovers.
- Customer 105 is the only one who exclusively ordered Vegetarian.

---

### AQ6 — What was the maximum number of pizzas delivered in a single order?

```sql
WITH pizza_count AS (
    SELECT
        cot.order_id,
        COUNT(cot.pizza_id) AS total_pizza
    FROM cleaned_customer_orders AS cot
    INNER JOIN cleaned_runner_orders AS rot
        ON  cot.order_id = rot.order_id
        AND rot.cancellation IS NULL
    GROUP BY cot.order_id
)
SELECT MAX(total_pizza) AS max_pizza_single_order
FROM pizza_count;
```

**Approach:**
- Two steps: count pizzas per order in a CTE, then take the `MAX` from that result
- Flattening to one step with `MAX(COUNT(...))` would require nesting aggregates which PostgreSQL doesn't allow directly — the CTE keeps it clean

**Result:**
| max_pizza_single_order |
|---|
| 3 |

- Order 4 (customer 103) holds the record with 3 pizzas in a single delivery.

---

### AQ7 — For each customer, how many delivered pizzas had at least 1 change and how many had no changes?

```sql
SELECT
    cot.customer_id,
    SUM(CASE WHEN cot.exclusions IS NOT NULL OR cot.extras IS NOT NULL THEN 1 ELSE 0 END) AS with_changes,
    SUM(CASE WHEN cot.exclusions IS NULL AND cot.extras IS NULL THEN 1 ELSE 0 END) AS no_changes
FROM cleaned_customer_orders AS cot
INNER JOIN cleaned_runner_orders AS rot
    ON  cot.order_id = rot.order_id
    AND rot.cancellation IS NULL
GROUP BY cot.customer_id
ORDER BY cot.customer_id;
```

**Approach:**
- `SUM(CASE WHEN ...)` gets both counts in a single pass — avoids writing two separate queries
- Since `NULLIF` already standardised the nulls during cleaning, `IS NOT NULL` / `IS NULL` checks are reliable here
- A pizza "has a change" if it has at least one exclusion OR at least one extra

**Result:**
| customer_id | with_changes | no_changes |
|---|---|---|
| 101 | 0 | 2 |
| 102 | 0 | 3 |
| 103 | 3 | 0 |
| 104 | 2 | 1 |
| 105 | 1 | 0 |

- Customer 103 never ordered a pizza without customisation.
- Customers 101 and 102 never requested any changes at all.

---

### AQ8 — How many pizzas were delivered that had both exclusions AND extras?

```sql
SELECT COUNT(*) AS delivered_with_both_changes
FROM cleaned_customer_orders AS cot
INNER JOIN cleaned_runner_orders AS rot
    ON  cot.order_id = rot.order_id
    AND rot.cancellation IS NULL
WHERE cot.exclusions IS NOT NULL
  AND cot.extras IS NOT NULL;
```

**Approach:**
- Both conditions must be true simultaneously — `AND` not `OR`
- Order 9 also had both changes but was cancelled so it's correctly excluded by the join condition

**Result:**
| delivered_with_both_changes |
|---|
| 1 |

- Only 1 pizza was delivered with both exclusions and extras — order 10's second Meatlovers row.

---

### AQ9 — What was the total volume of pizzas ordered for each hour of the day?

```sql
SELECT
    EXTRACT(HOUR FROM order_time) AS hour_of_day,
    COUNT(order_id)               AS total_pizza
FROM cleaned_customer_orders
GROUP BY hour_of_day
ORDER BY hour_of_day;
```

**Approach:**
- `EXTRACT(HOUR FROM order_time)` pulls just the hour component from the timestamp
- No cancellation filter — question asks for ordered, not delivered

**Result:**
| hour_of_day | total_pizza |
|---|---|
| 11 | 1 |
| 13 | 3 |
| 18 | 3 |
| 19 | 1 |
| 21 | 3 |
| 23 | 3 |

- Peak ordering hours are 13:00, 18:00, 21:00, and 23:00 — all classic meal times.

---

### AQ10 — What was the volume of orders for each day of the week?

```sql
SELECT
    TRIM(TO_CHAR(order_time, 'Day')) AS day_of_week,
    COUNT(order_id)                  AS total_pizza
FROM cleaned_customer_orders
GROUP BY EXTRACT(ISODOW FROM order_time), TO_CHAR(order_time, 'Day')
ORDER BY EXTRACT(ISODOW FROM order_time);
```

**Approach:**
- `TO_CHAR(order_time, 'Day')` pads the result with trailing spaces, so `TRIM` is needed to clean the output
- `ORDER BY EXTRACT(ISODOW ...)` sorts by ISO day of week (Mon=1, Sun=7) for natural week ordering — without it the days sort alphabetically which is misleading

**Result:**
| day_of_week | total_pizza |
|---|---|
| Monday | 5 |
| Friday | 5 |
| Saturday | 3 |
| Sunday | 1 |

- Monday and Friday are the busiest days — both with 5 pizzas ordered.

---

## B. Runner and Customer Experience

> This section analyses runner performance and the customer-facing delivery experience — signup trends, pickup times, delivery speeds, and success rates. A key thing to watch here is deduplication: multiple pizza rows per order share the same `pickup_time`, so queries involving time calculations deduplicate at the order level first to avoid inflating averages. The year shift done during cleaning (2020 → 2021) is what makes all the time-gap arithmetic in this section correct.

---

### BQ1 — How many runners signed up for each 1-week period?

```sql
SELECT
    FLOOR((registration_date - '2021-01-01'::DATE) / 7) + 1 AS week_number,
    COUNT(runner_id) AS runners_signed_up
FROM runners
GROUP BY week_number
ORDER BY week_number;
```

**Approach:**
- `DATE - DATE` in PostgreSQL returns an `INTEGER` (days) directly — `EXTRACT` is not needed and would throw an error since it expects an interval, not an integer
- `FLOOR(days / 7) + 1` converts the day offset into a 1-indexed week number
- Week 1 starts on 2021-01-01 as specified

**Result:**
| week_number | runners_signed_up |
|---|---|
| 1 | 2 |
| 2 | 1 |
| 3 | 1 |

- 2 runners joined in the first week, and 1 each in weeks 2 and 3.

---

### BQ2 — What was the average time in minutes for each runner to arrive at HQ to pick up the order?

```sql
WITH order_pickup AS (
    SELECT DISTINCT
        rot.runner_id,
        rot.order_id,
        EXTRACT(EPOCH FROM (rot.pickup_time - cot.order_time)) / 60 AS pickup_minutes
    FROM cleaned_customer_orders AS cot
    INNER JOIN cleaned_runner_orders AS rot
        ON  cot.order_id = rot.order_id
        AND rot.pickup_time IS NOT NULL
)
SELECT
    runner_id,
    ROUND(AVG(pickup_minutes)::NUMERIC, 2) AS avg_pickup_minutes
FROM order_pickup
GROUP BY runner_id
ORDER BY runner_id;
```

**Approach:**
- `DISTINCT` on `(runner_id, order_id)` is critical — without it, orders with multiple pizzas would contribute multiple identical pickup times and inflate the average
- `EXTRACT(EPOCH FROM ...)` returns seconds, dividing by 60 converts to minutes
- The year shift done during cleaning is essential here — without it, 2020 order times subtracted from 2021 pickup times would produce wildly incorrect durations

**Result:**
| runner_id | avg_pickup_minutes |
|---|---|
| 1 | 14.33 |
| 2 | 20.01 |
| 3 | 10.47 |

- Runner 3 is the fastest to pick up on average at ~10.5 minutes, though they only had one successful delivery to average over.

---

### BQ3 — Is there any relationship between the number of pizzas and how long the order takes to prepare?

```sql
WITH order_prep AS (
    SELECT
        cot.order_id,
        COUNT(cot.row_id) AS pizza_count,
        ROUND(
            (EXTRACT(EPOCH FROM (rot.pickup_time - cot.order_time)) / 60)::NUMERIC
        , 2) AS prep_minutes
    FROM cleaned_customer_orders AS cot
    INNER JOIN cleaned_runner_orders AS rot
        ON  cot.order_id = rot.order_id
        AND rot.pickup_time IS NOT NULL
    GROUP BY cot.order_id, cot.order_time, rot.pickup_time
)
SELECT
    pizza_count,
    ROUND(AVG(prep_minutes), 2) AS avg_prep_minutes
FROM order_prep
GROUP BY pizza_count
ORDER BY pizza_count;
```

**Approach:**
- Grouping by `(order_id, order_time, pickup_time)` first collapses multiple pizza rows into one observation per order — `COUNT(row_id)` then gives the correct pizza count for that order
- `row_id` is used instead of `pizza_id` for the count since `row_id` is guaranteed unique per row, whereas `pizza_id` repeats within an order

**Result:**
| pizza_count | avg_prep_minutes |
|---|---|
| 1 | 12.36 |
| 2 | 18.38 |
| 3 | 29.28 |

- Yes — clear positive relationship. Each additional pizza adds roughly 8–10 minutes of prep time. A 3-pizza order takes about 2.5× longer than a 1-pizza order.

---

### BQ4 — What was the average distance travelled for each customer?

```sql
SELECT
    cot.customer_id,
    ROUND(AVG(rot.distance_km)::NUMERIC, 2) AS avg_distance_km
FROM cleaned_customer_orders AS cot
INNER JOIN cleaned_runner_orders AS rot
    ON  cot.order_id = rot.order_id
    AND rot.cancellation IS NULL
GROUP BY cot.customer_id
ORDER BY cot.customer_id;
```

**Approach:**
- `AVG(distance_km)` per customer across delivered orders only
- Distance is per order, not per pizza — but since we're averaging at the customer level and each order has one distance value, this is correct as-is

**Result:**
| customer_id | avg_distance_km |
|---|---|
| 101 | 20.00 |
| 102 | 18.40 |
| 103 | 23.40 |
| 104 | 10.00 |
| 105 | 25.00 |

- Customer 105 lives the furthest from HQ at 25 km, while Customer 104 is the closest at 10 km.

---

### BQ5 — What was the difference between the longest and shortest delivery times for all orders?

```sql
SELECT MAX(duration_mins) - MIN(duration_mins) AS delivery_time_range_mins
FROM cleaned_runner_orders
WHERE cancellation IS NULL;
```

**Approach:**
- Simple `MAX - MIN` on `duration_mins` after filtering out cancelled orders
- The `REGEXP_REPLACE` cleaning done earlier stripped all the unit labels so this arithmetic is possible

**Result:**
| delivery_time_range_mins |
|---|
| 30 |

- The fastest delivery took 10 minutes, the slowest 40 minutes — a 30 minute spread.

---

### BQ6 — What was the average speed for each runner per delivery?

```sql
SELECT
    runner_id,
    order_id,
    distance_km,
    duration_mins,
    ROUND((distance_km / duration_mins * 60)::NUMERIC, 2) AS avg_speed_kmh
FROM cleaned_runner_orders
WHERE cancellation IS NULL
ORDER BY runner_id, order_id;
```

**Approach:**
- Speed = distance / time. Since distance is in km and duration in minutes, multiplying by 60 converts to km/h
- Showing `distance_km` and `duration_mins` alongside speed makes it easier to spot anomalies in the raw inputs

**Result:**
| runner_id | order_id | distance_km | duration_mins | avg_speed_kmh |
|---|---|---|---|---|
| 1 | 1 | 20.0 | 32 | 37.50 |
| 1 | 2 | 20.0 | 27 | 44.44 |
| 1 | 3 | 13.4 | 20 | 40.20 |
| 1 | 10 | 10.0 | 10 | 60.00 |
| 2 | 4 | 23.4 | 40 | 35.10 |
| 2 | 7 | 25.0 | 25 | 60.00 |
| 2 | 8 | 23.4 | 15 | **93.60** |
| 3 | 5 | 10.0 | 15 | 40.00 |

- Runner 1 trends upward across deliveries (37.5 → 60.0 km/h) — possible route familiarity or lighter traffic.
- **Runner 2's order 8 at 93.6 km/h is a likely data quality issue** — almost certainly a distance or duration entry error. This should be flagged for investigation before using speed data in any downstream reporting.

---

### BQ7 — What is the successful delivery percentage for each runner?

```sql
SELECT
    runner_id,
    COUNT(order_id)     AS total_orders,
    COUNT(pickup_time)  AS successful_orders,
    ROUND(COUNT(pickup_time)::NUMERIC / COUNT(order_id) * 100, 0) AS success_pct
FROM cleaned_runner_orders
GROUP BY runner_id
ORDER BY runner_id;
```

**Approach:**
- `COUNT(pickup_time)` automatically skips `NULL` values — cancelled orders have `NULL` pickup times after cleaning, so they naturally don't count as successful
- `COUNT(order_id)` includes all orders for the correct denominator

**Result:**
| runner_id | total_orders | successful_orders | success_pct |
|---|---|---|---|
| 1 | 4 | 4 | 100 |
| 2 | 4 | 3 | 75 |
| 3 | 2 | 1 | 50 |

- Runner 1 has a perfect record. Runner 2 had one customer cancellation. Runner 3 had one restaurant cancellation.

---

## C. Ingredient Optimisation

> This is the most complex section — it deals with the messy comma-separated `exclusions` and `extras` columns and requires unnesting, anti-joins, and careful deduplication to get correct results.
>
> 💡 All C queries rely on `pizza_recipes_expanded` — the temp table created during cleaning that unnests the comma-separated toppings string into one row per topping per pizza. This avoids repeating the `UNNEST` logic across every query. The core pattern used throughout CQ4–CQ6 is: **standard toppings `UNION ALL` extras → anti-join out exclusions → count and label**.

---

### CQ1 — What are the standard ingredients for each pizza?

```sql
SELECT
    pn.pizza_name,
    STRING_AGG(pt.topping_name, ', ' ORDER BY pt.topping_name) AS standard_ingredients
FROM pizza_recipes_expanded AS pre
INNER JOIN pizza_names    AS pn ON pre.pizza_id   = pn.pizza_id
INNER JOIN pizza_toppings AS pt ON pre.topping_id = pt.topping_id
GROUP BY pn.pizza_name
ORDER BY pn.pizza_name;
```

**Approach:**
- `pizza_recipes_expanded` already has one row per topping — so this is just a join and aggregate
- `STRING_AGG(...ORDER BY pt.topping_name)` produces an alphabetically ordered comma-separated list

**Result:**
| pizza_name | standard_ingredients |
|---|---|
| Meatlovers | Bacon, BBQ Sauce, Beef, Cheese, Chicken, Mushrooms, Pepperoni, Salami |
| Vegetarian | Cheese, Mushrooms, Onions, Peppers, Tomato Sauce, Tomatoes |

---

### CQ2 — What was the most commonly added extra?

```sql
SELECT
    pt.topping_name,
    COUNT(*) AS times_added
FROM cleaned_customer_orders AS cot
CROSS JOIN LATERAL UNNEST(string_to_array(cot.extras, ',')) AS extr
INNER JOIN pizza_toppings AS pt ON TRIM(extr)::INT = pt.topping_id
WHERE cot.extras IS NOT NULL
GROUP BY pt.topping_name
ORDER BY times_added DESC
LIMIT 1;
```

**Approach:**
- `string_to_array` splits the comma-separated extras string, `UNNEST` explodes it into individual rows — necessary because a single extras field can contain multiple topping IDs
- `TRIM(extr)::INT` handles the spaces between commas before casting to integer for the join

**Result:**
| topping_name | times_added |
|---|---|
| Bacon | 4 |

- Bacon is the most requested extra — added in orders 5, 7, 9, and 10.

---

### CQ3 — What was the most common exclusion?

```sql
SELECT
    pt.topping_name,
    COUNT(*) AS times_excluded
FROM cleaned_customer_orders AS cot
CROSS JOIN LATERAL UNNEST(string_to_array(cot.exclusions, ',')) AS excl
INNER JOIN pizza_toppings AS pt ON TRIM(excl)::INT = pt.topping_id
WHERE cot.exclusions IS NOT NULL
GROUP BY pt.topping_name
ORDER BY times_excluded DESC
LIMIT 1;
```

**Approach:**
- Same `UNNEST` pattern as CQ2 — exclusions also store multiple values comma-separated
- Cheese appears as an exclusion across all 3 rows of order 4 plus order 9

**Result:**
| topping_name | times_excluded |
|---|---|
| Cheese | 4 |

- Cheese is the most excluded topping — interesting contrast to it also being the most added extra (order 10).

---

### CQ4 — Generate an order item label for each pizza row

```sql
WITH order_exclusions AS (
    SELECT
        cot.row_id,
        STRING_AGG(pt.topping_name, ', ' ORDER BY pt.topping_name) AS excluded_list
    FROM cleaned_customer_orders AS cot
    CROSS JOIN LATERAL UNNEST(string_to_array(cot.exclusions, ',')) AS excl
    INNER JOIN pizza_toppings AS pt ON TRIM(excl)::INT = pt.topping_id
    WHERE cot.exclusions IS NOT NULL
    GROUP BY cot.row_id
),
order_extras AS (
    SELECT
        cot.row_id,
        STRING_AGG(pt.topping_name, ', ' ORDER BY pt.topping_name) AS extras_list
    FROM cleaned_customer_orders AS cot
    CROSS JOIN LATERAL UNNEST(string_to_array(cot.extras, ',')) AS extr
    INNER JOIN pizza_toppings AS pt ON TRIM(extr)::INT = pt.topping_id
    WHERE cot.extras IS NOT NULL
    GROUP BY cot.row_id
)
SELECT
    cot.order_id,
    cot.customer_id,
    pn.pizza_name
        || COALESCE(' - Exclude ' || oe.excluded_list, '')
        || COALESCE(' - Extra '   || ox.extras_list,   '')
    AS order_item
FROM cleaned_customer_orders AS cot
INNER JOIN pizza_names       AS pn ON cot.pizza_id = pn.pizza_id
LEFT  JOIN order_exclusions  AS oe ON cot.row_id   = oe.row_id
LEFT  JOIN order_extras      AS ox ON cot.row_id   = ox.row_id
ORDER BY cot.order_id, cot.row_id;
```

**Approach:**
- Exclusion and extras label lists are built separately in CTEs then joined back — keeps the final SELECT clean and readable
- `COALESCE(' - Exclude ' || list, '')` handles pizzas with no exclusions gracefully — without `COALESCE`, concatenating with `NULL` would wipe the entire label string
- `row_id` is the join key instead of `order_id + pizza_id` — order 10 has two Meatlovers rows with completely different customisations that would collide without a unique row identifier

**Result:**
| order_id | customer_id | order_item |
|---|---|---|
| 1 | 101 | Meatlovers |
| 2 | 101 | Meatlovers |
| 3 | 102 | Meatlovers |
| 3 | 102 | Vegetarian |
| 4 | 103 | Meatlovers - Exclude Cheese |
| 4 | 103 | Meatlovers - Exclude Cheese |
| 4 | 103 | Vegetarian - Exclude Cheese |
| 5 | 104 | Meatlovers - Extra Bacon |
| 6 | 101 | Vegetarian |
| 7 | 105 | Vegetarian - Extra Bacon |
| 8 | 102 | Meatlovers |
| 9 | 103 | Meatlovers - Exclude Cheese - Extra Bacon, Chicken |
| 10 | 104 | Meatlovers |
| 10 | 104 | Meatlovers - Exclude BBQ Sauce, Mushrooms - Extra Bacon, Cheese |

---

### CQ5 — Generate an alphabetically ordered ingredient list per pizza with `2x` for doubles

```sql
WITH base AS (
    -- Standard toppings for each ordered pizza row
    SELECT cot.row_id, cot.order_id, cot.customer_id, cot.pizza_id,
           pre.topping_id, pt.topping_name
    FROM cleaned_customer_orders AS cot
    INNER JOIN pizza_recipes_expanded AS pre ON cot.pizza_id   = pre.pizza_id
    INNER JOIN pizza_toppings         AS pt  ON pre.topping_id = pt.topping_id
),
added AS (
    -- Extras requested — UNION ALL preserves duplicates so the 2x count works correctly
    SELECT cot.row_id, cot.order_id, cot.customer_id, cot.pizza_id,
           TRIM(extr)::INT AS topping_id, pt.topping_name
    FROM cleaned_customer_orders AS cot
    CROSS JOIN LATERAL UNNEST(string_to_array(cot.extras, ',')) AS extr
    INNER JOIN pizza_toppings AS pt ON TRIM(extr)::INT = pt.topping_id
    WHERE cot.extras IS NOT NULL
),
excluded AS (
    SELECT cot.row_id, TRIM(excl)::INT AS topping_id
    FROM cleaned_customer_orders AS cot
    CROSS JOIN LATERAL UNNEST(string_to_array(cot.exclusions, ',')) AS excl
    WHERE cot.exclusions IS NOT NULL
),
all_ingredients AS (
    SELECT row_id, order_id, customer_id, pizza_id, topping_id, topping_name FROM base
    UNION ALL
    SELECT row_id, order_id, customer_id, pizza_id, topping_id, topping_name FROM added
),
filtered AS (
    -- Anti-join pattern: LEFT JOIN + WHERE NULL removes excluded toppings
    SELECT ai.*
    FROM all_ingredients AS ai
    LEFT JOIN excluded AS ex ON ai.row_id = ex.row_id AND ai.topping_id = ex.topping_id
    WHERE ex.topping_id IS NULL
),
counted AS (
    SELECT row_id, order_id, customer_id, pizza_id, topping_name, COUNT(*) AS qty
    FROM filtered
    GROUP BY row_id, order_id, customer_id, pizza_id, topping_name
),
labelled AS (
    SELECT row_id, order_id, customer_id, pizza_id,
        CASE WHEN qty > 1 THEN qty || 'x' || topping_name ELSE topping_name END AS ingredient_label
    FROM counted
)
SELECT
    lb.order_id,
    lb.customer_id,
    pn.pizza_name || ': ' || STRING_AGG(lb.ingredient_label, ', ' ORDER BY lb.ingredient_label) AS ingredient_list
FROM labelled AS lb
INNER JOIN pizza_names AS pn ON lb.pizza_id = pn.pizza_id
GROUP BY lb.row_id, lb.order_id, lb.customer_id, lb.pizza_id, pn.pizza_name
ORDER BY lb.order_id, lb.row_id;
```

**Approach:**
- Three-step logic: `base` (standard recipe) `UNION ALL` `added` (extras) → anti-join out `excluded` → count occurrences per topping
- `UNION ALL` is intentional over `UNION` — it preserves the duplicate when a topping appears in both the recipe and as a requested extra, which is exactly what creates the `2x` count
- The anti-join (`LEFT JOIN ... WHERE ex.topping_id IS NULL`) is cleaner than `NOT IN` and handles `NULL` correctly

**Result (selected rows):**
| order_id | customer_id | ingredient_list |
|---|---|---|
| 1 | 101 | Meatlovers: Bacon, BBQ Sauce, Beef, Cheese, Chicken, Mushrooms, Pepperoni, Salami |
| 5 | 104 | Meatlovers: 2xBacon, BBQ Sauce, Beef, Cheese, Chicken, Mushrooms, Pepperoni, Salami |
| 9 | 103 | Meatlovers: 2xBacon, BBQ Sauce, Beef, 2xChicken, Mushrooms, Pepperoni, Salami |
| 10 | 104 | Meatlovers: Bacon, Beef, 2xCheese, Chicken, Pepperoni, Salami |

---

### CQ6 — What is the total quantity of each ingredient used in all delivered pizzas?

```sql
-- Same base + extras − exclusions logic as CQ5
-- Scoped to delivered orders only by joining cleaned_runner_orders
-- Orders 6 and 9 were cancelled so their ingredients are excluded from the count
WITH base AS ( ... ),
added AS ( ... ),
excluded AS ( ... ),
all_ingredients AS ( ... ),
filtered AS ( ... )
SELECT
    pt.topping_name,
    COUNT(*) AS total_used
FROM filtered AS fi
INNER JOIN pizza_toppings AS pt ON fi.topping_id = pt.topping_id
GROUP BY pt.topping_name
ORDER BY total_used DESC;
```

**Result:**
| topping_name | total_used |
|---|---|
| Bacon | 12 |
| Mushrooms | 11 |
| Cheese | 10 |
| Pepperoni | 9 |
| Salami | 9 |
| Chicken | 9 |
| Beef | 9 |
| BBQ Sauce | 8 |
| Tomato Sauce | 3 |
| Onions | 3 |
| Tomatoes | 3 |
| Peppers | 3 |

- Bacon leads the count heavily — it's both a standard Meatlovers topping and the most requested extra.

---

## D. Pricing and Ratings

> This section moves from operational questions into business financials and system design. DQ1–DQ2 calculate revenue under different pricing rules, DQ3 involves designing a new ratings table from scratch, DQ4 joins everything together into a single delivery summary, and DQ5 calculates net profit after runner costs. The ratings table I designed uses a `CHECK` constraint to enforce the 1–5 range at the database level and a nullable `comment` column to reflect how real rating systems work.

---

### DQ1 — Total revenue with fixed prices, no extras charge, no delivery fees

```sql
SELECT
    SUM(CASE WHEN cot.pizza_id = 1 THEN 12 ELSE 10 END) AS total_revenue
FROM cleaned_customer_orders AS cot
INNER JOIN cleaned_runner_orders AS rot
    ON  cot.order_id = rot.order_id
    AND rot.cancellation IS NULL;
```

**Approach:**
- `CASE WHEN pizza_id = 1` maps Meatlovers to $12 and Vegetarian to $10
- Only delivered orders are counted — cancelled orders are filtered out by the join condition

**Result:**
| total_revenue |
|---|
| $138 |

---

### DQ2 — Total revenue with $1 charge per extra topping

```sql
WITH extra_charges AS (
    SELECT cot.row_id, COUNT(extr) AS extra_count
    FROM cleaned_customer_orders AS cot
    CROSS JOIN LATERAL UNNEST(string_to_array(cot.extras, ',')) AS extr
    WHERE cot.extras IS NOT NULL
    GROUP BY cot.row_id
)
SELECT
    SUM(
        CASE WHEN cot.pizza_id = 1 THEN 12 ELSE 10 END
        + COALESCE(ec.extra_count, 0)
    ) AS total_revenue_with_extras
FROM cleaned_customer_orders AS cot
INNER JOIN cleaned_runner_orders AS rot
    ON  cot.order_id = rot.order_id
    AND rot.cancellation IS NULL
LEFT JOIN extra_charges AS ec ON cot.row_id = ec.row_id;
```

**Approach:**
- Extra charges are calculated separately in a CTE by unnesting and counting the extras per pizza row
- `LEFT JOIN` with `COALESCE(..., 0)` ensures pizzas with no extras still appear in the sum with zero extra charge — an `INNER JOIN` would drop them entirely

**Result:**
| total_revenue_with_extras |
|---|
| $142 |

- The extra $1 per topping adds $4 to total revenue — from 4 extra toppings across delivered orders.

---

### DQ3 — Design a runner ratings table

```sql
DROP TABLE IF EXISTS runner_ratings;
CREATE TABLE runner_ratings (
    "rating_id"    SERIAL PRIMARY KEY,
    "order_id"     INTEGER   NOT NULL,
    "runner_id"    INTEGER   NOT NULL,
    "customer_id"  INTEGER   NOT NULL,
    "rating"       INTEGER   NOT NULL CHECK (rating BETWEEN 1 AND 5),
    "comment"      TEXT,
    "rated_at"     TIMESTAMP DEFAULT NOW()
);

INSERT INTO runner_ratings ("order_id", "runner_id", "customer_id", "rating", "comment", "rated_at")
VALUES
    (1,  1, 101, 5, 'Super fast delivery!',          '2021-01-01 19:30:00'),
    (2,  1, 101, 4, NULL,                             '2021-01-01 20:10:00'),
    (3,  1, 102, 5, 'Great service as always',        '2021-01-03 01:00:00'),
    (4,  2, 103, 3, 'Took a while but food was hot',  '2021-01-04 14:30:00'),
    (5,  3, 104, 4, NULL,                             '2021-01-08 21:45:00'),
    (7,  2, 105, 5, 'Delivered right on time!',       '2021-01-08 22:00:00'),
    (8,  2, 102, 2, 'A bit late but no complaints',   '2021-01-10 01:00:00'),
    (10, 1, 104, 5, 'Perfect, will order again',      '2021-01-11 19:15:00');
```

**Approach:**
- `SERIAL PRIMARY KEY` auto-increments `rating_id` — no need to manually manage it
- `CHECK (rating BETWEEN 1 AND 5)` enforces the 1–5 constraint at the database level rather than relying on application logic
- `comment TEXT` is nullable — not every customer leaves a comment, keeping it optional is more realistic
- `rated_at TIMESTAMP DEFAULT NOW()` records when the rating was submitted — useful for time-based analysis later
- Orders 6 and 9 are intentionally excluded — they were cancelled, so there is no delivery to rate

---

### DQ4 — Full delivery summary table

```sql
SELECT
    cot.customer_id,
    cot.order_id,
    rot.runner_id,
    rr.rating,
    cot.order_time,
    rot.pickup_time,
    ROUND(EXTRACT(EPOCH FROM (rot.pickup_time - cot.order_time)) / 60) AS mins_to_pickup,
    rot.duration_mins                                                    AS delivery_duration_mins,
    ROUND((rot.distance_km / rot.duration_mins * 60)::NUMERIC, 2)       AS avg_speed_kmh,
    COUNT(cot.row_id)                                                    AS total_pizzas
FROM cleaned_customer_orders AS cot
INNER JOIN cleaned_runner_orders AS rot
    ON  cot.order_id = rot.order_id
    AND rot.cancellation IS NULL
INNER JOIN runner_ratings AS rr ON cot.order_id = rr.order_id
GROUP BY cot.customer_id, cot.order_id, rot.runner_id, rr.rating,
         cot.order_time, rot.pickup_time, rot.duration_mins, rot.distance_km
ORDER BY cot.order_id;
```

**Approach:**
- Grouping by all non-aggregated columns and using `COUNT(row_id)` correctly collapses multiple pizza rows per order into a single summary row while still counting the total pizzas
- `EXTRACT(EPOCH FROM ...)` returns seconds — dividing by 60 gives minutes to pickup
- All columns from the question spec are included in the output

**Result:**
| customer_id | order_id | runner_id | rating | order_time | pickup_time | mins_to_pickup | delivery_duration_mins | avg_speed_kmh | total_pizzas |
|---|---|---|---|---|---|---|---|---|---|
| 101 | 1 | 1 | 5 | 2021-01-01 18:05:02 | 2021-01-01 18:15:34 | 11 | 32 | 37.50 | 1 |
| 101 | 2 | 1 | 4 | 2021-01-01 19:00:52 | 2021-01-01 19:10:54 | 10 | 27 | 44.44 | 1 |
| 102 | 3 | 1 | 5 | 2021-01-02 23:51:23 | 2021-01-03 00:12:37 | 21 | 20 | 40.20 | 2 |
| 103 | 4 | 2 | 3 | 2021-01-04 13:23:46 | 2021-01-04 13:53:03 | 29 | 40 | 35.10 | 3 |
| 104 | 5 | 3 | 4 | 2021-01-08 21:00:29 | 2021-01-08 21:10:57 | 11 | 15 | 40.00 | 1 |
| 105 | 7 | 2 | 5 | 2021-01-08 21:20:29 | 2021-01-08 21:30:45 | 10 | 25 | 60.00 | 1 |
| 102 | 8 | 2 | 2 | 2021-01-09 23:54:33 | 2021-01-10 00:15:02 | 21 | 15 | 93.60 | 1 |
| 104 | 10 | 1 | 5 | 2021-01-11 18:34:49 | 2021-01-11 18:50:20 | 16 | 10 | 60.00 | 2 |

---

### DQ5 — Profit after paying runners $0.30 per km

```sql
WITH revenue AS (
    SELECT SUM(CASE WHEN cot.pizza_id = 1 THEN 12 ELSE 10 END) AS total_revenue
    FROM cleaned_customer_orders AS cot
    INNER JOIN cleaned_runner_orders AS rot
        ON  cot.order_id = rot.order_id
        AND rot.cancellation IS NULL
),
delivery_cost AS (
    SELECT ROUND(SUM(distance_km) * 0.30, 2) AS total_runner_cost
    FROM cleaned_runner_orders
    WHERE cancellation IS NULL
)
SELECT
    rv.total_revenue,
    dc.total_runner_cost,
    ROUND(rv.total_revenue - dc.total_runner_cost, 2) AS profit
FROM revenue AS rv
CROSS JOIN delivery_cost AS dc;
```

**Approach:**
- Revenue and runner cost are calculated in separate CTEs since they aggregate over different tables
- `CROSS JOIN` on two single-row aggregates is the cleanest way to combine them into one output row
- Distance is per order, not per pizza — so no deduplication is needed since `cleaned_runner_orders` already has one row per order

**Result:**
| total_revenue | total_runner_cost | profit |
|---|---|---|
| $138.00 | $43.56 | $94.44 |

- After paying runners, Pizza Runner keeps $94.44 — about 68% of gross revenue.

---

## E. Bonus Questions

> This section is a DML (Data Manipulation Language) challenge — it's less about querying and more about thinking through how the existing schema handles change. The key insight here is that adding a new pizza only requires `INSERT` statements, not `ALTER TABLE`. The schema was designed well enough that extending the menu has zero structural impact on any other table.

---

### EQ1 — What would happen if a new Supreme pizza was added to the menu?

```sql
-- Only two tables need to be updated — pizza_names and pizza_recipes
-- The existing schema already supports new pizzas without any structural changes
INSERT INTO pizza_names ("pizza_id", "pizza_name")
VALUES (3, 'Supreme');

INSERT INTO pizza_recipes ("pizza_id", "toppings")
VALUES (3, '1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12');

-- Verify the new pizza and all its toppings appear correctly
SELECT
    pn.pizza_name,
    STRING_AGG(pt.topping_name, ', ' ORDER BY pt.topping_name) AS ingredients
FROM pizza_recipes AS pr
CROSS JOIN LATERAL UNNEST(string_to_array(pr.toppings, ',')) AS t
INNER JOIN pizza_toppings AS pt ON TRIM(t)::INT = pt.topping_id
INNER JOIN pizza_names    AS pn ON pr.pizza_id  = pn.pizza_id
WHERE pn.pizza_name = 'Supreme'
GROUP BY pn.pizza_name;
```

**Approach:**
- Only two `INSERT` statements are needed — `pizza_names` for the name and ID, `pizza_recipes` for the topping list
- No `ALTER TABLE` required anywhere — the existing schema already supports new pizzas by design
- `customer_orders`, `runner_orders`, and `pizza_toppings` are completely unaffected
- The verification query re-uses the `UNNEST` approach to confirm all 12 toppings are mapped correctly

**Result:**
| pizza_name | ingredients |
|---|---|
| Supreme | Bacon, BBQ Sauce, Beef, Cheese, Chicken, Mushrooms, Onions, Pepperoni, Peppers, Salami, Tomato Sauce, Tomatoes |

- The Supreme pizza uses all 12 available toppings — no new toppings need to be added to `pizza_toppings` either.

---

*Case Study #2 Complete ✅ — Built with PostgreSQL*
