from sqlalchemy import create_engine, text
import pandas as pd

def report_orders_by_city_and_period(engine):
    query = """
        SELECT c.city,
               CASE 
                   WHEN EXTRACT(HOUR FROM (o.hour)) BETWEEN 6 AND 13 THEN '6am-1pm'
                   WHEN EXTRACT(HOUR FROM (o.hour)) BETWEEN 14 AND 17 THEN '2pm-5m'
                   WHEN EXTRACT(HOUR FROM (o.hour)) BETWEEN 18 AND 21 THEN '6pm-9pm'
                   ELSE 'After 9pm'
               END AS period_of_day,
               COUNT(*) AS total_orders
        FROM orders o
        INNER JOIN restaurants r ON o.restaurant_id = r.id
        JOIN cities c ON r.city_id = c.id
        GROUP BY c.city, period_of_day
        ORDER BY total_orders DESC;
    """
    return pd.read_sql_query(query, engine)

def report_meal_type_ratio_per_city(engine):
    query = """
            SELECT c.city, mt.meal_type, COUNT(*) AS order_count,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY c.city), 2) AS percentage
            FROM orders o
            INNER JOIN restaurants r ON o.restaurant_id = r.id
            INNER JOIN cities c ON r.city_id = c.id
            INNER JOIN order_details od ON o.id = od.order_id
            INNER JOIN meals m ON od.meal_id = m.id
            INNER JOIN meal_types mt ON m.meal_type_id = mt.id
            GROUP BY c.city, mt.meal_type
            ORDER BY c.city, percentage DESC;
        """
    return pd.read_sql_query(query, engine)

def report_order_ratio_in_italian_cities(engine):
    query = """
            WITH italian_restaurant_counts AS (
                SELECT c.id AS city_id, c.city, COUNT(*) AS italian_restaurant_count
                FROM restaurants r
                INNER JOIN restaurant_types rt ON r.restaurant_type_id = rt.id
                INNER JOIN cities c ON r.city_id = c.id
                WHERE rt.restaurant_type = 'Italian'
                GROUP BY c.id, c.city
            ),
            max_italian_cities AS (
                SELECT city_id, city, italian_restaurant_count
                FROM italian_restaurant_counts
                WHERE italian_restaurant_count = (SELECT MAX(italian_restaurant_count) FROM italian_restaurant_counts)
            ),
            orders_in_max_italian_cities AS (
                SELECT c.city, COUNT(*) AS orders_in_city
                FROM orders o
                INNER JOIN restaurants r ON o.restaurant_id = r.id
                INNER JOIN cities c ON r.city_id = c.id
                WHERE r.city_id IN (SELECT city_id FROM max_italian_cities)
                GROUP BY c.city
            ),
            total_orders AS (
                SELECT COUNT(*) AS total_orders FROM orders
            )
            SELECT oimc.city, oimc.orders_in_city, t.total_orders,
                   ROUND(oimc.orders_in_city * 100.0 / t.total_orders, 2) AS order_percentage
            FROM orders_in_max_italian_cities oimc
            CROSS JOIN total_orders t;
        """
    return pd.read_sql_query(query, engine)


def report_cities_with_most_vegan_meals(engine):
    # Assumption: The cities offer the most vegan dishes as "cities have the most vegan meals"
    query = """
            SELECT c.city, COUNT(*) AS vegan_menu_count
            FROM meals m
            INNER JOIN meal_types mt ON m.meal_type_id = mt.id
            INNER JOIN restaurants r ON m.restaurant_id = r.id
            INNER JOIN cities c ON r.city_id = c.id
            WHERE m.meal_type_id = 1
            GROUP BY c.city
            ORDER BY vegan_menu_count DESC;
        """
    return pd.read_sql_query(query, engine)

def report_price_range_hot_cold_meals(engine):
    query = """
        SELECT hot_cold,
               MIN(price) AS min_price,
               MAX(price) AS max_price,
               AVG(price) AS avg_price
        FROM meals
        GROUP BY hot_cold;
    """
    return pd.read_sql_query(query, engine)

def report_sex_vs_serve_type(engine):
    query = """
        SELECT m.sex, st.serve_type, COUNT(*) AS order_count
        FROM orders o
        INNER JOIN members m ON o.member_id = m.id
        INNER JOIN order_details od ON o.id = od.order_id
        INNER JOIN meals ml ON od.meal_id = ml.id
        INNER JOIN serve_types st ON ml.serve_type_id = st.id
        GROUP BY m.sex, st.serve_type
        ORDER BY m.sex, st.serve_type;
    """
    return pd.read_sql_query(query, engine)


db_url = 'postgresql://zhangyuyuan@localhost:5432/restaurant'
engine = create_engine(db_url)
df1 = report_orders_by_city_and_period(engine)
df2 = report_meal_type_ratio_per_city(engine)
df3 = report_order_ratio_in_italian_cities(engine)
df4 = report_cities_with_most_vegan_meals(engine)
df5 = report_price_range_hot_cold_meals(engine)
df6 = report_sex_vs_serve_type(engine)
