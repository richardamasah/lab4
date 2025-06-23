# --highest revenue-generating location

SELECT 
  location_name, 
  total_revenue
FROM 
  location_metrics
ORDER BY 
  total_revenue DESC
LIMIT 1;


--Identify top-spending users
SELECT 
  user_id, 
  first_name, 
  last_name, 
  user_total_spent
FROM 
  rental.user_metrics
ORDER BY 
  user_total_spent DESC
LIMIT 5;


-- daily revenue trends
SELECT 
  rental_date, 
  total_revenue
FROM 
  rental.daily_metrics
ORDER BY 
  rental_date;



SELECT 
  vehicle_type, 
  COUNT(*) AS total_rentals
FROM 
  rental.vehicle_metrics
ORDER BY 
  total_rentals DESC
LIMIT 1;