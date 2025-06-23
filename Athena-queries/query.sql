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