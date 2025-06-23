# --highest revenue-generating location

SELECT 
  location_name, 
  total_revenue
FROM 
  location_metrics
ORDER BY 
  total_revenue DESC
LIMIT 1;

