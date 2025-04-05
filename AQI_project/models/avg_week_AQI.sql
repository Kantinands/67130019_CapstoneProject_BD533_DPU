SELECT 
    DATE_TRUNC('week', timestamp) AS week_start,
    AVG(aqi) AS avg_aqi
FROM 
    public.aqi_data
GROUP BY 
    week_start
