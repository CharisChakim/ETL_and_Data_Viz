SELECT 
    EXTRACT(YEAR FROM creation_date) AS year,
    SUM(CASE WHEN churn_date <> '' THEN 1 ELSE 0 END) AS churned_memberships,
    COUNT(membership_id) AS total_memberships,
    SUM(CASE WHEN churn_date <> '' THEN 1 ELSE 0 END)::float / COUNT(*)  AS churn_rate
FROM {{ref ('fct_table')}}
GROUP BY 1
ORDER BY 1

