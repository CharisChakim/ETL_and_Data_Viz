SELECT 
    state,
    key_account_manager,
    membership_plan,
    COUNT(*) AS membership_count
FROM {{ref ('fct_table')}}
GROUP BY 1,2,3
ORDER BY 1,2,3