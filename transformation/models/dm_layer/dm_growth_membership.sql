select
    extract(year from creation_date) as year,
    count(membership_id) as member
from 
    {{ref ('fct_table')}}
group by 1
order by 1