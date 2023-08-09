{{ config(materialized='view') }}


with

source as(

    select * from {{ source('DBT_NMATHUR', 'ACTIVITIES_2022_23') }}

),

acts as (    
    
    select customer, count(activity) as num_activities 
    from source 
    where activity != 'gs_deployment' 
    group by customer    

),

account as (
    
    SELECT ACCOUNT_ID, ACCOUNT_ORACLE_ID
    FROM {{ source('EDW_CE', 'ACCOUNT') }}

),

employees as (

    SELECT ACCOUNT_ID,
    COUNT(DISTINCT(CONTACT_ID)) as ACTIVITY_MEMBER_COUNT
    FROM {{ source('EDW_CE', 'COMMUNITY_ACTIVITY') }}
    WHERE CREATED_DATE BETWEEN '2022-01-01' AND GETDATE()
    GROUP BY ACCOUNT_ID

),

combined as(

    SELECT e.ACCOUNT_ID, A.ACCOUNT_ORACLE_ID, e.ACTIVITY_MEMBER_COUNT
    FROM employees e
    LEFT JOIN account A ON e.ACCOUNT_ID = A.ACCOUNT_ID
    
),

final as(

    select c.ACCOUNT_ORACLE_ID as customer, c.ACTIVITY_MEMBER_COUNT as num_employees, a.num_activities, a.num_activities/num_employees as ratio 
    FROM combined c
    left join acts a on a.customer = c.ACCOUNT_ORACLE_ID 
    where a.num_activities > 200 and num_employees > 10 and customer!=3174 and customer!=1888

)


select * from final where customer is not null and num_activities is not null order by ratio desc limit 5



