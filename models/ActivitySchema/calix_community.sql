{{ config(materialized='view', features=['subject']) }}

with

source as(

    select * from {{ source('EDW_CE', 'COMMUNITY_ACTIVITY') }}

),

account as(

    select ACCOUNT_ID, ACCOUNT_ORACLE_ID from {{ source('EDW_CE', 'ACCOUNT') }}
    
),

final as (

  select 
    concat('CC_', row_number() OVER (ORDER BY s.ID)) as activity_id,
    to_date(s.CREATED_DATE) as ts,

    CONCAT(s.ENTITY_TYPE, '_CC') as activity,

    a.ACCOUNT_ORACLE_ID as customer,

    s.TOPIC_NAME as subject

  from source s 

  LEFT JOIN account a ON s.ACCOUNT_ID = a.ACCOUNT_ID
  
)

select * from {{ make_activity('final') }} WHERE customer IS NOT NULL