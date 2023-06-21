{{ config(materialized='view', features=['subject']) }}

with

source as(

    select * from {{ source('EDW_CE', 'COMMUNITY_ACTIVITY') }}

),

final as (

  select 
    ID as activity_id,
    CREATED_DATE as ts,

    CONCAT(ENTITY_TYPE, '_CC') as activity,

    ACCOUNT_ID as customer,

    TOPIC_NAME as subject

  from source WHERE CREATED_DATE BETWEEN '2022-01-01' AND '2022-12-31' 
  
)

select * from {{ make_activity('final') }}