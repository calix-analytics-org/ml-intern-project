{{ config(materialized='view') }}

with

source as(

    select * from {{ source('DBT_NMATHUR', 'ACTIVITY_SCHEMA') }}

)

select * from source where ts > '2021-12-31'