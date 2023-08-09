{{ config(materialized='view') }}

with

source as(

    select * from {{ source('DBT_NMATHUR', 'ACTIVITIES_2022_23') }}

),

deps as (

    select customer, max(feature_json['num_subscribers']) as num_subscribers, max(feature_json['cumulative_value']) as max_deps, max_deps/num_subscribers as ratio
    from source
    where activity = 'gs_deployment'
    group by customer  

)

select * from deps where max_deps > 5000 order by ratio desc limit 5