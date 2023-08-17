{{ config(materialized='view') }}

with 

dataset_cte as (
    {{dbt_activity_schema.dataset(
        activity_stream = ref("activities_2022_23"),

        primary_activity = dbt_activity_schema.activity(
            dbt_activity_schema.all_ever(), "TextComment_CC"),

        appended_activities = [
          dbt_activity_schema.activity(
              dbt_activity_schema.aggregate_in_between(), "gs_deployment"),
        ] 
    ) }}
),

val as (

    select ts, customer, feature_json['cumulative_value'] as Deps from {{ source('DBT_NMATHUR', 'ACTIVITY_SCHEMA') }} where activity='gs_deployment' order by ts asc

),

inter as(
    select 
    DISTINCT d.ACTIVITY, d.TS, d.CUSTOMER, d.ACTIVITY_REPEATED_AT, d.AGGREGATE_IN_BETWEEN_GS_DEPLOYMENT_ACTIVITY as COUNT_IN_BETWEEN_GS_DEPLOYMENT_ACTIVITY, c.Deps  
    from dataset_cte d
    left join val c
    on c.ts >= d.ts and c.ts <= d.activity_repeated_at and d.customer = c.customer
    order by d.ts, d.activity_repeated_at asc
),

final as (

    select * from inter 
    where Deps is not null and COUNT_IN_BETWEEN_GS_DEPLOYMENT_ACTIVITY > 0 
    QUALIFY Deps = MAX(Deps) over (partition by customer, ts)
    order by ts, activity_repeated_at
)


select * from final