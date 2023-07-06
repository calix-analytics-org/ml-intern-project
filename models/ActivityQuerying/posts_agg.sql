{{ config(materialized='view') }}

with 

dataset_cte as (
    {{ dbt_activity_schema.dataset(
        activity_stream = ref("activities_2022_23"),

        primary_activity = dbt_activity_schema.activity(
            dbt_activity_schema.all_ever(), "QuestionPost_CC"),

        appended_activities = [
          dbt_activity_schema.activity(
              dbt_activity_schema.aggregate_in_between(), "gs_deployment"),
        ]
    ) }}
),

final as(
    select 
    DISTINCT ACTIVITY, TS, CUSTOMER, ACTIVITY_REPEATED_AT, AGGREGATE_IN_BETWEEN_GS_DEPLOYMENT_ACTIVITY as COUNT_IN_BETWEEN_GS_DEPLOYMENT_ACTIVITY  
    from dataset_cte 
    order by ts, activity_repeated_at asc
)

select * from final