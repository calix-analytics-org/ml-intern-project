{{ config(materialized='view') }}

with 

dataset_cte as (
    {{ dbt_activity_schema.dataset(
        activity_stream = ref("activities_2022_23"),

        primary_activity = dbt_activity_schema.activity(
            dbt_activity_schema.first_ever(), "TextComment_CC"),

        appended_activities = [
          dbt_activity_schema.activity(
              dbt_activity_schema.first_after(), "gs_deployment"),
        ]
    ) }}
)

select * from dataset_cte