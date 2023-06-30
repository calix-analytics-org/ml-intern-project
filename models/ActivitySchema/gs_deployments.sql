{{ config(materialized='view', features=['cumulative_value', 'num_subscribers']) }}

with

source as(

    select * from {{ source('EDW_CE', 'GIGASPIRE_DEPLOYMENTS_HISTORICAL') }}

),

inter as(

    SELECT
    SNAPSHOT_DATE,
    ORACLE_ID, 
    sum(GS_ALL_TIME_COUNT) as GS_CUMULATIVE,
    sum(GATEWAY_30_DAYS_COUNT) as GS_SUBSCRIBERS
    FROM
    source
    WHERE 
    (PREMISE_TYPE = 'GigaSpire' OR PREMISE_TYPE = 'GigaSpire - DTM')
    AND ORACLE_ID is not null
    GROUP BY SNAPSHOT_DATE, ORACLE_ID
    order by SNAPSHOT_DATE asc
    
),

inter2 as(

    select 
    SNAPSHOT_DATE, 
    ORACLE_ID, 
    GS_CUMULATIVE,
    GS_SUBSCRIBERS 
    from inter 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY GS_CUMULATIVE ORDER BY GS_CUMULATIVE) = 1 
    order by SNAPSHOT_DATE, ORACLE_ID asc

),

final as (

  select 
    concat('DEP_', row_number() OVER (ORDER BY ORACLE_ID)) as activity_id,
    SNAPSHOT_DATE as ts,

    'gs_deployment' as activity,

    ORACLE_ID as customer,

    GS_CUMULATIVE as cumulative_value,
    GS_SUBSCRIBERS as num_subscribers

  from inter2
  order by ts
  
)

select * from {{ make_activity('final') }} WHERE customer IS NOT NULL