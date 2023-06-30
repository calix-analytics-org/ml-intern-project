{{ config(materialized='table') }}


with 

calix_community as(

    select * from {{ref('calix_community')}}

),


deps as(

    select * from {{ref('gs_deployments')}}

),

final as(

    select * from calix_community
    UNION ALL
    select * from deps
    
)

select * from final order by ts asc