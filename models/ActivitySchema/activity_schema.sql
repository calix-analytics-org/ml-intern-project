{{ config(materialized='table') }}


with 

calix_community as(

    select * from {{ref('calix_community')}}

),

select * from calix_community