{# Creates the two activity occurrence columns: activity_occurrence and activity_repeated_at  #}

{% macro activity_occurrence() %}
    {# row_number() over (
        partition by coalesce (
            {{ safe_cast("customer", type_string()) }}, NULL
            ) order by ts asc ) as activity_occurrence,
    lead(ts) over (
        partition by coalesce (
            {{ safe_cast("customer", type_string()) }}, NULL
        ) order by ts asc) as activity_repeated_at
    #}
    row_number() over (partition by activity, customer order by ts asc) as activity_occurrence,
    lead(ts) over (partition by activity, customer order by ts asc) as activity_repeated_at
{% endmacro %}