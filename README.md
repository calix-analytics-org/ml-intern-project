ML Intern Project

About: This project's purpose is to build the Activity Schema and use it to see whether Calix community activities impact GigaSpire Deployments.

BUGS:

There is only one main issue, and that is that there is a few lines in a single file in the dbt_activity_schema package that is not compliant with Snowflake (not sure if the same behavior exists for Postgres, etc.). 

In order to make it work, here are the replacements that need to be made to the new package file created by 'dbt deps':

File: dbt_packages/dbt_activity_schema/macros/utils/aggregations/_min_or_max.sql

Around line 30, replace:

{{ dbt.concat([
            dbt.safe_cast(qualified_ts_col, dbt.type_string()),
            dbt.safe_cast(qualified_col, dbt.type_string())
]) }}

with:

cast({{qualified_ts_col}} as varchar) || cast({{qualified_col}} as varchar)

AND

around line 40, replace:

{{ aggregation }}( {{ dbt.safe_cast(qualified_ts_col, dbt.type_string()) }} )

with:


{{ aggregation }}( cast({{qualified_ts_col}} as varchar) )


REMINDER: These replacements are only necessary if using Snowflake as database.


### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
