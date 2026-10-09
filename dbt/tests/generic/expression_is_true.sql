{# Fails for every row where the SQL expression is false. Same idea as dbt_utils'
   version, written here so the project needs no packages (no `dbt deps`). #}
{% test expression_is_true(model, expression, column_name=None) %}
select *
from {{ model }}
where not ({{ expression }})
{% endtest %}
