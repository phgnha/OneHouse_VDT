{% test not_empty(model) %}
select *
from (
    select count(*) as row_count
    from {{ model }}
)
where row_count = 0
{% endtest %}
