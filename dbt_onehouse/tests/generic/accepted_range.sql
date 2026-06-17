{% test accepted_range(model, column_name, min_value=None, max_value=None) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and (
    1 = 0
    {% if min_value is not none %}
      or {{ column_name }} < {{ min_value }}
    {% endif %}
    {% if max_value is not none %}
      or {{ column_name }} > {{ max_value }}
    {% endif %}
  )
{% endtest %}
