{% test vietnam_coordinate_range(model, latitude_column, longitude_column) %}
select *
from {{ model }}
where {{ latitude_column }} is null
   or {{ longitude_column }} is null
   or {{ latitude_column }} < 8.0
   or {{ latitude_column }} > 23.5
   or {{ longitude_column }} < 102.0
   or {{ longitude_column }} > 110.5
{% endtest %}
