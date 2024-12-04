with source as (
    select * from bronze
)
select
    "time" as 'time',
    position as position,
    neural_data as neural_data,
    list_any_value(neural_data) is null as neural_data_is_empty,
    lap as lap,
    lap_2 as lap_cumulative,
    pump as pump,
    file_name
from source
where not neural_data_is_empty