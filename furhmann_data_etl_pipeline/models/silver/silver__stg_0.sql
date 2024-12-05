{{config(materialized='table')}}

with source as (
    select 
        "Time" as t,
        Position as position,
        neural_data,
        Lap as lap,
        Lap_2 as lap_cumulative,
        Pump as pump,
        file_name
    from bronze
)
select
    t,
    position,
    neural_data as neural_data,
    list_any_value(neural_data) is null as neural_data_is_empty,
    lap,
    lap_cumulative,
    pump,
    file_name
from source
where not neural_data_is_empty