with source as (select * from {{ref('silver__stg_0')}}),
cte as (
select
    *,
    position - lag(position) over t_window as position_change,
    t - lag(t) over t_window as t_change
from source
window t_window as (
    partition by file_name
    order by t
)
)
select
    t,
    position,
    position_change/t_change as velocity,
    neural_data,
    lap,
    lap_cumulative,
    pump,
    file_name
from cte