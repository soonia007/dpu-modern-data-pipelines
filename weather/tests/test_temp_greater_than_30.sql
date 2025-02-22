select temp
from {{ ref ('temp_greater_than_30')}}
where temp < 35