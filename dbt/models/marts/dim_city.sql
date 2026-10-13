select city_id as city_key, city_name, region
from {{ ref('cities') }}
