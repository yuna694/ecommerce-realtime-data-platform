select product_id, product_name, category, list_price
from {{ ref('products') }}
