select
    {{ surrogate_key(['status_id']) }} as status_type_sk,
    status_id    as status_code,
    status_name
from {{ ref('silver_status_type') }}