select
    {{ surrogate_key(['employee_id']) }} as broker_sk,
    employee_id,
    manager_id       as manager_employee_id,
    first_name,
    last_name,
    middle_initial,
    job_code,
    branch           as branch_name,
    office           as office_code,
    phone            as phone_number
from {{ ref('silver_hr') }}