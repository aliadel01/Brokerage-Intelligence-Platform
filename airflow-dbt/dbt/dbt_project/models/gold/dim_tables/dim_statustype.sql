{{ config(
    materialized='table',
    post_hook=apply_classification_tags(
        relation='gold.dim_statustype',
      tags={
        'status_type_sk': 'internal',
        'status_code': 'public',
        'status_name': 'public'
      }
    )
) }}
with final as (
    select
        {{ gen_surrogate_key(['status_id']) }} as status_type_sk,
        status_id                          as status_code,
        coalesce(status_name, 'Unknown')   as status_name
    from {{ ref('silver_status_type') }}
),

unknown as (
    select
        '-1'   as status_type_sk,
        'UNK'  as status_code,
        'Unknown' as status_name
)

select * from final
union all
select * from unknown
