{% snapshot dim_product_snapshot %}

{{
    config(
      target_database='FSJ_PRD',
      target_schema='SILVER',
      alias='DIM_PRODUCT',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='last_modified_date'
    )
}}

select *
from {{ ref('stg_products') }}

{% endsnapshot %}