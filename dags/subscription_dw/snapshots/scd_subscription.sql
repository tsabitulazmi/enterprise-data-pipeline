{% snapshot scd_subscription %}

{{
    config(
      target_schema='dbt_subscription',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

SELECT *
FROM {{ ref('raw_subscription') }}

{% endsnapshot %}