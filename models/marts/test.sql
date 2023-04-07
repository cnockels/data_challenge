with
    qryProducts as (
        select * from {{ ref('stg_products') }}
    )

select * from qryProducts
