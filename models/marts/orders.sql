with
    qryOrders as (
        select * from {{ ref('stg_orders') }}
    ),

    qryPrepared as 
    (
        select
            order_wid,		
            order_id,		
            order_line_id,
            order_last_updated_date,
            order_last_updated_time
        from
            qryOrders		
    )

select * from qryPrepared