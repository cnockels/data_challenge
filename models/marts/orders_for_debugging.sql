with
    qryOrders as (
        select * from {{ ref('stg_orders') }}
    ),

    qryDebug as 
    (
        select
            order_wid,		
            order_id,		
            order_line_id,							
            order_has_no_product_id,			
            order_has_duplicate_line
        from
            qryOrders		
    )

select * from qryDebug