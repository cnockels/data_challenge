with
    qryOrders as (
        select * from {{ ref('stg_orders') }}
    ),
    qryProducts as (
        select * from {{ ref('stg_products') }}
    ),

    qryJoinWids as 
    (
        select
            {{ generateNumericalHash
            ([
                'order_wid',
                'product_wid'
            ]) 
            }} as order_fact_wid,
            qryOrders.order_wid,
            qryProducts.product_wid,
            qryOrders.order_created_date as order_date,
            qryOrders.order_created_time as order_time,
            qryOrders.order_total,
            qryOrders.order_subtotal,
            qryOrders.order_line_item_quantity,
            qryOrders.order_line_item_price,
            qryOrders.order_line_item_total_discount,
            qryOrders.order_line_item_price * qryOrders.order_line_item_quantity - qryOrders.order_line_item_total_discount as order_line_item_revenue
        from 
            qryOrders left join 
            qryProducts on
                qryOrders.order_product_variant_id = qryProducts.product_variant_id
    )

select * from qryJoinWids