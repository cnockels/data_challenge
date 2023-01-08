with
    qryWebEvents as (
        select * from {{ ref('stg_web_events') }}
    ),
    qryOrders as (
        select * from {{ ref('orders') }}
    ),
    qryProducts as (
        select * from {{ ref('products') }}
    ),

    qryJoinWids as 
    (
        select
            {{ generateNumericalHash
            ([
                'web_event_wid',
                'order_wid',
                'product_wid'
            ]) 
            }} as web_event_fact_wid,
            qryWebEvents.web_event_wid,
            qryWebEvents.web_event_date,
            qryOrders.order_wid,
            qryProducts.product_wid
        from 
            qryWebEvents left join 
            qryOrders on 
                qryWebEvents.web_event_order_id = qryOrders.order_id left join
            qryProducts on 
                qryWebEvents.web_event_product_id = qryProducts.product_id
    )

select * from qryJoinWids