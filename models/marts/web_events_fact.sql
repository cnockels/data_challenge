with
    qryWebEvents as (
        select * from {{ ref('int_web_session_firsts') }}
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
                'qryWebEvents.web_event_wid',
                'qryOrders.order_wid',
                'qryProducts.product_wid'
            ]) 
            }} as web_event_fact_wid,
            qryWebEvents.web_event_wid,
            qryWebEvents.web_session_wid,
            qryWebEvents.web_event_date,
            qryWebEvents.web_event_time,
            qryOrders.order_wid,
            qryProducts.product_wid
        from 
            qryWebEvents left join 
            qryOrders on 
                qryWebEvents.web_event_order_id = qryOrders.order_line_id left join
            qryProducts on 
                qryWebEvents.web_event_product_id = qryProducts.product_variant_id
    )

select * from qryJoinWids