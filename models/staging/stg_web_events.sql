with
    qrySource as (
        select * from {{ source('source_tables', 'web_events') }}
    ),

    qryRenamed as 
    (
        select 
            {{ generateNumericalHash(['_id']) }} as web_event_wid,
            _id as web_event_id,
            trim(cast(cookie_id as string), '\" ') as web_event_cookie_id,
            if(customer_id = 'NaN', cast(null as string), customer_id) as web_event_customer_id,
            event_name as web_event_name,
            event_url as web_event_url,
            cast(timestamp as date) as web_event_date,
            timestamp as web_event_time,
            utm_campaign as web_event_utm_campaign,
            utm_medium as web_event_utm_medium,
            utm_source as web_event_utm_source,
            json_extract_scalar(event_properties, '$.order_id') as web_event_order_id,
            json_extract_scalar(event_properties, '$.product_id') as web_event_product_id
        from 
            qrySource
    )

select * from qryRenamed