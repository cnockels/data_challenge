with
    qrySource as (
        select * from {{ ref('stg_web_events') }}
    ),

    qryFinal as 
    (
        select  
            web_event_wid,	
            web_event_id,		
            web_event_cookie_id,		
            web_event_customer_id,		
            web_event_name,		
            web_event_url,		
            web_event_utm_campaign,		
            web_event_utm_medium,	
            web_event_utm_source
        from    
            qrySource
    )

select * from qryFinal