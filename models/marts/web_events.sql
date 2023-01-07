with
    qryFirstEvents as (
        select * from {{ ref('int_web_event_first_events') }}
    ),

    qryFinal as 
    (
        select  
            * except
            (
                session_medium,
                session_source,
                session_campaign,
                web_event_sequence_number,
                web_event_product_id,
                web_event_order_id                
            ),
            if(web_event_sequence_number = 1, web_event_url, null) as web_session_landing_page_url,
            max(session_medium) over (partition by web_session_wid) as web_session_medium,
            max(session_source) over (partition by web_session_wid) as web_session_source,
            max(session_campaign) over (partition by web_session_wid) as web_session_campaign
        from    
            qryFirstEvents
    )

select * from qryFinal