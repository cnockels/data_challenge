with
    qryWebEvents as (
        select * from {{ ref('stg_web_events') }}
        where web_event_name = 'email_sign_up'
    ),

    qryRenamed as 
    (
        select
            web_event_wid as email_sign_up_event_wid,
            web_event_id as email_sign_up_event_id,
            web_event_cookie_id as email_sign_up_event_cookie_id,
            web_event_customer_id as email_sign_up_event_customer_id,
            web_event_date as email_sign_up_event_date,
            web_event_time as email_sign_up_event_time,
            web_event_utm_campaign as email_sign_up_event_utm_campaign,
            web_event_utm_medium as email_sign_up_event_utm_medium,
            web_event_utm_source as email_sign_up_event_utm_source
        from
            qryWebEvents
    )

select * from qryRenamed