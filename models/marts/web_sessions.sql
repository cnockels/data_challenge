with
    qrySource as (
        select * from {{ ref('int_web_session_firsts') }}
    ),

    qryLandingPage as 
    (
        select 
            web_session_wid,
            web_session_start_time,
            web_session_date,
            web_session_user,
            max(session_medium) over (partition by web_session_wid) as web_session_medium,
            max(session_source) over (partition by web_session_wid) as web_session_source,
            max(session_campaign) over (partition by web_session_wid) as web_session_campaign,
            if(web_event_sequence_number = 1, web_event_url, null) as session_landing_page_url
        from
            qrySource
    ),

    qryFinal as 
    (
        select distinct
            * except(session_landing_page_url),
            max(session_landing_page_url) over (partition by web_session_wid) as web_session_landing_page_url
        from    
            qryLandingPage
    )

select * from qryFinal