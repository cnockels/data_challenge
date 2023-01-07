{{ config(materialized='table') }}
with
    qryWebEvents as (
        select * from {{ ref('stg_web_events') }}
    ),

    qryCookieEvents as 
    (
        select 
            web_event_id,
            web_event_cookie_id,
            web_event_time,
            lead(web_event_time, 1) over 
            (
                partition by web_event_cookie_id
                order by web_event_time asc 
            ) as cookie_next_web_event_time
        from 
            qryWebEvents
    ),

    qryEventTimeGap as 
    (
        select
            web_event_id,
            web_event_cookie_id,
            web_event_time,
            cookie_next_web_event_time,
            round(timestamp_diff(cookie_next_web_event_time, web_event_time, second) / 60, 2) as cookie_time_gap_between_events
        from 
            qryCookieEvents
    ),

    qrySessionTimes as 
    (        
        select
            *,
            lag(cookie_time_gap_between_events, 1) over 
            (
                partition by web_event_cookie_id
                order by web_event_time asc 
            ) as cookie_next_session_flag
        from
            qryEventTimeGap
    ),

    qryNextSession as 
    (
        select
            *,
            case 
                when web_event_time = min(web_event_time) over (partition by web_event_cookie_id) then 
                    web_event_time 
                when cookie_next_session_flag > 30  then 
                        cookie_next_web_event_time 
            end as cookie_session_start_time,
            if(cookie_next_session_flag > 30, web_event_time, null) as cookie_previous_session_end_time
        from
            qrySessionTimes
    ),

    qrySessionNumber as 
    (
        select 
            web_event_id,
            web_event_cookie_id,
            web_event_time,
            cookie_next_web_event_time,
            cookie_session_start_time,
            cookie_previous_session_end_time,
            cookie_time_gap_between_events,     
            dense_rank() over (partition by web_event_cookie_id order by cookie_previous_session_end_time asc) as session_number
        from 
            qryNextSession       
    ),

    qrySessionWid as 
    (
        select
            qryWebEvents.*,
            {{ generateNumericalHash
            ([
                'qrySessionNumber.web_event_cookie_id', 
                'qrySessionNumber.session_number'
            ]) 
            }} as web_session_wid,
            qrySessionNumber.session_number,
            max(qrySessionNumber.cookie_session_start_time) over 
            (
                partition by 
                    qrySessionNumber.web_event_cookie_id,
                    qrySessionNumber.session_number
            ) as web_session_start_time
        from
            qryWebEvents left join
            qrySessionNumber on 
                qryWebEvents.web_event_id = qrySessionNumber.web_event_id
    )

select * from qrySessionWid