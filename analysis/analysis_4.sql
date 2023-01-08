with
    SourceWebEvents as (
        select * from `data-recruiting.ae_data_challenge_v1.web_events`
    ),
    qryProducts as (
        select * from `data-recruiting.ae_data_challenge_v1.products` 
        qualify updated_at = max(updated_at) over (partition by _id)
    ),

    qryWebEvents as 
    (
        select 
            _id as web_event_id,
            trim(cast(cookie_id as string), '\" ') as web_event_cookie_id,
            if(customer_id = 'NaN', cast(null as string), customer_id) as web_event_customer_id,
            event_name as web_event_name,
            event_url as web_event_url,
            timestamp as web_event_time,
            json_extract_scalar(event_properties, '$.product_id') as web_event_product_id
        from 
            SourceWebEvents
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
            qryWebEvents.web_event_id,
            qryWebEvents.web_event_name,
            qryWebEvents.web_event_product_id,
            FARM_FINGERPRINT(coalesce(cast(qrySessionNumber.web_event_cookie_id as STRING), '') || '-' || coalesce(cast('qrySessionNumber.session_number' as STRING), '')) as web_session_wid,
        from
            qryWebEvents left join
            qrySessionNumber on 
                qryWebEvents.web_event_id = qrySessionNumber.web_event_id
    ),

    qryAllSessions as 
    (
        select distinct
            web_session_wid,
            web_event_name
        from 
            qrySessionWid
    ),

    qryProductSessions as 
    (
        select distinct
            qrySessionWid.web_session_wid,
            qrySessionWid.web_event_name
        from
            qrySessionWid left join 
            qryProducts on
                qrySessionWid.web_event_product_id = cast(qryProducts._id as string)
        where 
            qrySessionWid.web_event_name = 'product_added' and
            qryProducts.title = 'Plush Bath Towel Set'
    ),

    qryConversionRate as
    (
        select 
            concat(round(sum(case when qryAllSessions.web_event_name = 'order_completed' then 1.0 end) / count(distinct qryAllSessions.web_session_wid) * 100, 2), ' %') as conversion_rate
        from 
            qryAllSessions join
            qryProductSessions on 
                qryAllSessions.web_session_wid = qryProductSessions.web_session_wid
    )

select * from qryConversionRate