with
    WebEvents as (
        select * from `data-recruiting.ae_data_challenge_v1.web_events`
    ),
    qryOrders as (
        select * from `data-recruiting.ae_data_challenge_v1.orders`,
        unnest(line_items) as line_items
        qualify updated_at = max(updated_at) over (partition by _id)
    ),

    qryWebEvents as 
    (
        select 
            _id as web_event_id,
            trim(cast(cookie_id as string), '\" ') as web_event_cookie_id,
            event_name as web_event_name,
            event_url as web_event_url,
            timestamp as web_event_time,
            json_extract_scalar(event_properties, '$.order_id') as web_event_order_id,
            utm_source as web_event_utm_source
        from 
            WebEvents
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
            FARM_FINGERPRINT(coalesce(cast(qrySessionNumber.web_event_cookie_id as STRING), '') || '-' || coalesce(cast('qrySessionNumber.session_number' as STRING), '')) as web_session_wid,
        from
            qryWebEvents left join
            qrySessionNumber on 
                qryWebEvents.web_event_id = qrySessionNumber.web_event_id
    ),

    qrySessionFirstEvent as 
    (
        select 
            qrySessionWid.*,
            rank() over (partition by qrySessionWid.web_session_wid order by qrySessionWid.web_event_time asc) as web_event_sequence_number
        from
            qrySessionWid 
    ),

    qryFirstSource as 
    (
        select 
            web_session_wid, 
            min(web_event_time) as first_source_event_time
        from
            (
                select *
                from qrySessionFirstEvent
                qualify
                    min(web_event_utm_source) over (partition by web_session_wid order by web_event_time desc rows unbounded preceding) is not null
            ) 
        group by 
            web_session_wid
    ),

    qryAddOrderRev as 
    (
        select
            qrySessionFirstEvent.*,
            if(qrySessionFirstEvent.web_event_time = qryFirstSource.first_source_event_time, web_event_utm_source, null) as session_source,
            if(web_event_order_id is not null, (qryOrders.price * qryOrders.quantity - qryOrders.line_total_discount), null) as order_gross_revenue
        from
            qrySessionFirstEvent left join
            qryFirstSource on 
                qrySessionFirstEvent.web_session_wid = qryFirstSource.web_session_wid left join 
            qryOrders on 
                qrySessionFirstEvent.web_event_order_id = qryOrders._id
    ),

    qryGrossRevenue as 
    (
        select 
            session_source,
            sum(order_gross_revenue) over (partition by web_session_wid) as session_gross_revenue
        from 
            qryAddOrderRev
    ),

    qryFinal as
    (
        select distinct
            session_source,
            sum(session_gross_revenue) over (partition by session_source) as session_source_gross_revenue
        from
            qryGrossRevenue
        where 
            session_gross_revenue is not null and 
            session_source is not null
        order by 
            session_source_gross_revenue desc
        limit 5
    )

select session_source from qryFinal