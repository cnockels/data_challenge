with
    WebEvents as (
        select * from `data-recruiting.ae_data_challenge_v1.web_events`
    ),

    qryWebEvents as 
    (
        select 
            _id as web_event_id,
            trim(cast(cookie_id as string), '\" ') as web_event_cookie_id,
            event_name as web_event_name,
            event_url as web_event_url,
            timestamp as web_event_time,
            utm_campaign as web_event_utm_campaign,
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

    qryFirstCampaign as 
    (
        select 
            web_session_wid, 
            min(web_event_time) as first_campaign_event_time
        from
            (
                select *
                from qrySessionFirstEvent
                qualify min(web_event_utm_campaign) over (partition by web_session_wid order by web_event_time desc rows unbounded preceding) is not null
            ) 
        group by 
            web_session_wid
    ),

    qryAddSessionFields as 
    (
        select
            qrySessionFirstEvent.*,
            if(qrySessionFirstEvent.web_event_time = qryFirstCampaign.first_campaign_event_time, web_event_utm_campaign, null) as session_campaign
        from
            qrySessionFirstEvent left join
            qryFirstCampaign on 
                qrySessionFirstEvent.web_session_wid = qryFirstCampaign.web_session_wid
    ),

    qryUserCounts as 
    (
        select 
            session_campaign,
            count(web_event_cookie_id) as user_count
        from 
            qryAddSessionFields
        where 
            session_campaign is not null
        group by
            session_campaign
    ),

    qryFinal as
    (
        select
            rank()over(order by user_count desc) as rank,
            session_campaign,
            user_count
        from
            qryUserCounts
        order by 
            rank 
        limit 5
    )

select * from qryFinal