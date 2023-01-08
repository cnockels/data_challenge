with
    SourceWebEvents as (
        select * from `data-recruiting.ae_data_challenge_v1.web_events`
    ),

    qryWebEvents as 
    (
        select 
            _id as web_event_id,
            trim(cast(cookie_id as string), '\" ') as web_event_cookie_id,
            event_name as web_event_name,
            event_url as web_event_url,
            timestamp as web_event_time
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
            qryWebEvents.*,
            FARM_FINGERPRINT(coalesce(cast(qrySessionNumber.web_event_cookie_id as STRING), '') || '-' || coalesce(cast('qrySessionNumber.session_number' as STRING), '')) as web_session_wid,
        from
            qryWebEvents left join
            qrySessionNumber on 
                qryWebEvents.web_event_id = qrySessionNumber.web_event_id
    ),

    qryPageOrder as 
    (
    	select
    	    web_event_id,
            web_session_wid,
    	    web_event_url,
            web_event_time,
    		rank() over (partition by web_event_cookie_id, qrySessionWid.web_session_wid order by web_event_time asc) as session_page_order
    	from
    		qrySessionWid
    	where 
    		web_event_name = 'page' 
    ),  

    qryAfterCheckout as 
    (
    	select
            web_event_id,
            web_session_wid,
            session_page_order,
            case
                when qryPageOrder.web_event_url like ('%https://checkout.bollandbranch.com/%') then  
                    lag(qryPageOrder.web_event_url , 1) over 
                        (
                            partition by web_session_wid 
                            order by qryPageOrder.session_page_order asc 
                        ) 
            end as event_url_before_checkout,
            web_event_url,
    		case
                when qryPageOrder.web_event_url like ('%https://checkout.bollandbranch.com/%') then  
                    lead(qryPageOrder.web_event_url , 1) over 
                        (
                            partition by web_session_wid 
                            order by qryPageOrder.session_page_order asc 
                        ) 
            end as event_url_after_checkout_domain 
    	from 
    		qryPageOrder 
    ),

    qryUrlCount as 
    (
    	select distinct
            event_url_after_checkout_domain,
            count(*) over (partition by event_url_after_checkout_domain) as event_url_count
    	from
    		qryAfterCheckout
        where 
            web_event_url like('%https://checkout.bollandbranch.com/%') and
            event_url_before_checkout not like('%https://checkout.bollandbranch.com/%') and
            event_url_after_checkout_domain is not null
    ),

    qryFinal as
    (
        select  
            rank()over(order by event_url_count desc) as rank,
            event_url_after_checkout_domain,
            event_url_count
        from 
            qryUrlCount 
        order by 
            rank 
        limit 5
    )

select * from qryFinal