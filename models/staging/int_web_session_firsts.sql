{{ config(materialized='table') }}
with
    qrySessions as (
        select * from {{ ref('int_web_event_sessions') }}
    ),

    qrySessionUser as 
    (
        select 
            *,                        
            if(web_event_time = min(web_event_time) over (partition by web_session_wid), web_event_customer_id, null) as web_session_user
        from 
            qrySessions
        where
            web_event_customer_id is not null
    ),

    qrySessionFirstEvent as 
    (
        select 
            qrySessions.* except(session_number),
            cast(qrySessions.web_session_start_time as date) as web_session_date,
            ifnull(max(qrySessionUser.web_session_user) over (partition by qrySessions.web_session_wid), qrySessions.web_event_cookie_id) as web_session_user,
            rank() over (partition by qrySessions.web_session_wid order by qrySessions.web_event_time asc) as web_event_sequence_number
        from
            qrySessions left join
            qrySessionUser on
                qrySessions.web_event_id = qrySessionUser.web_event_id
    ),

    qryFirstMedium as 
    (
        select 
            web_session_wid, 
            min(web_event_time) as first_medium_event_time
        from
            (
                select *
                from qrySessionFirstEvent
                qualify
                    min(web_event_utm_medium) over (partition by web_session_wid order by web_event_time desc rows unbounded preceding) is not null
            ) 
        group by 
            web_session_wid
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

    qryFirstCampaign as 
    (
        select 
            web_session_wid, 
            min(web_event_time) as first_campaign_event_time
        from
            (
                select *
                from qrySessionFirstEvent
                qualify
                    min(web_event_utm_campaign) over (partition by web_session_wid order by web_event_time desc rows unbounded preceding) is not null
            ) 
        group by 
            web_session_wid
    ),

    qryAddSessionFields as 
    (
        select distinct
            qrySessionFirstEvent.*,
            if(qrySessionFirstEvent.web_event_time = qryFirstMedium.first_medium_event_time, web_event_utm_medium, null) as session_medium,
            if(qrySessionFirstEvent.web_event_time = qryFirstSource.first_source_event_time, web_event_utm_source, null) as session_source,
            if(qrySessionFirstEvent.web_event_time = qryFirstCampaign.first_campaign_event_time, web_event_utm_campaign, null) as session_campaign
        from
            qrySessionFirstEvent left join
            qryFirstMedium on 
                qrySessionFirstEvent.web_session_wid = qryFirstMedium.web_session_wid left join
            qryFirstSource on 
                qrySessionFirstEvent.web_session_wid = qryFirstSource.web_session_wid left join
            qryFirstCampaign on 
                qrySessionFirstEvent.web_session_wid = qryFirstCampaign.web_session_wid
    )

select * from qryAddSessionFields