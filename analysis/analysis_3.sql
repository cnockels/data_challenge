with 
    qryOrders as (
        select * from `data-recruiting.ae_data_challenge_v1.orders`,
        unnest(line_items) as line_items
        qualify updated_at = max(updated_at) over (partition by _id)
    ),

    qryOrderDates as
    (
        select 
            cast(created_at as date) as order_date,
            round(avg(quantity), 4) as average_order_units
        from 
            qryOrders
        group by  
            order_date
        qualify rank() over (order by average_order_units desc) = 1
    )
    
select * from qryOrderDates