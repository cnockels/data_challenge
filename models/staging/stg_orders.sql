with
    qrySource as (
        select * from {{ source('source_tables', 'orders') }},
        unnest(line_items) as line_items
    ),

    qryOrders as 
    (
    	select
    		line_id,
    		product_id,
    		variant_id,
    		price,
    		quantity,
    		line_total_discount,
    		_id as order_id,
    		created_at,
    	from
    		qrySource
    )

select * from qryOrders
