with
    qrySource as (
        select * except(line_items) from {{ source('source_tables', 'orders') }},
        unnest(line_items) as line_items
        qualify updated_at = max(updated_at) over (partition by _id)        
    ),

    qryRenamed as 
    (
    	select distinct
            {{ generateNumericalHash
            ([
                'line_id',
                'product_id'                
            ]) 
            }} as order_wid,
            cast(_id as string) as order_id,
            cast(line_id as string) as order_line_id,
    	    cast(product_id as string) as order_product_id,
    	    cast(variant_id as string) as order_product_variant_id,
            cast(total as numeric) as order_total,
            cast(subtotal as numeric) as order_subtotal,
    	    cast(price as numeric) as order_line_item_price,
    	    cast(quantity as integer) as order_line_item_quantity,
    	    cast(line_total_discount as numeric) as order_line_item_total_discount,
    	    cast(created_at as date) as order_created_date,
            cast(created_at as timestamp) as order_created_time,
            cast(updated_at as date) as order_last_updated_date,
            cast(updated_at as timestamp) as order_last_updated_time,            
            product_id is null as order_has_no_product_id
    	from
            qrySource
    ),

    qryDeduped as 
    (
        select *, order_id is not null as order_has_duplicate_line
        from qryRenamed
        qualify count(order_line_id) over (partition by order_line_id) > 1
    ),

    qryFinal as 
    (
        select distinct
            qryRenamed.*,
            qryDeduped.order_has_duplicate_line
        from 
            qryRenamed left join
            qryDeduped on 
                qryRenamed.order_id = qryDeduped.order_id
    )

select * from qryFinal
