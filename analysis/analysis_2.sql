with 
    SourceProducts as (
        select * from `data-recruiting.ae_data_challenge_v1.products` 
        qualify updated_at = max(updated_at) over (partition by _id)
    ),
    SourceOrders as (
        select * from `data-recruiting.ae_data_challenge_v1.orders`,
        unnest(line_items) as line_items
        qualify updated_at = max(updated_at) over (partition by _id)
    ),

    qryProducts as 
    (	
        select 
            SourceProducts.* except(variants),
            unnested.variant_id as product_variant_id,
            unnested.title as variant_title,
            unnested.sku as product_sku,
            unnested.created_at as variant_created_at,
            unnested.updated_at as variant_updated_at,
            unnested.option1 as product_style,
            unnested.option2 as product_size
        from 
            SourceProducts cross join
            unnest(variants) as unnested
        qualify unnested.updated_at = max(unnested.updated_at) over (partition by unnested.variant_id)
    ),

    qryLineRevenue as 
    (
    	select 
    		SourceOrders._id,
        	SourceOrders.price * SourceOrders.quantity - SourceOrders.line_total_discount as order_line_gross_revenue,
    		qryProducts.product_sku
    	from 
    		SourceOrders left join
    		qryProducts on
    			SourceOrders.variant_id = qryProducts.product_variant_id
    ),

    qrySkuTotals as 
    (
    	select distinct
        	product_sku,
    		sum(order_line_gross_revenue) over (partition by product_sku) as total_gross_revenue
    	from
            qryLineRevenue
    )

select * from qrySkuTotals
qualify rank() over (order by total_gross_revenue desc) = 1