with 
    SourceOrders as (
        select * from `data-recruiting.ae_data_challenge_v1.orders`, 
        unnest(line_items) as line_items 
        qualify updated_at = max(updated_at) over (partition by _id)
    ),
    SourceProducts as (
        select * from `data-recruiting.ae_data_challenge_v1.products` 
        qualify updated_at = max(updated_at) over (partition by _id)
    ),  
    
    qryProducts as 
    (
        select 
            Products.* except(variants),
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

    qryJoin as 
    (
        select 
    		SourceOrders._id, 
    		qryProducts.category,
    		qryProducts.product_size,
        from 
    		SourceOrders left join
    		qryProducts on
    			SourceOrders.variant_id = qryProducts.product_variant_id
    ),  

    qryKingSheets as 
    (
        select
            concat(round(sum(case when category = 'Sheet Sets' and product_size = 'King' then 1.0 end) / count(*) * 100, 2), ' %') as king_sheet_order_percent
        from
            qryJoin
    )

select * from qryKingSheets