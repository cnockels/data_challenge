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
            SourceProducts.* except(variants),
            unnested.variant_id as product_variant_id,
            unnested.title as variant_title,
            unnested.sku as product_sku,
            unnested.created_at as variant_created_at,
            unnested.updated_at as variant_updated_at,
            unnested.option1,
            unnested.option2 
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
    		qryProducts.variant_title,
            qryProducts.option1,
            qryProducts.option2 
            
        from 
    		SourceOrders left join
    		qryProducts on
    			SourceOrders.variant_id = qryProducts.product_variant_id
    ),  

    qryKingSheets as 
    (
        select
            concat(round(sum
                (
                    case 
                        when category = 'Sheet Sets' and 
                        (variant_title like('%King%') or variant_title like('%King%')) then 
                            1.0 
                    end
                ) / count(*) * 100, 2), ' %') as king_sheet_order_percent
        from
            qryJoin
    )

select * from qryKingSheets