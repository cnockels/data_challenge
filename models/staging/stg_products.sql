with
    qrySource as (
        select * from {{ source('source_tables', 'products') }}
        qualify updated_at = max(updated_at) over (partition by _id)
    ),
    
    qryUnnest as 
    (
        select 
            qrySource.* except(variants),
            unnested.variant_id as variant_id,
            unnested.title as variant_title,
            unnested.sku as product_sku,
            unnested.created_at as variant_created_at,
            unnested.updated_at as variant_updated_at,
            unnested.option1 as product_style,
            unnested.option2 as product_size
        from 
            qrySource cross join
            unnest(variants) as unnested
        qualify unnested.updated_at = max(unnested.updated_at) over (partition by unnested.variant_id)
    ),

    qryProducts as 
    (
    	select distinct
            {{ generateNumericalHash(['variant_id']) }} as product_wid,
    		cast(_id as string) as product_id,
            title as product_title,
            if(trim(category) = '', cast(null as string), trim(category)) as product_category,
            cast(created_at as date) as product_created_date,
            created_at as product_created_time,
            cast(updated_at as date) as product_last_updated_date,
            updated_at as product_last_updated_time,
    		cast(variant_id as string) as product_variant_id,
            variant_title as product_variant_title,
            product_sku,
            product_style,
            product_size
    	from
    		qryUnnest
    )

select * from qryProducts