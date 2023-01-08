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
            lower(unnested.option1) as option1,
            lower(unnested.option2) as option2
        from 
            qrySource cross join
            unnest(variants) as unnested
        qualify unnested.updated_at = max(unnested.updated_at) over (partition by unnested.variant_id)
    ),

    qryProductSize as 
    (
    	select distinct
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
            option1,
            option2,
            case
                when option1 like('%king%') or 
                    option1 like('%queen%') or 
                    option1 like('%twin%') or 
                    option1 like('%full%') or 
                    option1 like('%standard%') or
                    option1 like('%one size%') or
                    option1 like('%unisex%') or
                    option1 like('%0x%') or
                    option1 like('%euro%') or
                    option1 like('% x %') or 
                    option1 like('%small%') or 
                    option1 like('%medium%') or 
                    option1 like('%large%') then
                        option1
                when option2 like('%king%') or 
                    option2 like('%queen%') or 
                    option2 like('%twin%') or 
                    option2 like('%full%') or 
                    option2 like('%standard%') or
                    option2 like('%0x%') or
                    option2 like('%euro%') or
                    option2 like('%one size%') or
                    option2 like('%unisex%') or
                    option2 like('% x %') or 
                    option2 like('%small%') or 
                    option2 like('%medium%') or 
                    option2 like('%large%') then
                        option2
            end as product_size
    	from
    		qryUnnest
    ),

    qryAddStyle as 
    (
        select 
            * except(option1, option2), 
            case
                when option1 not like('%$%') and 
                (product_size <> option1 or 
                product_size is null) then
                    option1
                when option1 not like('%$%') and 
                (product_size <> option2 or
                product_size is null) then 
                    option2
                when option1 not like('%$%') and
                (product_size <> option1 and 
                product_size <> option2) then
                    product_variant_title
            end as product_style
        from 
            qryProductSize
    )

select * from qryAddStyle
