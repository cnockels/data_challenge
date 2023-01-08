{% macro getTrimester(in_month_field) -%}
    CASE
        WHEN {{ in_month_field }} BETWEEN 1 AND 4 
            THEN 1
        WHEN {{ in_month_field }} BETWEEN 5 AND 8 
            THEN 2
        ELSE
            3
    END
{%- endmacro %}