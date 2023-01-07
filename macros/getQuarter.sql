{% macro getQuarter(in_month_field) -%}
    CASE
        WHEN {{ in_month_field }} BETWEEN 1 AND 3 
            THEN 1
        WHEN {{ in_month_field }} BETWEEN 4 AND 6 
            THEN 2
        WHEN {{ in_month_field }} BETWEEN 7 AND 9 
            THEN 3
        ELSE
            4
    END
{%- endmacro %}