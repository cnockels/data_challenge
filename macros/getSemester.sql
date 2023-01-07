{% macro getSemester(in_month_field) -%}
    CASE
        WHEN {{ in_month_field }} BETWEEN 1 AND 6 
            THEN 1
        ELSE
            2
    END
{%- endmacro %}