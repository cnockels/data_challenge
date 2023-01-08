{% macro getOffsetYear(in_date_field, in_offset_year_start_month) -%}
    CASE
        WHEN EXTRACT(month FROM {{ in_date_field }}) >= {{ in_offset_year_start_month }} THEN EXTRACT(year FROM {{ in_date_field }}) + 1
    ELSE
        EXTRACT(year FROM {{ in_date_field }})
    END
{%- endmacro %}
