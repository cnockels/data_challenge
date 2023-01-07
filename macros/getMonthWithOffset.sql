{% macro getMonthWithOffset(in_date_field, in_offset_year_start_month) -%}
    EXTRACT(MONTH FROM DATE_ADD(DATE_TRUNC({{ in_date_field }}, MONTH), INTERVAL {{ in_offset_year_start_month }} MONTH))
{%- endmacro %}

 