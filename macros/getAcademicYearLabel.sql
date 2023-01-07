{% macro getAcademicYearLabel(in_date_field, in_offset_year_start_month) -%}
    CASE
        WHEN EXTRACT(month FROM {{ in_date_field }}) >= {{ in_offset_year_start_month }} 
            THEN "'" || FORMAT_DATE('%y', {{ in_date_field }}) || "-'" || FORMAT_DATE('%y', DATE_ADD({{ in_date_field }}, INTERVAL 1 YEAR))
    ELSE
        "'" || FORMAT_DATE('%y', DATE_SUB({{ in_date_field }}, INTERVAL 1 YEAR)) || "-'" || FORMAT_DATE('%y', {{ in_date_field }})
    END
{%- endmacro %}
