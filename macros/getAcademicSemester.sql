{% macro getAcademicSemester(in_month_field) -%}
    CASE
        WHEN {{ in_month_field }} BETWEEN 1 AND 5 
            THEN "Fall"
        WHEN {{ in_month_field }} BETWEEN 6 AND 10 
            THEN "Spring"
        ELSE
            "Summer"
    END
{% endmacro %}