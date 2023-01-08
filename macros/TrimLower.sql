{% macro TrimLower(in_string_column) -%}
    if(trim(lower({{ in_string_column }})) = '', cast(null as string), trim(lower({{ in_string_column }})))
{%- endmacro %}