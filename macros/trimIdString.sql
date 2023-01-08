{% macro trimIdString(in_string_column) -%}
    if(trim(lower({{ in_string_column }})) = '"' or trim(lower({{ in_string_column }})) = '\' or trim(lower({{ in_string_column }})) = ' ', cast(null as string), trim({{ in_string_column }} ))
{%- endmacro %}