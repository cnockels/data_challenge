{%- macro generateNumericalHash(field_list, null_value = 'null') -%}

{%- if varargs|length >= 1 or field_list is string %}

{%- set error_message = '
Warning: the `generateNumericalHash` macro now takes a single list argument instead of \
multiple string arguments. Support for multiple string arguments will be \
deprecated in a future release of dbt-utils. The {}.{} model triggered this warning. \
'.format(model.package_name, model.name) -%}

{%- do exceptions.warn(error_message) -%}

{# first argument is not included in varargs, so add first element to field_list_xf #}
{%- set field_list_xf = [field_list] -%}

{%- for field in varargs %}
{%- set _ = field_list_xf.append(field) -%}
{%- endfor -%}

{%- else -%}

{# if using list, just set field_list_xf as field_list #}
{%- set field_list_xf = field_list -%}

{%- endif -%}


{%- set fields = [] -%}
{%- set tests = [] -%}

{%- for field in field_list_xf -%}

    {%- set _ = fields.append(
        "coalesce(cast(" ~ field ~ " as " ~ type_string() ~ "), '')"
    ) -%}

    {%- set _ = tests.append("cast(" ~ field ~ " as string)") -%}

    {%- if not loop.last %}
        {%- set _ = fields.append("'-'") -%}
    {%- endif -%}

{%- endfor -%}
if (coalesce({%- for test in tests %}{{test}}{%- if not loop.last %},{%- endif -%}{%- endfor -%}) is null, {{null_value}},
FARM_FINGERPRINT({{concat(fields)}}))

{%- endmacro -%}