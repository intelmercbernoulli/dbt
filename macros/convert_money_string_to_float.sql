{% macro convert_money_string_to_float(column_name) %}
    CAST(
        REPLACE(
            REPLACE(
                REPLACE(
                    LTRIM(RTRIM(REPLACE({{ column_name }}, 'R$', ''))), -- remove R$ e espaços laterais
                    '.', ''                                               -- remove separador de milhar
                ),
                ',', '.'                                                  -- troca vírgula decimal
            ),
            ' ', ''                                                       -- remove espaços internos
        ) AS FLOAT
    )
{% endmacro %}