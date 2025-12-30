{% set models_to_generate = codegen.get_models(directory='staging', prefix='stg') %}

{% if models_to_generate is defined and models_to_generate | length > 0 %}
{{ codegen.generate_model_yaml(model_names=models_to_generate) }}
{% else %}
-- No models found to generate
{% endif %}