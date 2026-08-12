{#-
    Logs every test result (pass/fail/warn/error) from THIS invocation into
    governance.dbt_test_results.

    `results` is a built-in dbt on-run-end variable: a list of Result
    objects, one per node dbt ran (models AND tests) in this invocation.
    It's the same data dbt serializes to target/run_results.json -- we're
    just also writing it somewhere queryable via SQL instead of only a
    JSON file on disk. Runs automatically, no manual call needed, as long
    as this macro is wired into on-run-end (see dbt_project.yml snippet).

    Native `store_failures: true` (set in dbt_project.yml) is what stores
    the ACTUAL failing rows as a queryable table per test. This macro logs
    the SUMMARY (which test, pass/fail, how many rows, when) -- the two
    are complementary: store_failures = the evidence, this table = the
    index/log of when + what.
-#}
{% macro log_test_results(results) %}
  {% if execute %}
    {% set test_rows = [] %}

    {% for r in results %}
      {# skip models/seeds/snapshots -- only log test nodes #}
      {% if r.node.resource_type == 'test' %}
        {% set model_name = r.node.attached_node if r.node.attached_node else r.node.depends_on.nodes[0] %}
        {% set severity = r.node.config.severity | lower if r.node.config is defined else 'error' %}
        {% set msg = (r.message or '') | replace("'", "''") %}
        {% set row %}
          ('{{ invocation_id }}', '{{ run_started_at }}', '{{ r.node.name }}',
           '{{ model_name }}', '{{ r.status }}', '{{ severity }}',
           {{ r.failures if r.failures is not none else 'null' }},
           {{ r.execution_time }}, '{{ msg }}')
        {% endset %}
        {% do test_rows.append(row) %}
      {% endif %}
    {% endfor %}

    {% if test_rows | length > 0 %}
      {% set query %}
        insert into governance.dbt_test_results
          (invocation_id, run_started_at, test_name, model_name, status,
           severity, failures, execution_time, message)
        values
          {{ test_rows | join(',\n') }}
      {% endset %}
      {% do run_query(query) %}
      {% do log('logged ' ~ (test_rows | length) ~ ' test results to governance.dbt_test_results', info=true) %}
    {% endif %}

  {% endif %}
{% endmacro %}