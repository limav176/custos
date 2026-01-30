WITH billing_info(product_name, committed_unit_cost, committed_total_cost, committed_total_units, billing_unit, on_demand_unit_cost) AS (
    VALUES
        ('logs_indexed_30day', 2, 20, 0.1, 'Per 1M indexed logs (30-day retention), per month', 3.75),
        ('logs_ingested', 0.1, 0, 0, 'Per ingested logs (1GB), per month', 0.1),
        ('error_tracking', 0.25, 0, 0, 'Per 1k error events, per month', 0.36),
        ('application_security_host', 31, 0, 0, 'Per ASM host, per month', 36),
        ('workflow_execution', 10, 0, 0, 'Per 100 Workflow Executions, per month', 14),
        ('serverless_apm', 10, 0, 0, 'Per 1M traced invocations, per month', 15),
        ('network_device', 7, 21, 3, 'Per network device, per month', 10.2),
        ('prof_host', 15, 285, 15, 'Per profiled host, per month', 23),
        ('serverless_infra', 5, 0, 0, 'Per active function, per month', 7.2),
        ('rum_replay', 1.8, 0, 0, 'Per 1k sessions, per month', 2.6),
        ('logs_indexed_3day', 1.06, 0, 0, 'Per 1M indexed logs (3-day retention), per month', 1.59),
        ('incident_management', 20, 0, 0, 'Per monthly active user, per month', 30),
        ('logs_indexed_15day', 1.07, 0, 0, 'Per 1M indexed logs (15-day retention), per month', 2.55),
        ('logs_indexed_7day', 1.02, 16320, 16000, 'Per 1M indexed logs (7-day retention), per month', 1.9),
        ('synthetics_api_tests', 5, 55, 11, 'per 10K API test runs, per month', 7.2),
        ('apm_host', 31, 2480, 80, 'Per APM host, per month', 35),
        ('infra_container_excl_agent', 1, 300, 300, 'Per container, per month', 1),
        ('timeseries', 5, 800, 160, 'Per 100 custom metrics, per month', 5),
        ('dbm_host', 70, 0, 0, 'Per database host, per month', 84),
        ('data_stream_monitoring', 15, 0, 0, 'Per host, per month', 18),
        ('ingested_spans', 0.1, 0, 0, 'Per ingested GB, per month', 0.1),
        ('apm_trace_search', 0.1, 0, 0, 'Per ingested GB, per month', 0.1),
        ('infra_host', 15, 6210, 414, 'Per infra host, per month', 18),
        ('ci_pipeline', 8, 1440, 180, 'Per active Git committer (3 times per month), per month', 12)
)

SELECT * FROM billing_info
