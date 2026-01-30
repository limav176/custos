WITH aux_datadog_product(product_name,product_group) AS (
    VALUES
		('infra_host','shared')
		,('apm_host','shared')
		,('network_device','shared')
		,('logs_indexed_30day','logging')
		,('logs_indexed_7day','logging')
		,('logs_indexed_15day','logging')
		,('logs_indexed_1day','logging')
		,('logs_indexed_3day','logging')
		,('logs_ingested','logging')
		,('dbm_host','database')
		,('prof_host','apm')
		,('timeseries','apm')
		,('ingested_spans','apm')
		,('error_tracking','apm')
		,('apm_trace_search','apm')
		,('infra_container_excl_agent','apm')
)

SELECT product_name,product_group FROM aux_datadog_product