cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} \
  -f /opt/ga_chp_bq/training/pipeline_setup/ga_chp_bq_truncate_tables_before_training_pipeline.cql

