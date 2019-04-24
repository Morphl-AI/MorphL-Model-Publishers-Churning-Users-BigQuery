cp -r /opt/ga_chp_bq /opt/code
cd /opt/code
git pull
spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar,/opt/spark/jars/spark-avro.jar /opt/code/pre_processing/scaling_transformation/ga_chp_bq_advanced_preprocessor.py

