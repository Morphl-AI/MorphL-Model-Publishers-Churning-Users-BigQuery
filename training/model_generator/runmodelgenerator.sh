cp -r /opt/ga_chp_bq /opt/code
cd /opt/code
git pull
spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar,/opt/spark/jars/spark-avro.jar /opt/code/training/model_generator/ga_chp_bq_model_generator.py

