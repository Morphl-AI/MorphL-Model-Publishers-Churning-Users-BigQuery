cp -r /opt/ga_chp_bq /opt/code
cd /opt/code
git pull
spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar /opt/code/prediction/batch_inference/ga_chp_bq_batch_inference.py

