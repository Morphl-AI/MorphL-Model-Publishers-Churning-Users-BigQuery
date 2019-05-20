cp -r /opt/ga_chp_bq /opt/code
cd /opt/code
git pull
spark-submit --jars /opt/spark/jars/jsr166e.jar /opt/code/training/model_generator/ga_chp_bq_model_generator.py

