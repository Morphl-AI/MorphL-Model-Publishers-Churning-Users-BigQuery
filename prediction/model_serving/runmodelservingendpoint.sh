cp -r /opt/ga_chp_bq /opt/code
cd /opt/code
git pull
python /opt/code/prediction/model_serving/model_serving_endpoint.py

