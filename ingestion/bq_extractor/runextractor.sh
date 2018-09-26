set -e

cp -r /opt/ga_chp_bq /opt/code
cd /opt/code
git pull
DATE_FROM=$(cut -d'|' -f1 /opt/secrets/pipe_delimited_date_range.txt)
DATE_TO=$(cut -d'|' -f2 /opt/secrets/pipe_delimited_date_range.txt)
[[ "${DAY_OF_DATA_CAPTURE}" < "${DATE_FROM}" || "${DAY_OF_DATA_CAPTURE}" > "${DATE_TO}" ]] && exit 0
GCP_PROJECT_ID=$(jq -r '.project_id' ${KEY_FILE_LOCATION})
HDFS_PORT=9000
FQ_BQ_AVRO_HDFS_DIR=hdfs://${MORPHL_SERVER_IP_ADDRESS}:${HDFS_PORT}/${BQ_AVRO_HDFS_DIR}
GA_SESSIONS_DATA_ID=ga_sessions_$(echo ${DAY_OF_DATA_CAPTURE} | sed 's/-//g')
DEST_TABLE=${DEST_BQ_DATASET}.${GA_SESSIONS_DATA_ID}
DEST_GCS_AVRO_FILE=gs://${DEST_GCS_BUCKET}/${GA_SESSIONS_DATA_ID}.avro
WEBSITE_URL=$(</opt/secrets/website_url.txt)
LOCAL_AVRO_FILE=/opt/landing/${DAY_OF_DATA_CAPTURE}_${WEBSITE_URL}.avro
gcloud config set project ${GCP_PROJECT_ID}
gcloud auth activate-service-account --key-file=${KEY_FILE_LOCATION}
bq ls &>/dev/null
sed "s/GCP_PROJECT_ID/${GCP_PROJECT_ID}/g;s/SRC_BQ_DATASET/${SRC_BQ_DATASET}/g;s/GA_SESSIONS_DATA_ID/${GA_SESSIONS_DATA_ID}/g;s/WEBSITE_URL/${WEBSITE_URL}/g" /opt/code/ingestion/bq_extractor/query.sql.template > /opt/code/ingestion/bq_extractor/query.sql
bq query --use_legacy_sql=false --destination_table=${DEST_TABLE} < /opt/code/ingestion/bq_extractor/query.sql &>/dev/null
bq extract --destination_format=AVRO ${DEST_TABLE} ${DEST_GCS_AVRO_FILE}
echo ${DEST_TABLE} | grep ^bq_avro_morphl.ga_sessions_ && bq rm -f ${DEST_TABLE}
gsutil cp ${DEST_GCS_AVRO_FILE} /opt/landing/
echo ${DEST_GCS_AVRO_FILE} | grep '^gs://bq_avro_morphl/ga_sessions_.*.avro$' && gsutil rm ${DEST_GCS_AVRO_FILE}
mv /opt/landing/${GA_SESSIONS_DATA_ID}.avro ${LOCAL_AVRO_FILE}
hdfs dfs -mkdir -p ${FQ_BQ_AVRO_HDFS_DIR}
hdfs dfs -copyFromLocal -f ${LOCAL_AVRO_FILE} ${FQ_BQ_AVRO_HDFS_DIR}/${DAY_OF_DATA_CAPTURE}_${WEBSITE_URL}.avro
export LOCAL_AVRO_FILE
export WEBSITE_URL
spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar,/opt/spark/jars/spark-avro.jar /opt/code/ingestion/bq_extractor/ga_chp_bq_ingest_avro_file.py
# rm ${LOCAL_AVRO_FILE}
