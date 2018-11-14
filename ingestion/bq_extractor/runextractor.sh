set -e

rm -r /opt/code
cp -r /opt/ga_chp_bq /opt/code
cd /opt/code
git pull

DATE_FROM=$(cut -d'|' -f1 /opt/secrets/pipe_delimited_date_range.txt)
DATE_TO=$(cut -d'|' -f2 /opt/secrets/pipe_delimited_date_range.txt)
[[ "${DAY_OF_DATA_CAPTURE}" < "${DATE_FROM}" || "${DAY_OF_DATA_CAPTURE}" > "${DATE_TO}" ]] && exit 0

# Get project id from the service account file
GCP_PROJECT_ID=$(jq -r '.project_id' ${KEY_FILE_LOCATION})

# Compose source BQ table name
GA_SESSIONS_DATA_ID=ga_sessions_$(echo ${DAY_OF_DATA_CAPTURE} | sed 's/-//g')

# Compose destination BQ table name
DEST_TABLE=${DEST_BQ_DATASET}.${GA_SESSIONS_DATA_ID}

# Compose avro path file for Google Cloud Storage
DEST_GCS_AVRO_FILE=gs://${DEST_GCS_BUCKET}/${GA_SESSIONS_DATA_ID}.avro

# Compose avro path file for local filesystem
WEBSITE_URL=$(</opt/secrets/website_url.txt)
LOCAL_AVRO_FILE=/opt/landing/${DAY_OF_DATA_CAPTURE}_${WEBSITE_URL}.avro

# Load Google Cloud service account credentials
gcloud config set project ${GCP_PROJECT_ID}
gcloud auth activate-service-account --key-file=${KEY_FILE_LOCATION}
bq ls &>/dev/null

# Write dynamic variables to the query template file
sed "s/GCP_PROJECT_ID/${GCP_PROJECT_ID}/g;s/SRC_BQ_DATASET/${SRC_BQ_DATASET}/g;s/GA_SESSIONS_DATA_ID/${GA_SESSIONS_DATA_ID}/g;s/WEBSITE_URL/${WEBSITE_URL}/g" /opt/code/ingestion/bq_extractor/query.sql.template > /opt/code/ingestion/bq_extractor/query.sql

# Run query and save result to a temporary BQ destination table 
bq query --use_legacy_sql=false --destination_table=${DEST_TABLE} < /opt/code/ingestion/bq_extractor/query.sql &>/dev/null

# Extract destination table to an Avro file from Google Cloud Storage
bq extract --destination_format=AVRO ${DEST_TABLE} ${DEST_GCS_AVRO_FILE}

# Remove temporary destionation table
echo ${DEST_TABLE} | grep ^bq_avro_morphl.ga_sessions_ && bq rm -f ${DEST_TABLE}

# Download Avro file from Google Cloud Storage to filesystem
gsutil cp ${DEST_GCS_AVRO_FILE} /opt/landing/

# Remove Avro file from Google Cloud Storage
echo ${DEST_GCS_AVRO_FILE} | grep '^gs://bq_avro_morphl/ga_sessions_.*.avro$' && gsutil rm ${DEST_GCS_AVRO_FILE}

# Copy downloaded Avro file to the landing location
mv /opt/landing/${GA_SESSIONS_DATA_ID}.avro ${LOCAL_AVRO_FILE}
export LOCAL_AVRO_FILE
export WEBSITE_URL

spark-submit --jars /opt/spark/jars/spark-cassandra-connector.jar,/opt/spark/jars/jsr166e.jar,/opt/spark/jars/spark-avro.jar /opt/code/ingestion/bq_extractor/ga_chp_bq_ingest_avro_file.py
rm ${LOCAL_AVRO_FILE}
