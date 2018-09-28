Prerequisites before the `runextractor.sh` can be executed:
1. The file `/opt/secrets/keyfile.json` contains a valid service account / JSON-format key corresponding to a service account that has the permissions `BigQuery User` and `Storage Object Creator`.
2. The whitelisted date range has been configured like this:
```
cat > /opt/secrets/pipe_delimited_date_range.txt << EOF
2018-09-17|2018-09-19
EOF
```
3. The website's domain URL has been configured like this:
```
cat > /opt/secrets/website_url.txt << EOF
www.websitename.com
EOF
```
The following commands are part of `runextractor.sh` and their purpose is to create the authenticated environment necessary for the CLI utilities `bq` and `gsutil` to run successfully:
```
gcloud config set project ${GCP_PROJECT_ID}
gcloud auth activate-service-account --key-file=${KEY_FILE_LOCATION}
bq ls &>/dev/null
```
Note: `bq` and `gsutil` are companion utilities to `gcloud`. All three are installed as components of the Google Cloud SDK.

Next, there is a `sed` command that dynamically generates the BQ query to execute by substituting the necessary variables in the template `/opt/code/ingestion/bq_extractor/query.sql.template`.

The query generated above is executed by the BigQuery engine, and the results are saved in the table `DEST_TABLE`:
```
bq query --use_legacy_sql=false --destination_table=${DEST_TABLE} < /opt/code/ingestion/bq_extractor/query.sql &>/dev/null
```
The results of the query are converted into the Avro format and saved to Google Cloud Storage (S3 equivalent in GCP):
```
bq extract --destination_format=AVRO ${DEST_TABLE} ${DEST_GCS_AVRO_FILE}
```
The BigQuery table DEST_TABLE is deleted (following a safeguard conditional):
```
echo ${DEST_TABLE} | grep ^bq_avro_morphl.ga_sessions_ && bq rm -f ${DEST_TABLE}
```
The resulting Avro file DEST_GCS_AVRO_FILE is downloaded from GCS to the local directory `/opt/landing`:
```
gsutil cp ${DEST_GCS_AVRO_FILE} /opt/landing/
```
The remote Avro file DEST_GCS_AVRO_FILE is deleted (following a safeguard conditional):
```
echo ${DEST_GCS_AVRO_FILE} | grep '^gs://bq_avro_morphl/ga_sessions_.*.avro$' && gsutil rm ${DEST_GCS_AVRO_FILE}
```
A PySpark script then converts the contents of LOCAL_AVRO_FILE into a DataFrame.
Finally, the DataFrame is saved to Cassandra.
