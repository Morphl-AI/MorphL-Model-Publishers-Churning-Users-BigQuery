CREATE KEYSPACE IF NOT EXISTS morphl WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE morphl.ga_chp_bq_features_raw (
  client_id text,
  day_of_data_capture date,
  website_url text,
  sessions double,
  bounces double,
  events double,
  session_duration double,
  page_views double,
  is_mobile double,
  is_desktop double,
  is_tablet double,
  PRIMARY KEY ((client_id), day_of_data_capture, website_url)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_chp_bq_config_parameters (
  morphl_component_name text,
  parameter_name text,
  parameter_value text,
  PRIMARY KEY ((morphl_component_name, parameter_name))
);

INSERT INTO morphl.ga_chp_bq_config_parameters (morphl_component_name,parameter_name,parameter_value)
VALUES ('ga_chp_bq','days_worth_of_data_to_load','60');