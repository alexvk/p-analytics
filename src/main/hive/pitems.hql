CREATE EXTERNAL TABLE pitems ( serial_num STRING )
COMMENT "A sample item table from Protobuf serialization"
ROW FORMAT SERDE 'com.cloudera.fts.hive.PItemRecordSerDe'
STORED AS SEQUENCEFILE
LOCATION '/data/raw/pitems';
