CREATE EXTERNAL TABLE aitems
COMMENT "A sample items table from Crunch Avro serialization"
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/data/raw/aitems'
TBLPROPERTIES ('avro.schema.url'='hdfs:///data/schema/item.avsc');
