--
-- Pig script to read items from an Avro file
--

register 'target/p-analytics.jar';

A = load 'items.avro' using AvroStorage();
B = foreach A generate serial_num, status, flatten(com.cloudera.fts.pig.MapToBag(attributes));
dump B;
