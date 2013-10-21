--
-- Pig script to compute time on pages and rank them accoring to time
--

register 'target/p-analytics.jar';

A = load 'view' using PigStorage(',') as (user_id:chararray, session_id:chararray, key:chararray, value:chararray);
B = foreach A generate user_id, session_id, [key, value] as attr;
C = group B by (user_id, session_id);
D = foreach C generate flatten(group), B.attr as attrs;
E = foreach D generate user_id, session_id, com.seagate.hadoop.pig.CollectMap(attrs) as attrs;
dump E;
