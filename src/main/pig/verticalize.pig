--
-- Pig script to unpivot data read from a text file
--

register 'target/profile.jar';

A = LOAD 'attr' using PigStorage('\t') as (serial_num:chararray, status:chararray, attrs:map[chararray]);
B = foreach A generate serial_num, status, flatten(com.cloudera.fts.pig.MapToBag(attrs)); 
STORE B into 'pivoted' using PigStorage(',');
