--
-- Pig script to compute time on pages and rank them accoring to time using Pythin UDF
--

register 'pig/sessionize.py' using jython as sessionize;

A = load 'visits' using PigStorage(',') as (user_id:chararray, session_id:chararray, page:chararray, timestamp:chararray);
B = group A by (user_id, session_id);
C = foreach B {
      session = order A.(timestamp, page) by timestamp desc;
      generate flatten(group), sessionize.process(session) as ranks;
}
D = foreach C generate user_id, session_id, flatten(ranks);
store D into 'ranked' using PigStorage(',');
