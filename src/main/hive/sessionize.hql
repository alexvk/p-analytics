--
-- The query to compute time on pages and rank them accoring to time
--

FROM (FROM (SELECT user_id, session_id, page, LEAD(time) OVER (PARTITION BY user_id, session_id ORDER BY time) as next, time from visits) t SELECT t.user_id, t.session_id, t.page, sum(if(t.next>0,t.next-t.time,0)) as len group by t.user_id, t.session_id, t.page) s SELECT s.user_id, s.session_id, s.page, s.len, RANK(len) OVER (PARTITION BY s.user_id, s.session_id ORDER BY -s.len);