----------------------------------------------------------------------------
-- Example for activation/deactivation of logging transactions on a table --
----------------------------------------------------------------------------
-- selective transaction logs: history.history_table(target_table regclass, audit_rows boolean, audit_query_text boolean, excluded_cols text[]) 
SELECT history.history_table('object_res1.main_detail');
SELECT history.history_table('object_res1.main_detail_qualifier', 'true', 'false');
SELECT history.history_table('object_res1.main', 'true', 'false', '{res2_id, res3_id}'::text[]);

--deactivate logging for row trigger
DROP TRIGGER history_trigger_row ON object_res1.main;
--deactivate logging for statement trigger
DROP TRIGGER history_trigger_stm ON object_res1.main;

------------------------------------------------------------------------
-- Example for "get history transaction time query" ttime_gethistory(tbl)--
------------------------------------------------------------------------
-- This gives the full transaction time history (all the logged changes) of a specified object primitive
--example query (note: structure of results table has to be defined in query)
SELECT * FROM history.ttime_gethistory('object_res1.main') 
	main (gid integer, 
	      survey_gid integer, 
	      description character varying, 
	      source text, 
	      res2_id integer, 
	      res3_id integer, 
	      the_geom geometry, 
	      transaction_timestamp timestamptz, 
	      transaction_type text) where gid=2;
--example view
CREATE OR REPLACE VIEW object_res1.ttime_gethistory AS
SELECT ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) AS rowid, * FROM history.ttime_gethistory('object_res1.main') 
	main (gid integer, 
	      survey_gid integer, 
	      description character varying, 
	      source text, 
	      res2_id integer, 
	      res3_id integer, 
	      the_geom geometry, 
	      transaction_timestamp timestamptz, 
	      transaction_type text) where gid=2;

-------------------------------------------------------------------
-- Example for "equals transaction time query" ttime_equal(ttime)--
-------------------------------------------------------------------
-- This gives all the object primitives that were modified at the queried transaction time ("AT t")
CREATE OR REPLACE VIEW object_res1.ttime_equal AS
SELECT * FROM history.ttime_equal('object_res1.main', '2014-07-19 13:56:18.714175+02')
	main (gid integer, 
	      survey_gid integer, 
	      description character varying, 
	      source text, 
	      res2_id integer, 
	      res3_id integer, 
	      the_geom geometry, 
	      transaction_timestamp timestamptz, 
	      transaction_type text);

-----------------------------------------------------------------------------------
-- Example for "inside transaction time query" ttime_inside(ttime_from, ttime_to)--
-----------------------------------------------------------------------------------
-- This gives all the object primitives that were modified within the queried transaction time range
CREATE OR REPLACE VIEW object_res1.ttime_inside AS
SELECT * FROM history.ttime_inside('object_res1.main', '2014-07-19 16:00:00', '2014-07-19 16:40:00')
	main (gid integer, 
	      survey_gid integer, 
	      description character varying, 
	      source text, 
	      res2_id integer, 
	      res3_id integer, 
	      the_geom geometry, 
	      transaction_timestamp timestamptz, 
	      transaction_type text);


--TODO: adjust following queries
------------------------------------------------------------------
-- Example for "get history valid time query" vtime_gethistory()--
------------------------------------------------------------------
-- This gives the valid time history of a specified object primitive (only the real world changes - it gives the latest version of the object primitives at each real world change time)
CREATE OR REPLACE VIEW object.vtime_gethistory AS
SELECT ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) AS rowid,* FROM history.vtime_gethistory() WHERE gid=278;


-----------------------------------------------------------------------------------------------------------------
-------------- Examples for "intersect valid time query" vtime_intersect(vtime_from, vtime_to) ------------------
-----------------------------------------------------------------------------------------------------------------
-- These queries search for object primitives whose valid time intersects with queried time range or timestamp --
-----------------------------------------------------------------------------------------------------------------
-- This gives the latest version of all the object primitives at a defined resolution that were valid at some time during the queried time range and still may be valid ("BETWEEN t1 and t2")
CREATE OR REPLACE VIEW object.vtime_intersect AS
SELECT * FROM history.vtime_intersect('1991-02-15','1992-05-14') WHERE resolution=1;

-- This gives the latest version of all the object primitives at a defined resolution that were valid at some time from the queried timestamp until now ("AFTER t")
CREATE OR REPLACE VIEW object.vtime_intersect AS
SELECT * FROM history.vtime_intersect('2005-05-16') WHERE resolution=1;

-- This gives the latest version of all the object primitives at a defined resolution that were valid at some time before or at the queried timestamp and still may be valid ("BEFORE t")
CREATE OR REPLACE VIEW object.vtime_intersect AS
SELECT * FROM history.vtime_intersect('0001-01-01','2005-05-16') WHERE resolution=1;

-- This gives the latest version of all the object primitives at a defined resolution that were valid at the queried timestamp and still may be valid ("AT t")
CREATE OR REPLACE VIEW object.vtime_intersect AS
SELECT * FROM history.vtime_intersect('2001-05-16','2001-05-16') WHERE resolution=1;

-- This gives the latest version of all the object primitives at a defined resolution that were valid at some time from yesterday and still may be valid ("AT t")
CREATE OR REPLACE VIEW object.vtime_intersect AS
SELECT * FROM history.vtime_intersect('yesterday','yesterday') WHERE resolution=3;


-------------------------------------------------------------------------------------------------------------
------------ Examples for "inside valid time query" vtime_inside(vtime_from, vtime_to) ----------------------
-------------------------------------------------------------------------------------------------------------
-- These queries search for object primitives whose valid time is completely inside the queried time range --
-------------------------------------------------------------------------------------------------------------
-- This gives the latest version of all the object primitives at a defined resolution that were valid only within the queried time range ("BETWEEN t1 and t2")
CREATE OR REPLACE VIEW object.vtime_inside AS
SELECT * FROM history.vtime_inside('1980-02-15','2001-05-16') WHERE resolution=1;

-- This gives the latest version of all the object primitives at a defined resolution that were valid only within the time range from the queried timestamp until now ("AFTER t UNTIL now")
CREATE OR REPLACE VIEW object.vtime_inside AS
SELECT * FROM history.vtime_inside('1980-05-16') WHERE resolution=1;

-- This gives the latest version of all the object primitives at a defined resolution that were valid only until the queried timestamp ("BEFORE t")
CREATE OR REPLACE VIEW object.vtime_inside AS
SELECT * FROM history.vtime_inside('0001-01-01','2001-05-16') WHERE resolution=1;


-------------------------------------------------------------------------------------------------------
------------ Example for "equal valid time query" vtime_equal(vtime_from, vtime_to) -------------------
-------------------------------------------------------------------------------------------------------
-- These queries search for object primitives whose valid time range is equal the queried time range --
-------------------------------------------------------------------------------------------------------
-- This gives the latest version of all the object primitives at a defined resolution that have the same valid time range as the queried time range ("BETWEEN t1 and t2")
CREATE OR REPLACE VIEW object.vtime_equal AS
SELECT * FROM history.vtime_equal('1980-05-15 00:00:00+02','2000-05-15 00:00:00+02');


------------------------------------------------------------------
------------ Example for "spatio-temporal queries" ---------------
------------------------------------------------------------------
CREATE OR REPLACE VIEW object.spatio_temporal AS
SELECT * FROM history.vtime_inside('0001-01-01','2001-05-16')
WHERE resolution=1 
AND ST_Intersects(the_geom, (SELECT the_geom FROM object.main_detail WHERE gid=901));


---------------------------------------------------------------------------------
-------------------- Other temporal queries and useful functions ----------------
---------------------------------------------------------------------------------
-- See also: http://www.postgresql.org/docs/9.1/static/functions-datetime.html --
---------------------------------------------------------------------------------

-- This gives the latest version of all the object primitives at a defined resolution that have a "valid from" time (valid_timestamp_1) that equals the defined timestamp ("ONLY t")
CREATE OR REPLACE VIEW object.vtime_intersect AS
SELECT * FROM history.vtime_intersect() WHERE resolution=1 AND valid_timestamp_1='1980-05-15';

-- Truncate timestamp to desired unit
SELECT date_trunc('minute', transaction_time) FROM object.ttime_inside_all; 

-- Convert timestamptz to timestamp
SELECT transaction_time AT TIME ZONE 'UTC-2' FROM object.ttime_inside_all;

-- Create input for time series visualisation with for example QGIS time manager plugin
CREATE OR REPLACE VIEW object.ttime_inside_all AS
SELECT ROW_NUMBER() OVER (ORDER BY gid ASC) AS maingid, * FROM history.ttime_inside_all();

CREATE OR REPLACE VIEW object.ttime_timeseries AS
SELECT * FROM 
(SELECT maingid, the_geom FROM object.ttime_inside_all WHERE resolution=1) a
LEFT JOIN
(SELECT maingid AS id1, transaction_time AT TIME ZONE 'UTC-2' AS ttime_start FROM object.ttime_inside_all WHERE transaction_type='U' OR transaction_type='I') b
ON (a.maingid = b.id1)
left JOIN
(SELECT maingid AS id2, transaction_time AT TIME ZONE 'UTC-2' AS ttime_stop FROM  object.ttime_inside_all WHERE transaction_type='D') c
ON (a.maingid = c.id2);