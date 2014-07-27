----------------------------------------------------------------------------
-- Example for activation/deactivation of logging transactions on a table --
----------------------------------------------------------------------------
-- selective transaction logs: history.history_table(target_table regclass, history_view boolean, history_query_text boolean, excluded_cols text[]) 
SELECT history.history_table('object_res1.main');	--short call to activate table log with query text activated and no excluded cols
SELECT history.history_table('object_res1.main', 'false', 'true');	--same as above but as full call
SELECT history.history_table('object_res1.main', 'false', 'false', '{res2_id, res3_id}'::text[]);	--activate table log with no query text activated and excluded cols specified
SELECT history.history_table('object_res1.ve_resolution1', 'true', 'false', '{source, res2_id, res3_id}'::text[]);	--activate logs for a view

--deactivate transaction logs on table
DROP TRIGGER IF EXISTS history_trigger_row ON object_res1.main;

--deactivate transaction logs on view
DROP TRIGGER IF EXISTS zhistory_trigger_row ON object_res1.ve_resolution1;
DROP TRIGGER IF EXISTS zhistory_trigger_row_modified ON history.logged_actions;

---------------------------------------------------------------------------------------
-- Example for "get history transaction time query" ttime_gethistory(tbl_in, tbl_out)--
---------------------------------------------------------------------------------------
-- This gives the full transaction time history (all the logged changes) of a table/view and writes it to a view
SELECT * FROM history.ttime_gethistory('object_res1.ve_resolution1', 'history.ttime_history');

-- Same as above, but output as records. 
-- Note: structure of results has to be defined manually (=structure of input table + transaction_timestamp timestamptz, transaction_type text). 
-- Note: this allows also to filter the results using WHERE statement.
SELECT * FROM history.ttime_gethistory('object_res1.ve_resolution1') 
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,yr_built_vt2 timestamptz,  
	      transaction_timestamp timestamptz, 
	      transaction_type text) WHERE gid=2;

-- Custom view
CREATE OR REPLACE VIEW history.ttime_gethistory_custom AS
SELECT ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) AS rowid, * FROM history.ttime_gethistory('object_res1.main') 
	main (gid integer, 
	      survey_gid integer, 
	      description character varying, 
	      source text, 
	      res2_id integer, 
	      res3_id integer, 
	      the_geom geometry, 
	      transaction_timestamp timestamptz, 
	      transaction_type text) WHERE gid=2;
	      
------------------------------------------------------------------------------------
-- Example for "equals transaction time query" ttime_equal(tbl_in, tbl_out, ttime)--
------------------------------------------------------------------------------------
-- This gives all the object primitives that were modified at the queried transaction time ("AT t") and writes the results to a view
SELECT * FROM history.ttime_equal('object_res1.ve_resolution1','history.ttime_equal','2014-07-27 16:38:53.344857+02');

-- Same as above, but output as records
SELECT * FROM history.ttime_equal('object_res1.ve_resolution1', '2014-07-27 16:38:53.344857+02')
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,yr_built_vt2 timestamptz, 
	      transaction_timestamp timestamptz, 
	      transaction_type text);

----------------------------------------------------------------------------------------------------
-- Example for "inside transaction time query" ttime_inside(tbl_in, tbl_out, ttime_from, ttime_to)--
----------------------------------------------------------------------------------------------------
-- This gives all the object primitives that were modified within the queried transaction time range and writes it to a view
SELECT * FROM history.ttime_inside('object_res1.ve_resolution1', 'history.ttime_inside', '2014-07-19 16:00:00', now()::timestamp);

-- Same as above, but output as records
SELECT * FROM history.ttime_inside('object_res1.ve_resolution1', '2014-07-19 16:00:00', now()::timestamp)
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,yr_built_vt2 timestamptz, 
	      transaction_timestamp timestamptz, 
	      transaction_type text);

/*
insert into object_res1.ve_resolution1 (source) values ('test');
insert into object_res1.ve_resolution1 (description) values ('marc321');
update object_res1.ve_resolution1 set description='marc111' where gid=15;
update object_res1.ve_resolution1 set source='marc111' where gid=15;
*/

--TODO: adjust following queries
------------------------------------------------------------------
-- Example for "get history valid time query" vtime_gethistory()--
------------------------------------------------------------------
-- This gives the valid time history of a specified object primitive (only the real world changes - it gives the latest version of the object primitives at each real world change time)
CREATE OR REPLACE VIEW object.vtime_gethistory AS
SELECT ROW_NUMBER() OVER (ORDER BY transaction_timestamp ASC) AS rowid,* FROM history.vtime_gethistory() WHERE gid=278;


select * from history.vtime_gethistory('object_res1', 'qualifier_timestamp_1', 'qualifier_timestamp_2')
	main (id int
	      gid int,
	      object_id int,
	      res2_id int,
	      res3_id int,
	      attribute_type_code varchar,
	      attribute_value varchar,
	      attribute_numeric_1 numeric,
	      attribute_numeric_2 numeric,
	      attribute_text_1 varchar,
	      the_geom geometry,
	      valid_timestamp_1 timestamptz,
	      valid_timestamp_2 timestamptz,
	      transaction_timestamp timestamptz,
	      transaction_type text);

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