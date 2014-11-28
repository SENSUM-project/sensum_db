------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Name: SENSUM multi-temporal support examples
-- Version: 0.9.1
-- Date: 03.08.14
-- Author: M. Wieland
-- DBMS: PostgreSQL9.2 / PostGIS2.0
-- Description: Some examples to 
--			- activate/deactivate logging of transactions
--			- properly insert, update and delete entries with temporal component
--			- transaction and valid time queries
--			- spatio-temporal queries
--			- other queries
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------

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


----------------------------------------------------------------------------------------
-- Example statements to properly INSERT, UPDATE or DELETE objects for different cases--
----------------------------------------------------------------------------------------
--INSERT an object cause of a real world construction: 1. mark the object change type as 'BUILT'; 2. set the date of construction; 3. insert it
insert into object_res1.ve_resolution1 (description, yr_built_vt, yr_built_vt1) values ('insert', 'BUILT', '01-01-2000');

--UPDATE an object cause of a real world modification: 1. mark the object change type as 'MODIF'; 2. set the date of modification; 3. update it
update object_res1.ve_resolution1 set description='modified', yr_built_vt='MODIF', yr_built_vt1='01-01-2002' where gid=1;

--DELETE an object cause of a real world destruction: 1. mark the object change type as 'DESTR'; 2. set the date of destruction; 3. delete it
update object_res1.ve_resolution1 set description='deleted', yr_built_vt='DESTR', yr_built_vt1='01-01-2014' where gid=1;
delete from object_res1.ve_resolution1 where gid=1;

--UPDATE an object cause of a correction or cause more information gets available (no real world change): update it without marking the object change type
update object_res1.ve_resolution1 set description='modified_corrected' where gid=1;


---------------------------------------------------------------------------------------
-- Example for "get history transaction time query" ttime_gethistory(tbl_in, tbl_out)--
---------------------------------------------------------------------------------------
-- This gives the full transaction time history (all the logged changes) of a table/view and writes it to a view
SELECT * FROM history.ttime_gethistory('object_res1.ve_resolution1', 'history.ttime_history');

-- Same as above, but output as records. 
-- Note: structure of results has to be defined manually (=structure of input table + transaction_timestamp timestamptz, transaction_type text). 
-- Note: this allows also to filter the results using WHERE statement.
SELECT * FROM history.ttime_gethistory('object_res1.ve_resolution1') 
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      transaction_timestamp timestamptz, 
	      transaction_type text) WHERE gid=1 ORDER BY transaction_timestamp;

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
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz, 
	      transaction_timestamp timestamptz, 
	      transaction_type text);


----------------------------------------------------------------------------------------------------
-- Example for "inside transaction time query" ttime_inside(tbl_in, tbl_out, ttime_from, ttime_to)--
----------------------------------------------------------------------------------------------------
-- This gives all the object primitives that were modified within the queried transaction time range and writes it to a view
SELECT * FROM history.ttime_inside('object_res1.ve_resolution1', 'history.ttime_inside', '2014-07-19 16:00:00', now()::timestamp);

-- Same as above, but output as records
SELECT * FROM history.ttime_inside('object_res1.ve_resolution1', '2014-07-19 16:00:00', now()::timestamp)
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz, 
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;

	      
------------------------------------------------------------------
-- Example for "get history valid time query" vtime_gethistory()--
------------------------------------------------------------------
-- This gives the valid time history of a specified object primitive (only the real world changes - it gives the latest version of the object primitives at each real world change time)
SELECT * FROM history.vtime_gethistory('object_res1.ve_resolution1', 'history.vtime_history', 'yr_built_vt', 'yr_built_vt1');

-- Same as above, but output as records. 
SELECT * FROM history.vtime_gethistory('object_res1.ve_resolution1', 'yr_built_vt', 'yr_built_vt1') 
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;


-----------------------------------------------------------------------------------------------------------------
-------------- Examples for "intersect valid time query" vtime_intersect(vtime_from, vtime_to) ------------------
-----------------------------------------------------------------------------------------------------------------
-- These queries search for object primitives whose valid time intersects with queried time range or timestamp --
-----------------------------------------------------------------------------------------------------------------
-- This gives the latest version of all the object primitives that were valid at some time during the queried time range and still may be valid ("BETWEEN t1 and t2")
SELECT * FROM history.vtime_intersect('object_res1.ve_resolution1', 'history.vtime_history', 'yr_built_vt', 'yr_built_vt1', '1991-02-15','2002-05-14');

-- Same as above, but output as records. 
SELECT * FROM history.vtime_intersect('object_res1.ve_resolution1', 'yr_built_vt', 'yr_built_vt1', '1991-02-15','2002-05-14')
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;

-- This gives the latest version of all the object primitives that were valid at some time from the queried timestamp until now ("AFTER t")
SELECT * FROM history.vtime_intersect('object_res1.ve_resolution1', 'yr_built_vt', 'yr_built_vt1', '2001-05-16')
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;

-- This gives the latest version of all the object primitives that were valid at some time before or at the queried timestamp and still may be valid ("BEFORE t")
SELECT * FROM history.vtime_intersect('object_res1.ve_resolution1', 'yr_built_vt', 'yr_built_vt1', '0001-01-01','2000-05-16')
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;

-- This gives the latest version of all the object primitives that were valid at the queried timestamp and still may be valid ("AT t")
SELECT * FROM history.vtime_intersect('object_res1.ve_resolution1', 'yr_built_vt', 'yr_built_vt1', '2000-01-01','2000-01-01')
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;

-- This gives the latest version of all the object primitives that were valid at some time from yesterday and still may be valid ("AT t")
SELECT * FROM history.vtime_intersect('object_res1.ve_resolution1', 'yr_built_vt', 'yr_built_vt1', 'yesterday','yesterday')
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;


-------------------------------------------------------------------------------------------------------------
------------ Examples for "inside valid time query" vtime_inside(vtime_from, vtime_to) ----------------------
-------------------------------------------------------------------------------------------------------------
-- These queries search for object primitives whose valid time is completely inside the queried time range --
-------------------------------------------------------------------------------------------------------------
-- This gives the latest version of all the object primitives at a defined resolution that were valid only within the queried time range ("BETWEEN t1 and t2")
SELECT * FROM history.vtime_inside('object_res1.ve_resolution1', 'history.vtime_inside', 'yr_built_vt', 'yr_built_vt1', '2000-01-01 00:00:00+01','2002-01-01 00:00:00+01')

-- This gives the latest version of all the object primitives at a defined resolution that were valid only until the queried timestamp ("BEFORE t")
SELECT * FROM history.vtime_inside('object_res1.ve_resolution1', 'yr_built_vt', 'yr_built_vt1', '0001-01-01','2013-05-16')
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      vtime_from timestamptz,
	      vtime_to timestamptz,
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;

-- This gives the latest version of all the object primitives at a defined resolution that were valid only within the time range from the queried timestamp until now ("AFTER t UNTIL now")
SELECT * FROM history.vtime_inside('object_res1.ve_resolution1', 'yr_built_vt', 'yr_built_vt1', '2001-05-16')
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      vtime_from timestamptz,
	      vtime_to timestamptz,
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;


-------------------------------------------------------------------------------------------------------
------------ Example for "equal valid time query" vtime_equal(vtime_from, vtime_to) -------------------
-------------------------------------------------------------------------------------------------------
-- These queries search for object primitives whose valid time range is equal the queried time range --
-------------------------------------------------------------------------------------------------------
-- This gives the latest version of all the object primitives at a defined resolution that have the same valid time range as the queried time range ("BETWEEN t1 and t2")
SELECT * FROM history.vtime_equal('object_res1.ve_resolution1', 'history.vtime_equal', 'yr_built_vt', 'yr_built_vt1', '2000-01-01 00:00:00+01','2002-01-01 00:00:00+01')

-- Same as above, but output as records. 
SELECT * FROM history.vtime_equal('object_res1.ve_resolution1', 'yr_built_vt', 'yr_built_vt1', '2000-01-01 00:00:00+01','2002-01-01 00:00:00+01')
	main (gid int4,survey_gid int4,description varchar,source text,res2_id int4,res3_id int4,the_geom geometry,object_id int4,mat_type varchar,mat_tech varchar,mat_prop varchar,llrs varchar,llrs_duct varchar,height varchar,yr_built varchar,occupy varchar,occupy_dt varchar,position varchar,plan_shape varchar,str_irreg varchar,str_irreg_dt varchar,str_irreg_type varchar,nonstrcexw varchar,roof_shape varchar,roofcovmat varchar,roofsysmat varchar,roofsystyp varchar,roof_conn varchar,floor_mat varchar,floor_type varchar,floor_conn varchar,foundn_sys varchar,build_type varchar,build_subtype varchar,vuln varchar,vuln_1 numeric,vuln_2 numeric,height_1 numeric,height_2 numeric,object_id1 int4,mat_type_bp int4,mat_tech_bp int4,mat_prop_bp int4,llrs_bp int4,llrs_duct_bp int4,height_bp int4,yr_built_bp int4,occupy_bp int4,occupy_dt_bp int4,position_bp int4,plan_shape_bp int4,str_irreg_bp int4,str_irreg_dt_bp int4,str_irreg_type_bp int4,nonstrcexw_bp int4,roof_shape_bp int4,roofcovmat_bp int4,roofsysmat_bp int4,roofsystyp_bp int4,roof_conn_bp int4,floor_mat_bp int4,floor_type_bp int4,floor_conn_bp int4,foundn_sys_bp int4,build_type_bp int4,build_subtype_bp int4,vuln_bp int4,yr_built_vt varchar,yr_built_vt1 timestamptz,  
	      vtime_from timestamptz,
	      vtime_to timestamptz,
	      transaction_timestamp timestamptz, 
	      transaction_type text) ORDER BY transaction_timestamp;


--TODO: adjust following queries
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
SELECT date_trunc('minute', transaction_timestamp) FROM history.ttime_history; 

-- Convert timestamptz to timestamp
SELECT transaction_timestamp AT TIME ZONE 'UTC' FROM history.ttime_history;

-- Create input for time series visualisation with for example QGIS time manager plugin
-- note: plugin runs much faster with a table than with a view!
CREATE OR REPLACE VIEW public.ttime_history AS
SELECT *, transaction_timestamp AT TIME ZONE 'UTC' AS transaction_timestamp_utc FROM history.ttime_history;
-- as table
SELECT *, transaction_timestamp AT TIME ZONE 'UTC' AS transaction_timestamp_utc INTO public.ttime_history_t FROM history.ttime_history;