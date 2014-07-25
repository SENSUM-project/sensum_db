----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
-- Name: SENSUM multi-temporal database support
-- Version: 0.91
-- Date: 25.07.14
-- Author: M. Wieland
-- DBMS: PostgreSQL9.2 / PostGIS2.0
-- Description: Adds the multi-temporal support to the basic SENSUM data model.
--		1. Adds trigger functions to log database transactions for selected tables or views
--		   (reference: http://wiki.postgresql.org/wiki/Audit_trigger_91plus)
--		2. Adds temporal query functions for transaction time and valid time
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------

------------------------------------------------
-- Create trigger function to log transactions--
------------------------------------------------
CREATE OR REPLACE FUNCTION history.if_modified() 
RETURNS TRIGGER AS 
$body$
DECLARE
    history_row history.logged_actions;
    include_values BOOLEAN;
    log_diffs BOOLEAN;
    h_old hstore;
    h_new hstore;
    excluded_cols text[] = ARRAY[]::text[];
BEGIN
    history_row = ROW(
        NEXTVAL('history.logged_actions_gid_seq'),    -- gid
        TG_TABLE_SCHEMA::text,                        -- schema_name
        TG_TABLE_NAME::text,                          -- table_name
        TG_RELID,                                     -- relation OID for much quicker searches
        txid_current(),                               -- transaction_id
        session_user::text,                           -- transaction_user
        current_timestamp,                            -- transaction_time
        current_query(),                              -- top-level query or queries (if multistatement) from client
        substring(TG_OP,1,1),                         -- transaction_type
        NULL, NULL, NULL                             -- old_record, new_record, changed_fields
        );
 
    IF NOT TG_ARGV[0]::BOOLEAN IS DISTINCT FROM 'f'::BOOLEAN THEN
        history_row.transaction_query = NULL;
    END IF;
 
    IF TG_ARGV[1] IS NOT NULL THEN
        excluded_cols = TG_ARGV[1]::text[];
    END IF;
 
    IF (TG_OP = 'UPDATE' AND TG_LEVEL = 'ROW') THEN
	history_row.old_record = hstore(OLD.*);
        history_row.new_record = hstore(NEW.*);
        history_row.changed_fields = (hstore(NEW.*) - history_row.old_record) - excluded_cols;
        IF history_row.changed_fields = hstore('') THEN
        -- All changed fields are ignored. Skip this update.
            RETURN NULL;
        END IF;
    ELSIF (TG_OP = 'DELETE' AND TG_LEVEL = 'ROW') THEN
	history_row.old_record = hstore(OLD.*);
    ELSIF (TG_OP = 'INSERT' AND TG_LEVEL = 'ROW') THEN
	history_row.new_record = hstore(NEW.*);
    ELSE
        RAISE EXCEPTION '[history.if_modified_func] - Trigger func added as trigger for unhandled case: %, %',TG_OP, TG_LEVEL;
        RETURN NULL;
    END IF;
    INSERT INTO history.logged_actions VALUES (history_row.*);
    RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public;

COMMENT ON FUNCTION history.if_modified() IS $body$
Track changes TO a TABLE or a VIEW at the row level.
Optional parameters TO TRIGGER IN CREATE TRIGGER call:
param 0: BOOLEAN, whether TO log the query text. DEFAULT 't'.
param 1: text[], COLUMNS TO IGNORE IN updates. DEFAULT [].

         Note: Updates TO ignored cols are included in new_record.
         Updates WITH only ignored cols changed are NOT inserted
         INTO the history log.
         There IS no parameter TO disable logging of VALUES. ADD this TRIGGER AS
         a 'FOR EACH STATEMENT' rather than 'FOR EACH ROW' TRIGGER IF you do NOT
         want TO log row VALUES.
$body$;

---------------------------------------------------------------------------------------
-- Create trigger function to update gid in logged transactions when a view is logged--
---------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION history.if_modified_view()
RETURNS TRIGGER AS 
$BODY$
DECLARE
    tbl regclass;
BEGIN
    --IF (SELECT transaction_type FROM history.logged_actions WHERE gid=(SELECT max(gid) FROM history.logged_actions)) = 'I' THEN
	FOR tbl IN
	    --get dynamic table name
	    SELECT schema_name::text || '.' || table_name::text FROM history.logged_actions WHERE gid=(SELECT max(gid) FROM history.logged_actions)
	LOOP
	    EXECUTE '
	    UPDATE history.logged_actions SET 
		new_record = (SELECT hstore('|| tbl ||'.*) FROM '|| tbl ||' WHERE gid=(SELECT max(gid) FROM '|| tbl ||' ))
		WHERE gid=(SELECT max(gid) FROM history.logged_actions);
	    ';
	END LOOP;
    --END IF;
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;
COMMENT ON FUNCTION history.if_modified_view() IS $body$
This function updates the gid of a view in the logged actions table for the INSERT statement.
$body$;

---------------------------------------------------------------------------------
-- Create function to activate transaction logging for a specific table or view--
---------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION history.history_table(target_table regclass, history_view BOOLEAN, history_query_text BOOLEAN, ignored_cols text[]) 
RETURNS void AS 
$body$
DECLARE
  _q_txt text;
  _ignored_cols_snip text = '';
BEGIN
    IF history_view THEN
	    --create trigger on view (use instead of trigger) - note: in case of multiple triggers on the same table/view the execution order is alphabetical
	    IF array_length(ignored_cols,1) > 0 THEN
		_ignored_cols_snip = ', ' || quote_literal(ignored_cols);
	    END IF;
	    
	    EXECUTE 'DROP TRIGGER IF EXISTS zhistory_trigger_row ON ' || target_table::text;
	    _q_txt = 'CREATE TRIGGER zhistory_trigger_row INSTEAD OF INSERT OR UPDATE ON ' ||
		     target_table::text ||
		     ' FOR EACH ROW EXECUTE PROCEDURE history.if_modified(' || 
			quote_literal(history_query_text) || _ignored_cols_snip || ');';
	    RAISE NOTICE '%',_q_txt;
	    EXECUTE _q_txt;

	    EXECUTE 'DROP TRIGGER IF EXISTS zhistory_trigger_row_modified ON history.logged_actions';
	    _q_txt = 'CREATE TRIGGER zhistory_trigger_row_modified AFTER INSERT ON history.logged_actions 
			FOR EACH ROW EXECUTE PROCEDURE history.if_modified_view();';
	    RAISE NOTICE '%',_q_txt;
	    EXECUTE _q_txt;
    ELSE
	    --create trigger on table (use after trigger)
	    IF array_length(ignored_cols,1) > 0 THEN
		_ignored_cols_snip = ', ' || quote_literal(ignored_cols);
	    END IF;

	    EXECUTE 'DROP TRIGGER IF EXISTS history_trigger_row ON ' || target_table::text;
            _q_txt = 'CREATE TRIGGER history_trigger_row AFTER INSERT OR UPDATE OR DELETE ON ' || 
                     target_table::text || 
                     ' FOR EACH ROW EXECUTE PROCEDURE history.if_modified(' ||
                     quote_literal(history_query_text) || _ignored_cols_snip || ');';
            RAISE NOTICE '%',_q_txt;
            EXECUTE _q_txt;
    END IF;
END;
$body$
LANGUAGE 'plpgsql';
 
COMMENT ON FUNCTION history.history_table(regclass, BOOLEAN, BOOLEAN, text[]) IS $body$
ADD transaction logging support TO a TABLE.

Arguments:
   target_table:       TABLE name, schema qualified IF NOT ON search_path
   history_view:       Activate trigger for view (true) or for table (false)
   history_query_text: Record the text of the client query that triggered the history event?
   ignored_cols:       COLUMNS TO exclude FROM UPDATE diffs, IGNORE updates that CHANGE only ignored cols.
$body$;

---------------------------------------------------------------------------------
-- Provide a wrapper because Pg does not allow variadic calls with 0 parameters--
---------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION history.history_table(target_table regclass, history_view BOOLEAN, history_query_text BOOLEAN) 
RETURNS void AS 
$body$
SELECT history.history_table($1, $2, $3, ARRAY[]::text[]);
$body$ 
LANGUAGE SQL;

------------------------------------------------------------------------------------------------------------------------------------------
-- Provide a convenience call wrapper for the simplest case (row-level logging on table with no excluded cols and query logging enabled)--
------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION history.history_table(target_table regclass) 
RETURNS void AS 
$$
SELECT history.history_table($1, BOOLEAN 'f', BOOLEAN 't');
$$ 
LANGUAGE 'sql';
 
COMMENT ON FUNCTION history.history_table(regclass) IS $body$
ADD auditing support TO the given TABLE. Row-level changes will be logged WITH FULL query text. No cols are ignored.
$body$;

------------------------------------------------------
-- Add transaction time query function (getHistory) --
------------------------------------------------------
CREATE OR REPLACE FUNCTION history.ttime_gethistory(tbl character varying)
RETURNS SETOF RECORD AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE '
	--query1: query new_record column to get the UPDATE and INSERT records
	(SELECT (populate_record(null::' ||tbl|| ', b.new_record)).*, b.transaction_time, b.transaction_type FROM history.logged_actions AS b 
	WHERE b.table_name = split_part('''||tbl||''', ''.'', 2) AND b.transaction_type=''U''
	OR b.table_name = split_part('''||tbl||''', ''.'', 2) AND b.transaction_type=''I''
	ORDER BY b.transaction_time DESC)	

	UNION ALL

	--query2: query old_record column to get the DELETE records
	(SELECT (populate_record(null::' ||tbl|| ', b.old_record)).*, b.transaction_time, b.transaction_type FROM history.logged_actions AS b 
	WHERE b.table_name = split_part('''||tbl||''', ''.'', 2) AND b.transaction_type=''D''
	ORDER BY b.transaction_time DESC);
	';
END;
$BODY$
LANGUAGE plpgsql;
COMMENT ON FUNCTION history.ttime_gethistory(tbl character varying) IS $body$
This function searches history.logged_actions to get all transactions of object primitives.
Arguments:
   tbl:		schema.table character varying
$body$;

-------------------------------------------------
-- Add transaction time query function (Equal) --
-------------------------------------------------
CREATE OR REPLACE FUNCTION history.ttime_equal(tbl character varying, ttime timestamp)
RETURNS SETOF RECORD AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE '
	--query1: query new_record column to get the UPDATE and INSERT records
	(SELECT (populate_record(null::'||tbl||', b.new_record)).*, b.transaction_time, b.transaction_type FROM history.logged_actions AS b 
	WHERE b.table_name = split_part('''||tbl||''', ''.'', 2) AND b.transaction_time = '''||ttime||''' AND b.transaction_type = ''U''
	OR b.table_name = split_part('''||tbl||''', ''.'', 2) AND b.transaction_time = '''||ttime||''' AND b.transaction_type = ''I''
	ORDER BY b.new_record->''gid'', b.transaction_time DESC)
	
	UNION ALL

	--query2: query old_record column to get the DELETE records
	(SELECT (populate_record(null::'||tbl||', b.old_record)).*, b.transaction_time, b.transaction_type FROM history.logged_actions AS b 
	WHERE b.table_name = split_part('''||tbl||''', ''.'', 2) AND b.transaction_time = '''||ttime||''' AND b.transaction_type = ''D''
	ORDER BY b.old_record->''gid'', b.transaction_time DESC);
	';
END;
$BODY$
LANGUAGE plpgsql;
COMMENT ON FUNCTION history.ttime_equal(tbl character varying, ttime timestamp) IS $body$
This function searches history.logged_actions to get the latest version of each object primitive whose transaction time equals the queried timestamp.

Arguments:
   tbl:		schema.table character varying
   ttime:	transaction time yyy-mm-dd hh:mm:ss
$body$;

--------------------------------------------------
-- Add transaction time query function (Inside) --
--------------------------------------------------
CREATE OR REPLACE FUNCTION history.ttime_inside(tbl character varying, ttime_from timestamp DEFAULT '0001-01-01 00:00:00', ttime_to timestamp DEFAULT now()) 
RETURNS SETOF RECORD AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE '
	--query1: query new_record column to get the UPDATE and INSERT objects
	(SELECT (populate_record(null::'||tbl||', b.new_record)).*, b.transaction_time, b.transaction_type FROM history.logged_actions AS b 
	WHERE b.table_name = split_part('''||tbl||''', ''.'', 2) AND b.transaction_time >= '''||ttime_from||''' AND b.transaction_time <= '''||ttime_to||''' AND b.transaction_type = ''U''
	OR b.table_name = split_part('''||tbl||''', ''.'', 2) AND b.transaction_time >= '''||ttime_from||''' AND b.transaction_time <= '''||ttime_to||''' AND b.transaction_type = ''I''
	ORDER BY b.new_record->''gid'', b.transaction_time DESC)
	
	UNION ALL

	--query2: query old_record column to get the DELETE objects
	(SELECT (populate_record(null::'||tbl||', b.old_record)).*, b.transaction_time, b.transaction_type FROM history.logged_actions AS b 
	WHERE b.table_name = split_part('''||tbl||''', ''.'', 2) AND b.transaction_time >= '''||ttime_from||''' AND b.transaction_time <= '''||ttime_to||''' AND b.transaction_type = ''D''
	ORDER BY b.old_record->''gid'', b.transaction_time DESC);
	';
END;
$BODY$
LANGUAGE plpgsql;
COMMENT ON FUNCTION history.ttime_inside(tbl character varying, ttime_from timestamp, ttime_to timestamp) IS $body$
This function searches history.logged_actions to get the latest version of each object primitive that has been modified only within the queried transaction time.

Arguments:
   tbl:		schema.table character varying
   ttime_from:	transaction time from yyy-mm-dd hh:mm:ss
   ttime_to:	transaction time to yyy-mm-dd hh:mm:ss
$body$;


/*

--TODO: adjust the following time queries to work on a single table or view

------------------------------------------------
-- Add valid time query function (getHistory) --
------------------------------------------------
CREATE OR REPLACE FUNCTION history.vtime_gethistory(schema character varying, vtime_col1 character varying, vtime_col2 character varying)
RETURNS SETOF RECORD AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE '
	SELECT * FROM (
		--query1: query new_record column to get the latest UPDATE records
		SELECT  n3.gid as id, n3.*, n2.*, n1.* FROM
			(SELECT DISTINCT ON (a.new_record->''gid'') (populate_record(null::'||schema||'.main_detail_qualifier, a.new_record)).*, a.transaction_time as ttime, a.transaction_type FROM history.logged_actions AS a 
			WHERE a.table_name = ''main_detail_qualifier'' AND exist(a.changed_fields,'''||vtime_col1||''')
			OR a.table_name = ''main_detail_qualifier'' AND exist(a.changed_fields,'''||vtime_col2||''')	
			ORDER BY a.new_record->''gid'', a.transaction_time DESC) n1
			LEFT JOIN 
			(SELECT DISTINCT ON (b.new_record->''gid'') (populate_record(null::'||schema||'.main_detail, b.new_record)).*, b.transaction_time FROM history.logged_actions AS b 
			WHERE b.table_name = ''main_detail''
			ORDER BY b.new_record->''gid'', b.transaction_time DESC) n2
			ON (n2.gid = n1.detail_id)
			JOIN 
			'||schema||'.main AS n3 
			ON (n3.gid = n2.object_id)
		UNION ALL
		--query2: query new_record column to get the UPDATE and INSERT records
		SELECT  n3.gid as id, n3.*, n2.*, n1.* FROM
			(SELECT (populate_record(null::'||schema||'.main_detail_qualifier, a.new_record)).*, a.transaction_time, a.transaction_type FROM history.logged_actions AS a 
			WHERE a.table_name = ''main_detail_qualifier'' AND exist(a.changed_fields,'''||vtime_col1||''')
			OR a.table_name = ''main_detail_qualifier'' AND exist(a.changed_fields,'''||vtime_col2||''')	--select records that were UPDATE and that caused a change to the transaction_time (=real world change)
			OR a.table_name = ''main_detail_qualifier'' AND a.transaction_type = ''I''	--select records that were INSERTED
			ORDER BY a.transaction_time DESC OFFSET 1) n1	--OFFSET 1 to remove the latest UPDATE record (will be substituted by query1 result which gives the latest version in the database and not the latest version before or at n1.transaction_time)
			LEFT JOIN 
			(SELECT (populate_record(null::'||schema||'.main_detail, b.new_record)).*, b.transaction_time FROM history.logged_actions AS b 
			WHERE b.table_name = ''main_detail''
			ORDER BY b.transaction_time) n2
			ON (n2.gid = n1.detail_id AND n2.transaction_time = (SELECT max(transaction_time) FROM history.logged_actions WHERE table_name = ''main_detail'' AND transaction_time <= n1.transaction_time))	--join only the records from main_detail that have the closest lesser transaction time to the selected main_detail_qualifier
			JOIN 
			'||schema||'.main AS n3 
			ON (n3.gid = n2.object_id)
		UNION ALL
		--query3: query old_record column to get the DELETE records
		SELECT  n3.gid as id, n3.*, n2.*, n1.* FROM 
			(SELECT (populate_record(null::'||schema||'.main_detail_qualifier, a.old_record)).*, a.transaction_time, a.transaction_type FROM history.logged_actions AS a 
			WHERE a.table_name = ''main_detail_qualifier'' AND a.old_record->'''||vtime_col1||'''!='''' AND a.transaction_type = ''D''
			OR a.table_name = ''main_detail_qualifier'' AND a.old_record->'''||vtime_col2||'''!='''' AND a.transaction_type = ''D''
			ORDER BY a.old_record->''gid'', a.transaction_time DESC) n1
			LEFT JOIN 
			(SELECT (populate_record(null::'||schema||'.main_detail, b.old_record)).*, b.transaction_time FROM history.logged_actions AS b 
			WHERE b.table_name = ''main_detail''
			ORDER BY b.old_record->''gid'', b.transaction_time DESC) n2
			ON (n2.gid = n1.detail_id 
			AND n2.transaction_time = (SELECT max(transaction_time) FROM history.logged_actions WHERE table_name = ''main_detail'' AND transaction_time <= n1.transaction_time))	--join only the records from main_detail that have the closest lesser transaction time to the selected main_detail_qualifier
			JOIN 
			'||schema||'.main AS n3 
			ON (n3.gid = n2.object_id)
	) n0 ORDER BY n0.id, n0.ttime DESC;
	';
END;
$BODY$
LANGUAGE plpgsql;
COMMENT ON FUNCTION history.vtime_gethistory(schema character varying, vtime_col1 character varying, vtime_col2 character varying) IS $body$
This function searches history.logged_actions to get all real world changes with the corresponding latest version for each object primitive at each valid time.

Arguments:
   schema:		schema that holds the main, main_detail and main_detail_qualifier tables character varying
   vtime_col1:	valid time column 1
   vtime_col2:	valid time column 2
$body$;
*/
/*
--TODO: ADJUST THE FOLLOWING TEMPORAL QUERIES TO NEW TABLE STRUCTURES AND MAKE THEM EASIER AND APPLICABLE TO ANY TABLE STRUCTURES!!!!
------------------------------------------------
-- Add valid time query function (getHistory) --
------------------------------------------------
CREATE OR REPLACE FUNCTION history.vtime_gethistory() 
RETURNS TABLE (
gid int,
object_id int,
resolution int,
resolution2_id int,
resolution3_id int,
attribute_type_code varchar,
attribute_value varchar,
attribute_numeric_1 numeric,
attribute_numeric_2 numeric,
attribute_text_1 varchar,
the_geom geometry,
valid_timestamp_1 timestamptz,
valid_timestamp_2 timestamptz,
transaction_timestamp timestamptz,
transaction_type text
) AS
$BODY$
BEGIN
	RETURN QUERY SELECT * FROM (

	--query1: query new_record column to get the latest UPDATE records
	SELECT  
	n2.gid,
	n2.object_id,
	n3.resolution,
	n2.resolution2_id,
	n2.resolution3_id,
	n2.attribute_type_code,
	n2.attribute_value,
	n2.attribute_numeric_1,
	n2.attribute_numeric_2,
	n2.attribute_text_1,
	n2.the_geom,
	n1.qualifier_timestamp_1,
	n1.qualifier_timestamp_2,
	n1.transaction_time,
	n1.transaction_type
	FROM
	
	(SELECT DISTINCT ON (a.new_record->'gid') (populate_record(null::object.main_detail_qualifier, a.new_record)).*, a.transaction_time, a.transaction_type FROM history.logged_actions AS a 
	WHERE a.table_name = 'main_detail_qualifier' AND exist(a.changed_fields,'qualifier_timestamp_1')
	OR a.table_name = 'main_detail_qualifier' AND exist(a.changed_fields,'qualifier_timestamp_2')	
	ORDER BY a.new_record->'gid', a.transaction_time DESC) n1

	LEFT JOIN 

	(SELECT DISTINCT ON (b.new_record->'gid') (populate_record(null::object.main_detail, b.new_record)).*, b.transaction_time FROM history.logged_actions AS b 
	WHERE b.table_name = 'main_detail'
	ORDER BY b.new_record->'gid', b.transaction_time DESC) n2
	
	ON (n2.gid = n1.detail_id)
	
	JOIN 
	object.main AS n3 
	ON (n3.gid = n2.object_id)

	UNION ALL

	--query2: query new_record column to get the UPDATE and INSERT records
	SELECT  
	n2.gid,
	n2.object_id,
	n3.resolution,
	n2.resolution2_id,
	n2.resolution3_id,
	n2.attribute_type_code,
	n2.attribute_value,
	n2.attribute_numeric_1,
	n2.attribute_numeric_2,
	n2.attribute_text_1,
	n2.the_geom,
	n1.qualifier_timestamp_1,
	n1.qualifier_timestamp_2,
	n1.transaction_time,
	n1.transaction_type
	FROM

	(SELECT (populate_record(null::object.main_detail_qualifier, a.new_record)).*, a.transaction_time, a.transaction_type FROM history.logged_actions AS a 
	WHERE a.table_name = 'main_detail_qualifier' AND exist(a.changed_fields,'qualifier_timestamp_1')
	OR a.table_name = 'main_detail_qualifier' AND exist(a.changed_fields,'qualifier_timestamp_2')	--select records that were UPDATE and that caused a change to the transaction_time (=real world change)
	OR a.table_name = 'main_detail_qualifier' AND a.transaction_type = 'I'	--select records that were INSERTED
	ORDER BY a.transaction_time DESC OFFSET 1) n1	--OFFSET 1 to remove the latest UPDATE record (will be substituted by query1 result which gives the latest version in the database and not the latest version before or at n1.transaction_time)

	LEFT JOIN 

	(SELECT (populate_record(null::object.main_detail, b.new_record)).*, b.transaction_time FROM history.logged_actions AS b 
	WHERE b.table_name = 'main_detail'
	ORDER BY b.transaction_time) n2
	
	ON (n2.gid = n1.detail_id 
	AND n2.transaction_time = (SELECT max(transaction_time) FROM history.logged_actions WHERE table_name = 'main_detail' AND transaction_time <= n1.transaction_time))	--join only the records from main_detail that have the closest lesser transaction time to the selected main_detail_qualifier

	JOIN 
	object.main AS n3 
	ON (n3.gid = n2.object_id)
	
	UNION ALL

	--query3: query old_record column to get the DELETE records
	SELECT  
	n2.gid,
	n2.object_id,
	n3.resolution,
	n2.resolution2_id,
	n2.resolution3_id,
	n2.attribute_type_code,
	n2.attribute_value,
	n2.attribute_numeric_1,
	n2.attribute_numeric_2,
	n2.attribute_text_1,
	n2.the_geom,
	n1.qualifier_timestamp_1,
	n1.qualifier_timestamp_2,
	n1.transaction_time,
	n1.transaction_type
	FROM

	(SELECT (populate_record(null::object.main_detail_qualifier, a.old_record)).*, a.transaction_time, a.transaction_type FROM history.logged_actions AS a 
	WHERE a.table_name = 'main_detail_qualifier' AND a.old_record->'qualifier_timestamp_1'!='' AND a.transaction_type = 'D'
	OR a.table_name = 'main_detail_qualifier' AND a.old_record->'qualifier_timestamp_2'!='' AND a.transaction_type = 'D'
	ORDER BY a.old_record->'gid', a.transaction_time DESC) n1
	
	LEFT JOIN 

	(SELECT (populate_record(null::object.main_detail, b.old_record)).*, b.transaction_time FROM history.logged_actions AS b 
	WHERE b.table_name = 'main_detail'
	ORDER BY b.old_record->'gid', b.transaction_time DESC) n2

	ON (n2.gid = n1.detail_id 
	AND n2.transaction_time = (SELECT max(transaction_time) FROM history.logged_actions WHERE table_name = 'main_detail' AND transaction_time <= n1.transaction_time))	--join only the records from main_detail that have the closest lesser transaction time to the selected main_detail_qualifier

	JOIN 
	object.main AS n3 
	ON (n3.gid = n2.object_id)

	) n0 ORDER BY n0.gid, n0.transaction_time DESC;
END;
$BODY$ 
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION history.vtime_gethistory() IS $body$
This function searches history.logged_actions to get all real world changes with the corresponding latest version for each object primitive at each valid time.
$body$;


-----------------------------------------------
-- Add valid time query function (Intersect) --
-----------------------------------------------
CREATE OR REPLACE FUNCTION history.vtime_intersect(vtime_from text DEFAULT '0001-01-01 00:00:00', vtime_to text DEFAULT now()) 
RETURNS TABLE (
gid int,
object_id int,
resolution int,
resolution2_id int,
resolution3_id int,
attribute_type_code varchar,
attribute_value varchar,
attribute_numeric_1 numeric,
attribute_numeric_2 numeric,
attribute_text_1 varchar,
the_geom geometry,
valid_timestamp_1 timestamptz,
valid_timestamp_2 timestamptz
) AS
$BODY$
BEGIN
	RETURN QUERY SELECT DISTINCT ON (n0.gid) * FROM (
	
	-- query1: query old_record column to get UPDATE records which have a qualifier_timestamp_2 (these are real world changes)
	SELECT  
	n2.gid,
	n2.object_id,
	n3.resolution,
	n2.resolution2_id,
	n2.resolution3_id,
	n2.attribute_type_code,
	n2.attribute_value,
	n2.attribute_numeric_1,
	n2.attribute_numeric_2,
	n2.attribute_text_1,
	n2.the_geom,
	n1.qualifier_timestamp_1,
	n1.qualifier_timestamp_2
	FROM
	
	(SELECT DISTINCT ON (a.old_record->'gid') (populate_record(null::object.main_detail_qualifier, a.old_record)).*, a.transaction_time FROM history.logged_actions AS a 
	WHERE a.table_name = 'main_detail_qualifier' AND a.old_record->'qualifier_timestamp_1' <= vtime_to AND a.old_record->'qualifier_timestamp_2' >=vtime_from
	ORDER BY a.old_record->'gid', a.transaction_time DESC) n1	--select only the latest version of each object primitive

	LEFT JOIN 

	(SELECT (populate_record(null::object.main_detail, b.old_record)).*, b.transaction_time FROM history.logged_actions AS b 
	WHERE b.table_name = 'main_detail'
	ORDER BY b.old_record->'gid', b.transaction_time DESC) n2
	
	ON (n2.gid = n1.detail_id 
	AND n2.transaction_time = (SELECT max(transaction_time) FROM history.logged_actions WHERE table_name = 'main_detail' AND transaction_time <= n1.transaction_time))	--join only the records from main_detail that have the closest lesser transaction time to the selected main_detail_qualifier

	JOIN 
	object.main AS n3 
	ON (n3.gid = n2.object_id)

	UNION ALL

	-- query2: query new_record column to get the UPDATE and INSERT records which do not have a qualifier_timestamp_2 (these are still valid)
	SELECT  
	n2.gid,
	n2.object_id,
	n3.resolution,
	n2.resolution2_id,
	n2.resolution3_id,
	n2.attribute_type_code,
	n2.attribute_value,
	n2.attribute_numeric_1,
	n2.attribute_numeric_2,
	n2.attribute_text_1,
	n2.the_geom,
	n1.qualifier_timestamp_1,
	n1.qualifier_timestamp_2
	FROM
	
	(SELECT DISTINCT ON (a.new_record->'gid') (populate_record(null::object.main_detail_qualifier, a.new_record)).*, a.transaction_time FROM history.logged_actions AS a 
	WHERE a.table_name = 'main_detail_qualifier' AND a.new_record->'qualifier_timestamp_1' <= vtime_to	--select all records that were inserted before or at the end of the given valid time range
	ORDER BY a.new_record->'gid', a.transaction_time DESC) n1	--select only the latest version of each object primitive

	LEFT JOIN 
	
	(SELECT DISTINCT ON (b.new_record->'gid') (populate_record(null::object.main_detail, b.new_record)).*, b.transaction_time FROM history.logged_actions AS b 
	WHERE b.table_name = 'main_detail'
	ORDER BY b.new_record->'gid', b.transaction_time DESC) n2	--select only the latest version of each object primitive

	ON (n1.detail_id = n2.gid)
	
	JOIN 
	object.main AS n3 
	ON (n3.gid = n2.object_id)

	EXCEPT	-- return all records that are in the result of query1 and query2 but not in the result of query3. this effectively removes the DELETE records (query3) from the UPDATE and INSERT records (query1 and query2).

	--query3: query old_record column to get the DELETE records
	SELECT  
	n2.gid,
	n2.object_id,
	n3.resolution,
	n2.resolution2_id,
	n2.resolution3_id,
	n2.attribute_type_code,
	n2.attribute_value,
	n2.attribute_numeric_1,
	n2.attribute_numeric_2,
	n2.attribute_text_1,
	n2.the_geom,
	n1.qualifier_timestamp_1,
	n1.qualifier_timestamp_2
	FROM
	
	(SELECT DISTINCT ON (a.old_record->'gid') (populate_record(null::object.main_detail_qualifier, a.old_record)).*, a.transaction_time FROM history.logged_actions AS a 
	WHERE a.table_name = 'main_detail_qualifier' AND a.old_record->'qualifier_timestamp_2' < vtime_from AND a.transaction_type = 'D'	--exclude from select results the records that where deleted before the requested time range and have a qualifier_timestamp_2 (these are real world deletes)
	OR a.table_name = 'main_detail_qualifier' AND NOT defined(a.old_record,'qualifier_timestamp_2') AND a.transaction_type = 'D'	--exclude from select results the records that where deleted but have no qualifier_timestamp_2 (these are error deletes)
	ORDER BY a.old_record->'gid', a.transaction_time DESC) n1	--select only the latest version of each object primitive

	LEFT JOIN
	
	(SELECT DISTINCT ON (b.old_record->'gid') (populate_record(null::object.main_detail, b.old_record)).*, b.transaction_time FROM history.logged_actions AS b 
	WHERE b.table_name = 'main_detail'
	ORDER BY b.old_record->'gid', b.transaction_time DESC) n2	--select only the latest version of each object primitive
	
	ON (n1.detail_id = n2.gid)

	JOIN 
	object.main AS n3 
	ON (n3.gid = n2.object_id)

	) n0 ORDER BY n0.gid, n0.qualifier_timestamp_1 DESC;

END;
$BODY$ 
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION history.vtime_intersect(vtime_from text, vtime_to text) IS $body$
This function searches history.logged_actions to get the latest version of each object primitive whose valid time intersects with the queried timerange.

Arguments:
   vtime_from:	valid time from yyy-mm-dd hh:mm:ss
   vtime_to:	valid time to yyy-mm-dd hh:mm:ss
$body$;


--------------------------------------------
-- Add valid time query function (Inside) --
--------------------------------------------
CREATE OR REPLACE FUNCTION history.vtime_inside(vtime_from text DEFAULT '0001-01-01 00:00:00', vtime_to text DEFAULT now()) 
RETURNS TABLE (
gid int,
object_id int,
resolution int,
resolution2_id int,
resolution3_id int,
attribute_type_code varchar,
attribute_value varchar,
attribute_numeric_1 numeric,
attribute_numeric_2 numeric,
attribute_text_1 varchar,
the_geom geometry,
valid_timestamp_1 timestamptz,
valid_timestamp_2 timestamptz
) AS
$BODY$
BEGIN
	RETURN QUERY SELECT DISTINCT ON (n0.gid) * FROM (

	-- query old_record column to get all records which have a qualifier_timestamp_1 and qualifier_timestamp_2 (these are records whose lifespan ended)
	SELECT  
	n2.gid,
	n2.object_id,
	n3.resolution,
	n2.resolution2_id,
	n2.resolution3_id,
	n2.attribute_type_code,
	n2.attribute_value,
	n2.attribute_numeric_1,
	n2.attribute_numeric_2,
	n2.attribute_text_1,
	n2.the_geom,
	n1.qualifier_timestamp_1,
	n1.qualifier_timestamp_2
	FROM
	
	(SELECT DISTINCT ON (a.old_record->'gid') (populate_record(null::object.main_detail_qualifier, a.old_record)).*, a.transaction_time FROM history.logged_actions AS a 
	WHERE a.table_name = 'main_detail_qualifier' AND a.old_record->'qualifier_timestamp_1' >= vtime_from AND a.old_record->'qualifier_timestamp_2' <= vtime_to
	ORDER BY a.old_record->'gid', a.transaction_time DESC) n1	--select only the latest version of each object primitive

	LEFT JOIN 

	(SELECT (populate_record(null::object.main_detail, b.old_record)).*, b.transaction_time FROM history.logged_actions AS b 
	WHERE b.table_name = 'main_detail'
	ORDER BY b.old_record->'gid', b.transaction_time DESC) n2

	ON (n2.gid = n1.detail_id 
	AND n2.transaction_time = (SELECT max(transaction_time) FROM history.logged_actions WHERE table_name = 'main_detail' AND transaction_time <= n1.transaction_time))	--join only the records from main_detail that have the closest lesser transaction time to the selected main_detail_qualifier

	JOIN 
	object.main AS n3 
	ON (n3.gid = n2.object_id)

	) n0 ORDER BY n0.gid ASC;
END;
$BODY$ 
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION history.vtime_inside(vtime_from text, vtime_to text) IS $body$
This function searches history.logged_actions to get the latest version of each object primitive whose valid time is completely inside the queried timerange.

Arguments:
   vtime_from:	valid time from yyy-mm-dd hh:mm:ss
   vtime_to:	valid time to yyy-mm-dd hh:mm:ss
$body$;


--------------------------------------------
-- Add valid time query function (equal) --
--------------------------------------------
CREATE OR REPLACE FUNCTION history.vtime_equal(vtime_from text, vtime_to text) 
RETURNS TABLE (
gid int,
object_id int,
resolution int,
resolution2_id int,
resolution3_id int,
attribute_type_code varchar,
attribute_value varchar,
attribute_numeric_1 numeric,
attribute_numeric_2 numeric,
attribute_text_1 varchar,
the_geom geometry,
valid_timestamp_1 timestamptz,
valid_timestamp_2 timestamptz
) AS
$BODY$
BEGIN
	RETURN QUERY SELECT DISTINCT ON (n0.gid) * FROM (

	-- query old_record column to get UPDATE records which have a qualifier_timestamp_2 (these are real world changes)
	SELECT  
	n2.gid,
	n2.object_id,
	n3.resolution,
	n2.resolution2_id,
	n2.resolution3_id,
	n2.attribute_type_code,
	n2.attribute_value,
	n2.attribute_numeric_1,
	n2.attribute_numeric_2,
	n2.attribute_text_1,
	n2.the_geom,
	n1.qualifier_timestamp_1,
	n1.qualifier_timestamp_2
	FROM

	(SELECT DISTINCT ON (a.old_record->'gid') (populate_record(null::object.main_detail_qualifier, a.old_record)).*, a.transaction_time FROM history.logged_actions AS a 
	WHERE a.table_name = 'main_detail_qualifier' AND a.old_record->'qualifier_timestamp_1' = vtime_from AND a.old_record->'qualifier_timestamp_2' = vtime_to
	ORDER BY a.old_record->'gid', a.transaction_time DESC) n1	--select only the latest version of each object primitive

	LEFT JOIN 
	
	(SELECT (populate_record(null::object.main_detail, b.old_record)).*, b.transaction_time FROM history.logged_actions AS b 
	WHERE b.table_name = 'main_detail'
	ORDER BY b.old_record->'gid', b.transaction_time DESC) n2
	
	ON (n2.gid = n1.detail_id 
	AND n2.transaction_time = (SELECT max(transaction_time) FROM history.logged_actions WHERE table_name = 'main_detail' AND transaction_time <= n1.transaction_time))	--join only the records from main_detail that have the closest lesser transaction time to the selected main_detail_qualifier

	JOIN 
	object.main AS n3 
	ON (n3.gid = n2.object_id)

	) n0 ORDER BY n0.gid ASC;
END;
$BODY$ 
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION history.vtime_equal(vtime_from text, vtime_to text) IS $body$
This function searches history.logged_actions to get the latest version of each object primitive whose valid time range equals the queried timerange.

Arguments:
   vtime_from:	valid time from yyy-mm-dd hh:mm:ss
   vtime_to:	valid time to yyy-mm-dd hh:mm:ss
$body$;
*/
