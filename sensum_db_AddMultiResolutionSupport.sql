------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Name: SENSUM multi-resolution database support
-- Version: 0.9
-- Date: 19.06.14
-- Author: M. Wieland
-- DBMS: PostgreSQL9.2 / PostGIS2.0
-- Description: Adds the multi-resolution support to the basic SENSUM data model.
--		1. Create editable views for three resolution levels with basic table structure
--		2. Auto-update resolution ids based on spatial join between resolution levels
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------

-----------------------------------------
-- resolution 1 view (high resolution) --
-----------------------------------------
CREATE OR REPLACE VIEW object.ve_resolution1 AS
SELECT 
a.gid,
a.survey_gid,
a.description,
a.source,
a.resolution,
a.resolution2_id,
a.resolution3_id,
a.the_geom,
b.object_id,
b.attribute_type_code,
b.attribute_value,
b.attribute_numeric_1,
b.attribute_numeric_2,
b.attribute_text_1,
c.detail_id,
c.qualifier_type_code,
c.qualifier_value,
c.qualifier_numeric_1,
c.qualifier_text_1,
c.qualifier_timestamp_1,
c.qualifier_timestamp_2
FROM object.main AS a
JOIN object.main_detail AS b ON (a.gid = b.object_id)
JOIN object.main_detail_qualifier AS c ON (b.gid = c.detail_id)
WHERE a.resolution = 1
ORDER BY a.gid ASC;

-------------------------------------------
-- resolution 2 view (medium resolution) --
-------------------------------------------
CREATE OR REPLACE VIEW object.ve_resolution2 AS
SELECT 
a.gid,
a.survey_gid,
a.description,
a.source,
a.resolution,
a.resolution2_id,
a.resolution3_id,
a.the_geom,
b.object_id,
b.attribute_type_code,
b.attribute_value,
b.attribute_numeric_1,
b.attribute_numeric_2,
b.attribute_text_1,
c.detail_id,
c.qualifier_type_code,
c.qualifier_value,
c.qualifier_numeric_1,
c.qualifier_text_1,
c.qualifier_timestamp_1,
c.qualifier_timestamp_2
FROM object.main AS a
JOIN object.main_detail AS b ON (a.gid = b.object_id)
JOIN object.main_detail_qualifier AS c ON (b.gid = c.detail_id)
WHERE a.resolution = 2
ORDER BY a.gid ASC;

----------------------------------------
-- resolution 3 view (low resolution) --
----------------------------------------
CREATE OR REPLACE VIEW object.ve_resolution3 AS
SELECT 
a.gid,
a.survey_gid,
a.description,
a.source,
a.resolution,
a.resolution2_id,
a.resolution3_id,
a.the_geom,
b.attribute_type_code,
b.attribute_value,
b.attribute_numeric_1,
b.attribute_numeric_2,
b.attribute_text_1,
c.detail_id,
c.qualifier_type_code,
c.qualifier_value,
c.qualifier_numeric_1,
c.qualifier_text_1,
c.qualifier_timestamp_1,
c.qualifier_timestamp_2
FROM object.main AS a
JOIN object.main_detail AS b ON (a.gid = b.object_id)
JOIN object.main_detail_qualifier AS c ON (b.gid = c.detail_id)
WHERE a.resolution = 3
ORDER BY a.gid ASC;


-------------------------
-- make views editable --
-------------------------
CREATE OR REPLACE FUNCTION object.edit_resolution_views()
RETURNS TRIGGER AS 
$BODY$
BEGIN
      IF TG_OP = 'INSERT' THEN
       INSERT INTO object.main (gid, survey_gid, description, source, resolution, resolution2_id, resolution3_id, the_geom) VALUES (DEFAULT, NEW.survey_gid, NEW.description, NEW. source, NEW.resolution, NEW.resolution2_id, NEW.resolution3_id, NEW.the_geom);
       INSERT INTO object.main_detail (object_id, attribute_type_code, attribute_value, attribute_numeric_1, attribute_numeric_2, attribute_text_1) VALUES ((SELECT max(gid) FROM object.main), NEW.attribute_type_code, NEW.attribute_value, NEW.attribute_numeric_1, NEW.attribute_numeric_2, NEW.attribute_text_1);
       INSERT INTO object.main_detail_qualifier (detail_id, qualifier_type_code, qualifier_value, qualifier_numeric_1, qualifier_text_1, qualifier_timestamp_1, qualifier_timestamp_2) VALUES ((SELECT max(gid) FROM object.main_detail), NEW.qualifier_type_code, NEW.qualifier_value, NEW.qualifier_numeric_1, NEW.qualifier_text_1, NEW.qualifier_timestamp_1, NEW.qualifier_timestamp_2);
       RETURN NEW;
      ELSIF TG_OP = 'UPDATE' THEN
       UPDATE object.main SET gid=NEW.gid, survey_gid=NEW.survey_gid, description=NEW.description, source=NEW.source, resolution=NEW.resolution, resolution2_id=NEW.resolution2_id, resolution3_id=NEW.resolution3_id, the_geom=NEW.the_geom WHERE gid=OLD.gid;
       UPDATE object.main_detail SET attribute_type_code=NEW.attribute_type_code, attribute_value=NEW.attribute_value, attribute_numeric_1=NEW.attribute_numeric_1, attribute_numeric_2=NEW.attribute_numeric_2, attribute_text_1=NEW.attribute_text_1 WHERE object_id=OLD.gid;
       UPDATE object.main_detail_qualifier SET qualifier_type_code=NEW.qualifier_type_code, qualifier_value=NEW.qualifier_value, qualifier_numeric_1=NEW.qualifier_numeric_1, qualifier_text_1=NEW.qualifier_text_1, qualifier_timestamp_1=NEW.qualifier_timestamp_1, qualifier_timestamp_2=NEW.qualifier_timestamp_2 WHERE detail_id=OLD.gid;
       RETURN NEW;
      ELSIF TG_OP = 'DELETE' THEN
       DELETE FROM object.main_detail_qualifier WHERE detail_id IN (SELECT gid FROM object.main_detail WHERE object_id=OLD.gid);
       DELETE FROM object.main_detail WHERE object_id=OLD.gid;
       DELETE FROM object.main WHERE gid=OLD.gid;
       RETURN NULL;
      END IF;
      RETURN NEW;
END;
$BODY$ 
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION object.edit_resolution_views() IS $body$
This function makes the resolution views editable and forwards the edits to the underlying tables.
$body$;

DROP TRIGGER IF EXISTS resolution1_trigger ON object.ve_resolution1;
CREATE TRIGGER resolution1_trigger
    INSTEAD OF INSERT OR UPDATE OR DELETE ON object.ve_resolution1 
      FOR EACH ROW 
      EXECUTE PROCEDURE object.edit_resolution_views();

DROP TRIGGER IF EXISTS resolution2_trigger ON object.ve_resolution2;
CREATE TRIGGER resolution2_trigger
    INSTEAD OF INSERT OR UPDATE OR DELETE ON object.ve_resolution2 
      FOR EACH ROW 
      EXECUTE PROCEDURE object.edit_resolution_views();

DROP TRIGGER  IF EXISTS resolution3_trigger ON object.ve_resolution3;
CREATE TRIGGER resolution3_trigger
    INSTEAD OF INSERT OR UPDATE OR DELETE ON object.ve_resolution3 
      FOR EACH ROW 
      EXECUTE PROCEDURE object.edit_resolution_views();


-----------------------------------------------------------------------------------------
-- Link resolutions: Update once the resolution_ids in case some records already exist --
-----------------------------------------------------------------------------------------
-- Update resolution2_ids for resolution1 records based on spatial join
UPDATE object.main SET resolution2_id=a.resolution2_id 
  FROM (SELECT res2.gid AS resolution2_id, res1.gid AS resolution1_id FROM (SELECT gid, the_geom FROM object.main WHERE resolution=1) res1 
    LEFT JOIN (SELECT gid, the_geom FROM object.main WHERE resolution=2) res2 
    ON ST_Contains(res2.the_geom, (SELECT ST_PointOnSurface(res1.the_geom)))) AS a
WHERE object.main.gid=a.resolution1_id;

-- Update resolution3_ids for resolution1 records based on spatial join
UPDATE object.main SET resolution3_id=a.resolution3_id 
  FROM (SELECT res3.gid AS resolution3_id, res1.gid AS resolution1_id FROM (SELECT gid, the_geom FROM object.main WHERE resolution=1) res1
    LEFT JOIN (SELECT gid, the_geom FROM object.main WHERE resolution=3) res3 
    ON ST_Contains(res3.the_geom, (SELECT ST_PointOnSurface(res1.the_geom)))) AS a
WHERE object.main.gid=a.resolution1_id;

-- Update resolution3_ids for resolution2 records based on spatial join
UPDATE object.main SET resolution3_id=a.resolution3_id 
  FROM (SELECT res3.gid AS resolution3_id, res2.gid AS resolution2_id FROM (SELECT gid, the_geom FROM object.main WHERE resolution=2) res2
    LEFT JOIN (SELECT gid, the_geom FROM object.main WHERE resolution=3) res3 
    ON ST_Contains(res3.the_geom, (SELECT ST_PointOnSurface(res2.the_geom)))) AS a
WHERE object.main.gid=a.resolution2_id;


-----------------------------------------------------------------------------------------
-- Link resolutions: Update resolution_ids on INSERT and UPDATE (main.the_geom) --
-----------------------------------------------------------------------------------------
-- Trigger function and trigger to update resolution_ids for each INSERT and UPDATE OF the_geom ON object.main
CREATE OR REPLACE FUNCTION object.update_resolution_ids() 
RETURNS TRIGGER AS
$BODY$
BEGIN 
     IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN	
	-- Update resolution2_ids for resolution1 records based on spatial join
	UPDATE object.main SET resolution2_id=a.resolution2_id 
	  FROM (SELECT res2.gid AS resolution2_id, res1.gid AS resolution1_id FROM (SELECT gid, resolution2_id, resolution3_id, the_geom FROM object.main WHERE resolution=1) res1 
	    LEFT JOIN (SELECT gid, the_geom FROM object.main WHERE resolution=2) res2 
	    ON ST_Contains(res2.the_geom, (SELECT ST_PointOnSurface(res1.the_geom))) 
		WHERE res1.gid=NEW.gid	-- if resolution1 record is updated
		OR res1.resolution2_id=NEW.gid	-- if resolution2 record is updated
		OR res1.resolution3_id=NEW.gid	-- if resolution3 record is updated
		OR ST_Intersects(res1.the_geom, NEW.the_geom)	-- update ids also for resolution1 records that intersect with the newly updated resolution2 or resolution3 records
		) AS a
	WHERE object.main.gid=a.resolution1_id OR object.main.gid=NEW.gid AND NEW.resolution=1;

	-- Update resolution3_ids for resolution1 records based on spatial join
	UPDATE object.main SET resolution3_id=a.resolution3_id 
	  FROM (SELECT res3.gid AS resolution3_id, res1.gid AS resolution1_id FROM (SELECT gid, resolution2_id, resolution3_id, the_geom FROM object.main WHERE resolution=1) res1
	    LEFT JOIN (SELECT gid, the_geom FROM object.main WHERE resolution=3) res3 
	    ON ST_Contains(res3.the_geom, (SELECT ST_PointOnSurface(res1.the_geom))) 
		WHERE res1.gid=NEW.gid 
		OR res1.resolution2_id=NEW.gid 
		OR res1.resolution3_id=NEW.gid 
		OR ST_Intersects(res1.the_geom, NEW.the_geom)
		) AS a
	WHERE object.main.gid=a.resolution1_id OR object.main.gid=NEW.gid AND NEW.resolution=1;

	-- Update resolution3_ids for resolution2 records based on spatial join
	UPDATE object.main SET resolution3_id=a.resolution3_id 
	  FROM (SELECT res3.gid AS resolution3_id, res2.gid AS resolution2_id FROM (SELECT gid, resolution3_id, the_geom FROM object.main WHERE resolution=2) res2
	    LEFT JOIN (SELECT gid, the_geom FROM object.main WHERE resolution=3) res3 
	    ON ST_Contains(res3.the_geom, (SELECT ST_PointOnSurface(res2.the_geom))) 
		WHERE res2.gid=NEW.gid 
		OR res2.resolution3_id=NEW.gid 
		OR ST_Intersects(res2.the_geom, NEW.the_geom)
		) AS a
	WHERE object.main.gid=a.resolution2_id OR object.main.gid=NEW.gid AND NEW.resolution=2;
	
     RETURN NEW;

     ELSIF TG_OP = 'DELETE' THEN
	-- Update resolution2_ids for resolution1 records based on spatial join
	UPDATE object.main SET resolution2_id=a.resolution2_id 
	  FROM (SELECT res2.gid AS resolution2_id, res1.gid AS resolution1_id FROM (SELECT gid, resolution2_id, resolution3_id, the_geom FROM object.main WHERE resolution=1) res1 
	    LEFT JOIN (SELECT gid, the_geom FROM object.main WHERE resolution=2) res2 
	    ON ST_Contains(res2.the_geom, (SELECT ST_PointOnSurface(res1.the_geom))) 
		WHERE res1.resolution2_id=OLD.gid	-- if resolution2 record is deleted
		OR res1.resolution3_id=OLD.gid	-- if resolution3 record is deleted
		) AS a
	WHERE object.main.gid=a.resolution1_id;

	-- Update resolution3_ids for resolution1 records based on spatial join
	UPDATE object.main SET resolution3_id=a.resolution3_id 
	  FROM (SELECT res3.gid AS resolution3_id, res1.gid AS resolution1_id FROM (SELECT gid, resolution2_id, resolution3_id, the_geom FROM object.main WHERE resolution=1) res1
	    LEFT JOIN (SELECT gid, the_geom FROM object.main WHERE resolution=3) res3 
	    ON ST_Contains(res3.the_geom, (SELECT ST_PointOnSurface(res1.the_geom))) 
		WHERE res1.resolution2_id=OLD.gid 
		OR res1.resolution3_id=OLD.gid
		) AS a
	WHERE object.main.gid=a.resolution1_id;

	-- Update resolution3_ids for resolution2 records based on spatial join
	UPDATE object.main SET resolution3_id=a.resolution3_id 
	  FROM (SELECT res3.gid AS resolution3_id, res2.gid AS resolution2_id FROM (SELECT gid, resolution3_id, the_geom FROM object.main WHERE resolution=2) res2
	    LEFT JOIN (SELECT gid, the_geom FROM object.main WHERE resolution=3) res3 
	    ON ST_Contains(res3.the_geom, (SELECT ST_PointOnSurface(res2.the_geom))) 
		WHERE res2.gid=OLD.gid 
		OR res2.resolution3_id=OLD.gid 
		OR ST_Intersects(res2.the_geom, OLD.the_geom)
		) AS a
	WHERE object.main.gid=a.resolution2_id;
     
     RETURN NULL;

     END IF;
     RETURN NEW;
END;
$BODY$
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION object.update_resolution_ids() IS $body$
This function updates the resolution_ids for an object when its geometry is updated or an object is inserted or deleted.
$body$;

DROP TRIGGER IF EXISTS resolution_id_trigger ON object.main;
CREATE TRIGGER resolution_id_trigger
    AFTER INSERT OR UPDATE OF the_geom ON object.main 
      FOR EACH ROW 
      WHEN (pg_trigger_depth() = 1)	-- current nesting level of trigger (1 if called once from inside a trigger)
      EXECUTE PROCEDURE object.update_resolution_ids();

DROP TRIGGER IF EXISTS resolution_id_trigger_del ON object.main;
CREATE TRIGGER resolution_id_trigger_del
    AFTER DELETE ON object.main 
      FOR EACH ROW 
      WHEN (pg_trigger_depth() = 1)	-- current nesting level of trigger (1 if called once from inside a trigger)
      EXECUTE PROCEDURE object.update_resolution_ids();
