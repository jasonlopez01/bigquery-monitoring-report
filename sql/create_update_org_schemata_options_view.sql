CREATE OR REPLACE PROCEDURE `CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_REF`()
BEGIN

  DECLARE project_ids ARRAY<STRING>;
  DECLARE select_stmt STRING;
  DECLARE create_view_ddl STRING;

  -- Collect the distinct project IDs for the Org this project ID is in
  SET project_ids = (
    SELECT ARRAY_AGG(DISTINCT project_id)
    FROM  `INFO_SCHEMA_TABLE_STORAGE_REF`
    -- Exclude system-generated projects
    WHERE project_id not like 'sys-%'
  );

  -- Build one SELECT … UNION ALL … SELECT on SCHEMATA_OPTIONS for all Org Projects
  SET select_stmt = (
    SELECT STRING_AGG(
             FORMAT("""
               SELECT
                *
               FROM `%s.region-us`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
             """, project_id),
             ' UNION ALL ')
    FROM UNNEST(project_ids) AS project_id
  );

  -- Wrap it in a CREATE VIEW DDL statement
  SET create_view_ddl = FORMAT("""
    CREATE OR REPLACE VIEW `INFO_SCHEMA_SCHEMATA_OPTS_REF` AS
    %s
  """, select_stmt)
  ;

  -- Execute the DDL query to create the view with the dynamic union
  EXECUTE IMMEDIATE create_view_ddl;
END;