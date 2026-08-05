-- Reviewed Unity Catalog privilege preflight.
-- Execute as a platform administrator; this script never self-grants.
SHOW GRANTS ON CATALOG ${catalog_name};
SHOW GRANTS ON SCHEMA ${profile_catalog}.${profile_schema};
SHOW GRANTS ON TABLE ${metadata_inventory_table};
SHOW GRANTS ON TABLE ${profile_catalog}.${profile_schema}.profile_table_profiles;
