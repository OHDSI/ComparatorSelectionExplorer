{DEFAULT @package_version = package_version}
{DEFAULT @version_number = '0.1.0'}

DELETE FROM @database_schema.@table_prefix@package_version;
INSERT INTO @database_schema.@table_prefix@package_version (version_number) VALUES ('@version_number');