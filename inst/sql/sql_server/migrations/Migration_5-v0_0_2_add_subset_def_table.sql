{DEFAULT @table_prefix = ''}
{DEFAULT @subset_definition = subset_definition}

CREATE TABLE @schema.@table_prefix@subset_definition (
   definition_id int primary key,
   definition_name varchar(max),
   json varchar(max)
);