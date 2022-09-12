--DROP DATABASE s3_to_snowflake;
-- Create dabase
CREATE DATABASE IF NOT EXISTS s3_to_snowflake;
USE s3_to_snowflake;

-- Create table
CREATE OR REPLACE TABLE public.iris_dataset(
    id             number(2),
    sepal_lenght   number(3,1),
    sepal_width    number(3,1),
    petal_lenght   number(3,1),
    petal_width    number(3,1),
    species        varchar(20)
    
);

-- Create file format
CREATE OR REPLACE FILE FORMAT csv_files
    type = csv
    field_delimiter = ','
    skip_header = 1
    field_optionally_enclosed_by = '"'
    null_if = ('NULL', 'null')
    empty_field_as_null = true;
    
-- Create external stage
CREATE OR REPLACE STAGE public.snow_stage
    storage_integration = s3_integration
    url = 's3://kevininidestination'
    file_format = csv_files;

-- Create snowpipe
CREATE OR REPLACE PIPE public.spa
auto_ingest = true AS COPY INTO s3_to_snowflake.public.iris_dataset FROM 
@s3_to_snowflake.public.snow_stage
file_format = (format_name = csv_files);

show pipes;

list @snow_stage;

select * from iris_dataset