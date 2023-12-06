import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node cleaned_data
cleaned_data_node1701426882060 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://db-project-staging-area/data/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_data_node1701426882060",
)

# Script generated for node cleaned_datatypes
cleaned_datatypes_node1701424536524 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://db-project-staging-area/datatype/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_datatypes_node1701424536524",
)

# Script generated for node cleaned_locations
cleaned_locations_node1701423647859 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://db-project-staging-area/location/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_locations_node1701423647859",
)

# Script generated for node cleaned_stationrelations
cleaned_stationrelations_node1701424728970 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options={
            "paths": ["s3://db-project-staging-area/stationrelation/"],
            "recurse": True,
        },
        transformation_ctx="cleaned_stationrelations_node1701424728970",
    )
)

# Script generated for node cleaned_stations
cleaned_stations_node1701424025970 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://db-project-staging-area/station/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_stations_node1701424025970",
)

# Script generated for node cleaned_dates
cleaned_dates_node1701412706836 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://db-project-staging-area/date/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_dates_node1701412706836",
)

# Script generated for node cleaned_locationcategories
cleaned_locationcategories_node1701419027713 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options={
            "paths": ["s3://db-project-staging-area/locationcategory/"],
            "recurse": True,
        },
        transformation_ctx="cleaned_locationcategories_node1701419027713",
    )
)

# Script generated for node stations_schema
stations_schema_node1701424273058 = ApplyMapping.apply(
    frame=cleaned_stations_node1701424025970,
    mappings=[
        ("id", "string", "id", "string"),
        ("name", "string", "name", "string"),
        ("latitude", "string", "latitude", "float"),
        ("longitude", "string", "longitude", "float"),
        ("elevation", "string", "elevation", "float"),
    ],
    transformation_ctx="stations_schema_node1701424273058",
)

# Script generated for node dates_schema
dates_schema_node1701416453438 = ApplyMapping.apply(
    frame=cleaned_dates_node1701412706836,
    mappings=[
        ("dateid", "string", "dateid", "int"),
        ("date", "string", "date", "date"),
        ("year", "string", "year", "int"),
        ("quarter", "string", "quarter", "int"),
        ("month", "string", "month", "int"),
        ("week", "string", "week", "int"),
        ("weekday", "string", "weekday", "int"),
        ("day_of_year", "string", "day_of_year", "int"),
        ("day", "string", "day", "int"),
        ("month_name", "string", "month_name", "string"),
        ("day_name", "string", "day_name", "string"),
        ("is_leap_year", "string", "is_leap_year", "string"),
    ],
    transformation_ctx="dates_schema_node1701416453438",
)

# Script generated for node load_data
load_data_node1701426917989 = glueContext.write_dynamic_frame.from_options(
    frame=cleaned_data_node1701426882060,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO noaaschema.data USING noaaschema.data_temp_58e70a ON data.dateid = data_temp_58e70a.dateid AND data.stationid = data_temp_58e70a.stationid AND data.datatypeid = data_temp_58e70a.datatypeid WHEN MATCHED THEN UPDATE SET datatypeid = data_temp_58e70a.datatypeid, value = data_temp_58e70a.value, stationid = data_temp_58e70a.stationid, dateid = data_temp_58e70a.dateid WHEN NOT MATCHED THEN INSERT VALUES (data_temp_58e70a.datatypeid, data_temp_58e70a.value, data_temp_58e70a.stationid, data_temp_58e70a.dateid); DROP TABLE noaaschema.data_temp_58e70a; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-004002991511-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "noaaschema.data_temp_58e70a",
        "connectionName": "NOAA Redshift Connection",
        "preactions": "CREATE TABLE IF NOT EXISTS noaaschema.data (datatypeid VARCHAR, value INTEGER, stationid VARCHAR, dateid INTEGER); DROP TABLE IF EXISTS noaaschema.data_temp_58e70a; CREATE TABLE noaaschema.data_temp_58e70a (datatypeid VARCHAR, value INTEGER, stationid VARCHAR, dateid INTEGER);",
    },
    transformation_ctx="load_data_node1701426917989",
)

# Script generated for node load_datatypes
load_datatypes_node1701424587530 = glueContext.write_dynamic_frame.from_options(
    frame=cleaned_datatypes_node1701424536524,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO noaaschema.datatypes USING noaaschema.datatypes_temp_e05e63 ON datatypes.id = datatypes_temp_e05e63.id WHEN MATCHED THEN UPDATE SET id = datatypes_temp_e05e63.id, name = datatypes_temp_e05e63.name WHEN NOT MATCHED THEN INSERT VALUES (datatypes_temp_e05e63.id, datatypes_temp_e05e63.name); DROP TABLE noaaschema.datatypes_temp_e05e63; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-004002991511-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "noaaschema.datatypes_temp_e05e63",
        "connectionName": "NOAA Redshift Connection",
        "preactions": "CREATE TABLE IF NOT EXISTS noaaschema.datatypes (id VARCHAR, name VARCHAR); DROP TABLE IF EXISTS noaaschema.datatypes_temp_e05e63; CREATE TABLE noaaschema.datatypes_temp_e05e63 (id VARCHAR, name VARCHAR);",
    },
    transformation_ctx="load_datatypes_node1701424587530",
)

# Script generated for node load_locations
load_locations_node1701423757311 = glueContext.write_dynamic_frame.from_options(
    frame=cleaned_locations_node1701423647859,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO noaaschema.locations USING noaaschema.locations_temp_ee1990 ON locations.id = locations_temp_ee1990.id WHEN MATCHED THEN UPDATE SET id = locations_temp_ee1990.id, name = locations_temp_ee1990.name WHEN NOT MATCHED THEN INSERT VALUES (locations_temp_ee1990.id, locations_temp_ee1990.name); DROP TABLE noaaschema.locations_temp_ee1990; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-004002991511-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "noaaschema.locations_temp_ee1990",
        "connectionName": "NOAA Redshift Connection",
        "preactions": "CREATE TABLE IF NOT EXISTS noaaschema.locations (id VARCHAR, name VARCHAR); DROP TABLE IF EXISTS noaaschema.locations_temp_ee1990; CREATE TABLE noaaschema.locations_temp_ee1990 (id VARCHAR, name VARCHAR);",
    },
    transformation_ctx="load_locations_node1701423757311",
)

# Script generated for node load_stationrelations
load_stationrelations_node1701424786461 = glueContext.write_dynamic_frame.from_options(
    frame=cleaned_stationrelations_node1701424728970,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO noaaschema.stationrelations USING noaaschema.stationrelations_temp_41681b ON stationrelations.stationid = stationrelations_temp_41681b.stationid AND stationrelations.locationid = stationrelations_temp_41681b.locationid AND stationrelations.locationcategoryid = stationrelations_temp_41681b.locationcategoryid WHEN MATCHED THEN UPDATE SET stationid = stationrelations_temp_41681b.stationid, locationid = stationrelations_temp_41681b.locationid, locationcategoryid = stationrelations_temp_41681b.locationcategoryid WHEN NOT MATCHED THEN INSERT VALUES (stationrelations_temp_41681b.stationid, stationrelations_temp_41681b.locationid, stationrelations_temp_41681b.locationcategoryid); DROP TABLE noaaschema.stationrelations_temp_41681b; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-004002991511-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "noaaschema.stationrelations_temp_41681b",
        "connectionName": "NOAA Redshift Connection",
        "preactions": "CREATE TABLE IF NOT EXISTS noaaschema.stationrelations (stationid VARCHAR, locationid VARCHAR, locationcategoryid VARCHAR); DROP TABLE IF EXISTS noaaschema.stationrelations_temp_41681b; CREATE TABLE noaaschema.stationrelations_temp_41681b (stationid VARCHAR, locationid VARCHAR, locationcategoryid VARCHAR);",
    },
    transformation_ctx="load_stationrelations_node1701424786461",
)

# Script generated for node load_locationcategories
load_locationcategories_node1701419241276 = glueContext.write_dynamic_frame.from_options(
    frame=cleaned_locationcategories_node1701419027713,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO noaaschema.locationcategories USING noaaschema.locationcategories_temp_6552db ON locationcategories.id = locationcategories_temp_6552db.id WHEN MATCHED THEN UPDATE SET id = locationcategories_temp_6552db.id, name = locationcategories_temp_6552db.name WHEN NOT MATCHED THEN INSERT VALUES (locationcategories_temp_6552db.id, locationcategories_temp_6552db.name); DROP TABLE noaaschema.locationcategories_temp_6552db; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-004002991511-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "noaaschema.locationcategories_temp_6552db",
        "connectionName": "NOAA Redshift Connection",
        "preactions": "CREATE TABLE IF NOT EXISTS noaaschema.locationcategories (id VARCHAR, name VARCHAR); DROP TABLE IF EXISTS noaaschema.locationcategories_temp_6552db; CREATE TABLE noaaschema.locationcategories_temp_6552db (id VARCHAR, name VARCHAR);",
    },
    transformation_ctx="load_locationcategories_node1701419241276",
)

# Script generated for node load_stations
load_stations_node1701424388679 = glueContext.write_dynamic_frame.from_options(
    frame=stations_schema_node1701424273058,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO noaaschema.stations USING noaaschema.stations_temp_17fd5b ON stations.id = stations_temp_17fd5b.id WHEN MATCHED THEN UPDATE SET id = stations_temp_17fd5b.id, name = stations_temp_17fd5b.name, latitude = stations_temp_17fd5b.latitude, longitude = stations_temp_17fd5b.longitude, elevation = stations_temp_17fd5b.elevation WHEN NOT MATCHED THEN INSERT VALUES (stations_temp_17fd5b.id, stations_temp_17fd5b.name, stations_temp_17fd5b.latitude, stations_temp_17fd5b.longitude, stations_temp_17fd5b.elevation); DROP TABLE noaaschema.stations_temp_17fd5b; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-004002991511-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "noaaschema.stations_temp_17fd5b",
        "connectionName": "NOAA Redshift Connection",
        "preactions": "CREATE TABLE IF NOT EXISTS noaaschema.stations (id VARCHAR, name VARCHAR, latitude REAL, longitude REAL, elevation REAL); DROP TABLE IF EXISTS noaaschema.stations_temp_17fd5b; CREATE TABLE noaaschema.stations_temp_17fd5b (id VARCHAR, name VARCHAR, latitude REAL, longitude REAL, elevation REAL);",
    },
    transformation_ctx="load_stations_node1701424388679",
)

# Script generated for node load_dates
load_dates_node1701413117781 = glueContext.write_dynamic_frame.from_options(
    frame=dates_schema_node1701416453438,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO noaaschema.dates USING noaaschema.dates_temp_1674ea ON dates.dateid = dates_temp_1674ea.dateid WHEN MATCHED THEN UPDATE SET dateid = dates_temp_1674ea.dateid, date = dates_temp_1674ea.date, year = dates_temp_1674ea.year, quarter = dates_temp_1674ea.quarter, month = dates_temp_1674ea.month, week = dates_temp_1674ea.week, weekday = dates_temp_1674ea.weekday, day_of_year = dates_temp_1674ea.day_of_year, day = dates_temp_1674ea.day, month_name = dates_temp_1674ea.month_name, day_name = dates_temp_1674ea.day_name, is_leap_year = dates_temp_1674ea.is_leap_year WHEN NOT MATCHED THEN INSERT VALUES (dates_temp_1674ea.dateid, dates_temp_1674ea.date, dates_temp_1674ea.year, dates_temp_1674ea.quarter, dates_temp_1674ea.month, dates_temp_1674ea.week, dates_temp_1674ea.weekday, dates_temp_1674ea.day_of_year, dates_temp_1674ea.day, dates_temp_1674ea.month_name, dates_temp_1674ea.day_name, dates_temp_1674ea.is_leap_year); DROP TABLE noaaschema.dates_temp_1674ea; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-004002991511-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "noaaschema.dates_temp_1674ea",
        "connectionName": "NOAA Redshift Connection",
        "preactions": "CREATE TABLE IF NOT EXISTS noaaschema.dates (dateid INTEGER, date DATE, year INTEGER, quarter INTEGER, month INTEGER, week INTEGER, weekday INTEGER, day_of_year INTEGER, day INTEGER, month_name VARCHAR, day_name VARCHAR, is_leap_year VARCHAR); DROP TABLE IF EXISTS noaaschema.dates_temp_1674ea; CREATE TABLE noaaschema.dates_temp_1674ea (dateid INTEGER, date DATE, year INTEGER, quarter INTEGER, month INTEGER, week INTEGER, weekday INTEGER, day_of_year INTEGER, day INTEGER, month_name VARCHAR, day_name VARCHAR, is_leap_year VARCHAR);",
    },
    transformation_ctx="load_dates_node1701413117781",
)

job.commit()
