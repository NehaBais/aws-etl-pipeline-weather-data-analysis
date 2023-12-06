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
        "paths": ["s3://db-project-staging-area/data/year=2023/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_data_node1701426882060",
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

job.commit()
