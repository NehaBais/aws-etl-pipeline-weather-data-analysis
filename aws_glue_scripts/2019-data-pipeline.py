import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_now
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node raw_data
raw_data_node1701332725943 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "multiline": False,
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://noaa-ghcn-pds/csv/by_year/2019.csv"],
        "recurse": True,
    },
    transformation_ctx="raw_data_node1701332725943",
)

# Script generated for node cleaned_station_data
cleaned_station_data_node1701333323611 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://db-project-staging-area/station/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_station_data_node1701333323611",
)

# Script generated for node filter_datatype
filter_datatype_node1701333650075 = Filter.apply(
    frame=raw_data_node1701332725943,
    f=lambda row: (
        bool(re.match("TMAX", row["ELEMENT"]))
        or bool(re.match("TMIN", row["ELEMENT"]))
        or bool(re.match("PRCP", row["ELEMENT"]))
        or bool(re.match("SNOW", row["ELEMENT"]))
        or bool(re.match("SNWD", row["ELEMENT"]))
    ),
    transformation_ctx="filter_datatype_node1701333650075",
)

# Script generated for node drop_station_fields
drop_station_fields_node1701333858597 = DropFields.apply(
    frame=cleaned_station_data_node1701333323611,
    paths=["name", "latitude", "longitude", "elevation"],
    transformation_ctx="drop_station_fields_node1701333858597",
)

# Script generated for node realtime_change_names
realtime_change_names_node1701332992771 = ApplyMapping.apply(
    frame=filter_datatype_node1701333650075,
    mappings=[
        ("ID", "string", "stationid", "string"),
        ("DATE", "string", "dateid", "int"),
        ("ELEMENT", "string", "datatypeid", "string"),
        ("DATA_VALUE", "string", "value", "int"),
    ],
    transformation_ctx="realtime_change_names_node1701332992771",
)

# Script generated for node filter_on_station
filter_on_station_node1701333376561 = Join.apply(
    frame1=realtime_change_names_node1701332992771,
    frame2=drop_station_fields_node1701333858597,
    keys1=["stationid"],
    keys2=["id"],
    transformation_ctx="filter_on_station_node1701333376561",
)

# Script generated for node drop_extra_fields
drop_extra_fields_node1701333527436 = DropFields.apply(
    frame=filter_on_station_node1701333376561,
    paths=["id"],
    transformation_ctx="drop_extra_fields_node1701333527436",
)

# Script generated for node add_partition_key
add_partition_key_node1701405203932 = drop_extra_fields_node1701333527436.gs_now(
    colName="year", dateFormat="2019"
)

# Script generated for node cleaned_data
cleaned_data_node1701333173008 = glueContext.write_dynamic_frame.from_options(
    frame=add_partition_key_node1701405203932,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://db-project-staging-area/data/",
        "partitionKeys": ["year"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="cleaned_data_node1701333173008",
)

job.commit()
