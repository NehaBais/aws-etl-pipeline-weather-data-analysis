import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node archived_data
archived_data_node1701332725943 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://noaa-ghcn-pds/csv/by_year"]},
    transformation_ctx="archived_data_node1701332725943",
)

# Script generated for node cleaned_station_data
cleaned_station_data_node1701333323611 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://db-project-staging-area/station/run-1701341055025-part-block-0-r-00000-snappy.parquet"
        ],
        "recurse": True,
    },
    transformation_ctx="cleaned_station_data_node1701333323611",
)

# Script generated for node archived_change_names
archived_change_names_node1701332992771 = ApplyMapping.apply(
    frame=archived_data_node1701332725943,
    mappings=[
        ("id", "string", "stationid", "string"),
        ("date", "string", "dateid", "long"),
        ("element", "string", "datatypeid", "string"),
        ("data_value", "string", "data_value", "long"),
    ],
    transformation_ctx="archived_change_names_node1701332992771",
)

# Script generated for node drop_station_fields
drop_station_fields_node1701333858597 = DropFields.apply(
    frame=cleaned_station_data_node1701333323611,
    paths=["name", "latitude", "longitude", "elevation"],
    transformation_ctx="drop_station_fields_node1701333858597",
)

# Script generated for node date_filter
date_filter_node1701342771881 = Filter.apply(
    frame=archived_change_names_node1701332992771,
    f=lambda row: (row["dateid"] >= 20100101 and row["dateid"] <= 20221231),
    transformation_ctx="date_filter_node1701342771881",
)

# Script generated for node filter_on_station
filter_on_station_node1701333376561 = Join.apply(
    frame1=date_filter_node1701342771881,
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

# Script generated for node filter_datatype
filter_datatype_node1701333650075 = Filter.apply(
    frame=drop_extra_fields_node1701333527436,
    f=lambda row: (
        bool(re.match("TMAX", row["datatypeid"]))
        or bool(re.match("TMIN", row["datatypeid"]))
        or bool(re.match("PRCP", row["datatypeid"]))
        or bool(re.match("SNOW", row["datatypeid"]))
        or bool(re.match("SNWD", row["datatypeid"]))
    ),
    transformation_ctx="filter_datatype_node1701333650075",
)

# Script generated for node cleaned_data
cleaned_data_node1701333173008 = glueContext.write_dynamic_frame.from_options(
    frame=filter_datatype_node1701333650075,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://db-project-staging-area/data/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="cleaned_data_node1701333173008",
)

job.commit()
