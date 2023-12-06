import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node cleaned_data
cleaned_data_node1701646356772 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://db-project-staging-area/data/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_data_node1701646356772",
)

# Script generated for node cleaned_station
cleaned_station_node1701648360648 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://db-project-staging-area/station/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_station_node1701648360648",
)

# Script generated for node cleaned_date
cleaned_date_node1701648439503 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://db-project-staging-area/date/"],
        "recurse": True,
    },
    transformation_ctx="cleaned_date_node1701648439503",
)

# Script generated for node date filter
datefilter_node1701646427672 = Filter.apply(
    frame=cleaned_data_node1701646356772,
    f=lambda row: (row["dateid"] >= 20100101 and row["dateid"] <= 20221231),
    transformation_ctx="datefilter_node1701646427672",
)

# Script generated for node station_schema
station_schema_node1701648403935 = ApplyMapping.apply(
    frame=cleaned_station_node1701648360648,
    mappings=[
        ("id", "string", "id", "string"),
        ("latitude", "string", "latitude", "float"),
        ("longitude", "string", "longitude", "float"),
        ("elevation", "string", "elevation", "float"),
    ],
    transformation_ctx="station_schema_node1701648403935",
)

# Script generated for node date_schema
date_schema_node1701648469714 = ApplyMapping.apply(
    frame=cleaned_date_node1701648439503,
    mappings=[
        ("dateid", "string", "id", "int"),
        ("year", "string", "year", "int"),
        ("quarter", "string", "quarter", "int"),
        ("month", "string", "month", "int"),
        ("week", "string", "week", "int"),
        ("day_of_year", "string", "day_of_year", "int"),
        ("is_leap_year", "string", "is_leap_year", "string"),
    ],
    transformation_ctx="date_schema_node1701648469714",
)

# Script generated for node TMAX filter
TMAXfilter_node1701646507581 = Filter.apply(
    frame=datefilter_node1701646427672,
    f=lambda row: (bool(re.match("TMAX", row["datatypeid"]))),
    transformation_ctx="TMAXfilter_node1701646507581",
)

# Script generated for node TMAX drop
TMAXdrop_node1701648015712 = DropFields.apply(
    frame=TMAXfilter_node1701646507581,
    paths=["datatypeid"],
    transformation_ctx="TMAXdrop_node1701648015712",
)

# Script generated for node TMAX date join
TMAXdrop_node1701648015712DF = TMAXdrop_node1701648015712.toDF()
date_schema_node1701648469714DF = date_schema_node1701648469714.toDF()
TMAXdatejoin_node1701649642020 = DynamicFrame.fromDF(
    TMAXdrop_node1701648015712DF.join(
        date_schema_node1701648469714DF,
        (
            TMAXdrop_node1701648015712DF["dateid"]
            == date_schema_node1701648469714DF["id"]
        ),
        "left",
    ),
    glueContext,
    "TMAXdatejoin_node1701649642020",
)

# Script generated for node TMAX drop dateid
TMAXdropdateid_node1701649705888 = DropFields.apply(
    frame=TMAXdatejoin_node1701649642020,
    paths=["dateid", "id"],
    transformation_ctx="TMAXdropdateid_node1701649705888",
)

# Script generated for node TMAX station join
TMAXdropdateid_node1701649705888DF = TMAXdropdateid_node1701649705888.toDF()
station_schema_node1701648403935DF = station_schema_node1701648403935.toDF()
TMAXstationjoin_node1701649895276 = DynamicFrame.fromDF(
    TMAXdropdateid_node1701649705888DF.join(
        station_schema_node1701648403935DF,
        (
            TMAXdropdateid_node1701649705888DF["stationid"]
            == station_schema_node1701648403935DF["id"]
        ),
        "left",
    ),
    glueContext,
    "TMAXstationjoin_node1701649895276",
)

# Script generated for node TMAX drop stationid
TMAXdropstationid_node1701649952838 = DropFields.apply(
    frame=TMAXstationjoin_node1701649895276,
    paths=["stationid", "id"],
    transformation_ctx="TMAXdropstationid_node1701649952838",
)

# Script generated for node TMAX write
TMAXwrite_node1701650101681 = glueContext.write_dynamic_frame.from_options(
    frame=TMAXdropstationid_node1701649952838,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://db-project-ml-models/tmax/", "partitionKeys": []},
    transformation_ctx="TMAXwrite_node1701650101681",
)

job.commit()
