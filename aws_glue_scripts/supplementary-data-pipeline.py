import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node dates_data
dates_data_node1701331888630 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://db-project-supplementary-data/dates.csv"],
        "recurse": True,
    },
    transformation_ctx="dates_data_node1701331888630",
)

# Script generated for node datatypes_data
datatypes_data_node1701331881496 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://db-project-supplementary-data/datatypes.csv"],
        "recurse": True,
    },
    transformation_ctx="datatypes_data_node1701331881496",
)

# Script generated for node stationrelations_data
stationrelations_data_node1701331899509 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://db-project-supplementary-data/stationrelations.csv"],
        "recurse": True,
    },
    transformation_ctx="stationrelations_data_node1701331899509",
)

# Script generated for node stations_data
stations_data_node1701331880089 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://db-project-supplementary-data/stations.csv"],
        "recurse": True,
    },
    transformation_ctx="stations_data_node1701331880089",
)

# Script generated for node locations_data
locations_data_node1701330522736 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://db-project-supplementary-data/locations.csv"],
        "recurse": True,
    },
    transformation_ctx="locations_data_node1701330522736",
)

# Script generated for node locationcategories_data
locationcategories_data_node1701331898261 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": ",",
            "optimizePerformance": False,
        },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": ["s3://db-project-supplementary-data/locationcategories.csv"],
            "recurse": True,
        },
        transformation_ctx="locationcategories_data_node1701331898261",
    )
)

# Script generated for node datatypes_drop_fields
datatypes_drop_fields_node1701332061917 = DropFields.apply(
    frame=datatypes_data_node1701331881496,
    paths=["mindate", "maxdate", "datacoverage"],
    transformation_ctx="datatypes_drop_fields_node1701332061917",
)

# Script generated for node stations_drop_fields
stations_drop_fields_node1701331947903 = DropFields.apply(
    frame=stations_data_node1701331880089,
    paths=["elevationUnit", "mindate", "maxdate", "datacoverage"],
    transformation_ctx="stations_drop_fields_node1701331947903",
)

# Script generated for node locations_drop_fields
locations_drop_fields_node1701330850785 = DropFields.apply(
    frame=locations_data_node1701330522736,
    paths=["mindate", "maxdate", "datacoverage"],
    transformation_ctx="locations_drop_fields_node1701330850785",
)

# Script generated for node cleaned_dates
cleaned_dates_node1701332209445 = glueContext.write_dynamic_frame.from_options(
    frame=dates_data_node1701331888630,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://db-project-staging-area/date/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="cleaned_dates_node1701332209445",
)

# Script generated for node cleaned_stationrelations
cleaned_stationrelations_node1701332372313 = (
    glueContext.write_dynamic_frame.from_options(
        frame=stationrelations_data_node1701331899509,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": "s3://db-project-staging-area/stationrelation/",
            "partitionKeys": [],
        },
        format_options={"compression": "snappy"},
        transformation_ctx="cleaned_stationrelations_node1701332372313",
    )
)

# Script generated for node cleaned_locationcategories
cleaned_locationcategories_node1701332303129 = (
    glueContext.write_dynamic_frame.from_options(
        frame=locationcategories_data_node1701331898261,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": "s3://db-project-staging-area/locationcategory/",
            "partitionKeys": [],
        },
        format_options={"compression": "snappy"},
        transformation_ctx="cleaned_locationcategories_node1701332303129",
    )
)

# Script generated for node cleaned_datatypes
cleaned_datatypes_node1701332090491 = glueContext.write_dynamic_frame.from_options(
    frame=datatypes_drop_fields_node1701332061917,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://db-project-staging-area/datatype/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="cleaned_datatypes_node1701332090491",
)

# Script generated for node cleaned_stations
cleaned_stations_node1701331998154 = glueContext.write_dynamic_frame.from_options(
    frame=stations_drop_fields_node1701331947903,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://db-project-staging-area/station/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="cleaned_stations_node1701331998154",
)

# Script generated for node cleaned_locations
cleaned_locations_node1701330991921 = glueContext.write_dynamic_frame.from_options(
    frame=locations_drop_fields_node1701330850785,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://db-project-staging-area/location/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="cleaned_locations_node1701330991921",
)

job.commit()
