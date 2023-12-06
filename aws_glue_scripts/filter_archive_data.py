import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import re


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node raw_location_stations
raw_location_stations_node1701214533735 = glueContext.create_dynamic_frame.from_catalog(
    database="db-project-archive-data-db",
    table_name="location_stations_csv",
    transformation_ctx="raw_location_stations_node1701214533735",
)

# Script generated for node raw_locationcategories_locations
raw_locationcategories_locations_node1701215423521 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-project-archive-data-db",
        table_name="locationcategories_locations_csv",
        transformation_ctx="raw_locationcategories_locations_node1701215423521",
    )
)

# Script generated for node raw_datatypes
raw_datatypes_node1701197829562 = glueContext.create_dynamic_frame.from_catalog(
    database="db-project-archive-data-db",
    table_name="datatypes_csv",
    transformation_ctx="raw_datatypes_node1701197829562",
)

# Script generated for node raw_locations
raw_locations_node1701196733185 = glueContext.create_dynamic_frame.from_catalog(
    database="db-project-archive-data-db",
    table_name="locations_csv",
    transformation_ctx="raw_locations_node1701196733185",
)

# Script generated for node raw_dates
raw_dates_node1701218456944 = glueContext.create_dynamic_frame.from_catalog(
    database="db-project-archive-data-db",
    table_name="dates_csv",
    transformation_ctx="raw_dates_node1701218456944",
)

# Script generated for node raw_stations
raw_stations_node1701197827561 = glueContext.create_dynamic_frame.from_catalog(
    database="db-project-archive-data-db",
    table_name="stations_csv",
    transformation_ctx="raw_stations_node1701197827561",
)

# Script generated for node raw_locationcategories
raw_locationcategories_node1701215351761 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="db-project-archive-data-db",
        table_name="locationcategories_csv",
        transformation_ctx="raw_locationcategories_node1701215351761",
    )
)

# Script generated for node datatypes_drop_coverage
datatypes_drop_coverage_node1701213290377 = DropFields.apply(
    frame=raw_datatypes_node1701197829562,
    paths=["datacoverage"],
    transformation_ctx="datatypes_drop_coverage_node1701213290377",
)

# Script generated for node locations_drop_coverage
locations_drop_coverage_node1701213287930 = DropFields.apply(
    frame=raw_locations_node1701196733185,
    paths=["datacoverage"],
    transformation_ctx="locations_drop_coverage_node1701213287930",
)

# Script generated for node dates_filter_dates
dates_filter_dates_node1701218603227 = Filter.apply(
    frame=raw_dates_node1701218456944,
    f=lambda row: (row["year"] >= 2010 and row["year"] <= 2023),
    transformation_ctx="dates_filter_dates_node1701218603227",
)

# Script generated for node stations_drop_coverage
stations_drop_coverage_node1701214274802 = DropFields.apply(
    frame=raw_stations_node1701197827561,
    paths=["datacoverage", "elevationunit"],
    transformation_ctx="stations_drop_coverage_node1701214274802",
)

# Script generated for node datatypes_filter_dates
SqlQuery1196 = """
select * from datatypes
where
    mindate <= '2023-09-30' and
    maxdate >= '2010-01-01' and
    mindate <= '2010-01-01' and
    maxdate >= '2023-09-30'
"""
datatypes_filter_dates_node1701214082168 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1196,
    mapping={"datatypes": datatypes_drop_coverage_node1701213290377},
    transformation_ctx="datatypes_filter_dates_node1701214082168",
)

# Script generated for node locations_filter_dates
SqlQuery1201 = """
select * from locations
where
    mindate <= '2023-09-30' and
    maxdate >= '2010-01-01' and
    mindate <= '2010-01-01' and
    maxdate >= '2023-09-30'
"""
locations_filter_dates_node1701213624255 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1201,
    mapping={"locations": locations_drop_coverage_node1701213287930},
    transformation_ctx="locations_filter_dates_node1701213624255",
)

# Script generated for node stations_filter_dates
SqlQuery1199 = """
select * from stations
where
    mindate <= '2023-09-30' and
    maxdate >= '2010-01-01' and
    mindate <= '2010-01-01' and
    maxdate >= '2023-09-30'
"""
stations_filter_dates_node1701214130805 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1199,
    mapping={"stations": stations_drop_coverage_node1701214274802},
    transformation_ctx="stations_filter_dates_node1701214130805",
)

# Script generated for node datatypes_drop_dates
datatypes_drop_dates_node1701214398375 = DropFields.apply(
    frame=datatypes_filter_dates_node1701214082168,
    paths=["mindate", "maxdate"],
    transformation_ctx="datatypes_drop_dates_node1701214398375",
)

# Script generated for node locations_drop_dates
locations_drop_dates_node1701214215956 = DropFields.apply(
    frame=locations_filter_dates_node1701213624255,
    paths=["mindate", "maxdate"],
    transformation_ctx="locations_drop_dates_node1701214215956",
)

# Script generated for node stations_drop_dates
stations_drop_dates_node1701214475691 = DropFields.apply(
    frame=stations_filter_dates_node1701214130805,
    paths=["mindate", "maxdate"],
    transformation_ctx="stations_drop_dates_node1701214475691",
)

# Script generated for node filter_location_stations
SqlQuery1197 = """
SELECT ls.stationid, ls.locationid
FROM location_stations AS ls
JOIN locations AS l
    ON ls.locationid = l.id
JOIN stations AS s
    ON ls.stationid = s.id
"""
filter_location_stations_node1701214671844 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1197,
    mapping={
        "locations": locations_drop_dates_node1701214215956,
        "stations": stations_drop_dates_node1701214475691,
        "location_stations": raw_location_stations_node1701214533735,
    },
    transformation_ctx="filter_location_stations_node1701214671844",
)

# Script generated for node filter_locationcategories_locations
SqlQuery1200 = """
SELECT lcl.locationid, lcl.locationcategoryid
FROM locationcategorieslocations AS lcl
JOIN locations AS l
    ON lcl.locationid = l.id
JOIN locationcategories as lc
    ON lcl.locationcategoryid = lc.id
"""
filter_locationcategories_locations_node1701215500002 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1200,
    mapping={
        "locationcategories": raw_locationcategories_node1701215351761,
        "locationcategorieslocations": raw_locationcategories_locations_node1701215423521,
        "locations": locations_drop_dates_node1701214215956,
    },
    transformation_ctx="filter_locationcategories_locations_node1701215500002",
)

# Script generated for node stations_relations
SqlQuery1198 = """
SELECT ls.stationid, ls.locationid, lcl.locationcategoryid
FROM ls
JOIN lcl ON ls.locationid = lcl.locationid
"""
stations_relations_node1701218089554 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1198,
    mapping={
        "lcl": filter_locationcategories_locations_node1701215500002,
        "ls": filter_location_stations_node1701214671844,
    },
    transformation_ctx="stations_relations_node1701218089554",
)

# Script generated for node clean_locationcategories
clean_locationcategories_node1701217153279 = glueContext.getSink(
    path="s3://db-project-staging-area/filtered_archive_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="clean_locationcategories_node1701217153279",
)
clean_locationcategories_node1701217153279.setCatalogInfo(
    catalogDatabase="db-project-filtered-archive-data-db",
    catalogTableName="filtered_locationcategories",
)
clean_locationcategories_node1701217153279.setFormat("csv")
clean_locationcategories_node1701217153279.writeFrame(
    raw_locationcategories_node1701215351761
)
# Script generated for node clean_dates
clean_dates_node1701218712406 = glueContext.getSink(
    path="s3://db-project-staging-area/filtered_archive_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="clean_dates_node1701218712406",
)
clean_dates_node1701218712406.setCatalogInfo(
    catalogDatabase="db-project-filtered-archive-data-db",
    catalogTableName="filtered_dates",
)
clean_dates_node1701218712406.setFormat("csv")
clean_dates_node1701218712406.writeFrame(dates_filter_dates_node1701218603227)
# Script generated for node clean_datatypes
clean_datatypes_node1701215008228 = glueContext.getSink(
    path="s3://db-project-staging-area/filtered_archive_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="clean_datatypes_node1701215008228",
)
clean_datatypes_node1701215008228.setCatalogInfo(
    catalogDatabase="db-project-filtered-archive-data-db",
    catalogTableName="filtered_datatypes",
)
clean_datatypes_node1701215008228.setFormat("csv")
clean_datatypes_node1701215008228.writeFrame(datatypes_drop_dates_node1701214398375)
# Script generated for node clean_locations
clean_locations_node1701217650777 = glueContext.getSink(
    path="s3://db-project-staging-area/filtered_archive_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="clean_locations_node1701217650777",
)
clean_locations_node1701217650777.setCatalogInfo(
    catalogDatabase="db-project-filtered-archive-data-db",
    catalogTableName="filtered_locations",
)
clean_locations_node1701217650777.setFormat("csv")
clean_locations_node1701217650777.writeFrame(locations_drop_dates_node1701214215956)
# Script generated for node clean_stations
clean_stations_node1701217753700 = glueContext.getSink(
    path="s3://db-project-staging-area/filtered_archive_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="clean_stations_node1701217753700",
)
clean_stations_node1701217753700.setCatalogInfo(
    catalogDatabase="db-project-filtered-archive-data-db",
    catalogTableName="filtered_stations",
)
clean_stations_node1701217753700.setFormat("csv")
clean_stations_node1701217753700.writeFrame(stations_drop_dates_node1701214475691)
# Script generated for node clean_station_relations
clean_station_relations_node1701218304599 = glueContext.getSink(
    path="s3://db-project-staging-area/filtered_archive_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="clean_station_relations_node1701218304599",
)
clean_station_relations_node1701218304599.setCatalogInfo(
    catalogDatabase="db-project-filtered-archive-data-db",
    catalogTableName="filtered_station_relations",
)
clean_station_relations_node1701218304599.setFormat("csv")
clean_station_relations_node1701218304599.writeFrame(
    stations_relations_node1701218089554
)
job.commit()
