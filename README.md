# aws-etl-pipeline-weather-data-analysis

Build ETL pipeline to ingest data from NOAA Global Historical Climatology Network Daily (GHCN-D) AWS S3 bucket. Write queries, inference script and build visualizations for analysis. Schedule jobs using Apache Airflow.

## data schema

```mermaid
---
Snowflake Schema for Climate Data
---
erDiagram
    Date {
        int dateid
        string date
        int year
        int quarter
        int month
        int week
        int weekday
        int day_of_year
        int day
        string month_name
        string day_name
        bool is_leap_year
    }

    DataType {
        string datatypeid
        string name
    }

    Station {
        string stationid
        string name
        float latitude
        float longitude
        float elevation
    }

    Location {
        string locationid
        string name
    }

    LocationCategory {
        string locationcategoryid
        string name
    }

    StationRelation {
        string stationid
        string locationid
        string locationcategoryid
    }

    Data {
        int dateid
        string stationid
        string datatypeid
        int value
    }

    Data }o--|| Date: dimension
    Data }o--|| DataType: dimension
    Data }o--|| Station: dimension

    Station ||--|{ StationRelation: "part of"
    Location ||--|{ StationRelation: "part of"
    LocationCategory ||--|{ StationRelation: "part of"
```

## pipeline diagram

![aws_data_pipeline.png](aws_data_pipeline.png)
