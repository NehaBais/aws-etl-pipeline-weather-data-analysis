DROP DATABASE IF EXISTS ncdccdo;
CREATE DATABASE ncdccdo;
USE ncdccdo;
SET GLOBAL local_infile=1;

CREATE TABLE IF NOT EXISTS location_stations (
    stationid   VARCHAR(64) NOT NULL,
    locationid  VARCHAR(64) NOT NULL,

    CONSTRAINT pk_ls  PRIMARY KEY (stationid, locationid)
);

LOAD DATA LOCAL INFILE "/Users/caman/Code/hobby-projects/ncdccdo/data/location_stations.csv"
INTO TABLE location_stations
COLUMNS
    TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    ESCAPED BY '\\'
LINES
    TERMINATED BY '\n'
IGNORE 1 LINES;