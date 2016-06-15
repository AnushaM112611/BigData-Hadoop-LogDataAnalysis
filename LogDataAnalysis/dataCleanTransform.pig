/*	This script is to remove the empty and null values to load data to a hive table */

--The below jars are used for invoking Regex operations on the data to extract time related measures
REGISTER 'piggybank-0.15.0.jar';
REGISTER 'joda-time-2.9.2.jar';
REGISTER 'hive-hcatalog-core-1.1.0-cdh5.5.1.jar';

--Loading input data
rawData = LOAD '$input'
USING org.apache.pig.piggybank.storage.MyRegExLoader('(^[\\d.]+) (\\-) (\\-) \\[(\\d\\d\\/\\w+\\/\\d+\\:\\d+\\:\\d+\\:\\d+\\s\\-\\d+)\\] \\"(\\w+) \\/(.*|\\w+\\.\\w+) (\\w+\\/\\d\\.\\d)\\" (\\d+) (\\d+) \\"(\\-)\\" \\"(.+)\\".*') 
AS 
(originatingip:chararray , clientidentity:chararray , userid:chararray , timestamp:chararray , requesttype:chararray , 
requestpage:chararray , httpprotocolversion:chararray , responsecode:int , responsesize:int , referrer:chararray , useragent:chararray);


--Filter the null values and empty values
filteredData = filter rawData by 
originatingip is not null AND clientidentity is not null AND userid is not null  AND timestamp is not null  AND 
requesttype is not null  AND requestpage neq ''  AND httpprotocolversion is not null  AND responsecode is not null  AND 
responsesize is not null  AND referrer is not null  AND useragent is not null;

--Model the data to make it ready to load into Hive
modelledData = foreach filteredData generate
originatingip, clientidentity, userid, 
requesttype, requestpage, httpprotocolversion, responsecode,
responsesize, referrer, REGEX_EXTRACT(useragent , '(.*$browserRegEx.*)', 1) as (browser:chararray),
REGEX_EXTRACT(timestamp , '(.*$zoneRegEx.*)' , 1) as (zone: chararray),
GetYear(ToDate(timestamp , 'dd/MMM/yyyy:HH:mm:ss Z')) as (year:int),
GetMonth(ToDate(timestamp , 'dd/MMM/yyyy:HH:mm:ss Z')) as (month:int),
GetWeek(ToDate(timestamp , 'dd/MMM/yyyy:HH:mm:ss Z')) as (week:int),
GetDay(ToDate(timestamp , 'dd/MMM/yyyy:HH:mm:ss Z')) as (day:int),
GetHour(ToDate(timestamp , 'dd/MMM/yyyy:HH:mm:ss Z')) as (hour:int);


--Store data into Hive table
STORE modelledData into 'click_stream.staging_table' using org.apache.hive.hcatalog.pig.HCatStorer();
