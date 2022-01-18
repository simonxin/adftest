from pyspark import SparkContext
from pyspark.sql import *

# drop the tables if they already exist
sc = SparkContext()
sqlContext = HiveContext(sc)
sqlContext.sql('drop table if exists hvacsampletable')
sqlContext.sql('drop table if exists hvac')

# Create an RDD from sample data
hvacText = sc.textFile("wasbs:///HdiSamples/HdiSamples/SensorSampleData/hvac/HVAC.csv")

# Create a schema for our data
Entry = Row('Date', 'Time', 'TargetTemp', 'ActualTemp', 'BuildingID')
# Parse the data and create a schema
hvacParts = hvacText.map(lambda s: s.split(',')).filter(lambda s: s[0] != 'Date')
hvac = hvacParts.map(lambda p: Entry(str(p[0]), str(p[1]), int(p[2]), int(p[3]), int(p[6])))

# Infer the schema and create a table       
hvacTable = sqlContext.createDataFrame(hvac)
hvacTable.registerTempTable('hvactemptable')
dfw = DataFrameWriter(hvacTable)
dfw.saveAsTable('hvac')