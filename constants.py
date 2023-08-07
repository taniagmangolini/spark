from pyspark.sql.types import StructField, StructType, DateType, \
    StringType, IntegerType

# Row Schema definition as StructType
FLIGHT_SCHEMA_STRUCT = StructType([
    StructField("FL_DATE", DateType()),
    StructField("OP_CARRIER", StringType()),
    StructField("OP_CARRIER_FL_NUM", IntegerType()),
    StructField("ORIGIN", StringType()),
    StructField("ORIGIN_CITY_NAME", StringType()),
    StructField("DEST", StringType()),
    StructField("DEST_CITY_NAME", StringType()),
    StructField("CRS_DEP_TIME", IntegerType()),
    StructField("DEP_TIME", IntegerType()),
    StructField("WHEELS_ON", IntegerType()),
    StructField("TAXI_IN", IntegerType()),
    StructField("CRS_ARR_TIME", IntegerType()),
    StructField("ARR_TIME", IntegerType()),
    StructField("CANCELLED", IntegerType()),
    StructField("DISTANCE", IntegerType())
])

# Row Schema definition as DDL
FLIGHT_SCHEMA_DDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
      ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
      WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

# File types
CSV = 'csv'
PARQUET = 'parquet'
JSON = 'json'

# File modes
FAILFAST = 'FAILFAST'
PERMISSIVE = 'PERMISSIVE'
DROPMALFORMED = 'DROPMALFORMED'
