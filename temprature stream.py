from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql import functions as func

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
    
# Create DataFrame representing the stream of input lines from connection to localhost:9999
stream = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into room , temprature columns
df_room_temp_stream = stream.selectExpr("split(value, ' ')[0] as room","split(value, ' ')[1] as temp")

df_room_temp = df_room_temp_stream.selectExpr("CAST(room AS STRING)", "CAST(temp AS INT)")

#calculate average of temprature groub by temp

roomAVGtemp=df_room_temp.groupBy('room').avg('temp')



# Start running the query that prints the running counts to the console
query = roomAVGtemp \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()













































