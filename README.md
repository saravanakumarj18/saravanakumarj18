--pyspark code for Aidetic assignment



from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.types import *
from math import radians, sin, cos, sqrt, atan2
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import create_map
#import folium 
#import os
#from folium import Map
#from folium.plugins import MarkerCluster




# creating spark session

spark = SparkSession.Builder().appName("Aidetic_assignment").master("local[*]").getOrCreate()

ip = 'C:\\Users\\sarav\\Documents\\study\\Aidetic_assignment\\data\\database.csv'

# 1) reading the csv file

df = spark.read.format("csv").option("header", True).load(ip)


#columns needs be coverted to the float data type as mentioned in assignment

columns_to_convert = ["Latitude", "Longitude", "Depth", "Magnitude"]

# Converting specific columns to the float data types as mentioned in assignment

for column in columns_to_convert:
    df = df.withColumn(column, col(column).cast("float"))
    
df.printSchema()



# converting the Date column(string) to Date datatype 

df1 = df.withColumn(
    "Date",
    when(col("Date").contains("-"), to_date(col("Date"), "MM-dd-yyyy"))
    .when(col("Date").contains("/"), to_date(col("Date"), "MM/dd/yyyy"))
    .otherwise(None)
)

# 2) converting Date and time into timestamp in a new column named Timestamp

earthquake_df = df1.withColumn("Timestamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")), "yyyy-MM-dd HH:mm:ss"))

# caching it

earthquake_df.cache()


# 3) filtering data having magnitude greater than 5

eq_filtered = earthquake_df.filter(col("Magnitude")>5.0)

# 4)  finding average depth and magnitude

avg_depth_and_magnitude = eq_filtered.groupBy(col("Type")). \
   agg(avg(col("Depth")).alias("avg_depth"),avg(col("Magnitude")).alias("avg_magnitude"))
   
avg_depth_and_magnitude.show()

   
# 5) implementing UDF to categorize the earthquake   

def earthquake_category(magnitude):
    if magnitude < 5:
        return 'Low'
    elif magnitude < 6:
        return 'Moderate'
    else:
        return 'High'
   

earthquake_udf = udf(earthquake_category,returnType=StringType())

eq_filtered = eq_filtered.withColumn("category", earthquake_udf(col("Magnitude")))

eq_filtered.show()



# 6) Calculate distance from reference location

reference_location = (0, 0)

#udf for calcultion
def distance_from_earth(lat, lon):
    ref_latitude = radians(reference_location[0])
    ref_longitude = radians(reference_location[1])
    actual_latitude  = radians(lat)
    actual_longitude = radians(lon)
    
    diff_latitude = actual_latitude - ref_latitude
    diff_longitude = actual_longitude - ref_longitude
    
    a = sin(diff_latitude/2)**2 + cos(ref_latitude)*cos(actual_latitude)* sin(diff_longitude/2)**2
    
    c = 2* atan2(sqrt(a),sqrt(1-a))
    
    distance  = 6371 * c #radius of earth
    
    return distance



distance_calculation_udf = udf(distance_from_earth) 

#adding a new column
eq_filtered = eq_filtered.withColumn("distance_from_reference",distance_calculation_udf(col("Latitude"),col("Longitude")))



eq_filtered.show()



# 7. Visualize the geographical distribution of earthquakes on a world map using Folium.
#note : I don't have folio in my local system, i ran it in databricks, so i commented the visualization part here 

"""

earthquake_map = folium.Map(location=[0, 0], zoom_start=2)

# Create a MarkerCluster to add markers for each earthquake
marker_cluster = MarkerCluster().add_to(earthquake_map)

# Iterate through the DataFrame and add markers to the map
for row in eq_filtered.collect():
    folium.Marker(
        location=[row["Latitude"], row["Longitude"]],
        popup=f"Magnitude: {row['Magnitude']}, Depth: {row['Depth']}",
        icon=None, 
    ).add_to(marker_cluster)

display(earthquake_map)

earthquake_map.save("earthquake_map.html")

"""

# 8) writing the file to the directory

eq_filtered.write.format("csv").option("header", True).mode("overwrite").save("C:\\Users\\sarav\\Documents\\study\\Aidetic_assignment\\data\\output\\")

# stopping sparksession
spark.stop()



