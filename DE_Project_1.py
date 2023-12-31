# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Data

# COMMAND ----------

#create a spark session
from pyspark.sql.session import * 
SparkSession.builder.master("local").appName("test").config("test", "--").getOrCreate()

# COMMAND ----------

files = dbutils.fs.ls('/FileStore/tables/yelp_data')
display(files)

# COMMAND ----------

# %fs rm "dbfs:/FileStore/tables/yelp_data/business.csv"

# COMMAND ----------

# get the path list of all yelp datasets
json_files = [file.path for file in files if file.path.endswith('.json')]
print(json_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkin Table

# COMMAND ----------

checkin_raw = spark.read.json("dbfs:/FileStore/tables/yelp_data/yelp_academic_dataset_checkin.json")
display(checkin_raw)

# COMMAND ----------

from pyspark.sql.functions import split, explode, regexp_replace, to_timestamp

# split "date" with ","
checkin = checkin_raw.withColumn("dates", split("date", ", "))

# flattern "dates"
checkin = checkin.withColumn("checkin_dates", explode("dates")).select("business_id", "checkin_dates")

# remove --- before business_id
# checkin = checkin.withColumn("business_id", regexp_replace("business_id", "-", ""))

# change "checkin_dates" to DateType
checkin = checkin.withColumn("checkin_dates", to_timestamp("checkin_dates", "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(checkin)

# COMMAND ----------

# checkin.write.parquet("dbfs:/FileStore/tables/yelp_data/checkin_cleaned.parquet")
checkin.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/checkin_delta")
# (checkin.coalesce(1)
#        .write
#        .json("dbfs:/FileStore/tables/yelp_data/checkin_cleaned.json"))

# COMMAND ----------

# %fs rm -r "dbfs:/FileStore/tables/yelp_data/user_cleaned.parquet/"

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/tables/yelp_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tip Table

# COMMAND ----------

tip_raw = spark.read.json("dbfs:/FileStore/tables/yelp_data/yelp_academic_dataset_tip.json")
display(tip_raw)

# COMMAND ----------

tip = tip_raw.withColumn("tip_date", to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
display(tip)

# COMMAND ----------

from pyspark.sql.functions import col

total_count = tip.count()
distinct_count = tip.select("user_id").distinct().count()

if total_count > distinct_count:
    print("Duplicates exist")
else:
    print("No duplicates")


# COMMAND ----------

# tip.write.parquet("dbfs:/FileStore/tables/yelp_data/tip_cleaned.parquet")
tip.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/tip_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Table

# COMMAND ----------

reviewraw1 = spark.read.json("dbfs:/FileStore/tables/yelp_data/yelp_review_part_1.json")
reviewraw2 = spark.read.json("dbfs:/FileStore/tables/yelp_data/yelp_review_part_2.json")
reviewraw3 = spark.read.json("dbfs:/FileStore/tables/yelp_data/yelp_review_part_3.json")
review_raw = reviewraw1.union(reviewraw2).union(reviewraw3)

# COMMAND ----------

display(review_raw)

# COMMAND ----------

review_raw = review_raw.drop("_corrupt_record")
review_raw = review_raw.withColumn("review_date", to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
display(review_raw)

# COMMAND ----------

# drop Null for review_id
review = review_raw.filter(col("review_id").isNotNull())

# COMMAND ----------

# check duplicates of key col
total_count = review.count()
distinct_count = review.select("review_id").distinct().count()

if total_count > distinct_count:
    print("Duplicates exist")
else:
    print("No duplicates")

# COMMAND ----------

# from pyspark.sql.functions import count

# review_id_counts = review.groupBy("review_id").agg(count("*").alias("count"))

# duplicates = review_id_counts.filter("count > 1")

# # duplicated_rows = review.join(duplicates, "review_id")

# display(duplicates)
# # display(duplicated_rows)

# COMMAND ----------

# rename duplicate col name
columns_rename = ["cool",
                  "funny",
                  "stars",
                  "text",
                  "useful"]

for col_name in columns_rename:
    review = review.withColumnRenamed(col_name, "review_" + col_name)

display(review)

# COMMAND ----------

# %fs rm -r "dbfs:/FileStore/tables/yelp_data/review_delta"

# COMMAND ----------

# save to dbfs in delta format
review.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/review_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Table

# COMMAND ----------

userraw1 = spark.read.json("dbfs:/FileStore/tables/yelp_data/yelp_user_part_1.json")
userraw2 = spark.read.json("dbfs:/FileStore/tables/yelp_data/yelp_user_part_2.json")
# user_raw = userraw1.union(userraw2)

# display(user_raw)

# COMMAND ----------

userraw2 = userraw2.drop("_corrupt_record")

# COMMAND ----------

user_raw = userraw1.union(userraw2)
# user_raw = user_raw.withColumnRenamed("name", "user_name")
display(user_raw)

# COMMAND ----------

# check duplicates of key col
total_count = user_raw.count()
distinct_count = user_raw.select("user_id").distinct().count()

if total_count > distinct_count:
    print("Duplicates exist")
else:
    print("No duplicates")

# COMMAND ----------

# rename duplicate col name
columns_rename = ["cool", "funny", "name", "review_count", "useful"]

user = user_raw.withColumnRenamed("name", "user_name").withColumnRenamed("review_count", "user_review_count")

# for col_name in columns_rename:
#     user = user_raw.withColumnRenamed(col_name, "user_" + col_name)

display(user)

# COMMAND ----------

# %fs rm -r "dbfs:/FileStore/tables/yelp_data/user_delta"

# COMMAND ----------

user.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/user_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Table

# COMMAND ----------

business_raw = spark.read.json("dbfs:/FileStore/tables/yelp_data/yelp_academic_dataset_business.json")
display(business_raw)

# COMMAND ----------

from pyspark.sql.functions import col

# Drop Ambience/BestNights/BusinessParking/GoodForMeal/HairSpecializesIn/Music for later 
business = business_raw.select(
    col("business_id"),
    col("name").alias("business_name"),
    col('categories'),
    col("city"),
    col("state"),
    col("postal_code"),
    col("latitude"),
    col("longitude"),
    col("stars"),
    col("review_count"),
    col("is_open"),
    col("hours.Monday").alias("Monday"),
    col("hours.Tuesday").alias("Tuesday"),
    col("hours.Wednesday").alias("Wednesday"),
    col("hours.Thursday").alias("Thursday"),
    col("hours.Friday").alias("Friday"),
    col("hours.Saturday").alias("Saturday"),
    col("hours.Sunday").alias("Sunday"),
    col("attributes.AcceptsInsurance").alias("AcceptsInsurance"),
    col("attributes.AgesAllowed").alias("AgesAllowed"),
    col("attributes.Alcohol").alias("Alcohol"),
    col("attributes.BYOB").alias("BYOB"),
    col("attributes.BYOBCorkage").alias("BYOBCorkage"),
    col("attributes.BikeParking").alias("BikeParking"),
    col("attributes.BusinessAcceptsBitcoin").alias("BusinessAcceptsBitcoin"),
    col("attributes.BusinessAcceptsCreditCards").alias("BusinessAcceptsCreditCards"),
    col("attributes.ByAppointmentOnly").alias("ByAppointmentOnly"),
    col("attributes.Caters").alias("Caters"),
    col("attributes.CoatCheck").alias("CoatCheck"),
    col("attributes.Corkage").alias("Corkage"),
    col("attributes.DietaryRestrictions").alias("DietaryRestrictions"),
    col("attributes.DogsAllowed").alias("DogsAllowed"),
    col("attributes.DriveThru").alias("DriveThru"),
    col("attributes.GoodForDancing").alias("GoodForDancing"),
    col("attributes.GoodForKids").alias("GoodForKids"),
    col("attributes.HappyHour").alias("HappyHour"),
    col("attributes.HasTV").alias("HasTV"),
    col("attributes.NoiseLevel").alias("NoiseLevel"),
    col("attributes.Open24Hours").alias("Open24Hours"),
    col("attributes.OutdoorSeating").alias("OutdoorSeating"),
    col("attributes.RestaurantsAttire").alias("RestaurantsAttire"),
    col("attributes.RestaurantsCounterService").alias("RestaurantsCounterService"),
    col("attributes.RestaurantsDelivery").alias("RestaurantsDelivery"),
    col("attributes.RestaurantsGoodForGroups").alias("RestaurantsGoodForGroups"),
    col("attributes.RestaurantsPriceRange2").alias("RestaurantsPriceRange2"),
    col("attributes.RestaurantsReservations").alias("RestaurantsReservations"),
    col("attributes.RestaurantsTableService").alias("RestaurantsTableService"),
    col("attributes.RestaurantsTakeOut").alias("RestaurantsTakeOut"),
    col("attributes.Smoking").alias("Smoking"),
    col("attributes.WheelchairAccessible").alias("WheelchairAccessible"),
    col("attributes.WiFi").alias("WiFi")
)

display(business)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

def clean_string_column(column):
    # remove u''
    return regexp_replace(column, "(^u'|^'|'$)", "")

def clean_all_columns(df):
    # get all column name of df
    columns = df.columns
    for column in columns:
        # clean each column
        df = df.withColumn(column, clean_string_column(col(column)))
    return df

business = clean_all_columns(business)
display(business)

# COMMAND ----------

# from pyspark.sql.functions import col, when
# from pyspark.sql.types import BooleanType

# def convert_columns_to_boolean(df):
#     # convert True or False columns to boolean type
#     for column in df.columns:
#         # check col with only 'True' or 'False'
#         if df.select(column).distinct().rdd.flatMap(lambda x: x).collect() == ['True', 'False']:
#             df = df.withColumn(column, when(col(column) == "True", True).otherwise(False).cast(BooleanType()))
#     return df

# business = convert_columns_to_boolean(business)
# display(business)

# COMMAND ----------

# check duplicates of key col
total_count = business.count()
distinct_count = business.select("business_id").distinct().count()

if total_count > distinct_count:
    print("Duplicates exist")
else:
    print("No duplicates")

# COMMAND ----------

# %fs rm -r "dbfs:/FileStore/tables/yelp_data/business_delta"

# COMMAND ----------

business.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/business_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type II

# COMMAND ----------

# MAGIC %md
# MAGIC ## User

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp, when, to_timestamp

# user data 
# # assume the data was read in on 2023/12/24
user = user.withColumn("start_date", lit("2023-12-24 20:00:00.000")).withColumn("end_date", lit("9999-99-99")).withColumn("is_current", lit(True))
user = user.withColumn("start_date", to_timestamp("start_date"))

display(user)

# COMMAND ----------

# daily update table named "user_new"
# assume we update the name of user with user_id "-TT5e-YQU9xLb1JAGCGkQw"
condition = (col("user_id") == "-TT5e-YQU9xLb1JAGCGkQw")
user_new = user.withColumn("user_name", when(condition, "Daniel_update").otherwise(col("user_name")))

user_new = user_new.withColumn("is_current", lit(True)).withColumn("start_date", current_timestamp())
display(user_new)

# COMMAND ----------

from pyspark.sql.functions import col, when

# identify any change of user_new
# add more filter if need
changed_records = user.join(user_new, "user_id", "inner").filter(user["user_name"] != user_new["user_name"])

# get user_id in changed_records
user_ids = [row.user_id for row in changed_records.select("user_id").collect()]

# update start date and end date
user = user.withColumn("is_current", when(col("user_id").isin(user_ids), False).otherwise(col("is_current")))
user = user.withColumn("end_date", when(col("user_id").isin(user_ids), current_timestamp()).otherwise(col("end_date")))

user_update = user_new.filter(col("user_id").isin(user_ids))

display(user_update)

# COMMAND ----------

# user.unpersist()
# user_update.unpersist()
# # userDf.unpersist()

# COMMAND ----------

# user.printSchema()

# COMMAND ----------

# user_update.printSchema()

# COMMAND ----------

userDf = user_update.union(user)
display(userDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business

# COMMAND ----------

# business data 
# # assume the data was read in on 2023/12/24
business = business.withColumn("start_date", lit("2023-12-24 20:00:00.000")).withColumn("end_date", lit("9999-99-99")).withColumn("is_current", lit(True))
business = business.withColumn("start_date", to_timestamp("start_date"))

# COMMAND ----------

display(business)

# COMMAND ----------

# daily update table named "business_new"
# assume we update the business_name of business with business_id "96752mk7VlAUtWg8o02Tvw"
condition = (col("business_id") == "96752mk7VlAUtWg8o02Tvw")
business_new = business.withColumn("business_name", when(condition, "Center City Pediatrics_update").otherwise(col("business_name")))

business_new = business_new.withColumn("is_current", lit(True)).withColumn("start_date", current_timestamp())
display(business_new)

# COMMAND ----------

# identify any change of business_new
# add more filter if need
changed_records = business.join(business_new, "business_id", "inner").filter(business["business_name"] != business_new["business_name"])

# get business_id in changed_records
business_ids = [row.business_id for row in changed_records.select("business_id").collect()]

# update start date and end date
business = business.withColumn("is_current", when(col("business_id").isin(business_ids), False).otherwise(col("is_current")))
business = business.withColumn("end_date", when(col("business_id").isin(business_ids), current_timestamp()).otherwise(col("end_date")))

business_update = business_new.filter(col("business_id").isin(business_ids))

display(business_update)

# COMMAND ----------

businessDf = business_update.union(business)
display(businessDf)

# COMMAND ----------

# MAGIC %md
# MAGIC # Full Table

# COMMAND ----------

# select latest data
user_current = userDf.filter(col("is_current") == True)
user_current = user_current.drop("start_date", "end_date", "is_current")
business_current = businessDf.filter(col("is_current") == True)
business_current = business_current.drop("start_date", "end_date", "is_current")

# COMMAND ----------

fullDf = review.join(user_current, "user_id").join(business_current, "business_id")
display(fullDf)

# COMMAND ----------

fullDf.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/yelp_full_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC # For DA Team

# COMMAND ----------

da_df = fullDf.select("business_id",
               "state",
               "city",
               "review_id",
               "user_id",
               "review_date")
display(da_df)

# COMMAND ----------

from pyspark.sql.functions import date_format, hour, when
# for further date info 

# add "weekday" col
da_df = da_df.withColumn("weekday", date_format("review_date", "E"))

# add "time_of_day" col
da_df = da_df.withColumn("time_of_day", when(hour("review_date") < 12, "morning").otherwise("evening"))

display(da_df)

# COMMAND ----------



# COMMAND ----------

spark.stop()
