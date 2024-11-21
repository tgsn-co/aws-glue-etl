
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from datetime import datetime
import pandas as pd
from pyspark.sql.functions import to_timestamp

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# Read CSV files from KoBo landing layer
dyf_kobo_landing = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": "|"}, 
    connection_type="s3", format="csv", 
    connection_options={"paths": ["s3://tgsn-landing/dummy_kobo_data_landing/"], "recurse": True}, 
    transformation_ctx="Amaz|onS3")

dyf_kobo_landing.printSchema()
# dyf_kobo_landing = glueContext.create_dynamic_frame.from_catalog(database='tgsn_landing', table_name='kobo_data_landing')
# dyf_kobo_landing.printSchema()
spark_df_kobo = dyf_kobo_landing.toDF()
spark_df_kobo = spark_df_kobo.withColumn("_submission_time",to_timestamp("_submission_time"))
spark_df_kobo_today = spark_df_kobo.where(spark_df_kobo['_submission_time'] >= datetime.today().strftime('%Y-%m-%d'))
# spark_df_kobo.write.mode('overwrite') \
#         .format('parquet') \
#         .save('s3://tgsn-bronze/kobo/Moth/')
from awsglue.dynamicframe import DynamicFrame

# Convert Spark DataFrame to Glue DynamicFrame 
df_kobo = DynamicFrame.fromDF(spark_df_kobo_today, glueContext, "df_kobo")

s3output = glueContext.getSink(
  path="s3://tgsn-bronze/kobo/Moth/",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output"
)
s3output.setCatalogInfo(
  catalogDatabase="tgsn_bronze", catalogTableName="kobo_moth"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(df_kobo)
# 
job.commit()