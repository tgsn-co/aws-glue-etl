
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import to_timestamp
dyf_kobo_bronze = glueContext.create_dynamic_frame.from_catalog(database='tgsn_bronze', table_name='kobo_moth')
dyf_kobo_bronze.printSchema()
spark_df_kobo = dyf_kobo_bronze.toDF()
spark_df_kobo.createOrReplaceTempView("kobo_bronze")
spark.sql("""SELECT 
_id as ID,
to_timestamp(start) as Start_Date,
to_timestamp(end) as End_Date,
Camp_Name,
Respondent_gender as Respondent_Gender,
Full_name,
to_date(What_is_your_date_of_birth) as Birth_Date,
cast(In_what_year_did_you_leave as int) as Year_Leave,
cast(In_what_month as int) as Month_Leave,
cast(Hight as double) as Hight,
cast(How_many_family_members_live_with_you as int) as No_Family_Members,
to_date(_submission_time) as Submission_Time
FROM kobo_bronze
WHERE to_date(_submission_time) >= current_date()
order by _id""").show()
query_to_silver = '''
SELECT 
_id as ID,
to_timestamp(start) as Start_Date,
to_timestamp(end) as End_Date,
Camp_Name,
Respondent_gender as Respondent_Gender,
Full_name,
to_date(What_is_your_date_of_birth) as Birth_Date,
cast(In_what_year_did_you_leave as int) as Year_Leave,
cast(In_what_month as int) as Month_Leave,
cast(Hight as double) as Hight,
cast(How_many_family_members_live_with_you as int) as No_Family_Members,
to_date(_submission_time) as Submission_Time
FROM kobo_bronze
WHERE to_date(_submission_time) >= current_date()
order by _id
'''
spark_df_kobo_silver = spark.sql(query_to_silver)
spark_df_kobo_silver.show()
from awsglue.dynamicframe import DynamicFrame

# Convert Spark DataFrame to Glue DynamicFrame 
df_kobo = DynamicFrame.fromDF(spark_df_kobo_silver, glueContext, "df_kobo")

s3output = glueContext.getSink(
  path="s3://tgsn-silver-bucket/kobo/Moth/",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output"
)
s3output.setCatalogInfo(
  catalogDatabase="tgsn_silver", catalogTableName="kobo_moth"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(df_kobo)
job.commit()