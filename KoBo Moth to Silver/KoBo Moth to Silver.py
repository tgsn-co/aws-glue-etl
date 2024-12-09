
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
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import to_timestamp
dyf_kobo_bronze = glueContext.create_dynamic_frame.from_catalog(database='tgsn_bronze', table_name='kobo_moth', 
    transformation_ctx="dyf_kobo_bronze")
spark_df_kobo = dyf_kobo_bronze.toDF()
spark_df_kobo.createOrReplaceTempView("kobo_bronze")
query_to_silver_test = '''
SELECT 
_id as ID
from tgsn_bronze.kobo_moth
'''
spark_df_kobo_silver_test = spark.sql(query_to_silver_test)
spark_df_kobo_silver_test.show()
query_to_silver = '''
SELECT 
_id as ID,
to_timestamp(start) as start_date,
to_timestamp(end) as end_date,
deviceid,
interviewer_username,
pre_interview_instructions,
camp_name,
gps_coordinates,
respondent_gender,
interview_introduction,
consent_understood,
end_interview_1,
consent_request,
accuracy_request,
consent_signature,
to_date(consent_date) as consent_date,
consent_rejection_reason,
end_interview_2,
end_interview_3,
respondent_name_first,
respondent_name_last,
respondent_name_other,
campidcard_yes_no,
campidcard_cardnumber,
campidcard_caseid,
to_date(respondent_birth_date) as respondent_birth_date,
respondent_birth_country,
respondent_birth_place,
respondent_nationality_first,
respondent_nationality_second,
respondent_nationality_differentatbirth_yes_no,
respondent_nationality_atbirth,
education,
prewar_residence_country,
prewar_residence_province,
prewar_residence_place,
prewar_occupation_yes_no,
prewar_occupation,
cast(departure_year as int) as departure_year,
departure_month,
cast(camp_arrival_year as int) as camp_arrival_year,
camp_arrival_month,
camp_occupation_yes_no,
camp_occupation,
camp_occupation_other,
health_physical_treated,
health_physical_untreated,
health_physical_symptoms,
health_psychological_treated,
health_psychological_untreated,
health_psychological_symptoms,
pss_interest,
family_introduction,
marital_status,
wives_number,
children_yes_no,
children_number,
other_relatives_yes_no,
relatives_number,
reintegration_preference,
reintegration_preference_reason,
photo_question,
photo_respondent,
photo_children,
interview_conclusion,
post_interview_instructions,
respondent_comfort,
respondent_comprehension,
interviewer_feedback_notes,
final_instructions,
to_date(_submission_time) as submission_time,
_submitted_by as submitted_by,
_attachments as attachments
FROM kobo_bronze
WHERE to_date(_submission_time) >= current_date()
order by _id
'''
spark_df_kobo_silver = spark.sql(query_to_silver)
spark_df_kobo_silver.select('id','start_date', 'respondent_name_first','submission_time').show()
# Script generated for node Amazon S3
additional_options = {}
tables_collection = spark.catalog.listTables("tgsn_silver")
table_names_in_db = [table.name for table in tables_collection]
table_exists = "kobo_moth" in table_names_in_db
if table_exists:
    spark_df_kobo_silver.sortWithinPartitions("submission_time") \
        .writeTo("glue_catalog.tgsn_silver.kobo_moth") \
        .tableProperty("format-version", "2") \
        .tableProperty("location", "s3://tgsn-silver-bucket/kobo/Moth/meta/tgsn_silver/kobo_moth") \
        .tableProperty("write.parquet.compression-codec", "gzip") \
        .options(**additional_options) \
.append()
else:
    spark_df_kobo_silver.writeTo("glue_catalog.tgsn_silver.kobo_moth") \
        .tableProperty("format-version", "2") \
        .tableProperty("location", "s3://tgsn-silver-bucket/kobo/Moth/meta/tgsn_silver/kobo_moth") \
        .tableProperty("write.parquet.compression-codec", "gzip") \
        .options(**additional_options) \
        .partitionedBy("submission_time") \
.create()
job.commit()