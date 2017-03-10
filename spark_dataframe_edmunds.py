# coding: utf-8

from pyspark.sql.functions import desc, explode, col
from pyspark import SparkConf, SparkContext
from pytz import timezone
from datetime import datetime

ALL_MODELS = spark.read.json("s3a://edmundsvehicle/2017/*/*/*/*")

ALL_MODELS.cache()
ALL_YEARS = ALL_MODELS.select(ALL_MODELS['id'], explode(ALL_MODELS['years']))

ALL_YEARS = ALL_YEARS.withColumn("year_id", ALL_YEARS['col'].getField("id"))
ALL_YEARS = ALL_YEARS.withColumn("year", ALL_YEARS['col'].getField("year"))
ALL_YEARS = ALL_YEARS.withColumn("styles", ALL_YEARS['col'].getField("styles"))

ALL_STYLES = ALL_YEARS.select(ALL_YEARS['id'], explode(ALL_YEARS['styles']))

ALL_STYLES = ALL_STYLES.withColumn("trim_id", ALL_STYLES['col'].getField("id"))
ALL_STYLES = ALL_STYLES.withColumn("name", ALL_STYLES['col'].getField("name"))
ALL_STYLES = ALL_STYLES.withColumn("submodel", ALL_STYLES['col'].getField("submodel"))
ALL_STYLES = ALL_STYLES.withColumn("trim", ALL_STYLES['col'].getField("trim"))

sqlCtx.registerDataFrameAsTable(ALL_MODELS, "ALL_MODELS")
sqlCtx.registerDataFrameAsTable(ALL_YEARS, "ALL_YEARS")
sqlCtx.registerDataFrameAsTable(ALL_STYLES, "ALL_STYLES")

# Save as parquet file in S3
ALL_MODELS.write.parquet("s3a://parquetedmundstables/allmodels", mode=('overwrite'))
ALL_YEARS.write.parquet("s3a://parquetedmundstables/allyears", mode=('overwrite'))
ALL_STYLES.write.parquet("s3a://parquetedmundstables/allstyles", mode=('overwrite'))

years_query = sqlCtx.sql("""SELECT m.id,
                     m.name,
                     y.year,
                     y.year_id
                    from ALL_MODELS m
                    INNER JOIN ALL_YEARS y on y.id = m.id
                    """)
styles_query = sqlCtx.sql("""SELECT m.id,
                     s.trim,
                     s.name as style_name,
                     submodel.body,
                    submodel.modelName as model_body_name
                    from ALL_MODELS m
                    INNER JOIN ALL_STYLES s on s.id = m.id
                    """)
APP_NAME = "Top 20 Vehicle Models by Years"


def main():
    try:
        # chart showing the models that have the most years
        top_models = years_query.groupBy(years_query['id']).count().sort(col("count").desc()).toPandas()

        # chart showing counts of body styles in the database
        body_style_counts = styles_query.groupBy(styles_query['body']).count().sort(col("count").desc()).toPandas()

        # get the current local time
        pacific = timezone('US/Pacific')
        pacific_time = datetime.now(pacific)
        top_html = body_style_counts.to_html() + top_models.to_html()

        html = "<!DOCTYPE html><html><body>{}</body></html>".format(top_html.encode('utf-8'))
        results = open("topVehicles.html", 'w')
        results.write('<div style="background-image:url(https://raw.githubusercontent.com/justwjr/Edmunds-Car-Data'
                      '-Pipeline-sdk-python/master/images/minato_firefox.png '
                      ');width:300px;height:200px;">')
        results.write("Last Updated: " + pacific_time.strftime('%A, %B, %d %Y %H:%M:%S') + " Pacific Time")
        results.write(
            "<hr>Hi!  This webpage shows counts of body styles in the database and the number of years for each "
            "model.<br>It serves as the front end of my Edmunds car data pipeline.  - Justin J. Wang<hr>")
        results.write(html)
        results.write('</div>')
        results.close()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    # Configure spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
main()


# """
#     :file: spark_dataframe_edmunds.py
#     :brief: This program uses spark to load json objects from S3,
#      creates Spark DataFrame in third normal form, then runs SQL
#      queries to summarize charts that are output in a .html file.
# """
# # In[18]:
#
# # http://ec2-54-159-219-64.compute-1.amazonaws.com:8888/notebooks/edmunds_spark_dataframe.ipynb#
#
#
# # ALL MODELS
# #  - ['errorType',
# #  'id',
# #  'makes',
# #  'makesCount',
# #  'message',
# #  'moreInfoUrl',
# #  'name',
# #  'niceName',
# #  'status',
# #  'time_stamp',
# #  'years']
# #
# # ALL YEARS
# #  - ['id', 'col', 'year_id', 'year', 'styles']
# #
# # ALL STYLES
# #  - ['id', 'col', 'trim_id', 'name', 'submodel', 'trim']
# #
# # ALL SUBMODELS
# #  - ['id', 'submodel.body', 'submodel.modelName', 'submodel.niceName']
#
# # # Edmunds Spark DataFrames
#
# # In[19]:
#
# # !sudo pip install pandas
#
# from datetime import datetime
#
# import sys
# import json
# import pandas as pd
# from pyspark.sql.functions import desc, explode, col
# from pyspark import SparkConf, SparkContext
# from pytz import timezone
#
# """
# # In[20]:
#
# # A way to read json files:
# # df_0 = spark.read.load("s3a://edmundsvehicle/2017/03/06/21/*", format="json")
#
# # # Create 3 tables that conform to third normal form:
# # ### MODEL, YEARS, STYLES
#
# # In[21]:
#
# # first work with a single json object
# MODEL = spark.read.json("s3a://edmundsvehicle/2017/03/06/21/*")
#
# # In[22]:
#
# MODEL.printSchema()
#
# # In[23]:
#
# # Create First DataFrame.
# # Let's test first our query.
#
# # years[0]
# MODEL.selectExpr('id').show()
#
# # In[24]:
#
# YEARS = MODEL.select(MODEL['id'], explode(MODEL['years']))
# YEARS.show()
#
# # In[25]:
#
# YEARS.printSchema()
#
# # In[26]:
#
# YEARS = YEARS.withColumn("year_id", YEARS['col'].getField("id"))
# YEARS = YEARS.withColumn("year", YEARS['col'].getField("year"))
# YEARS = YEARS.withColumn("styles", YEARS['col'].getField("styles"))
# YEARS.show()
#
# # In[27]:
#
# STYLES = YEARS.select(YEARS['id'], explode(YEARS['styles']))
# STYLES.show()
#
# # In[28]:
#
# STYLES.printSchema()
#
# # In[29]:
#
# STYLES = STYLES.withColumn("trim_id", STYLES['col'].getField("id"))
# STYLES = STYLES.withColumn("name", STYLES['col'].getField("name"))
# STYLES = STYLES.withColumn("submodel", STYLES['col'].getField("submodel"))
# STYLES = STYLES.withColumn("trim", STYLES['col'].getField("trim"))
# STYLES.show()
#
# # In[30]:
#
# # SUBMODELS = STYLES.select(STYLES['id'], STYLES.submodel.body, STYLES.submodel.modelName, STYLES.submodel.niceName)
# # SUBMODELS.show()
#
#
# # In[31]:
#
# # run a SQL query
#
# sqlCtx.registerDataFrameAsTable(MODEL, "MODEL")
# sqlCtx.registerDataFrameAsTable(YEARS, "YEARS")
# sqlCtx.registerDataFrameAsTable(STYLES, "STYLES")
# # sqlCtx.registerDataFrameAsTable(SUBMODELS, "SUBMODELS")
#
#
# # In[32]:
# """
# # df2 = sqlCtx.sql("""SELECT m.id, y.year from MODEL m
# #                     LEFT JOIN YEARS y on m.id = y.id""")
# # df2.collect()
# # df2.show()
#
# # Create Tables Using All the Models in the dataset
#
# # In[33]:
#
# # new_df = df.select(df['id'], explode(df['years']['years']))
#
#
# # In[34]:
#
# # We read all files:
# ALL_MODELS = spark.read.json("s3a://edmundsvehicle/2017/*/*/*/*")
#
# # In[35]:
#
# ALL_MODELS.cache()
# # type(ALL_MODELS)
#
# # In[36]:
#
# # df_all.selectExpr('id',
# #                   'niceName',
# #                   'time_stamp',
# #                   'years[0].id as model_year_id',
# #                   'years[0].styles[0].id as style_id',
# #                   'years[0].styles[0].name as style',
# #                   'years[0].styles[0].submodel.body as body',
# #                   'years[0].styles[0].submodel.modelName as modelName',
# #                   'years[0].styles[0].trim as trim').show(500)
# # df_all.selectExpr('years').show(500)
#
#
# # In[37]:
#
# # convert all to 3rd normal form schema
#
# ALL_YEARS = ALL_MODELS.select(ALL_MODELS['id'], explode(ALL_MODELS['years']))
#
# ALL_YEARS = ALL_YEARS.withColumn("year_id", ALL_YEARS['col'].getField("id"))
# ALL_YEARS = ALL_YEARS.withColumn("year", ALL_YEARS['col'].getField("year"))
# ALL_YEARS = ALL_YEARS.withColumn("styles", ALL_YEARS['col'].getField("styles"))
#
# ALL_STYLES = ALL_YEARS.select(ALL_YEARS['id'], explode(ALL_YEARS['styles']))
#
# ALL_STYLES = ALL_STYLES.withColumn("trim_id", ALL_STYLES['col'].getField("id"))
# ALL_STYLES = ALL_STYLES.withColumn("name", ALL_STYLES['col'].getField("name"))
# ALL_STYLES = ALL_STYLES.withColumn("submodel", ALL_STYLES['col'].getField("submodel"))
# ALL_STYLES = ALL_STYLES.withColumn("trim", ALL_STYLES['col'].getField("trim"))
#
# # ALL_SUBMODELS = ALL_STYLES.select(ALL_STYLES['id'], ALL_STYLES.submodel.body, ALL_STYLES.submodel.modelName)
#
# # register as SQL table
# sqlCtx.registerDataFrameAsTable(ALL_MODELS, "ALL_MODELS")
# sqlCtx.registerDataFrameAsTable(ALL_YEARS, "ALL_YEARS")
# sqlCtx.registerDataFrameAsTable(ALL_STYLES, "ALL_STYLES")
# # sqlCtx.registerDataFrameAsTable(ALL_SUBMODELS, "ALL_SUBMODELS")
#
# # # run a SQL query
# # test_query = sqlCtx.sql("""SELECT m.id, y.year from ALL_MODELS m
# #                     LEFT JOIN ALL_YEARS y on m.id = y.id
# #                     WHERE m.id is not NULL
# #                     LIMIT 5""")
# # test_query.collect()
# # test_query.show()
#
# # In[38]:
#
# # info_query = sqlCtx.sql("""SELECT m.id, y.year from ALL_MODELS m
# #                     LEFT JOIN ALL_YEARS y on m.id = y.id
# #                     WHERE m.id is not NULL
# #                     LIMIT 500""")
#
# # In[39]:
#
# # info_query.toPandas().tail()
#
# # In[40]:
#
# # query = sqlCtx.sql("""SELECT * from ALL_MODELS m""")
# # query.toPandas().head()
#
#
# # In[41]:
#
# # model years
#
# years_query = sqlCtx.sql("""SELECT m.id,
#                      m.name,
#                      y.year,
#                      y.year_id
#                     from ALL_MODELS m
#                     INNER JOIN ALL_YEARS y on y.id = m.id
#                     """)
# # model_years_df = years_query.toPandas()
# # model_years_df.tail(2)
#
# # In[42]:
#
# # model styles
#
# styles_query = sqlCtx.sql("""SELECT m.id,
#                      s.trim,
#                      s.name as style_name,
#                      submodel.body,
#                     submodel.modelName as model_body_name
#                     from ALL_MODELS m
#                     INNER JOIN ALL_STYLES s on s.id = m.id
#                     """)
# # model_styles_df = styles_query.toPandas()
# # model_styles_df.tail()
#
# # In[43]:
#
# # info_query = sqlCtx.sql("""SELECT b.id,
# #                     m.name,
# #                     y.year,
# #                     y.year_id,
# #                     s.trim,
# #                     s.name as style_name,
# #                     submodel.body,
# #                     submodel.modelName as model_body_name
# #                     from ALL_SUBMODELS b
# #                     INNER JOIN ALL_STYLES s on s.id = b.id
# #                     INNER JOIN ALL_YEARS y on y.id = b.id
# #                     INNER JOIN ALL_MODELS m on m.id = b.id""")
# # df = info_query.toPandas()
# # df
# # df.columns
#
# """
# # In[44]:
#
# years_query.groupBy(years_query['id']).count().sort(col("count").desc()).show()
#
# # In[45]:
#
# years_query.groupBy(years_query['id']).count().sort(col("count").desc()).toPandas()
#
# # In[46]:
#
# styles_query.groupBy(styles_query['body']).count().sort(col("count").desc()).show()
# """
# # In[48]:
#
# # # from time import localtime, strftime
# # # # datetime.datetime.now().strftime("%A, %d. %B %Y %I:%M%p")
# # # # from time import gmtime, strftime
# # # strftime("%a, %d %b %Y %H:%M:%S +0000", localtime())
#
# # from datetime import datetime, timedelta
# # from pytz import timezone
# # import pytz
# # utc = pytz.utc
# # utc.zone
#
# # pacific = timezone('US/Pacific')
# # pacific.zone
#
# # amsterdam = timezone('Europe/Amsterdam')
# # fmt = '%Y-%m-%d %H:%M:%S %Z%z'
#
# # loc_dt = pacific.localize(datetime.now())
# # print loc_dt.strftime(fmt)
#
# # # ams_dt = loc_dt.astimezone(amsterdam)
# # # ams_dt.strftime(fmt)
#
# # pacific = timezone('US/Pacific')
# # pacific_time = datetime.now(pacific)
# # print pacific_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')
# # print
# # pacific_time.strftime('%A, %B, %d %Y %H:%M:%S')
#
# # In[65]:
#
# # edmunds.py
# # Constants
# APP_NAME = "Top 20 Vehicle Models by Years"
#
#
# def main():
#     try:
#         # chart showing the models that have the most years
#         top_models = years_query.groupBy(years_query['id']).count().sort(col("count").desc()).toPandas()
#
#         # chart showing counts of body styles in the database
#         body_style_counts = styles_query.groupBy(styles_query['body']).count().sort(col("count").desc()).toPandas()
#
#         # get the current local time
#         pacific = timezone('US/Pacific')
#         pacific_time = datetime.now(pacific)
#         # print pacific_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')
#         #         print pacific_time.strftime('%A, %B, %d %Y %H:%M:%S')
#
#
#         # export to html
#         top_html = body_style_counts.to_html() + top_models.to_html()
#
#         html = "<!DOCTYPE html><html><body>{}</body></html>".format(top_html.encode('utf-8'))
#         results = open("topVehicles.html", 'w')
#         results.write("Last Updated: " + pacific_time.strftime('%A, %B, %d %Y %H:%M:%S') + " Pacific Time")
#         results.write(
#             "<hr>Hi!  This webpage shows counts of body styles in the database and the number of years for each "
#             "model.<br>It serves as the front end of my Edmunds car data pipeline.  - Justin J. Wang<hr>")
#         results.write(html)
#         results.close()
#
#     # top_RDD = sc.parallelize(hashtags_counts.top(20))
#     #         top_RDD.saveAsTextFile('s3a://sparksubmitresultsjw/results')
#
#     except Exception as e:
#         print(e)
#
#
# if __name__ == "__main__":
#     # Configure spark
#     conf = SparkConf().setAppName(APP_NAME)
#     conf = conf.setMaster("local[*]")
# # sc = SparkContext(conf=conf)
# # filename = sys.argv[1]
# # Execute Main functionality
# main()
#
#
# # In[123]:
#
# # https://github.com/justwjr/DSCI6007-student/blob/master/5.4%20-%20Spark%20Submit/steps%20for%20LAB.txt
#
# # run this .py file in local, after doing a "reverse scp" after getting .pem file into EMR
#
# # reverse scp, run command on local machine: scp -i ~/the_cloud.pem
# # hadoop@ec2-54-159-219-64.compute-1.amazonaws.com:~/topVehicles.html ~/OneDrive/Edmunds-Car-Data-Pipeline-sdk-python
# #  /Users/justw/OneDrive/Edmunds-Car-Data-Pipeline-sdk-python
#
# # # spark_results_boto.py
#
# # import ssl
# # import boto
# # from boto.s3.connection import S3Connection
#
# # def boto_upload_s3(html_file):
# #     conn = S3Connection()
# #     if hasattr(ssl, '_create_unverified_context'):
# #         ssl._create_default_https_context = ssl._create_unverified_context
# #     website_bucket = conn.get_bucket('edmundssparksubmitresults')
# #     output_file = website_bucket.new_key('top_models_spark.html')
# #     output_file.content_type = 'text/html'
# #     output_file.set_contents_from_string(html_file, policy='public-read')
#
# # if __name__ == '__main__':
# #     top = open("topVehicles.html", "r")
# #     html_file = top.read()
# #     boto_upload_s3(html_file)
# # top.close()
#
#
# # Steps
# #
# # 1) Setup EMR: Spark, 2xlarge, or just do m3.xlarge, 3 machines
# # 2) ssh: ssh -i ~/.ssh/the_cloud.pem hadoop@ec2-54-84-250-89.compute-1.amazonaws.com
# #
# # put URL of master node into web browser
# # Make sure that the security group for your master node allows connections on that port.
# # http://ec2-54-84-250-89.compute-1.amazonaws.com:8890
# #
# # 3) copy .py to emr
# #
# # 4) sudo pip install pandas
# #
# # 5) unset PYSPARK_DRIVER_PYTHON
# #
# # 6) create the S3 folder for results
# #
# # 7) spark-submit tweets.py --> top20hashtags.html
# #
# #
# # reverse scp
# # 8) scp from emr to local (top20hashtags)
# # run in local:
# #
# # scp -i ~/the_cloud.pem hadoop@ec2-54-84-250-89.compute-1.amazonaws.com:~/top20hashtags.html ~/OneDrive/…
# #
# # 9) run sparkresultsboto.py in local, which puts top20hashtags.html to s3 bucket
# #
# #
# # 10) enable static website hosting in s3: http://sparksubmitresults.s3-website-us-east-1.amazonaws.com/
# #
# # http://sparksubmitresultsjw.s3-website-us-west-1.amazonaws.com/
# #
# #
# # add policy in permissions
# # {
# # 	"Version": "2008-10-17",
# # 	"Statement": [
# # 		{
# # 			"Sid": "AllowPublicRead",
# # 			"Effect": "Allow",
# # 			"Principal": {
# # 				"AWS": "*"
# # 			},
# # 			"Action": [
# # 				"s3:GetObject"
# # 			],
# # 			"Resource": [
# # 				"arn:aws:s3:::edmundssparksubmitresults/*"
# # 			]
# # 		}
# # 	]
# # }
# # run python spark_results_boto.py in local to update static webpage
# # https://s3-us-west-1.amazonaws.com/edmundssparksubmitresults/topVehicles.html
