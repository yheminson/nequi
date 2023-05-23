import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://prueba-nequi-yheminson/imdb/title-ratings.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1684727732141 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://prueba-nequi-yheminson/imdb/title-metadata.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1684727732141",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1684727822975 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1684727732141,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO public.imdb_title_metadata USING public.imdb_title_metadata_temp_76925c ON imdb_title_metadata.tconst = imdb_title_metadata_temp_76925c.tconst AND imdb_title_metadata.primaryTitle = imdb_title_metadata_temp_76925c.primaryTitle WHEN MATCHED THEN UPDATE SET tconst = imdb_title_metadata_temp_76925c.tconst, titletype = imdb_title_metadata_temp_76925c.titletype, primarytitle = imdb_title_metadata_temp_76925c.primarytitle, originaltitle = imdb_title_metadata_temp_76925c.originaltitle, isadult = imdb_title_metadata_temp_76925c.isadult, startyear = imdb_title_metadata_temp_76925c.startyear, endyear = imdb_title_metadata_temp_76925c.endyear, runtimeminutes = imdb_title_metadata_temp_76925c.runtimeminutes, genres = imdb_title_metadata_temp_76925c.genres WHEN NOT MATCHED THEN INSERT VALUES (imdb_title_metadata_temp_76925c.tconst, imdb_title_metadata_temp_76925c.titletype, imdb_title_metadata_temp_76925c.primarytitle, imdb_title_metadata_temp_76925c.originaltitle, imdb_title_metadata_temp_76925c.isadult, imdb_title_metadata_temp_76925c.startyear, imdb_title_metadata_temp_76925c.endyear, imdb_title_metadata_temp_76925c.runtimeminutes, imdb_title_metadata_temp_76925c.genres); DROP TABLE public.imdb_title_metadata_temp_76925c; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-968831013055-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.imdb_title_metadata_temp_76925c",
        "connectionName": "redshift",
        "preactions": "CREATE TABLE IF NOT EXISTS public.imdb_title_metadata (tconst VARCHAR, titletype VARCHAR, primarytitle VARCHAR, originaltitle VARCHAR, isadult VARCHAR, startyear VARCHAR, endyear VARCHAR, runtimeminutes VARCHAR, genres VARCHAR); DROP TABLE IF EXISTS public.imdb_title_metadata_temp_76925c; CREATE TABLE public.imdb_title_metadata_temp_76925c (tconst VARCHAR, titletype VARCHAR, primarytitle VARCHAR, originaltitle VARCHAR, isadult VARCHAR, startyear VARCHAR, endyear VARCHAR, runtimeminutes VARCHAR, genres VARCHAR);",
        "aws_iam_user": "AWSGlueServiceRole",
    },
    transformation_ctx="AmazonRedshift_node1684727822975",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node3 = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node1,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO public.imdb_title_rating USING public.imdb_title_rating_temp_bf7d2d ON imdb_title_rating.tconst = imdb_title_rating_temp_bf7d2d.tconst WHEN MATCHED THEN UPDATE SET tconst = imdb_title_rating_temp_bf7d2d.tconst, averagerating = imdb_title_rating_temp_bf7d2d.averagerating, numvotes = imdb_title_rating_temp_bf7d2d.numvotes WHEN NOT MATCHED THEN INSERT VALUES (imdb_title_rating_temp_bf7d2d.tconst, imdb_title_rating_temp_bf7d2d.averagerating, imdb_title_rating_temp_bf7d2d.numvotes); DROP TABLE public.imdb_title_rating_temp_bf7d2d; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-968831013055-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.imdb_title_rating_temp_bf7d2d",
        "connectionName": "redshift",
        "preactions": "CREATE TABLE IF NOT EXISTS public.imdb_title_rating (tconst VARCHAR, averagerating VARCHAR, numvotes INTEGER); DROP TABLE IF EXISTS public.imdb_title_rating_temp_bf7d2d; CREATE TABLE public.imdb_title_rating_temp_bf7d2d (tconst VARCHAR, averagerating VARCHAR, numvotes INTEGER);",
        "aws_iam_user": "arn:aws:iam::968831013055:role/service-role/AWSGlueServiceRole",
    },
    transformation_ctx="AmazonRedshift_node3",
)

job.commit()
