import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3
import datetime
import kaggle
import logging
import numpy as np
import os
import pandas as pd
import zipfile

# Declaracion de variables

bucket_name = f'prueba-nequi-yheminson'
data_set_1='riyapatel1697/imdb-official-movies-dataset'
name_data_set_1='imdb-official-movies-dataset.zip'
name_process='imdb'
path_1='./download/'
path_2='./destination/'
path_3='./log/'
path_4=f'./procesado_{name_process}/'
current_date = datetime.datetime.now()
access_key='AKIA6DEWUCC7WWLTEQ6Q'
secret_key='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'
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Manejo de logs
logging.basicConfig(filename=f'{path_3}log_{name_process}.txt', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

# Descarga DataSet

# Crear el directorio si no existe
try:
    os.makedirs(path_1)
    logging.info(f'Se creo el directorio {path_1}')
except:
    logging.info(f'No se crea el directorio {path_1} porque ya existe')

try:
    # Descargar archivos utilizando kaggle.api
    kaggle.api.dataset_download_files(data_set_1, path=path_1)
    logging.info(f'DataSet {name_data_set_1} descargados exitosamente.')
except Exception as e:
    logging.error(f"Error al descargar el DataSet {name_data_set_1}: {str(e)}")
    
# Descomprime el DataSet

# Crear el directorio si no existe
try:
    os.makedirs(path_2)
    logging.info(f'Se creo el directorio {path_2}')
except:
    logging.info(f'No se crea el directorio {path_2} porque ya existe')

try:
    with zipfile.ZipFile(path_1 + name_data_set_1, 'r') as data_set_comp:
        data_set_comp.extractall(path_2)
    logging.info(f'Se descomprimio el DataSet {name_data_set_1}')

except Exception as e:
    logging.error(f"Error al descomprimir el DataSet {name_data_set_1}: {str(e)}")
    
#Se borra el DataSet que se descargo y ya se descomprimio
try:
    os.remove(path_1 + name_data_set_1)
    logging.info(f'Se elimina el DataSet {name_data_set_1}')
    
except Exception as e:
    logging.error(f"Error al borrar el DataSet {name_data_set_1}: {str(e)}")

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
