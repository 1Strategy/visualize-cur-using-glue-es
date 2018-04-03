#  Copyright 2018 1Strategy, LLC

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import datetime
import json
import re
import sys
from collections import deque

import boto3
import botocore
import elasticsearch
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from elasticsearch import ElasticsearchException, helpers
from elasticsearch.connection import create_ssl_context
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window

# Glue job init
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'report_folder_prefix', 'index_name_prefix_template', 'index_pattern_prefix', 'es_domain_url'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameter init
source_bucket = args['source_bucket']
report_folder_prefix = args['report_folder_prefix']
index_name_prefix_template = args['index_name_prefix_template']
index_pattern_prefix = args['index_pattern_prefix']
es_domain_url = args['es_domain_url']
es_domain_url_shared = sc.broadcast(es_domain_url)
succeed = sc.accumulator(0)
failed = sc.accumulator(0)
now = datetime.datetime.now()
index_name_base = index_name_prefix_template.format(str(now.year), str(now.month))
index_name = index_name_base + "-" + str(now.day)
index_name_shared = sc.broadcast(index_name)

def doc_generator(source):
    for row in source:
        updated_row = row.asDict()
        index_name = index_name_shared.value
        new_row = {
            '_index': index_name,
            '_type': 'cur',
            '_source': updated_row
        }

        yield new_row

def bulk_upload(records):
    context = create_ssl_context(cafile=None, capath=None, cadata=None)
    es_domain_url = es_domain_url_shared.value
    es = elasticsearch.Elasticsearch(
        [
            es_domain_url
        ],
        verify_certs=False,
        ssl_context=context
    )
    try:
        result = helpers.bulk(
            es,
            doc_generator(records),
            stats_only=True,
            raise_on_error=False,
            raise_on_exception=False,
            max_retries=1,
            initial_backoff=1,
            chunk_size=1000
        )
        succeed.add(result[0])
        failed.add(result[1])
    except ElasticsearchException as ex:
        print "bulk API error"
        print ex

def get_assembly_id(bucket_name, prefix):
    key = prefix + "CostAndUsageReport-Manifest.json"

    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket_name, key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content["assemblyId"]

def switch_alias():
    global es_domain_url
    global index_name
    global index_name_base
    global index_pattern_prefix
    alias_name = index_pattern_prefix + index_name_base
    context = create_ssl_context(cafile=None, capath=None, cadata=None)
    es = elasticsearch.Elasticsearch(
        [
            es_domain_url
        ],
        verify_certs=False,
        ssl_context=context
    )

    current_index_name = ""
    try:
        try:
            alias_info = es.get_alias(
                name=alias_name
            )
            current_index_name = alias_info.keys()[0]
        except:
            pass

        es.delete_alias(
            index=index_name_base + "*",
            name=alias_name
        )
        es.put_alias(
            index=index_name,
            name=alias_name
        )
    except ElasticsearchException as ex:
        raise ex

    if current_index_name != "":
        es.delete(
            index=current_index_name
        )

# Get latest report folder
next_month = (now.month + 1)%12 if (now.month + 1)%12 else 12
next_year = now.year if now.month < 12 else (now.year + 1)
date_folder = str(now.year)+str(now.month)+"01-"+str(next_year)+str(next_month)+"01"
assembly_id = get_assembly_id(source_bucket, report_folder_prefix + "/" + date_folder + "/")
report_folder = "s3://" + source_bucket + "/" + report_folder_prefix + "/" + date_folder + "/" + assembly_id + "/"
print report_folder

# Get all report partitions
s3_client = boto3.client(service_name='s3', region_name='us-west-2')
file_names = []
for o in s3_client.list_objects(Bucket=source_bucket, Prefix=report_folder)['Contents']:
    if o['key'].endswith(".gz"):
        file_names.append("s3://" + source_bucket + "/" + o['Key'])

dynamic_frame0 = glueContext.create_dynamic_frame.from_options(connection_type="s3", connection_options={"paths": file_names}, format="CSV", format_options={"withHeader": True}, transformation_ctx = "dynamic_frame0")
print datetime.datetime.now()
print "Finish loading csv"

df1 = dynamic_frame0.toDF()
print datetime.datetime.now()
print "Finish toDF"

for column in df1.columns:
    if ":" in column:
        df1 = df1.withColumnRenamed(column, column.replace(":", "_"))
    if bool(re.match('.*cost$', column, re.I)) or bool(re.match('.*rate$', column, re.I)) or bool(re.match('.*amount$', column, re.I)):
        df1 = df1.withColumn(column, df1[column].cast("float"))
print datetime.datetime.now()
print "Finish type transformation"

df1.foreachPartition(bulk_upload)
print datetime.datetime.now()
print "Finish upload"
print "Documents Uploaded:"
print succeed.value
print "Failed:"
print failed.value

switch_alias()

job.commit()
