# -*- coding: utf-8 -*-
"""
Created on Tue Oct  6 10:05:57 2020

@author: gerson
"""

#lbrary to access bigquery
from google.cloud import bigquery
import requests, json
import json
#library to maniputale data
import pandas as pd
from urllib2 import urlopen
#read  bigquery as client
client = bigquery.Client()
query="""
#made a query   of  database
SELECT 
#DISTINCT SPLIT(a.name, ' ')[OFFSET(0)] AS name, c.alpha_2 as country
DISTINCT    REGEXP_REPLACE(NORMALIZE(SPLIT(a.name, ' ')[OFFSET(0)], NFD), r'\pM', '')  AS name, c.alpha_2 as country
FROM `impressive-hall-197914.OSIPTEL.base` a
FULL OUTER JOIN `impressive-hall-197914.segmentation.gender_dic` b
ON SPLIT(a.name, ' ')[OFFSET(0)] = UPPER(b.name)
LEFT JOIN `impressive-hall-197914.lab.country_codes` c
ON a.naci = c.alpha_3
WHERE b.name IS NULL
limit 50
# api > python > diccionario
 """
query_job = client.query(query)  # Make an API request.
print("The query data:")
#key cuy
myKey = "lqcFYPwtWcCUpwhTCz" # to be obtained from our gender-api account #key cuy
# my key
#myKey="AMAxQWXXyxuTcwQgYw"
#myKey="cxTNDaPYmsXZYSVoDJ"
# Define an empty list to store the predictions
names_list = []
#iterate  through result   of query  job
for row in query_job:
    #Row values can be accessed by field name or index.
    #made a request to api gender
    url= "https://gender-api.com/get?name="+row["name"]+"&country="+row["country"]+"&key="+myKey 
    response = urlopen(url)
   response = urlopen(url)
    decoded = response.read().decode('utf-8')
    data = json.loads(decoded)
    names_list.append([data["name"], data["gender"]])
#print(names_list[0])
#past list to dataframe
df = pd.DataFrame(names_list,columns=['name','gender'])
#pasto name  to  capital letters
df["name"]=df["name"].str.upper()
# TODO(developer): Set table_id to the ID of the table to create.
table_id = "impressive-hall-197914.segmentation.gender_dic"
#read table of bigquery  
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
    ],
  
)
#send  dataframe  to bigquery

job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)
# Wait for the load job to complete.
job.result()
#table = client.get_table(table_id)
#print("Loaded {} rows to table {}".format(table.num_rows, table_id))
