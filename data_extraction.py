
import yaml
from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import tabula
import requests
import numpy as np
import boto3 
import json


class DataExtractor:

    #def read_RDS_data(self):


    def read_rds_table(self, table_name):
        engine = DatabaseConnector().init_db_engine()
        df = pd.read_sql_table(table_name, engine, index_col='index')
        return df

    def retrieve_pdf_data(self, link):
        
        # Read remote pdf into list of DataFrame
        pdf_data = tabula.read_pdf(link, pages='all')#returns a list of data frames
    
        return pdf_data

    def  list_number_of_stores(self, number_of_stores_endpoint,  header_dict):
        
        no_of_stores=requests.get(url=number_of_stores_endpoint, headers=header_dict) #returns number of stores
        jsn=no_of_stores.json()#json file type dict
        return jsn['number_stores']

    def retrieve_stores_data(self, retrieve_a_store_endpoint, header_dict):
        retrieve_stores=requests.get(retrieve_a_store_endpoint, headers=header_dict)
        stores_data = retrieve_stores.json()
        df = pd.DataFrame(stores_data, index=[0])
     
        return df

    def store_dataframe(self):
        number_of_stores=self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',{'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
        list_of_dataframes=[]

        for store in range(number_of_stores):
            list_of_dataframes.append(self.retrieve_stores_data(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store}', {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}))
        
        #store_details=pd.concat(list_of_dataframes)

        store_data=pd.concat(list_of_dataframes)
        return  store_data

    def extract_from_s3(self,url):
        
        s3 = boto3.client('s3')
# 's3' is a key word. create connection to S3 using default config and all buckets within S3

        obj = s3.get_object(Bucket='data-handling-public' , Key='products.csv') 
        # get object and file (key) from bucket

        initial_df = pd.read_csv(obj['Body']) 

        #file=s3.download_file('data-handling-public', 'Documents', 'products.csv')
        products_dataframe = pd.read_csv(url)#read csv file in s3 bucket
        
        
        return initial_df
    
    def extract_s3_json(self):#JSON file containing the details of when each sale happened, as well as related attributes.
        """JSON file containing the details of when each sale happened, as well as related attributes.
        """
        url= 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        
     

         
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket='data-handling-public' , Key='date_details.json') # get object and file (key) from bucket

        dict_json = pd.read_json(obj['Body']) 
    
        return dict_json
        






  



if __name__ == '__main__':
    #print(DataExtractor().read_rds_table('orders_table'))
    
    #print(DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"))#can index each page
    #print(DataExtractor().extract_rds_table('legacy_users').head(20))
    #sales['cost'] = sales['cost'].astype('float64')
    #print(df.isna().sum())
    #pd.reset_option('max_columns')
    #print(df['card_number'].astype("int64"))
    #duplicated=df.duplicated(subset=['date_uuid', 'first_name', 'last_name', 'user_uuid', 'card_number', 'product_code'], keep=False)
    #print(df[duplicated])
    #print(df.duplicated().sum())
    #print(df.dropna(axis=0, subset=None, inplace=False))
    #df[['date_of_birth','join_date']] = df[['date_of_birth','join_date']].apply(pd.to_datetime)
   # print(DataExtractor().store_dataframe(DataExtractor().list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',{'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})))
    #number_of_stores=DataExtractor().list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',{'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})

    #print(DataExtractor().extract_from_s3('s3://data-handling-public/products.csv'))

    print(DataExtractor().extract_s3_json())