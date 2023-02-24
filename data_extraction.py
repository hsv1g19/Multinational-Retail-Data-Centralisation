
from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import boto3 


class DataExtractor:


    def read_rds_table(self, table_name):
        """_summary: will extract the database table to a pandas DataFrame.
            It will take in an instance of your DatabaseConnector 
            class and the table name as an argument and return a pandas DataFrame._

        Parameters
        ----------
        table_name : _type: string_
        Returns
        -------
        _type: pandas dataframe
        """
        engine = DatabaseConnector().init_db_engine()
        df = pd.read_sql_table(table_name, engine, index_col='index')
        return df

    def retrieve_pdf_data(self, link):
        """_summary

        retrieve_pdf_data, takes in a link as an argument and returns a pandas DataFrame.
        Using the tabular-py Python package, imported with tabula to extract all pages from the pdf document at following link .
        Then returns a DataFrame of the extracted data._

        Parameters
        ----------
        link : _https link _
            _description_

        Returns
        -------
        _type: pandas dataframe_
        """
        
        # Read remote pdf into list of DataFrame
        pdf_data = tabula.read_pdf(link, pages='all')#returns a list of data frames
    
        return pdf_data

    def  list_number_of_stores(self, number_of_stores_endpoint,  header_dict):
        """_summary
        list_number_of_stores which returns the number of stores to extract. 
        It takes in the number of stores endpoint and header dictionary as an argument._

        Parameters
        ----------
        number_of_stores_endpoint : _type: integer_

        header_dict : _type: dictionary_

        Returns
        -------
        _type: integer number of stores_
        """
        
        no_of_stores=requests.get(url=number_of_stores_endpoint, headers=header_dict) #returns number of stores
        jsn=no_of_stores.json()#json file type dict
        return jsn['number_stores']

    def retrieve_stores_data(self, retrieve_a_store_endpoint, header_dict):
        """_summary
        retrieve_stores_data which will take the retrieve a store endpoint as an
        argument and header dicttionary and extracts all the stores from the API saving them in a pandas DataFra_

        Parameters
        ----------
        retrieve_a_store_endpoint : _type: https link_

        header_dict : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        retrieve_stores=requests.get(retrieve_a_store_endpoint, headers=header_dict)
        stores_data = retrieve_stores.json()
        df = pd.DataFrame(stores_data, index=[0])
     
        return df

    def store_dataframe(self):
        """_summary
        using the number of stores from the list_number_of_stores function we iterate through the each store and 
        concatinate the list to return the full stores dataframe_

        Returns
        -------
        _type: pandas dataframe_
        """
        number_of_stores=self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',{'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
        list_of_dataframes=[]

        for store in range(number_of_stores):# iterate through the range of stores
            list_of_dataframes.append(self.retrieve_stores_data(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store}', {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}))
        
     
        store_data=pd.concat(list_of_dataframes)#concatinate the list of dataframes into a single one
        return  store_data

    def extract_from_s3(self,url):
        """_summary
        extract_from_s3 which uses the boto3 package to download and extract the information returning 
        a pandas DataFrame.The S3 address for the products data is the 
        following s3://data-handling-public/products.csv the method will take this address in as an argument 
        and return the pandas DataFrame._

        Parameters
        ----------
        url : _type: s3 link_
        """
        
        s3 = boto3.client('s3')
        # 's3' is a key word. create connection to S3 using default config and all buckets within S3

        obj = s3.get_object(Bucket='data-handling-public' , Key='products.csv') # get object and file (key) from bucket
        initial_df = pd.read_csv(obj['Body'])        
        return initial_df
    
    def extract_s3_json(self):#JSON file containing the details of when each sale happened, as well as related attributes.
        """JSON file containing the details of when each sale happened, as well as related attributes.
        The final source of data is a JSON file containing the details of when each sale happened, as well as related attributes.
        The file is currently stored on S3 and can be found at the following link https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json.
        the method extracts the file 
        """
        url= 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        
        s3 = boto3.client('s3')#To connect to the low-level client interface, you must use Boto3’s client(). You then pass in the name of the service you want to connect to, in this case, s3:
        obj = s3.get_object(Bucket='data-handling-public' , Key='date_details.json') # get object and file (key) from bucket
        dict_json = pd.read_json(obj['Body']) #read the body of the object json
        return dict_json#returns dictionary 
        






  



if __name__ == '__main__':
#here I call all the function within the class DataExtractor()
  #  print(DataExtractor().read_rds_table('orders_table'))
    
    #print(DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"))#can index each page
   # print(DataExtractor().store_dataframe(DataExtractor().list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',{'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})))
   
    #print(DataExtractor().extract_from_s3('s3://data-handling-public/products.csv'))

    print(DataExtractor().extract_s3_json())
    #print(DataExtractor().store_dataframe())