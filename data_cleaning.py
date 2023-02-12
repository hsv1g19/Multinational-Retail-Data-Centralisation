
import yaml
from data_extraction import DataExtractor
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table

class DataClean:

    def clean_user_data(self):#cleans user data
        df = DataExtractor().extract_rds_table('legacy_users')
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'],  infer_datetime_format=True, errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True, errors='coerce')
        #print(df['date_of_birth'])
        df.dropna(subset=['date_of_birth','join_date'], inplace=True)
        #print(df.head(20))
       # print(df.isna().sum())
        #print(df.columns)

        #print(df['address'].duplicated().value_counts())
        #print(df.pivot_table(columns=['phone_number'], aggfunc='size'))
        return df #cleaned data 

    def clean_card_data(self):

        list_of_data_frames = DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        list2=[]
        for data_frame in list_of_data_frames:
            #data_frame['expiry_date']=pd.to_datetime(data_frame['expiry_date'],  format='%m/%d', errors='coerce')
            data_frame['date_payment_confirmed']=pd.to_datetime(data_frame['date_payment_confirmed'],  format='%Y-%m-%d', errors='coerce')
            data_frame['card_number']=pd.to_numeric(data_frame['card_number'], errors="coerce")#invalid entries will be passed as NA
            data_frame.dropna(subset=['card_number'], inplace=True)
            list2.append(data_frame)
        card_data=pd.concat(list2)
        return card_data

    def clean_store_data(self):
        #number_of_stores=DataExtractor().list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',{'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
        store_data=DataExtractor().store_dataframe()#data frame from store data strated from the api
        store_data.replace({'continent': ['eeEurope', 'eeAmerica']}, {'continent': ['Europe', 'America']}, inplace=True)
        store_data.drop( columns='lat', inplace=True)
        store_data.drop_duplicates(subset=['address', 'longitude','store_type', 'latitude', 'continent'], keep=False, inplace=True)
        store_data['opening_date']=pd.to_datetime(store_data['opening_date'], infer_datetime_format=True, errors='coerce')
        store_data.dropna(subset=['opening_date', 'latitude','country_code', 'continent' ], inplace=True)
        store_data['staff_numbers']= pd.to_numeric(store_data['staff_numbers'], errors="coerce")
        store_data.dropna(subset=['staff_numbers'], inplace=True)
        return store_data


    def convert_product_weights(self, products_dataframe):

        products_dataframe['weight']=products_dataframe['weight'].apply(str)
        products_dataframe.replace({'weight':['12 x 100g', '8 x 150g']}, {'weight':['1200g', '1200g']}, inplace=True)
        filter_letters = lambda x: ''.join(y for y in x if not y.isdigit())#lambda function returns units/letters in each row
        products_dataframe['units']=products_dataframe['weight'].apply(filter_letters)
        products_dataframe["weight"]= products_dataframe['weight'].str.extract('([\d.]+)').astype(float)
        products_dataframe['weight'] = products_dataframe.apply(lambda x: x['weight']/1000 if x['units']=='g' or x['units']=='ml' else x['weight'], axis=1)
        products_dataframe.drop(columns='units', inplace=True)
        return products_dataframe

    def  clean_products_data(self, products_dataframe):
        products_df=self.convert_product_weights(products_dataframe)
        #print(products_df[products_df.duplicated(subset=['uuid', 'product_code'])])
        products_df.dropna(subset=['uuid', 'product_code'], inplace=True)
        products_df['date_added']=pd.to_datetime(products_df['date_added'], format='%Y-%m-%d', errors='coerce')
        drop_prod_list=['S1YB74MLMJ','C3NCA2CL35', 'WVPMHZP59U']# drop rows from list
        # products_dataframe[products_dataframe.category.isin(drop_prod_list) == False]
        products_df.drop(products_df[products_df['category'].isin(drop_prod_list)].index, inplace=True)
        return products_df
    
    def clean_orders_data(self):
        orders_dataframe=DataExtractor().read_rds_table('orders_table')
        orders_dataframe.drop(columns=['1', 'first_name', 'last_name'], inplace=True)
        #print(orders_dataframe.duplicated(subset=['product_code', 'user_uuid']).sum())
        #DO I CHANGE LATE_HOURS TO EVENING
        #orders_dataframe['date_uuid'] = orders_dataframe['date_uuid'].astype('uuid')
        return orders_dataframe.dtypes
    
    def clean_date_times(self):

        date_time_dataframe=DataExtractor().extract_s3_json()
        #print(date_time_dataframe['year'].value_counts())
        #print(date_time_dataframe['day'].value_counts())
        #print(date_time_dataframe['month'].value_counts())
        
        date_time_dataframe['day'] = pd.to_numeric(date_time_dataframe['day'], errors='coerce')
        date_time_dataframe.dropna(subset=['day', 'year', 'month'], inplace=True)
        date_time_dataframe['timestamp']=pd.to_datetime(date_time_dataframe['timestamp'], format='%H:%M:%S', errors='coerce')
        return date_time_dataframe
       
        




      
            




if __name__ == '__main__':
    #print(DataClean().clean_user_data())
    #print(DataClean().clean_card_data())
    #print(DataClean().clean_store_data())
    #print(DataClean().convert_product_weights(DataExtractor().extract_from_s3('s3://data-handling-public/products.csv')))
    #print(DataClean().clean_products_data(DataExtractor().extract_from_s3('s3://data-handling-public/products.csv')))
    #DataClean().clean_products_data()
    print(DataClean().clean_orders_data())