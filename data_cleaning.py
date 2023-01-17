
import yaml
from data_extraction import DataExtractor
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table

class DataClean:

    def clean_user_data(self):
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

        return list2
      
            




if __name__ == '__main__':
    #print(DataClean().clean_user_data())
    print(DataClean().clean_card_data())
    
