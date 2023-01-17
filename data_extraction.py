
import yaml
from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import tabula


class DataExtractor:

    #def read_RDS_data(self):


    def extract_rds_table(self, table_name):
        engine = DatabaseConnector().init_db_engine()
        df = pd.read_sql_table(table_name, engine, index_col='index')
        return df

    def retrieve_pdf_data(self, link):
        
        # Read remote pdf into list of DataFrame
        pdf_data = tabula.read_pdf(link, pages='all')#returns a list of data frames
    
        return pdf_data




if __name__ == '__main__':
   # df = DataExtractor().extract_rds_table('legacy_users')
    print(DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"))#can index each page
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

