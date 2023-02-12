
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy import inspect


# Create a db_creds.yaml file containing the database credentials, they are as follows:
        # RDS_HOST: data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com
        # RDS_PASSWORD: AiCore2022
        # RDS_USER: aicore_admin
        # RDS_DATABASE: postgres
        # RDS_PORT: 5432

my_dict1 = {'RDS_HOST': 'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com', 'RDS_PASSWORD': 'AiCore2022',
            'RDS_USER': 'aicore_admin',
            'RDS_DATABASE': 'postgres',
            'RDS_PORT': 5432}

with open('db_creds.yaml', 'w') as f:
    yaml.dump(my_dict1, f)

class DatabaseConnector:

    def read_db_creds(self):


        """
        Reads the credentials of the RDS database from the locally stored YAML file
        
        """
      
        with open('db_creds.yaml', "r") as stream:
            try:
                return(yaml.safe_load(stream))
            except yaml.YAMLError as exc:
                return(exc)
        
    def init_db_engine(self):
        cred = self.read_db_creds()#returns a dictionary of credentials
        engine_url= URL.create('postgresql',
                username= cred['RDS_USER'], password =cred['RDS_PASSWORD'],
                host= cred['RDS_HOST'], port= cred['RDS_PORT'],
                database=cred['RDS_DATABASE'])
        engine = create_engine(engine_url)#create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def list_db_tables(self):
        '''
        Returns list of tables in database engine
        '''
        engine = self.init_db_engine()
        inspector = inspect(engine)
        
        return inspector.get_table_names()

    def upload_to_db(self, data_frame, table_name):
    
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'Harish2001'
        DATABASE = 'Sales_Data'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return data_frame.to_sql(table_name, engine)
                

    


if __name__ == '__main__':
    print('Tables in Database:',DatabaseConnector().list_db_tables())
    # 
    # #DatabaseConnector().upload_to_db(DataClean().clean_user_data(),'dim_users' )
    # for df in DataClean().clean_card_data():
    from data_cleaning import DataClean
    from data_extraction import DataExtractor
    #DatabaseConnector().upload_to_db(DataClean().clean_card_data(),'dim_card_details' )
    #DatabaseConnector().upload_to_db(DataClean().clean_store_data(),'dim_store_details' )

    DatabaseConnector().upload_to_db((DataClean().clean_products_data(DataExtractor().extract_from_s3('s3://data-handling-public/products.csv'))), 'dim_products')
    #DatabaseConnector().upload_to_db(DataClean().clean_orders_data(), 'orders_table')
    #DatabaseConnector().upload_to_db((DataClean().clean_date_times()), 'dim_date_times')
    