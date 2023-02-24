
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy import inspect




class DatabaseConnector:

    def read_db_creds(self):
        """
        read_db_creds method will read the credentials yaml file and return a dictionary of the credentials.
        using pip install PyYAML and import yaml to do this.
        """
      
        with open('db_creds.yaml', "r") as stream:#We use a context manager, set the mode we want to use, and then use a method. In this case, for reading a file, we use the load method
            try:
                return(yaml.safe_load(stream))
            except yaml.YAMLError as exc:
                return(exc)
        
    def init_db_engine(self):

        """_reads the credentials from the return of read_db_creds and initialises and returns an 
             sqlalchemy database engine._

        Returns
        -------
        _type: sqlalchemy database engine_
        """
        cred = self.read_db_creds()#returns a dictionary of credentials
        engine_url= URL.create('postgresql',
                username= cred['RDS_USER'], password =cred['RDS_PASSWORD'],
                host= cred['RDS_HOST'], port= cred['RDS_PORT'],
                database=cred['RDS_DATABASE'])
        engine = create_engine(engine_url)#create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def list_db_tables(self):
        '''
        Using the engine from init_db_engine create a method list_db_tables to list 
        all the tables in the database so you know which tables you can extract data from.
        '''

        engine = self.init_db_engine()
        inspector = inspect(engine)
        
        return inspector.get_table_names()# returns a list of the table names inside the engine

    def upload_to_db(self, data_frame, table_name):
        """_summary
        This method will take in a Pandas DataFrame and table name to upload to as an argument._

        Parameters
        ----------
        data_frame : _type: pandas dataframe_
            _description_
        table_name : _string_
     
        Returns
        -------
        _type: pandas dataframe_
        """
    
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
    from data_extraction import DataExtractor #to prevent circular import I import dataclean and dataextraction after I use them in some of the functions within the class
    #DatabaseConnector().upload_to_db(DataClean().clean_card_data(),'dim_card_details' )
   # DatabaseConnector().upload_to_db(DataClean().clean_store_data(),'dim_store_details' )

    #DatabaseConnector().upload_to_db((DataClean().clean_products_data(DataExtractor().extract_from_s3('s3://data-handling-public/products.csv'))), 'dim_products')
    DatabaseConnector().upload_to_db(DataClean().clean_orders_data(), 'orders_table')
    #DatabaseConnector().upload_to_db((DataClean().clean_date_times()), 'dim_date_times')
    