
import pandas as pd
import numpy as np
import weakref
import pymysql
import ot_logging as otl

from tqdm import tqdm
from datetime import date
import threading
import math
from decimal import Decimal, ROUND_HALF_UP, Context, setcontext
import time
import gc

setcontext(Context(prec=64, rounding=ROUND_HALF_UP))
thread_local = threading.local()

class ot_db_manager:

    def __init__(self, system, uid, pwd, library, table_name, logg = None) -> None:
        self.system = system
        self.uid = uid
        self.pwd = pwd
        self.library = library

        if logg:
            self.logging = logg
        else:
            self.logging = otl.ot_logging('ot_db_manager')
        
        # we need to define columns before we can use the "get_columns" function
        self.columns_raw = None
        self.columns = None
        self.column_type = None
        self.column_length = None
        self.column_precision = None
        self.column_scale = None



        # register this object to execute "_on_delete" once faced with garbage collection --- close connection when object is no longer used.
        self._finalizer = weakref.finalize(self, self._on_delete)

        self.logging.info("creating connection")
        
        # create a connection with the DB defined by system for the provided user.



        # save the provided table name for this db instance
        self.logging.debug("Current table name:: " + table_name)
        self.table_name = table_name

        self.library = library
        # to help with future queries, save a copy of the existing columns in the provided table.

        self.get_columns()

    def get_connection(self):
        # Each thread gets its own connection
        if not hasattr(thread_local, 'conn') or thread_local.conn is None or thread_local.conn._closed:
            self.logging.info("Creating new database connection for thread.")
            thread_local.conn = self.connect()
        else:
            try:
                thread_local.conn.ping(reconnect=True)
            except pymysql.MySQLError as e:
                self.logging.warning(f"Connection lost. Reconnecting. Error: {e}")
                thread_local.conn = self.connect()
        return thread_local.conn

    def connect(self):

        self.logging.info(f"Connecting to {self.system} with {self.uid} and {self.library}")
        conn = pymysql.connect(
            host=self.system,
            user=self.uid,
            password=self.pwd,
            database=self.library,
            autocommit=False  # Ensure autocommit is disabled to manage transactions manually
        )

        return conn


    def table_exists(self, table_name):
        conn = self.get_connection()
        cu = conn.cursor()
        query = f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{self.library}'"
        cu.execute(query)
        table_exists = cu.fetchone()  # Either contains the first row or None

        # check if the table exists
        if table_exists:
            self.logging.debug(f"The table '{table_name}' exists in the '{self.library}' schema.")
            return True
        else:
            self.logging.debug(f"The table '{table_name}' does not exist in the '{self.library}' schema.")
            return False
        

    def compare_cols(self, df1, df2):
        """
        Compare two DataFrames by column and return a list of True or False values indicating equality.
        """
        if df1.columns.tolist() != df2.columns.tolist():
            raise ValueError("DataFrames have different column names.")

        equal_columns = []
        for column in df1.columns:
               equal_columns.append(df1[column].reset_index(drop=True).equals(df2[column].reset_index(drop=True)))

        return equal_columns    
    
    # NOTE: Assumes the global column definitions are accurate
    def fill_df_na(self, df):
        # df = df.replace(np.nan, 0).applymap(lambda x: x.replace('\x00', '') if type(x) == type('') else x).astype(str).where(pd.notna(df), None)
        for i, column in enumerate(df.columns):
            if 'DECIMAL' in self.column_type[i]:
                df[column] = df[column].fillna(0)
            elif 'VARCHAR' in self.column_type[i]:
                df[column] = df[column].fillna('')
            else:
                df[column] = df[column].fillna('')

        return df.astype(object).where(pd.notna(df), None)

    def push_blob_to_db(self, table_name, blob):
        if self.table_exists(table_name):
            # STEP 2: If there is a table, use set enviornment to use said table
            self.set_table(table_name)
        else:
            self.logging.info('SQL TABLE NOT FOUND')

            self.logging.info('CONSTRUCTING UNIQUE SQL QUERY TO GENERATE NEW TABLE')
            sql_query = self.generate_table_from_name(table_name)

            # STEP 3: Execute custom query to construct the new Table.
            self.logging.info('CONSTRUCTING NEW TABLE')
            self.wrapped_execute(sql_query)
            
            sql_query = self.add_columns_to_db_for_blob(table_name)

            self.logging.info('RE-ATTEMPTING TO PUSH DATA TO SQL DATABASE')
            
            # STEP 4: Re-call function.
            return self.push_blob_to_db(table_name, blob)
        
        
        query = f"INSERT INTO {self.library}.{self.table_name} ({','.join(self.columns)}) VALUES ({','.join(['%s']*len(self.columns))})"
        
        self.wrapped_execute(query, blob, blob=True)
    """
    Pushes dataframe to database if the corresponding table exists, otherwise creates the necessary table and tries to push again.
    Does all the necessary processing of the table.
    requres: The number of columns in the database must equal the number of columns in the provided df. Throws exception otherwise.
    """
    # Re-implement for pyodbc and our use case.
    # ASSUMPTION: the number of columns in the database will equal the number of columns in the provided df. Throw exception otherwise.
    # NOTE: HANDLING CHUNKS EXTERNAL TO THE WRAPPED_EXECUTE CALLING FUNCTION MAKES THIS WORK FOR SOME REASON.... IDK WHY
    def push_df_to_db(self, df, chunksize = 32767, silent = True, check_t_1 = True): 
        # NOTE: make sure the data being uploaded is unique relative to the existing data; only need to do this once per-dataset
        if df.empty or check_t_1 and self.compare_df_to_db(df):
            return
        
        if not silent:
            for df_chunk in tqdm(np.array_split(df, max(1, len(df)//chunksize)), desc = 'Chunk'):
                df_chunk.name = df.name
                self.push_df_to_db_helper(df_chunk)
        else:
            for df_chunk in np.array_split(df, max(1, len(df)//chunksize)):
                df_chunk.name = df.name
                self.push_df_to_db_helper(df_chunk)

    def compare_df_to_db(self, df):
        
        # subset dataset
        if len(df) > 5000:
            sqrt_of_n = int(math.sqrt(len(df)))
            name_tmp = df.name
            df = df.iloc[-sqrt_of_n:]
            df.name = name_tmp
        else:
            sqrt_of_n = int(len(df)//2)
            name_tmp = df.name
            df = df.iloc[-sqrt_of_n:]
            df.name = name_tmp
        df = self.prep_df(df)

        if self.table_exists(df.name):
            self.set_table(df.name)
        else:
            return False
        
        # verify the columns match; otherwise, return None
        if (set(self.columns) - set(df.columns)) != set():
            self.logging.error(f"{self.library}.{self.table_name} differs. IBM DB2 def: {self.columns} ..... DF def: {df.columns}")
            return True

        df = self.fix_column_order(df)  # reorder

        existing_data = self.select_most_recent_n_rows(sqrt_of_n)
        existing_data = existing_data.iloc[::-1, 2:]
        existing_data = existing_data.astype(df.dtypes)
        types = df.dtypes



        df = self.fill_df_na(df)
        # df = df.astype(object).where(pd.notna(df), None).applymap(lambda x: str(x).replace('\x00', ''))
        # necessary to handle NaN values. If astype(object) is not performed, float nan values persist and are not processed by IBM DB2

        if not existing_data.empty:
            df = df.astype(types)
            self.logging.debug(existing_data.reset_index(drop=True))
            self.logging.debug(df.reset_index(drop=True))
    
            try:
                pd.testing.assert_frame_equal(existing_data.reset_index(drop=True), df.reset_index(drop=True))
                self.logging.warning(F'FETCHED DATA IS THE SAME AS PREVIOUS DAYS FOR: {self.table_name}')
                print(F'\nFETCHED DATA IS THE SAME AS PREVIOUS DAYS FOR: {self.table_name}')
                return True
            except Exception as e:
                self.logging.error(e)    
        return False

        

    def push_df_to_db_helper(self, df):
        
        df = self.prep_df(df)
    
        # STEP 1: Check if there is a table for the given dataframe
        if self.table_exists(df.name):
            # STEP 2: If there is a table, use set enviornment to use said table
            self.set_table(df.name)
        else:
            self.logging.info('SQL TABLE NOT FOUND')

            self.logging.info('CONSTRUCTING UNIQUE SQL QUERY TO GENERATE NEW TABLE')
            sql_query = self.generate_table_from_name (df.name)

            # STEP 3: Execute custom query to construct the new Table.
            self.logging.info('CONSTRUCTING NEW TABLE')
            self.wrapped_execute(sql_query)
            
            # STEP 2: Otherwise, create a new table given the dataframe
            sql_query = self.add_columns_to_db_from_df(df, df.name, '')

            self.logging.info('RE-ATTEMPTING TO PUSH DATA TO SQL DATABASE')
            
            # STEP 4: Re-call function.
            return self.push_df_to_db_helper(df)
        
        
        # verify the columns match; otherwise, return None
        if (set(self.columns) - set(df.columns)) != set():
            self.logging.error(f"{self.library}.{self.table_name} differs. IBM DB2 def: {self.columns} ..... DF def: {df.columns}")
            return

        df = self.fix_column_order(df)  # reorder

        self.verify_df_rel_table(df) # fixes any sizing issues

        query = f"INSERT INTO {self.library}.{self.table_name} ({','.join(self.columns)}) VALUES ({','.join(['%s']*len(self.columns))})"        

        df = self.fill_df_na(df)
        # get numpy data representation in list form for sql query
        data = df.values
        # convert the numpy array to a list of tuples
        rows = data.tolist()
        # remove the index from each tuple
        records = [tuple(row) for row in rows]

        # Iterate over the rows and insert them into the table
        return self.wrapped_execute(query, records)

            

    def round_to_power_of_two(self, n, maxx = 9999999):
        """
        Rounds an integer to the next nearest power of two.
        """
        cur_round = 2**math.ceil(math.log2(abs(n) + 1))
        return cur_round if cur_round < maxx else maxx


    def generate_table_from_name(self,table_name):
        query = f"CREATE TABLE {self.library}.{table_name} ("

        query += '''
        ID BIGINT AUTO_INCREMENT PRIMARY KEY,
        ROW_CHANGE_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL
        '''
        query += " ) "

        return query

    def df_encoder(self, df):
        return df.applymap(lambda x: x.encode('utf-8', errors='ignore').decode('utf-8', errors='replace') if isinstance(x, str) else x)

    def prep_df(self, df):
        # fixing column naming scheme
        df_name = df.name
        cols = [self.convert_column_name(col) for col in df.columns]
        df.columns = cols

        dtypes = df.dtypes

        # encoding/filtering
        df = self.df_encoder(df)
        df = df.copy()
        df = df.astype(dtypes)
        
        df.name = df_name
        return df

    """ takes a DataFrame df and reorders its columns based on the order specified in self.columns"""
    def fix_column_order(self, df):
        df_name = df.name
        df = df.loc[:, self.columns].copy()
        df.name = df_name
        
        return df

    # convert the given column name to a SQL compatible column name
    def convert_column_name(self, col_name):
        if str(col_name)[0].isdigit():
            col_name = 'COL_' + str(col_name)
        
        return str(col_name).upper().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('%', 'PERC').replace('/', '_')

    def add_columns_to_db_for_blob(self, table_name):
        self.logging.info('Appending Columns to table for blob')
        
        query = f"ALTER TABLE {self.library}.{table_name} ADD COLUMN "
        query += f"FILE_ID VARCHAR(64) DEFAULT NULL "
        self.wrapped_execute(query)

        query = f"ALTER TABLE {self.library}.{table_name} ADD COLUMN "
        query += f"BLOBBED LONGBLOB DEFAULT NULL "
        self.wrapped_execute(query)
        

    def add_columns_to_db_from_df(self, df, table_name, system_name):

        self.logging.info('Appending Columns to table from dataframe')
        self.logging.info(f'CURRENT DF DATATYPES FOR IBM_DB:: {df.dtypes}')
        
        for col_name, col_type in df.dtypes.items():
            col_idx = list(df.columns).index(col_name)
            query = f"ALTER TABLE {self.library}.{table_name} ADD COLUMN "

            if col_type == 'float64' or col_type == 'int64':
                # NOTE: https://stackoverflow.com/questions/70428101/why-am-i-getting-numeric-value-out-of-range-error-when-running-query-with-pyodbc
                # I have not found a resolution to the above issue. I'm guessing that numeric values larger than 100b get split into multiple chunks?
                # It could be reasonable to consider Decimal columns where precision - scale > 12 (100b+ range) as objects.
                max_mag  = 12
                max_scal = 4
                for item in df[col_name]:
                    max_mag = max(max_mag, self.get_magnitude(item))
                    max_scal = max(max_scal, self.get_scale(item))
                
                # max_mag = self.round_to_power_of_two(max_mag, 38)
                # max_scal = self.round_to_power_of_two(max_scal, 18)

                # create 'custom column' query to give a best guess given the provided dataset.
                query += f"{col_name} DECIMAL({max_mag + max_scal}, {max_scal}) DEFAULT NULL "

            elif col_type == 'datetime64[ns]':
                query += f"{col_name} TIMESTAMP DEFAULT NULL "

            elif col_type == 'object':
                # determine the maximum length of items in this column. Only do this for columns with more than 50 characters to try and squeeze more space out of the database - MSCI files hit the DB limit
                max_len = self.get_var_char_len(df, col_idx)
                max_len = self.round_to_power_of_two(max_len)
                
                query += f"{col_name} VARCHAR({max_len}) DEFAULT NULL "

            else:
                raise ValueError(f"Unsupported datatype {col_type} for column {col_name}")
            
            self.wrapped_execute(query)

    def select_most_recent_n_rows(self, num_rows):
        query = f'''
        SELECT * FROM {self.library}.{self.table_name}
        ORDER BY ID DESC
        LIMIT {num_rows};
        '''
        return self.wrapped_execute(query) 

    def select_most_recent_single_day(self):
        query = f'''
        SELECT * FROM {self.library}.{self.table_name}
        WHERE DATE(ROW_CHANGE_TIMESTAMP) = (
            SELECT DATE(MAX(ROW_CHANGE_TIMESTAMP))
            FROM {self.library}.{self.table_name}
        );
         '''
        return self.wrapped_execute(query) 
    
    def select_specific_day(self, date_str):
        query = f'''
            SELECT * FROM {self.library}.{self.table_name}
            WHERE DATE(ROW_CHANGE_TIMESTAMP) = '{date_str}'
        '''
        return self.wrapped_execute(query) 


    def chunk_list(self, lst, chunk_size):
        return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]

    # generic query to select all elements from the database.
    def select_all(self):

        query = "SELECT * FROM " + self.library + "." + self.table_name + "\nORDER BY INSERT_ORDER"

        return self.wrapped_execute(query)

    def fetch_all(self, cu):
        results = []
        for row in cu.fetchall():
            results += [list(row)]

        return self.construct_df(results, self.columns_raw)

    # assumptions:
    # data is a list of tuples. where the number of elements in each tuple is equal to the number of columns
    # Data is returned element from a prior query in the database.
    # NOTE: THE ROLL BACK FEATURE OF THIS FUNCTION DOES NOT WORK.
    def construct_df(self, data, columns):
        try:
            df = pd.DataFrame(data = data, columns = columns)
        except Exception as e:
            try:
                self.logging.debug('failed construct_df@1')
                self.logging.debug(str(e))
                df = pd.DataFrame(data = data)
                self.logging.debug('df constructed')
            except Exception as e:
                self.logging.error('failed construct_df@2')
                self.logging.error(str(e))

        return df


    # Wrapper for most execution cases we can encounter.
    # Includes simple error handling with rollback funcionality to minimze malformed queries being pushed to the database.
    def wrapped_execute(self, query, data = None, blob = False):
        conn = self.get_connection()
        cu = conn.cursor()

        self.logging.debug('EXECUTING: ' + str(query))

        try: 
            # Ensure the connection is active
            conn.ping(reconnect=True)

            if data:
                if blob:
                    cu.execute(query, (data[0], data[1].read()))
                else:
                    cu.executemany(query, data)
                # for i in data:
                #     self.logging.debug('executing query for :: ' + str(i))
                #     cu.execute(query, i)
                #cu.executemany(query, data)
            else:
                cu.execute(query)
            
            self.logging.debug(f'EXECUTED')

            # NOTE: falling back to always auto_commiting.
            # if(not auto_commit):
            # if data:
            self.commit(conn)

            # self.logging.debug('Number of affected rows: ' + str(cu.rowcount))

            # note the auto_commit param is in direct response to the below function. According to documentation the fetchall function will automatically commit changes.
            # I would like to maintain control over this functionality. Just in case.
            if cu.description is None:
                return None
            else:
                return self.fetch_all(cu)

        except pymysql.MySQLError as e:
            self.logging.error(f"wrapped_execute error: {e}")
            # Attempt to reconnect on error
            self.logging.info("Attempting to reconnect due to wrapped_execute error.")
            thread_local.conn = self.connect()
        except Exception as e:
            self.logging.error(f"wrapped_execute unexpected error: {e}")
            # Depending on the nature of the error, you might want to reconnect
            thread_local.conn = self.connect()
            raise e
        
        finally:
            try:
                conn.autocommit(True)
            except pymysql.MySQLError as e:
                self.logging.error(f"Error setting autocommit in finally: {e}")

        return None


    def expand_varchar_column(self, col_idx, length):
        self.logging.warning(f'EXPANDING VARCHAR COLUMN {self.columns[col_idx]}|OLD: VARCHAR({self.column_length[col_idx]})|NEW: VARCHAR({self.column_length[col_idx] + length})')
        query = f'''
        ALTER TABLE {self.library}.{self.table_name}
        ALTER COLUMN {self.columns[col_idx]} SET DATA TYPE VARCHAR({self.column_length[col_idx] + length})
        '''
        self.wrapped_execute(query)
    

    '''
    Expands the given column based on index by magnitude and scale,
    this is done by copying the old table and creating an new one where the old table is renamed to tablenname +_old and the modified column in the new table is named col_name + _new
    '''
    def expand_decimal_column(self, col_idx, magnitude, scale):

        self.logging.warning(f'EXPANDING DECIMAL VARCHAR COLUMN {self.columns[col_idx]}|OLD: DECIMAL({self.column_precision[col_idx]}, {self.column_scale[col_idx]})|NEW: DECIMAL({self.column_precision[col_idx] + magnitude + self.column_scale[col_idx] + scale}, {self.column_scale[col_idx] + scale}) in {self.table_name}')
       
        col_list = list(self.columns)
        col_list.remove(self.columns[col_idx])
      
        rename_query = f'''RENAME {self.library}.{self.table_name} TO {self.table_name}_old'''
        self.wrapped_execute(rename_query)

        query = f'''
            CREATE TABLE {self.library}.{self.table_name} AS (
                SELECT CAST({self.columns[col_idx]} AS DECIMAL({self.column_precision[col_idx] + magnitude + self.column_scale[col_idx] + scale}, {self.column_scale[col_idx] + scale})) AS {self.columns[col_idx]}, {", ".join(col_list)}
                FROM {self.library}.{self.table_name}_old
            ) WITH DATA
        '''
        self.wrapped_execute(query)
    

    """
    Examines the column types in a table, cheks if any VARCHAR column needs to be expanded for longer values, 
    or if any DECIMAL column needs to be expanded for more significant digits or a larger scale.
    """
    def verify_df_rel_table(self, df):
        for i in range(len(self.column_type)):  
            if(self.column_type[i] == 'CHARACTER VARYING'):
                length_check = self.check_varchar_length(df, i)
                if 0 < length_check:
                    self.expand_varchar_column(i, length_check)
            elif(self.column_type[i] == 'DECIMAL'):
                magn_check = self.check_float_magnitude(df, i)
                scale_check = self.check_float_scale(df, i)
                if 0 < (magn_check + scale_check)- self.column_precision[i] or 0 < scale_check - self.column_scale[i]:
                    self.expand_decimal_column(i, magn_check, scale_check)

    def get_var_char_len(self, df, col_idx):
        return df.iloc[:, col_idx].apply(lambda x: len(repr(str(x).encode('utf-8', errors = 'replace')))).max()


    """
    Checks the length of the values in the VARCHAR column of the DataFrame and returns a non-negative value if the length needs to be expanded
    """
    def check_varchar_length(self, df, col_idx):
        return self.get_var_char_len(df, col_idx) - self.column_length[col_idx]

    def get_magnitude(self, value):
        str_value = str(value)
        if '.' in str_value:
            precision = len(str_value.split('.')[0])
        else:
            precision = len(str_value)
        return precision

    def get_scale(self, value):
        str_value = str(value)
        if '.' in str_value:
            scale = len(str_value.split('.')[-1])
        else:
            scale = 0
        return scale


    def check_float_magnitude(self, df, col_idx):
        # convert decimal to string to calculate precision
        
        df['dec_as_str'] = df.iloc[:, col_idx].apply(lambda x: self.get_magnitude(x))
        # max_prec = self.round_to_power_of_two(df['dec_as_str'].max(), 24)
        max_prec = df['dec_as_str'].max()
        df.drop('dec_as_str', axis=1, inplace=True)
        return max_prec 

    def check_float_scale(self, df, col_idx):
        # split number by decimal point and get length of fractional part
        # I should be fired. Stripped of pay. And forgotten. 
        # NOTE: I have to filter out empty strings because, for whatever reason, the prep_df function called before this does not filter out '' values from decimal columns... This is beyond me.
        df['num_decimal_points'] = df.iloc[:, col_idx].apply(lambda x: self.get_scale(x))
        # max_scl = self.round_to_power_of_two(df['num_decimal_points'].max(), 7)
        max_scl = df['num_decimal_points'].max()
        df.drop('num_decimal_points', axis=1, inplace=True)
        return max_scl 

    def get_columns(self):
        
        query = f'''
        SELECT
            COLUMN_NAME AS NAME,
            COLUMN_TYPE AS DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            CASE
                WHEN EXTRA = 'auto_increment' THEN 'True'
                WHEN COLUMN_NAME = 'ROW_CHANGE_TIMESTAMP' THEN 'True'
                ELSE 'False'
            END AS ColumnGeneration
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_NAME = '{self.table_name}'
            AND TABLE_SCHEMA = '{self.library}'
        ORDER BY ORDINAL_POSITION;
        '''

        self.columns_raw = [0, 1, 2, 3, 4, 5]
        col_tmp = self.wrapped_execute(query).fillna(-1)
        
        # im so so sorry for this atrocity.. i have worse cacophonies..
        self.columns_raw = col_tmp.loc[:, col_tmp.columns[0]]
        self.columns = col_tmp.loc[col_tmp.iloc[:, 5] == 'False', col_tmp.columns[0]]
        self.column_type = col_tmp.loc[col_tmp.iloc[:, 5] == 'False', col_tmp.columns[1]]
        self.column_length = col_tmp.loc[col_tmp.iloc[:, 5] == 'False', col_tmp.columns[2]]
        self.column_precision = col_tmp.loc[col_tmp.iloc[:, 5] == 'False', col_tmp.columns[3]]
        self.column_scale = col_tmp.loc[col_tmp.iloc[:, 5] == 'False', col_tmp.columns[4]]
        
        self.columns_raw = self.columns_raw.tolist()
        self.columns = self.columns.tolist()
        self.column_type = self.column_type.tolist()
        self.column_length = [int(_) for _ in self.column_length.tolist()]
        self.column_precision = [int(_) for _ in self.column_precision.tolist()]
        self.column_scale = [int(_) for _ in self.column_scale.tolist()]

        self.logging.debug("CURRENT COLUMNS_RAW: " + str(self.columns_raw))
        self.logging.debug("CURRENT COLUMNS: " + str(self.columns))
        self.logging.debug("CURRENT COLUMN_TYPE: " + str(self.column_type))
        self.logging.debug("CURRENT COLUMN_LENGTH: " + str(self.column_length))
        self.logging.debug("CURRENT COLUMN_PRECISION: " + str(self.column_precision))
        self.logging.debug("CURRENT COLUMN_SCALE: " + str(self.column_scale))

    def set_table(self, new_table):
        if self.table_name != new_table:
            self.table_name = new_table
            self.get_columns()


    def rollback(self):
        self.logging.debug('Rolling back prior query')
        conn = self.get_connection()
        conn.rollback()


    def commit(self, conn):
        conn = self.get_connection()
        conn.commit()


    def close(self):
        self.logging.info('CLOSING CONNECTION!')
        conn = self.get_connection()
        conn.close()


    #####################################################################################################################################
    # conf_bool is a mediocre way of preventing accidental miss-calls. 
    # This function will NOT appear in production code.
    def DELETE_TABLE_CONTENTS(self, conf_bool = False):
        query = f'''DELETE FROM {self.library}.{self.table_name}'''
        if conf_bool:
            self.wrapped_execute(query)
    #####################################################################################################################################

    def _on_delete(self):
        self.logging.info('The garbage eating moster is on its way to destroy the file. i guess it\'s time to force close our connection..')
        self.close()



    ######################################################################################################
    #                                               LOGGING                                              #
    ######################################################################################################
# if __name__ == "__main__":
#     test = ot_db_manager(system = '10.100.20.68', uid = 'tpratt', pwd = 'tpratt23', library = 'TMPLIB', table_name = 'CGLOG')