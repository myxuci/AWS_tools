import pandas as pd
import psycopg2
import logging
import time
import os

class Redshift_Tools(object):
    """
    A tool box for Redshift.
    Updates:
    Version 1.1: Update the module of executing SQL query. Previous version reported error while executing non-'SELECT'
    data reading queries, for example: 'DELETE FROM [table_name] WHERE [clause]'. Now this issue has been solved.
    """
    def __init__(self, query_file_path=None, auto_commit=True, **kwargs):

        # Truncated.
        # Input configuration is dict:
        # if isinstance(Redshift_config, dict):
        #     self.__Redshift_config = Redshift_config
        # # Input configuration is file:
        # else:
        #     self.__check_file_existence(Redshift_config)
        #     try:
        #         with open(Redshift_config, 'r') as file:
        #             self.__Redshift_config = json.load(file)
        #     except Exception as e:
        #         raise TypeError('Error: {}.\nThe input Redshift login credential can be dict or a json file.'.format(e))

        # Placeholder for connection to Redshift.
        self.__connection = None

        # Check input parameters.
        assert isinstance(auto_commit, bool), 'Input value of auto_commit can only be Boolean.'
        self.__auto_commit = auto_commit

        # Pre-check if the login is valid:
        assert self.__is_Redshift_login_valid(**kwargs), 'User-defined Redshift login is wrong.'
        self.__Redshift_config = kwargs

        # Load query file:
        if query_file_path != None:
            self.__check_file_existence(query_file_path)
            self.__query_file_path = query_file_path

        # Truncated.
        # # Get Redshift cursor
        # try:
        #     print('Starting connection to Redshift...')
        #     self.__connection = psycopg2.connect(
        #                             dbname=self.__Redshift_config['dbname'] ,
        #                             host=self.__Redshift_config['host'] ,
        #                             port=self.__Redshift_config['port'] ,
        #                             user=self.__Redshift_config['user'] ,
        #                             password=self.__Redshift_config['password'])
        #     self.__connection.autocommit = self.__auto_commit
        #     self.__connection_status = True
        #     print('Connetion established.')
        # except Exception as e:
        #     print('Cannot establish connection to Redshift. Reason:\n{}\n'.format(e))
        #     raise ConnectionError('Cannot establish connection to Redshift. Reason:\n{}\n'.format(e))

    @property
    def Redshift_config(self):
        Redshift_Tools.print_warning('Warning: Redshift login information is credential, please contact admin.')

    @Redshift_config.setter
    def Redshift_config(self, **kwargs):
        try:
            self.__update_Redshift_login(**kwargs)
        except:
            raise

    @property
    def connection(self):
        Redshift_Tools.print_warning('Warning: Redshift connection cannot be disclosed.')

    @connection.setter
    def connection(self, *args, **kwargs):
        raise ValueError('Redshift connection can only be defined during creating instances.')

    @property
    def auto_commit(self):
        return self.__auto_commit

    @auto_commit.setter
    def auto_commit(self, is_auto_commit):
        assert isinstance(is_auto_commit, bool), 'Parameter \'is_auto_commit\' can only be boolean.'
        self.__auto_commit = is_auto_commit
        print('Warning: Auto commit has been switched to {}.'.format(self.__auto_commit))

    # Truncated.
    # @property
    # def connection_status(self):
    #     return self.__connection_status
    #
    # @connection_status.setter
    # def connection_status(self, *args, **kwargs):
    #     raise ValueError('Connection status cannot be changed externally.')

    @property
    def query_file_path(self):
        return self.__query_file_path

    @query_file_path.setter
    def query_file_path(self, query_file_path):
        self.__check_file_existence(query_file_path)
        self.__query_file_path = query_file_path

    def __update_Redshift_login(self, **kwargs):
        __redshift_login_placeholder = self.__Redshift_config
        for _key in kwargs:
            __redshift_login_placeholder[_key] = kwargs[_key]

        if self.__is_Redshift_login_valid(**__redshift_login_placeholder):
            self.__Redshift_config = __redshift_login_placeholder
            print('Update Redshift login.')
        else:
            raise ValueError('New Redshift login is wrong.')

    def __est_connection_to_redshift(self, for_testing=True, **kwargs):
        assert isinstance(for_testing, bool), 'Parameter \'for_testing\' can only be boolean.'
        self.__connection = psycopg2.connect(
            dbname=kwargs['dbname'] ,
            host=kwargs['host'] ,
            port=kwargs['port'] ,
            user=kwargs['user'] ,
            password=kwargs['password']
        )

        if for_testing:
            try:
                self.__connection.close()
                print('Testing: Connection to Redshift has been closed.')
            except Exception as e:
                Redshift_Tools.print_warning('Warning: Cannot close connection to Redshift server. Error: {}'.format(e))
                raise
        else:
            self.__connection.autocommit = self.__auto_commit

    def __is_Redshift_login_valid(self, **kwargs):
        try:
            print('Testing Redshift login...')
            self.__est_connection_to_redshift(for_testing=True, **kwargs)
            print('Finished testing connection.')
            return True
        except Exception as e:
            print('Warning: Got an error while testing Redshift login: {}'.format(e))
            return False

    def __check_file_existence(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError('File: {} does not exist.'.format(file_path))
        else:
            return True

    @classmethod
    @contextmanager
    def timer(cls, title=None):
        assert isinstance(title, str), 'Title can only be strings.'
        print(title)

        _tic = time.time()
        yield
        _lag = time.time() - _tic

        if _lag <= 60:
            _unit = 'second(s)'
        elif (_lag > 60) & (_lag <= 3600):
            _unit = 'minute(s)'
            _lag /= 60
        else:
            _unit = 'hour(s)'
            _lag /= 3600

        Redshift_Tools.print_warning('Time cost: {:.2f} {}.'.format(_lag, _unit))

    @classmethod
    def print_warning(cls, content):
        assert isinstance(content, str), 'Parameter \'content\' must be in the format of string.'
        print('*'*len(content))
        print(content)
        print('*'*len(content))

    # Truncated.
    # def get_log(self):
    #     '''
    #     Retrieve log.
    #     :return: string of log stream. Type: string.
    #     '''
    #     logging.info('Generating log...')
    #     return self.__log_stream.getvalue()

    def __get_redshift_insertion_SQL(self, schema, table_name, values):
        '''
        Store the query for data insertion.
        schema: Schema name. Type: string.
        table_name: Table name. Type: string.
        values: Values needs to be insert into the target table. Type: list.
        :return: Null.
        '''
        script = 'INSERT INTO {}.{} VALUES '.format(schema, table_name)
        for val in values:
            tmp_string = '(' + ','.join(val) + ') ,'
            script += tmp_string
        return script.strip(',')

    def __insert_data_into_redshift(self, schema, table_name, values):
        '''
        Use Redshift client to execute data insertion query, by using provided database information. This function
        invokes self._get_redshift_insertion_SQL function to retrieve the query template used for data insertion.
        schema: Schema name. Type: string.
        table_name: Table name. Type: string.
        values: Values needs to be insert into the target table. Type: list.
        :return: Null.
        '''
        try:
            query = self.__get_redshift_insertion_SQL(schema, table_name, values)
            self.__est_connection_to_redshift(for_testing=False, **self.__Redshift_config)
            with self._connection.cursor() as cur:
                cur.execute(query)
                self.__connection.close()
        except Exception as e:
            print('Cannot insert data into Redshift. Reason:\n{}\n'.format(e))
            raise

    def batch_insertion_to_redshift(self, data, schema, table_name, batch_size=500, wait_time=15):
        '''
        Batch data insertion into redshift.
        :param data: Data needs to be inserted into Redshift. If the input data type is list, its elements should fit into
               the row of target table. Type: list, pd.dataframe, np.array.
        :param schema: Schema name. Type: string.
        :param table_name: Target table name. Type: string.
        :param batch_size: Batch size per data insertion. Type: int.
        :param wait_time: Time interval between two consecutive data insertion. Type: float.
        :return: Null
        '''
        # Precheck input values.
        assert (isinstance(schema, str)) & (isinstance(table_name, str)) & (isinstance(batch_size, int)), \
            'Schema and table_name can only be strings. Batch_size can only be integers.'

        # Check input data type.
        if not isinstance(data, list):
            try:
                _data = [list(ele) for ele in data.astype(str).values] # Type is dataframe.
            except:
                _data = [list(ele) for ele in data.astype(str)] # Type is np.array.
        elif isinstance(data, list):
            _data = data
        else:
            raise TypeError('Input data type should be list, pandas.core.frame.DataFrame or numpy.ndarray.')

        print('Inserting data into Redshift database: {}.{}'.format(schema, table_name))

        for count in range(int(len(_data)/batch_size)):
            self.__insert_data_into_redshift(schema=schema, table_name=table_name, \
                                            values=_data[count * batch_size:(count + 1) * batch_size])
            time.sleep(wait_time)
        self.__insert_data_into_redshift(schema=schema, table_name=table_name, \
                                        values=_data[int(len(_data) / batch_size) * batch_size::])
        print('Data insertion to {}.{} complete.'.format(schema, table_name))

    # # Using turicreate lib. In DEV.
    # def batch_db_insertion_tc(self, data, schema, table_name):
    #     '''
    #     Batch data insertion into databases by using lib of TuriCreate. This is function is still at the DEV stage, not
    #     validated.
    #     :param data: Data will be inserted. Type: list, np.array, pd.DataFrame.
    #     :param schema: Schema name. Type: string.
    #     :param table_name: Table name. Type: string.
    #     :return:
    #     '''
    #     try:
    #         _sdf = tc.SFrame(data)
    #         _schema_table = schema + '.' + table_name
    #         _sdf.to_sql(self.__connection, _schema_table)
    #         print('Data has been inserted into database.')
    #     except Exception as e:
    #         print('Failed in data insertion. Reason:\n{}\n'.format(e))
    #         raise

    # # Truncated.
    # def get_status(self):
    #     '''
    #     Get running status of Redshift client.
    #     :return: status. Type: int.
    #     '''
    #     logging.info('Retrieved Redshift client status.')
    #     return self.__status

    # # Truncated.
    # def __set_status(self, status):
    #     '''
    #     Set running status of Redshift client.
    #     :param status: running status. Type: int.
    #     :return: Null.
    #     '''
    #     if isinstance(status, int) == True:
    #         self.__status = status
    #         logging.info('Redshift client status: Successful.')
    #     else:
    #         raise TypeError('The running status of Redshift client can only be defined as the type of int.')
    #         logging.error('Provide a wrong format of status.')

    def get_SQL(self, query_file, inplace=False):
        """
        Read SQL query from file.
        :param query_file: Path of file containing Redshift query. Type: string.
        :param inplace: Whether to replace the class SQL. Type: bool.
        :return SQL_query: Loaded SQL query. Type: string.
        """
        self.__check_file_existence(query_file)
        assert isinstance(inplace, bool), 'Parameter inplace can only be boolean.'

        try:
            with open(query_file, 'r') as SQL_file:
                _SQL_query = SQL_file.read()
            print('Finish reading SQL script from file:\n{}.'.format(query_file))
            if inplace:
                self.__SQL_to_run = _SQL_query
            return _SQL_query
        except Exception as e:
            print('Cannot read SQL script. Reason:\n{}\n'.format(e))
            raise

    def __get_data(self, df_generator, verbose=False):
        '''
        Get data from executed query.
        :param: df_generator: Python iterator. It should be returned by executed SQL query. If non-exist, will try class
                iterator. Type: Iterator.
        :param: verbose: Turn on to data extraction process. Type: bool or int.
        :return: output_df: Extracted data by executed SQL query. Type: pd.dataframe.
        '''
        print('Start extracting data...')
        output_df = pd.DataFrame()
        _count = 1

        # if df_generator != None:
        #     while True:
        #         try:
        #             output_df = pd.concat([output_df, next(df_generator)], axis=0)
        #             if verbose:
        #                 print('Working on chunk {}...'.format(_count))
        #                 logging.info('Working on chunk {}...'.format(_count))
        #             _count += 1
        #         except:
        #             logging.info('Data extraction complete.')
        #             print('Data extraction complete.')
        #             return output_df
        # else:
        #     try:
        #         self.df_generator
        #     except Exception as e:
        #         logging.error('Failed in getting data from Redshift. Reason:\n{}\n'.format(e))
        #         print('Failed in getting data from Redshift. Reason:\n{}\n'.format(e))
        #         raise ValueError('No data generator was loaded.')

        while True:
            try:
                output_df = pd.concat([output_df, next(df_generator)], axis=0)
                if verbose:
                    print('Working on chunk {}...'.format(_count))
                _count += 1
            except:
                logging.info('Data extraction complete.')
                print('Data extraction complete.')
                if (output_df.shape[0] % 10 == 0) & (output_df.shape[0]>=50000):
                    print('\nWarning: Due to memory limitation, it\'s possible that only partial data was retrieved. Please double check!\n')
                return output_df

    def execute(self, SQL_query, chunksize=50000, verbose=False):
        '''
        Execute SQL query on Redshift server.
        :param SQL_query: SQL query to be executed. Type: string.
        :param chunksize: Size of data chunk in case to extract data from Redshift. Type: int.
        :param verbose: Turn on to monitor the data extraction process. Type: bool or int.
        :return: output_df: Extracted data. Type: pd.dataframe.
        '''
        assert (isinstance(SQL_query, str)) & (isinstance(chunksize, int)) & (isinstance(verbose, bool)), \
            'Data type of input parameters:\nSQL_query: string, chunksize: int, verbost: boolean'

        # if SQL_query != None:
        #     _SQL_query = SQL_query
        # elif self.__SQL_query != None:
        #     _SQL_query = self.__SQL_query
        # else:
        #     raise('Now SQL query was loaded to be executed.')

        # if isinstance(_SQL_query, str) != True:
        #     logging.error('Illegal input SQL query.')
        #     raise TypeError('Input SQL query can only be string.')
        # else:
        #     logging.info('Running SQL query...')
        #     print('Running SQL query...')
        #     _df_generator = None

        with Redshift_Tools.timer('Executing SQL on Redshift...'):
            # Create a placeholder for output dataframe generator.

            self.__est_connection_to_redshift(for_testing=False, **self.__Redshift_config)
            try:
                _df_generator = pd.read_sql(SQL_query, self.__connection, chunksize=chunksize)
                print('SQL query has been executed successfully.')
                output_df = self.__get_data(_df_generator, verbose)
                self.__connection.close()
                return output_df
            except:
                _cur = self.__connection.cursor()
                _cur.execute(SQL_query)
                self.__connection.close()
                print('SQL query has been executed successfully.')

    # Truncated.
    # def close(self):
    #     """
    #     Turn off connection to Redshift.
    #     :return: Null.
    #     """
    #     try:
    #         self.__connection.close()
    #         logging.info("Redshift connection has been closed.")
    #         print("Redshift connection has been closed.")
    #     except Exception as e:
    #         logging.error('Cannot disconnect from Redshift. Reason:\n{}'.format(e))
    #         print('Cannot disconnect from Redshift. Reason:\n{}'.format(e))
    #         raise ConnectionError('Cannot disconnect from Redshift. Reason:\n{}'.format(e))

# Truncated.
# def test_Redshift_connection(redshift_config):
#     try:
#         _ = Redshift_Tools(Redshift_config=redshift_config)
#     except:
#         print('\nFailed in connecting to Redshift. Please double check your config file.\n')

if __name__ == '__main__':
    Redshift_Tools.print_warning('Redshift Tool Box.\n(Required external libraries: pandas, psycopg2)')

    # Truncated.
    # parser = argparse.ArgumentParser(prog='Redshift Tool Box.\n(Required external libraries: pandas, psycopg2)')
    #
    # parser.add_argument('-t', metavar='\b', type=str, \
    #                     help='Test Redshift connection by using a JSON file. Connect to Redshift cluster by using config as:\n dbname, host, port, username, password.\nConfig can be in the format of dict or a json file path.')
    # parser.print_help()
    #
    # args = parser.parse_args()
    # if isinstance(args.t, str) == True:
    #     with open(args.t, 'r') as file:
