import snowflake.connector
# from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import dotenv_values
import pandas as pd

def create_sql_statement(schema_name, table_name, df):
    
    sql_type_mapping = {
        'int64':'INTEGER',
        'float64':'FLOAT',
        'object':'VARCHAR',
        'datetime64':'TIMESTAMP'
    }
    
    sql_statement = f'CREATE TABLE {schema_name}.{table_name}_staging ('
    
    for column, dtype in df.dtypes.items():
        sql_type = sql_type_mapping.get(str(dtype), 'VARCHAR')
        sql_statement += f'{column} {sql_type}, '
        
    sql_statement = sql_statement[:-2]
    sql_statement += ')'
    
    return sql_statement 

def check_if_table_exists(connection, schema_name, table_name):
    
    table_name = table_name.upper()
    schema_name = schema_name.upper()
    result_check = connection.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = '{schema_name}'
        AND table_name = '{table_name}'
        """
    )

    result_check = result_check.fetchone()[0]
    
    return result_check

def upload_csv_to_snowflake(env_file, path, schema_name, table_name):
    
    snowflake_cred = dotenv_values(env_file)
    
    snowflake_engine = create_engine(
        URL(
            user = snowflake_cred['SNOWFLAKE_USERNAME'],
            password = snowflake_cred['SNOWFLAKE_PASSWORD'],
            account = snowflake_cred['SNOWFLAKE_ACCOUNT'],
            warehouse = snowflake_cred['SNOWFLAKE_WAREHOUSE'],
            database = snowflake_cred['SNOWFLAKE_DATABASE'],
            role = snowflake_cred['SNOWFLAKE_ROLE']
        )
    )

    conn = snowflake_engine.connect()
    
    df = pd.read_csv(path, sep=';')
    key_column = df.columns[0]
    create_table_statement = create_sql_statement(schema_name = schema_name, table_name = table_name, df = df)
    
    try:
        conn.execute(f'USE SCHEMA {schema_name.upper()}')
        conn.execute(f'DROP TABLE IF EXISTS {schema_name}.{table_name}_staging')
        conn.execute(create_table_statement)
        
        df.to_sql(
            name = table_name + '_staging',
            con = snowflake_engine,
            schema = schema_name,
            index = False, 
            if_exists = 'append'
        )
        
        result_check = check_if_table_exists(connection = conn, schema_name = schema_name, table_name = table_name)
        
        if result_check == 1:
            conn.execute(f'DELETE FROM {schema_name}.{table_name} WHERE {key_column} IN (SELECT {key_column} FROM {schema_name}.{table_name}_staging)')
            conn.execute(f'INSERT INTO {schema_name}.{table_name} SELECT *, CURRENT_TIMESTAMP AS etl_date FROM {schema_name}.{table_name}_staging')
            conn.execute(f'DROP TABLE IF EXISTS {schema_name}.{table_name}_staging')
        
        else:
            conn.execute(f'ALTER TABLE {schema_name}.{table_name}_staging RENAME TO {table_name}')
            conn.execute(f'ALTER TABLE {schema_name}.{table_name} ADD COLUMN etl_date TIMESTAMP WITHOUT TIME ZONE')
            conn.execute(f'UPDATE {schema_name}.{table_name} SET etl_date = current_timestamp')
    
    except Exception as e:
        conn.close()
        snowflake_engine.dispose()
        print(f"Error occonnred: {str(e)}")
    
    finally:
        conn.close()
    
    snowflake_engine.dispose()

if __name__ == '__main__':

    upload_csv_to_snowflake(
        env_file = './.env',
        path = './data/test.csv',
        schema_name = 'public',
        table_name = 'test_table'
    )
