# standard library
import os
import logging

# third-party
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from omegaconf import OmegaConf 
import pandas as pd

# create s3 client
s3 = boto3.client('s3')

# athena client
ath = boto3.client('athena')

# logger settings
logging.basicConfig(level=logging.INFO)


def create_bucket(bucket_name):
    
    try:
        # check if bucket exists
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
        
    except ClientError as e:
        
        error_code = e.response['Error']['Code']
        
        if error_code == '404': # bucket does not exist code 404, create it
        
            try:
                # create s3 bucket
                s3.create_bucket(Bucket=bucket_name)
        
                print(f"Bucket {bucket_name} created successfully")
            except ClientError as e:
                print(f"filed to create bucket: {e}")
                
        else:
            print(f"Error: {e}")
            
    except NoCredentialsError:
        print("Credentials not available")

def create_folder(bucket_name, folder_name):
    
    try:
        # Create a folder-like object
        s3.put_object(Bucket=bucket_name, Key=folder_name)
        print(f"Folder '{folder_name}' created in bucket '{bucket_name}'.")
    except ClientError as e:
        print(f"Failed to create folder: {e}")
    except NoCredentialsError:
        print("Credentials not available")

def upload_file(file_name, bucket_name, s3_file_name):
    
    try:
        # upload file
        response = s3.upload_file(file_name, bucket_name, s3_file_name)
        print(f"File uploaded to {s3_file_name} in bucket {bucket_name}.")
    except ClientError as e:
        print(f"Error: {e}")
    except NoCredentialsError:
        print("Credentials not available")

def table_exists(database, table):
    """Check if a table exists in the given database."""
    # List tables in the database
    response = ath.list_table_metadata(CatalogName='AwsDataCatalog', DatabaseName=database)
    # Check if the table exists by name
    return any(metadata['Name'] == table for metadata in response['TableMetadataList'])

def drop_table(database, table, s3_output_location):
    """Drop the specified table."""
    query = f"DROP TABLE {database}.{table}"
    response = ath.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output_location
        }
    )
    logging.info(f"Table {table} dropped. QueryExecutionId: {response['QueryExecutionId']}")

    
def create_athena_table(ddl_file, table_name, s3_location, athena_cfg):
    
    with open(ddl_file) as create_table:
        query = create_table.read()
        query = query.replace('TABLENAME', table_name)
        query = query.replace('S3LOCATION', s3_location)
        
        database_name = athena_cfg.database
        
        # Check if the table exists and drop if it does
        if table_exists(database_name, table_name):
            logging.info(f"Table {table_name} exists. Dropping table...")
            # drop_table(database_name, 
            #            table_name, 
            #            s3_output_location=athena_cfg.output_config_location)
        else:
            logging.info(f"Table {table_name} does not exist. No action taken.")
    
        logging.info(f"Creating table {table_name} in database {database_name}...")
        
        # create new table
        ath.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
            'Database': database_name,
            'Catalog': 'AwsDataCatalog'
            },
            ResultConfiguration={'OutputLocation': athena_cfg.output_config_location}
        )

def remove_first_col_from_csv(file_path):
    df = pd.read_csv(file_path)

    # Check if the first column is an unintended index column
    if df.columns[0] == 'Unnamed: 0' or df.columns[0].startswith('Unnamed:'):
        # Drop the unintended index column
        df = df.drop(df.columns[0], axis=1)
        logging.info(f"Removed unintended index column from {file_path}")
        
    df.to_csv(file_path, index=False)

def main():
    
    conf = OmegaConf.load("aws_upload_cfg.yaml")
    
    # create bucket and folder if not exist already
    create_bucket(conf.s3.bucket)
    create_folder(bucket_name=conf.s3.bucket, folder_name=conf.s3.folder)
    
    # get local data folder files
    data_path = conf.local.folder
    
    if not os.path.isdir(data_path):
        raise FileNotFoundError(f"The directory {data_path} does not exist or is not a directory.")


    if conf.local.file: # only single file
        files = [conf.local.file]
    else:
        files = os.listdir(data_path)
    
    # loop through all files in the folder
    for file in files:
        
        if file.endswith(conf.local.extension):
            
            # upload file to s3
            file_path = os.path.join(data_path, file)
            file_no_ext = file.replace(conf.local.extension,'')
            s3_name = conf.s3.folder + file_no_ext + '/' + file
            
            logging.info(f'file {file_path} exists {os.path.isfile(file_path)}')
            logging.info(f's3 name {s3_name}')
                
            # upload file to s3
            remove_first_col_from_csv(file_path)
            upload_file(file_name=file_path,
                        bucket_name=conf.s3.bucket,
                        s3_file_name=s3_name)
    
            # create athena table
            s3_location = 's3://' + conf.s3.bucket + '/' + conf.s3.folder + file_no_ext + '/'
            logging.info(f"Creating Athena table for {file_no_ext} at s3 folder {s3_location}")
            create_athena_table(ddl_file=conf.athena.ddl_def,
                                table_name=file_no_ext,
                                s3_location=s3_location,
                                athena_cfg=conf.athena)

        
if __name__ == "__main__":
    main()