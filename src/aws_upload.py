# standard library
import os

# third-party
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from omegaconf import OmegaConf 


# create s3 client
s3 = boto3.client('s3')

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
        file_path = os.path.join(data_path, conf.local.file, conf.local.extension)
        print(os.path.isfile(file_path))
    else:
        
        # loop through all files in the folder
        for file in os.listdir(data_path):
            
            if file.endswith('.csv'):
                file_path = os.path.join(data_path, file)
                s3_name = conf.s3.folder + file
                print(f'file {file_path} exists {os.path.isfile(file_path)}')
                print(f's3 name {s3_name}')
                
                #upload file to s3
                upload_file(file_name=file_path,
                            bucket_name=conf.s3.bucket,
                            s3_file_name=s3_name)
    

        
if __name__ == "__main__":
    main()