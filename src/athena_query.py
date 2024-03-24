# standard
import logging
import time 

# third-party
import boto3
from omegaconf import OmegaConf 

# logging settings
logging.basicConfig(level=logging.INFO)

# Create an Athena client
athena = boto3.client('athena')


def execute_athena_query(query: str, database: str, table: str, output_location: str) -> str:
    """Function to execute an Athena query

    Args:
        query (str): execute query
        database (str): database name
        table (str): table name
        output_location (str): s3 output location

    Returns:
        _type_: return execution query id
    """
    query_execution_context = {'Database': database}
    
    result_configuration = {'OutputLocation': output_location}

    response = athena.start_query_execution(
                            QueryString=query,
                            QueryExecutionContext=query_execution_context,
                            ResultConfiguration=result_configuration
                            )
    
    query_execution_id = response['QueryExecutionId'] # get query execution id for checking status
    
    return query_execution_id


def log_msg_to_file(log_file: str, msg: str):
    """Given msg and log file, write msg to log file

    Args:
        log_file (str): log_file name
        msg (str): msg to record
    """
    
    with open(log_file, 'a') as file:
        file.write(msg)
        
        
def wait_for_query_completion(query_execution_id: str, query: str, wait_time: float=5):
    """
    Args:
        query_execution_id (str): execution_id to check status
        query (str): query ran, mainly for logging
        wait_time (float): wait time in seconds before checking status again
    """
    
    while True: # keep running until query is completed
        
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        print(status)
        
        # when query finishes executing
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            
            if status == 'SUCCEEDED':
                
                # Calculate the execution time
                start_time = response['QueryExecution']['Status']['SubmissionDateTime']
                end_time = response['QueryExecution']['Status']['CompletionDateTime']
                execution_time = end_time - start_time

                log_execute_time = f"Query {query} completed in {execution_time} seconds.\n"
                logging.info(log_execute_time)
                
                # Save run time to a text file
                log_msg_to_file(log_file='athena_query_run_time.txt', 
                                msg=log_execute_time)
            else:
                # log why query failed
                logging.info(f"""Query {query_execution_id} {status} because 
                         {response['QueryExecution']['Status']['StateChangeReason']}.""")
            
            return response, status
        
        # wait for wait_time before checking status again 
        time.sleep(wait_time)
            
            
def main():
    """main function to execute Athena query and extract execution time based 
    database and table names in the configuration file.
    """
    
    conf = OmegaConf.load("athena_query.yaml") # config file for athena query
    
    # get query from sql query file specified in conf.athena.query
    with open(conf.athena.query, "r") as file:
        query = file.read()
    
    logging.info(f"Query: {query}")
    
    tables = conf.athena.tables
    
    # run query for each table
    for t in tables:
        logging.info(f"Table: {t}")
        
        replace_query = query.replace("TABLENAME", t)
        
        # call api to execute query
        execution_id = execute_athena_query(query=replace_query, 
                                        database=conf.athena.database, 
                                        table=t, 
                                        output_location=conf.athena.output_location)
    
        # wait for query to complete, check status, and get execution time
        execution_final_response, status = wait_for_query_completion(query_execution_id=execution_id, 
                                                            query=replace_query)
        
        
            
if __name__ == "__main__":
    main()