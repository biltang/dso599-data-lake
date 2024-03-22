# standard library
import logging
import datetime
from functools import wraps
import time

# third party library
from faker import Faker
import pandas as pd

# set logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler() # Create console handler and set level to info
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler) # Add the handler to the logger


def log_runtime(func) -> callable:
    """Decorator to log the runtime of a function"""
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        """wrapper function to log the runtime of a function

        Returns:
            original function output
        """
        start_time = time.time()
        
        result = func(*args, **kwargs) # original function call
        
        end_time = time.time()
        logger.info(f"Function '{func.__name__}' took {end_time - start_time} seconds to run.")
        
        # Save run time to a text file
        with open('run_time.txt', 'a') as file:
            file.write(f"Run time: {end_time - start_time} seconds\n")
    
        return result
    
    return wrapper

def get_city_from_address(address: str, component: str) -> str:
    """Given string address format from faker.address(), extract city, state, country 
    based on string parsing of the address format

    Args:
        address (str): address output from faker.address()
        component (str): city, state, or country component to extract

    Returns:
        str: extracted city, state, or country
    """
    city_state_comp = address.splitlines()[1] # second line of address is city, state, zip
    city_state_split = city_state_comp.replace(",", "").split(' ') # split by space and remove commas
    
    if component == 'city':
        return city_state_split[0] # first element is city
    elif component == 'state':
        return city_state_split[1] # second element is state
    elif component == 'country':
        return 'United States' # country is always United States for this example, but TODO: would be to determine based on faker locale.
    
    
def generate_single_row(cur_id: int, fake: Faker, faker_config: dict, unique_cols: set) -> dict:
    """generate a single row of data based on faker_config and unique_cols

    Args:
        cur_id (int): current row id of data
        fake (Faker): faker object
        faker_config (dict): config settings for faker data generation of variables, where each key is a column to generate, 
                             and value contains the faker function to use, and any kwargs to pass to the function
        unique_cols (set): which columns to enforce uniqueness on

    Returns:
        dict: dictionary containing the generated data for a single row
    """
    cur_row = {}
    
    cur_row['person_id'] = cur_id # to ensure unique person_id, just use loop id
    
    # loop through the items of faker_config and generate the corresponding variable
    for key, value in faker_config.items():
        
        func_name = value['func'] # faker function to use
        
        if key in unique_cols: # if unique, ensure uniqueness
            unique_provider = getattr(fake, 'unique')
            gen_func = getattr(unique_provider, func_name)
            
        else: # else just use the regular faker function
            gen_func = getattr(fake, func_name, None)
            
        # check if kwargs are provided, and pass them to the function if so   
        if 'kwargs' in value:
            gen_val = gen_func(**value['kwargs'])
        else: # else just call the function
            gen_val = gen_func()
        
        cur_row[key] = gen_val
        
    # extract city, state, country from address if address is generated    
    if 'address' in faker_config:
        cur_row['city'] = get_city_from_address(cur_row['address'], 'city')
        cur_row['state'] = get_city_from_address(cur_row['address'], 'state')
        cur_row['country'] = get_city_from_address(cur_row['address'], 'country')
        
    return cur_row

@log_runtime 
def generate_fake_data(n: int=10, faker_config: dict=None, unique_cols: set=None, save_path='../data/') -> None:
    """Given number of samples to generate n, call generate_single_row to generate n samples of fake data based on faker_config, unique_cols, and save generated data
    as output csv to save_path if provided, else example faker_config and unique_cols are provided below in the case where the arguments are None (meaning not provided)

    Args:
        n (int, optional): Number of samples to generage. Defaults to 10.
        faker_config (dict, optional): dictionary containing config settings for variables to generate, where each key is a column to generate, 
                                       and value contains the faker function to use, and any kwargs to pass to the function. Defaults to None.
        unique_cols (set, optional): columns to enforce uniqueness on. Defaults to None.
        save_path (str, optional): path to save csv to. Defaults to '../data/'.
    """
    #########################################################
    # set some default config values in case not supplied
    #########################################################
    if faker_config is None:
        faker_config = {'first_name':  {'func':'first_name'},
                        'last_name': {'func':'last_name'},
                        'email': {'func': 'email'},
                        'date_of_birth': {'func': 'date_between',
                                           'kwargs': {'start_date': datetime.date(1990,1,1), 
                                                      'end_date': datetime.date(2000,1,1)}
                                         },
                        'address': {'func': 'address'},
                        'bank_balance': {'func': 'random_int',
                                         'kwargs': {'min': 100,
                                                    'max': 10000}}
                        }
        
    if unique_cols is None:
        unique_cols = {'person_id'}
    
    #########################################################
    
    fake = Faker()
    gen_data = [generate_single_row(cur_id=i,
                                    fake=fake,
                                    faker_config=faker_config,
                                    unique_cols=unique_cols) for i in range(n)] # call generate_single_row for each row n timesS
    
    gen_data = pd.DataFrame(gen_data)
    
    # save csv file down
    save_file_path = save_path + f'fake_data_{n}_sample.csv'
    logger.info(f'saving csv to {save_file_path}')
    gen_data.to_csv(save_file_path)
    

def main() -> None:
    num_samples = 10
    generate_fake_data(n=num_samples)


if __name__ == "__main__":
    main()
