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
    def wrapper(*args, **kwargs) -> callable:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Function '{func.__name__}' took {end_time - start_time} seconds to run.")
        return result
    return wrapper

def get_city_from_address(address: str, component: str) -> str:
    
    city_state_comp = address.splitlines()[1]
    city_state_split = city_state_comp.replace(",", "").split(' ')
    
    if component == 'city':
        return city_state_split[0]
    elif component == 'state':
        return city_state_split[1]
    elif component == 'country':
        return 'United States'
    
    
def generate_single_row(cur_id: int, fake: Faker, faker_config: dict, unique_cols: set) -> dict:
    
    cur_row = {}
    cur_row['person_id'] = cur_id
    for key, value in faker_config.items():
        func_name = value['func']
        if key in unique_cols:
            unique_provider = getattr(fake, 'unique')
            gen_func = getattr(unique_provider, func_name)
        else:
            gen_func = getattr(fake, func_name, None)
            
        if 'kwargs' in value:
            gen_val = gen_func(**value['kwargs'])
        else:
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
                                    unique_cols=unique_cols) for i in range(n)]
    
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
