# standard library
import logging 
import datetime
logging.basicConfig(level=logging.INFO)

# self imports
from fake_data_generation import generate_fake_data

def main():
    
    num_samples_generate = [100000, 1000000, 5000000, 10000000]
    
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
    unique_cols = {'person_id'}
    
    for n in num_samples_generate:
        logging.info(f'generating {n} samples')
        generate_fake_data(n=n, 
                           faker_config=faker_config,
                           unique_cols=unique_cols)
        
        
if __name__ == "__main__":
    main()