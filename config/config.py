import configparser
import os


class Config:
    def __init__(self, config_file='config.ini'):
        config_path = os.path.join(os.path.dirname(__file__), config_file)
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    def get_config(self):
        return {
            'orders': self.config['files']['orders'],
            'orders_sample': self.config['files']['orders_sample'],
            'orders_sample1': self.config['files']['orders_sample1'],
            'orders_sample2': self.config['files']['orders_sample2'],
            'orders_sample3': self.config['files']['orders_sample3'],
            'order_data': self.config['files']['order_data'],
            'orders_wh': self.config['files']['orders_wh'],
            'customers': self.config['files']['customers'],
            'customer_transfer': self.config['files']['customer_transfer'],
            'order_items': self.config['files']['order_items'],
            'covid19_cases': self.config['files']['covid19_cases'],
            'covid19_states': self.config['files']['covid19_states'],
            'student_review': self.config['files']['student_review'],
            'products': self.config['files']['products'],
            'groceries': self.config['files']['groceries'],
            'customer_nested': self.config['files']['customer_nested'],
            'library_data': self.config['files']['library_data'],
            'train': self.config['files']['train'],
            'sales_data': self.config['files']['sales_data'],
            'hospital': self.config['files']['hospital'],
            'windowdata': self.config['files']['windowdata'],
            'windowdata_modified': self.config['files']['windowdata_modified'],
            'logdata1m': self.config['files']['logdata1m']
        }
