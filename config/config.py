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
            'orders_wh': self.config['files']['orders_wh'],
            'customers': self.config['files']['customers'],
            'order_items': self.config['files']['order_items'],
            'covid19_cases': self.config['files']['covid19_cases'],
            'covid19_states': self.config['files']['covid19_states'],
            'student_review': self.config['files']['student_review']
        }
