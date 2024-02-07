import configparser


class Config:
    def __init__(self, config_file='config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def get_config(self):
        return {
            'orders': self.config['files']['orders']
        }
