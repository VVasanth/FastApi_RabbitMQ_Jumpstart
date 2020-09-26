import os
import time
import configparser
import argparse
from logging import getLogger, StreamHandler, DEBUG

class Manager:
    def __init__(self, program_name):

        self.logger = getLogger()
        handler = StreamHandler()
        handler.setLevel(DEBUG)
        self.logger.setLevel(DEBUG)
        self.logger.addHandler(handler)
        self.logger.propagate = False

        self.logger.info('########## INITIALISE #############')

        self.program_name = program_name
        self.logger.info('Program name: ' + program_name)

        parser = argparse.ArgumentParser()
        parser.add_argument('-d', '--debug', \
            action = 'store_const', \
            const = True, \
            default = False, \
            help = 'debug mode')
        parser.add_argument('-o', '--options', \
            nargs = '+', \
            type = str, \
            help = 'options')
        self.args = parser.parse_args()
        self.logger.info('Aags: ')
        for arg in vars(self.args):
            self.logger.info('  - ' + arg + ': ' + str(getattr(self.args, arg)))

        self.config = configparser.ConfigParser()
        self.config.read('config/' + os.environ['ENV'] + '.ini')
        self.logger.info('env: ' + os.environ['ENV'])

    def __enter__(self):
        self.start = time.time()
        self.logger.info('############# START ###############')
        return self

    def __exit__(self, type, value, traceback):
        self.end = time.time()
        self.logger.info('############ FINISH ###############')
        self.logger.info("{}: {:.3f} sec consumed.".format(self.program_name, self.end - self.start))