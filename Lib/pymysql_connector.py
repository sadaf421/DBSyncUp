from configparser import ConfigParser
import mysql.connector
from pathlib import Path
import os

config = ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'config', 'config.ini'))

def connect_src():
    return mysql.connector.connect(host = config['mysqlSourceDB']['host'],
                           user = config['mysqlSourceDB']['user'],
                           passwd = config['mysqlSourceDB']['pass'],
                           db = config['mysqlSourceDB']['db'])

def connect_trg():
    return mysql.connector.connect(host = config['mysqlTargetDB']['host'],
                           user = config['mysqlTargetDB']['user'],
                           passwd = config['mysqlTargetDB']['pass'],
                           db = config['mysqlTargetDB']['db'])

def schema_src():
	return config['mysqlSourceDB']['db']

def schema_trg():
	return config['mysqlTargetDB']['db']

def server_src():
	return config['mysqlSourceDB']['host']

def server_trg():
	return config['mysqlTargetDB']['host']
