import os
from fastapi import FastAPI

app = FastAPI()

basedir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
#servicedir = os.path.join(basedir, 'inventory')
confdir = os.path.join(basedir, 'resources')
datadir = os.path.join(basedir, 'data')

from src import routes


