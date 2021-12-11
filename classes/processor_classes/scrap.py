# Scrap code for html files
import bs4 as bs
import requests
import re
import asyncio
import aiohttp
from IPython.display import display, HTML
import pandas as pd
from pprint import pprint
import numpy as np
import xlwings as xw
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import threading
import json

import pathlib
script_path = str(pathlib.Path(__file__).parent.resolve()) + '/classes'
print(script_path)
import sys
sys.path.append(script_path)
import processor
