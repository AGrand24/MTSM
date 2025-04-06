import pandas as pd
import geopandas as gpd
import numpy as np
import os
import shutil
import warnings
from lxml import etree
from tabulate import tabulate
# import xmltodict
import re
# from pretty_html_table import build_table
# import sqlite3
from datetime import datetime,timedelta,time
import time
import errno, stat
import ppigrf
from shapely import Point,Polygon,LineString,MultiPolygon,MultiLineString
from pathlib import Path
import traceback

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)
pd.set_option('mode.chained_assignment', None)


