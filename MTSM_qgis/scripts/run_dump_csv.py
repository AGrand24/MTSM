import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *
from MTSM_import_export import dump_to_csv

os.chdir( Path(__file__).parents[2])
try:
	dump_to_csv()
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
