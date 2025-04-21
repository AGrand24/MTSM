import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *

os.chdir( Path(__file__).parents[2])

try:

	import_sites_csv()
	input('\nPress ENTER to continue!')
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')

