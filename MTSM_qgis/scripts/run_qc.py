import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *
from MTSM_qc import run_qc

os.chdir( Path(__file__).parents[2])
try:

	print('Ignoring exceptions during qc!')
	run_qc(ignore_exceptions=True,print_qc_msg=True)
	input('\nPress ENTER to continue!')
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
