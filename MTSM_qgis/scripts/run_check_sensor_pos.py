import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_qc import *
os.chdir( Path(__file__).parents[2])


try:
	run_check_sensor_pos()
except Exception as error:
	traceback.print_exc()
	# input('Press ENTER to continue!')