import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *
from MTSM_edi import clear_edi_img,run_plot_edi
os.chdir( Path(__file__).parents[2])
try:
	clear_edi_img(full=False)
	run_plot_edi()
	input('\nPress ENTER to continue!')
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')