import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_import_export import *
from MTSM_proc_rec import run_proc_rec
os.chdir( Path(__file__).parents[2])

try:
	with open('fp_rec.txt','r') as file:
		fpath=file.read().strip()
	fpath=import_rec(fpath)
	run_proc_rec()
	input(f'Rec FL and QC data imported from:\n{fpath}!\nRefresh QGIS project to see changes! Press ENTER to exit!')
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
