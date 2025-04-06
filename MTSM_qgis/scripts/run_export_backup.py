import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)


from MTSM_import_export import *
import traceback

try:
	export_backups()
except Exception as error:
	traceback.print_exc()
	input('')


input(f'Backups exported! Press ENTER to exit!')
