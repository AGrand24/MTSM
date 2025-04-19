import os



abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_report import*
os.chdir( Path(__file__).parents[2])
try:
	with open('tl_range.txt','r') as file:
		tl_range=int(file.read().strip())
	with open('tl_page_range.txt','r') as file:
		page_range=int(file.read().strip())
	run_proc_report(tl_range,page_range)
	input('\nPress ENTER to continue!')
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
