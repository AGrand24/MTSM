import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_read_xml import *
from MTSM_ts_sort import *
from MTSM_proc_rec import *
from MTSM_proc_jl import *
from MTSM_edi import *
from MTSM_id_rec_sync import *
import sys


try:
	proc_type = sys.argv[1]
except:
	proc_type='main'
# print(num1)
run_proc_jl()


if proc_type=='main':
	gdf_xml=run_xml_read('matched')
else:
	gdf_xml=run_xml_read('full')

empty_rec=pd.isna(gdf_xml['ID_rec'])==True
gdf_xml=gdf_xml[empty_rec]

if len (gdf_xml)>0:
	print(f'Found {len(gdf_xml)} unmatched xml data:')
	[print(x) for x in gdf_xml['ID_xml']]
	if input ('\nTo run distance synchronization type "y":\n')=='y':
		id_rec_by_distance()
		gdf_xml=run_xml_read('matched+unmatched')


run_ts_sort()

print('Sorting and reading edi...')
run_sort_edi()
run_read_edi()
print('\nProcessing rec database...')
run_proc_rec()


input('\n\nProccesing finished! Enter to exit!')