import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_proc_jl import *
from MTSM_read_xml import *
from MTSM_id_rec_sync import id_rec_by_distance
from MTSM_edi import *
from MTSM_proc_rec import *
from MTSM_ts_sort import run_ts_sort
from MTSM_qc import run_qc

os.chdir( Path(__file__).parents[2])

try:
	print('Chcecking folder structure..')
	create_folders(print=True)
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
print()
print(80*'_')

try:
	print('Loading sites...')
	load_new_sites()
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
print(80*'_')


try:
	run_proc_jl()
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
print(80*'_')


try:
	with open('xml_reload_type.txt','r') as file:
		reload=file.read().strip()

	ld=get_ld('ts',endswith='.xml')
	if len(ld)>0:
		gdf_xml=run_xml_read(reload)
		empty_rec=pd.isna(gdf_xml['ID_rec'])==True
		gdf_xml=gdf_xml[empty_rec]

		if len (gdf_xml)>0:
			print(f'\tFound {len(gdf_xml)} unmatched xml data:')
			[print('\t'+x) for x in gdf_xml['ID_xml']]
			print(f'\tFound {len(gdf_xml)} unmatched xml data:')

			with open('search_radius.txt') as file:
				search_radius=int(file.read().strip())

			print(f"\n\tSearh radius set to {search_radius}m!")
			search_dialog=input ('\tTo run distance synchronization type "y"! To change search radius type "c"! To skip sync press ENTER!\n\t\t')
			
			if search_dialog=='c':
				search_radius=int(input('\tEnter new search radius:\n\t\t'))
				id_rec_by_distance(search_radius)
				run_xml_read('smart')
			elif search_dialog=='y':
				id_rec_by_distance(search_radius)
				run_xml_read('smart')
			

	else:
		print("WARNING! '/ts/' folder is empty. Copy data and restart processing!")
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
	# print('Error reading joblists!')
print(80*'_')

try:
	run_ts_sort()
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
print()
print(80*'_')

try:
	run_sort_edi()
	run_read_edi()
	run_plot_edi()
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
print(80*'_')

try:
	run_proc_rec()
	reload_xml_paths()
	print()
	print(80*'_')
	run_qc()
	
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
print()
print(80*'_')

try:
	export_qc_geometry()
	delete_folder('MTSM_qgis/.qfieldsync')
except:
	pass



input('\n\nProccesing finished! Enter to exit! Refresh QGIS project to see changes!')