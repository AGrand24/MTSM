import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *
from MTSM_read_xml import load_gdf_xml,reload_xml_paths


os.chdir( Path(__file__).parents[2])

def run_ts_sort():
	print('Synchronizing /ts folder...')

	delete_empty_folders('ts')
	gdf_xml=load_gdf_xml()
	ld=get_ld('ts/')
	if len(ld)>0:
		# ld_delete=ld.copy().loc[~ld['file_name'].str.endswith(('xml','ats'))]

		# if len(ld_delete)>0:
		# 	for fp in ld_delete['file_path']:
		# 		os.remove(fp)
		gdf_xml=gdf_xml[['ID_rec','ID_xml']].set_index('ID_xml')
		ld=ld.loc[ld['file_name'].str.endswith(('xml','ats'))]
		ld['tmp']=ld['file_path'].str.findall(r'(?<=meas_)(.*)(?=)')
		ld['tmp']=[x[0] for x in ld['tmp']]
		ld['meas']=ld['tmp'].str.slice(0,19)
		ld['adu']=ld['tmp'].str.slice(20,23)
		ld['ID_xml']=ld['adu']+'_'+ld['meas']
		ld=ld[['file_name','file_path','ID_xml']]
		ld=pd.merge(ld,gdf_xml,how='left',left_on='ID_xml',right_index=True)

		ld['adu']=ld['ID_xml'].str.slice(None,3)
		ld['fp_sync']=ld['ID_rec'].astype(str).str.replace('.0','')

		fp_sync=[]
		for fp,adu in zip(ld['fp_sync'],ld['adu']):
			if fp=='0':
				fp_sync.append(f'ts/2_discarded/{adu}/')
			elif fp=='nan':
				fp_sync.append(f'ts/1_unmatched/{adu}/')
			else:
				fp_sync.append(f'ts/Site_{fp}/')

		ld['dir']=fp_sync
		ld['dir']+='meas_'+ld['ID_xml'].str.slice(4,None)+'/'
		ld['fp_sync']=ld['dir']+ld['file_name']
		ld['fp_sync']=ld['fp_sync'].str.replace('\\','/')
		ld['file_path']=ld['file_path'].str.replace('\\','/')
		ld=ld.loc[ld['file_path']!=ld['fp_sync']][['file_path','fp_sync','ID_rec','dir']]

		for dir,fp1,fp2 in zip(ld['dir'],ld['file_path'],ld['fp_sync']):
			if not os.path.exists(dir):
				os.makedirs(dir)
			shutil.move(fp1,fp2)
			try:
				print(f'\tMoving {fp1[3:46]}...\t to \t {fp2[3:44]}')
			except:
				pass

		if len(ld['fp_sync'])<1:
			print('\t/ts folder fully synchronized!')

		delete_empty_folders('ts')
		print()

		create_folders(folders='unmatched')
		reload_xml_paths()
		print()


