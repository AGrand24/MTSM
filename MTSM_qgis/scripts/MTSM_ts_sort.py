import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *
from MTSM_read_xml import load_gdf_xml,reload_xml_paths


os.chdir( Path(__file__).parents[2])



def ts_sync():
	ld=get_ld('ts')



	if len(ld)>0:
		ld_delete=ld.copy().loc[~ld['file_path'].str.endswith(('ats','xml'))]

		if len(ld_delete)>0:
			for fp in ld_delete['file_path']:
				os.remove(fp)
				print(f'\tDeleted {fp}')

		ld=ld.copy().loc[ld['file_path'].str.endswith(('ats','xml'))]
		ld['file_path']=ld['file_path'].str.replace('\\','/')
		# 
		ld['parent']=[Path(fp).parent.name.replace('meas_','') for fp in ld['file_path'] ]
		ld['child']='/meas_'+ld['parent']+'/'
		ld['ID_xml']=ld['file_name'].str.slice(0,3)+'_'+ld['parent']

		gdf_xml=load_gdf_xml()
		gdf_xml=gdf_xml.set_index('ID_xml')['ID_rec']

		ld=pd.merge(ld,gdf_xml,how='left',left_on='ID_xml',right_index=True)

		ld.loc[ld['ID_rec'].isnull(),'fp_dest']='ts/1_unmatched/'+ld['ID_xml'].str.slice(0,3)+ld['child']+ld['file_name']
		ld.loc[ld['ID_rec']==0,'fp_dest']='ts/2_discarded/'+ld['ID_xml'].str.slice(0,3)+ld['child']+ld['file_name']
		ld.loc[ld['ID_rec']>0,'fp_dest']='ts/Site_'+ld['ID_rec'].astype(str).str.replace('.0','')+ld['child']+ld['file_name']



		ld_move=ld.copy().loc[ld['file_path']!=ld['fp_dest']][['ID_rec','ID_xml','file_path','fp_dest','file_name']]
		ld_move=ld_move.dropna(subset='fp_dest').dropna(subset='ID_xml')
		
		move_msg=[]
		if len(ld_move)>0:
			mkdir=[]
			for fn,fp in zip(ld_move['file_name'],ld_move['fp_dest']):
				mkdir.append(fp.replace(fn,''))

			ld_move['mkdir']=mkdir

			for id_xml in ld_move['ID_xml'].sort_values().unique():
				ld=ld_move.copy().query(f"ID_xml=='{id_xml}'")
				print(f"\tMoving - {ld.iloc[0]['file_path'].replace(ld.iloc[0]['file_name'],'')}'   to   {ld.iloc[0]['mkdir']}")

				for mk,fp1,fp2 in zip(ld['mkdir'],ld['file_path'],ld['fp_dest']):
					if not os.path.exists(mk):
								os.makedirs(mk)
					move_msg.append(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\t{id_xml}\t{fp1}\t{fp2}'\n")
					shutil.move(fp1,fp2)
					pass
				
			if len(move_msg)>0:
				with open('MTSM_qgis/lib/tss_log.tsv','a+') as file:
					file.writelines(move_msg)
		else:
			print("\t'/ts/' folder is fully synchronized!")
		
	else:
		print("\t'/ts/' folder is empty! Copy data!")




def run_ts_sort():
	print('Synchronizing /ts folder...')
	ts_sync()
	delete_empty_folders('ts')
	delete_empty_folders('ts')
	reload_xml_paths()
	create_folders()






# def run_ts_sort():
# 	print('Synchronizing /ts folder...')

# 	delete_empty_folders('ts')
# 	gdf_xml=load_gdf_xml()
# 	ld=get_ld('ts/')
# 	if len(ld)>0:

# 		gdf_xml=gdf_xml[['ID_rec','ID_xml']].set_index('ID_xml')
# 		ld=ld.loc[ld['file_name'].str.endswith(('xml','ats'))]
# 		ld['tmp']=ld['file_path'].str.findall(r'(?<=meas_)(.*)(?=)')
# 		ld['tmp']=[x[0] for x in ld['tmp']]
# 		ld['meas']=ld['tmp'].str.slice(0,19)
# 		ld['adu']=ld['tmp'].str.slice(20,23)
# 		ld['ID_xml']=ld['adu']+'_'+ld['meas']
# 		ld=ld[['file_name','file_path','ID_xml']]
# 		ld=pd.merge(ld,gdf_xml,how='left',left_on='ID_xml',right_index=True)

# 		ld['adu']=ld['ID_xml'].str.slice(None,3)
# 		ld['fp_sync']=ld['ID_rec'].astype(str).str.replace('.0','')

# 		fp_sync=[]
# 		for fp,adu in zip(ld['fp_sync'],ld['adu']):
# 			if fp=='0':
# 				fp_sync.append(f'ts/2_discarded/{adu}/')
# 			elif fp=='nan':
# 				fp_sync.append(f'ts/1_unmatched/{adu}/')
# 			else:
# 				fp_sync.append(f'ts/Site_{fp}/')

# 		ld['dir']=fp_sync
# 		ld['dir']+='meas_'+ld['ID_xml'].str.slice(4,None)+'/'
# 		ld['fp_sync']=ld['dir']+ld['file_name']
# 		ld['fp_sync']=ld['fp_sync'].str.replace('\\','/')
# 		ld['file_path']=ld['file_path'].str.replace('\\','/')
# 		ld=ld.loc[ld['file_path']!=ld['fp_sync']][['file_path','fp_sync','ID_rec','dir']]

# 		for dir,fp1,fp2 in zip(ld['dir'],ld['file_path'],ld['fp_sync']):
# 			if not os.path.exists(dir):
# 				os.makedirs(dir)
# 			shutil.move(fp1,fp2)
# 			try:
# 				print(f'\tMoving {fp1[3:46]}...\t to \t {fp2[3:44]}')
# 			except:
# 				pass

# 		if len(ld['fp_sync'])<1:
# 			print('\t/ts folder fully synchronized!')

# 		delete_empty_folders('ts')
# 		print()

# 		create_folders(folders='unmatched')
		# reload_xml_paths()
# 		print()


