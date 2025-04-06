import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *

os.chdir( Path(__file__).parents[2])

def export_rec():
	gdf_rec=load_gdf('rec')
	export_fields=[col for col in gdf_rec.columns if col.startswith('rec_') or col.startswith('ID_')]
	path=f"MTSM_qgis/rec_import_export/export_rec_{datetime.now().strftime('%y%m%d_%H%M%S')}.rec"
	gdf_rec[export_fields].to_csv(path,sep='\t',index=False)
	print(f"\tExported REC data to:\n\t\t{path}")
	return path

def import_rec(fpath):
	# ld=get_ld('MTSM_qgis/rec_import_export/',endswith='.rec')

	# for index in ld.index:
	# 	print(index,ld.loc[index,'file_name'])

	# path_import=ld.loc[int(input('Select file index to import:\n')),'file_path']
	df_fields=pd.read_csv(f'MTSM_qgis/lib/fields/rec.csv')
	df=pd.DataFrame(columns=df_fields['field'])
	for field,dtype in zip(df_fields['field'],df_fields['dtype']):
		df[field]=df[field].astype(dtype)

	df_import=pd.read_csv(fpath,sep='\t')
	fields_rec=pd.read_csv('MTSM_qgis/lib/fields/rec.csv')['field'].to_list()
	fields_import=df_import.columns.to_list()
	fields_import=[field for field in fields_import if field in fields_rec]
	df_import=df_import[fields_import]
	df_import=pd.concat([df,df_import])
	# gdf=df_to_gdf(df_import,'rec')
	df_import['rec_fl_adu']=df_import['rec_fl_adu'].astype(str).str.replace('.0','').str.zfill(3)
	gdf=save_gdf(df_import,'rec')

	return fpath

def backup_id_xml_rec_match():
	path=f"MTSM_qgis/id_xml_bckp/{datetime.now().strftime('%y%m%d_%H%M%S')}.ids"
	print(f'\tExported ID_xml-ID_rec match to:\n\t\t{path}')
	gdf_xml=load_gdf('xml')

	gdf_xml[['ID_xml','ID_rec']].to_csv(path,index=False,sep='\t')


def export_backups():
	print('Creating backups...')
	export_rec()
	backup_id_xml_rec_match()
