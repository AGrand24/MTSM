from MTSM_python_modules import *
from MTSM_tools import *

def export_rec():
	gdf_rec=load_gdf('rec')
	export_fields=[col for col in gdf_rec.columns if col.startswith('rec_') or col.startswith('ID_')]
	path=f"MTSM/rec_import_export/export_rec_{datetime.now().strftime('%y_%m_%d_%H_%M_%S')}.rec"
	gdf_rec[export_fields].to_csv(path,sep='\t',index=False)
	return path

def import_rec():
	ld=get_ld('MTSM/rec_import_export/',endswith='.rec')

	for index in ld.index:
		print(index,ld.loc[index,'file_name'])

	path_import=ld.loc[int(input('Select file index to import:\n')),'file_path']

	df_fields=pd.read_csv(f'MTSM/lib/fields/rec.csv')
	df=pd.DataFrame(columns=df_fields['field'])
	for field,dtype in zip(df_fields['field'],df_fields['dtype']):
		df[field]=df[field].astype(dtype)

	df_import=pd.read_csv(path_import,sep='\t')
	fields_rec=pd.read_csv('MTSM/lib/fields/rec.csv')['field'].to_list()
	fields_import=df_import.columns.to_list()
	fields_import=[field for field in fields_import if field in fields_rec]
	df_import=df_import[fields_import]
	df_import=pd.concat([df,df_import])
	# gdf=df_to_gdf(df_import,'rec')
	df_import['rec_fl_adu']=df_import['rec_fl_adu'].astype(str).str.replace('.0','').str.zfill(3)
	gdf=save_gdf(df_import,'rec')

	return path_import

def import_xml_id_xml():
	gdf_rec=load_gdf('rec')

	gdf_rec=gdf_rec.copy().set_index('ID_rec').dropna(subset=['ID_xml'])['ID_xml']

	xml_sync=pd.DataFrame()
	for index in gdf_rec.index:
		xmls=gdf_rec[index]
		xmls=xmls.split(', ')
		tmp=pd.DataFrame(index=xmls)
		tmp['ID_rec']=float(index)
		xml_sync=pd.concat([tmp,xml_sync])

	gdf_xml=load_gdf('xml').drop(columns='ID_rec')
	gdf_xml=pd.merge(gdf_xml,xml_sync,how='left',left_on='ID_xml',right_index=True)
	gdf_xml=save_gdf(gdf_xml,'xml')
	return gdf_xml

def backup_id_xml_rec_match():
	path=f"MTSM/id_xml_bckp/{datetime.now().strftime('%y%m%d_%H%M%S')}.ids"
	print(f'\tCreated backup of ID_rec-ID_xml match in - \t{path}')
	gdf_xml=load_gdf('xml')

	gdf_xml[['ID_xml','ID_rec']].to_csv(path,index=False,sep='\t')


