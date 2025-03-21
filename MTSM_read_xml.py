from MTSM_python_modules import *
from MTSM_tools import *


def load_gdf_xml():
	if os.path.exists('MTSM_qgis/mtsm_xml.gpkg'):
		gdf_xml=load_gdf('xml')
		gdf_xml=gdf_xml.loc[gdf_xml['ID_xml']!='placeholder']
	return gdf_xml

def get_xml_ld(gdf_xml):
	'get list dir dataframe from/raw folder, filters dataframe for entries not in gdf_xml'
	ld_unsorted=get_ld('ts/',endswith='xml')
	if len (gdf_xml)>0:
		ld=ld_unsorted.copy().loc[~ld_unsorted['ID_xml'].isin(gdf_xml['ID_xml'])]
	else:
		ld=ld_unsorted
	return ld

def read_xml_data(ld):
	'reads xml data baset on ld dataframe into single dataframe'
	df_fields=pd.read_csv(f'MTSM/lib/fields/xml.csv').query('drop!=1').dropna(subset='orig_name')
	df_xml_read=pd.DataFrame()
	for i,index in enumerate(ld.index):
		xml_read=extract_xml(ld.loc[index,'file_path'])
		xml_single=pd.DataFrame(xml_read.loc[xml_read.index.isin(df_fields['orig_name'])]).T
		xml_single['xml_path']=ld.loc[index,'file_path'].replace('\\','/')
		df_xml_read=pd.concat([df_xml_read,xml_single])
		print(f"Reading \t{i+1}/{len(ld)}\t{ld.loc[index,'file_name']}",end='\r')
	return df_xml_read.reset_index(drop=True)

def fromat_xml_data(ld,df_xml_read):
	'formats and calculate derived field in xml_read dataframe'
	df_fields=pd.read_csv(f'MTSM/lib/fields/xml.csv').query('drop!=1').dropna(subset='orig_name')
	df_xml_read=df_xml_read.set_index(ld['ID_xml']).reset_index()
	df_xml_read=df_xml_read.rename(columns=dict(zip(df_fields['orig_name'],df_fields['field'])))
	df_xml_read['xml_rec_start']=pd.to_datetime(df_xml_read['xml_rec_start_date']+'T'+df_xml_read['xml_rec_start_time'])
	df_xml_read['xml_rec_end']=pd.to_datetime(df_xml_read['xml_rec_end_date']+'T'+df_xml_read['xml_rec_end_time'])
	df_xml_read['xml_x']=df_xml_read['xml_x'].astype(float)/3600000
	df_xml_read['xml_y']=df_xml_read['xml_y'].astype(float)/3600000
	df_xml_read['xml_rec_duration']=df_xml_read['xml_rec_end']-df_xml_read['xml_rec_start']
	df_xml_read['xml_rec_duration']=df_xml_read['xml_rec_duration'].dt.total_seconds()
	
	return df_xml_read.reset_index(drop=True)

def round_xml_data(gdf_xml):
	df_fields=pd.read_csv(f'MTSM/lib/fields/xml.csv').query("drop!=1 and dtype=='float'").dropna(subset='round')

	for index in df_fields.index:
		field=df_fields.loc[index,'field']
		round=int(df_fields.loc[index,'round'])
		gdf_xml[field]=np.round(gdf_xml[field].astype(float),round)
		
	return gdf_xml

def merge_xml_data(gdf_xml,df_xml_read):
	'merges data from gdf_xml with new data read into xml_read dataframe, prioritizes data from gdf'
	gdf_xml=pd.concat([gdf_xml,df_xml_read]).drop_duplicates(subset='ID_xml',keep='first')
	gdf_xml['xml_adu']=gdf_xml['xml_adu'].str.zfill(3)
	return gdf_xml

def run_xml_read():
	gdf_xml=load_gdf_xml()

	ld=get_xml_ld(gdf_xml)
	df_xml_read=read_xml_data(ld)
	if len(df_xml_read)>0:
		gdf_xml_read=fromat_xml_data(ld,df_xml_read)
		gdf_xml=merge_xml_data(gdf_xml,gdf_xml_read)
		gdf_xml=get_rec_duration_str(gdf_xml,'xml')
		gdf_xml=round_xml_data(gdf_xml)
		gdf_xml=save_gdf(gdf_xml,'xml')
	return gdf_xml

def run_xml_read_full():
	gdf_xml=load_gdf_xml()
	gdf_xml[['ID_xml','ID_rec']].to_csv('MTSM/lib/id_rec_xml_match.csv',index=False)


	gdf_xml=gdf_xml.drop(index=gdf_xml.index.to_list())
	gdf_xml=save_gdf(gdf_xml,'xml')
	gdf_xml=run_xml_read()
	gdf_xml=gdf_xml.drop(columns=['ID_rec'])
	gdf_xml=pd.merge(gdf_xml,pd.read_csv('MTSM/lib/id_rec_xml_match.csv').set_index('ID_xml'),left_on='ID_xml',right_index=True,how='left').sort_index(axis=1)
	gdf_xml=save_gdf(gdf_xml,'xml')
	return gdf_xml