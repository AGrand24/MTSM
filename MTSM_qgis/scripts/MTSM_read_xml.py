import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *
from MTSM_tools import *
from MTSM_import_export import backup_id_xml_rec_match

os.chdir( Path(__file__).parents[2])



def load_gdf_xml():
	if os.path.exists('MTSM_qgis/mtsm_xml.gpkg'):
		gdf_xml=load_gdf('xml')
		gdf_xml=gdf_xml.loc[gdf_xml['ID_xml']!='placeholder']
	return gdf_xml


def get_xml_ld(gdf_xml,reload):
	gdf_rec=load_gdf('rec')
	if reload=='full':
		rec_excluded=[]
	else:
		rec_excluded=gdf_rec.loc[~(gdf_rec['rec_qc_status'].isin(['Recording',None]))]['ID_rec'].astype(float)
		rec_excluded=rec_excluded.to_list()
		rec_excluded.append(0)
	
	ld=get_ld('ts/',endswith='.xml')
	ld=pd.merge(ld,gdf_xml.set_index('ID_xml')['ID_rec'],how='left',left_on='ID_xml',right_index=True)
	ld=ld.loc[~ld['ID_rec'].isin(rec_excluded)]

	if reload!='full':
		forced_reload=ld['ID_rec'].dropna().drop_duplicates().astype(int).to_list()
		if len (forced_reload)>0:
			print(f'\tForcing xml reload on recs: {forced_reload}')
	else:
		print('\tForcing full xml reload...')
	return ld.drop_duplicates('ID_xml',keep='first')

def read_xml_data(ld):
	'reads xml data baset on ld dataframe into single dataframe'
	df_fields=pd.read_csv(f'MTSM_qgis/lib/fields/xml.csv').query('drop!=1').dropna(subset='orig_name')
	df_xml_read=pd.DataFrame()
	for i,index in enumerate(ld.index):
		try:
			xml_read=extract_xml(ld.loc[index,'file_path'])
			xml_single=pd.DataFrame(xml_read.loc[xml_read.index.isin(df_fields['orig_name'])]).T
			xml_single['xml_path']=ld.loc[index,'file_path'].replace('\\','/')
			xml_single['ID_rec']=ld.loc[index,'ID_rec']
			df_xml_read=pd.concat([df_xml_read,xml_single])
			print(f"\tReading \t{i+1}/{len(ld)}\t{str(ld.loc[index,'ID_rec']).replace('.0','')}\t{ld.loc[index,'file_name']}",end='\n')
		except:
			print(f"\tError reading \t{i+1}/{len(ld)}\t{str(ld.loc[index,'ID_rec']).replace('.0','')}\t{ld.loc[index,'file_name']}",end='\n')

	return df_xml_read.reset_index(drop=True)

def fromat_xml_data(ld,df_xml_read):
	'formats and calculate derived field in xml_read dataframe'
	df_fields=pd.read_csv(f'MTSM_qgis/lib/fields/xml.csv').query('drop!=1').dropna(subset='orig_name')
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
	df_fields=pd.read_csv(f'MTSM_qgis/lib/fields/xml.csv').query("drop!=1 and dtype=='float'").dropna(subset='round')

	for index in df_fields.index:
		field=df_fields.loc[index,'field']
		round=int(df_fields.loc[index,'round'])
		gdf_xml[field]=np.round(gdf_xml[field].astype(float),round)
		
	return gdf_xml

def merge_xml_data(gdf_xml,df_xml_read):
	'merges data from gdf_xml with new data read into xml_read dataframe, prioritizes data from gdf'
	gdf_xml=pd.concat([gdf_xml,df_xml_read]).drop_duplicates(subset='ID_xml',keep='last')
	gdf_rec=load_gdf('rec')[['ID_rec','ID_xml']]
	gdf_xml['xml_adu']=gdf_xml['xml_adu'].str.zfill(3)
	return gdf_xml

def run_xml_read(reload):
	print('Reading xml data...')
	
	gdf_xml=load_gdf_xml()
	ld=get_xml_ld(gdf_xml,reload)
	if len(ld)>0:
		df_xml_read=read_xml_data(ld)
		gdf_xml_read=fromat_xml_data(ld,df_xml_read)
		gdf_xml=merge_xml_data(gdf_xml,gdf_xml_read)
		gdf_xml=get_rec_duration_str(gdf_xml,'xml')
		gdf_xml=round_xml_data(gdf_xml)
		gdf_xml=save_gdf(gdf_xml,'xml')
	print()
	return gdf_xml

# def run_xml_read_full():
# 	gdf_xml=load_gdf_xml()
# 	gdf_xml[['ID_xml','ID_rec']].to_csv('MTSM_qgis/lib/id_rec_xml_match.csv',index=False)


# 	gdf_xml=gdf_xml.drop(index=gdf_xml.index.to_list())
# 	gdf_xml=save_gdf(gdf_xml,'xml')
# 	gdf_xml=run_xml_read()
# 	gdf_xml=gdf_xml.drop(columns=['ID_rec'])
# 	gdf_xml=pd.merge(gdf_xml,pd.read_csv('MTSM_qgis/lib/id_rec_xml_match.csv').set_index('ID_xml'),left_on='ID_xml',right_index=True,how='left').sort_index(axis=1)
# 	gdf_xml=save_gdf(gdf_xml,'xml')
# 	return gdf_xml