from MTSM_python_modules import *
from MTSM_tools import *

def create_project():
	'creates and saves site_gdf and empty_rec gdf from MTSM/sites.csv'
	gdf_site=save_gdf(pd.read_csv('MTSM/sites.csv'),'site',geometry=True)
	
	gdf_rec=gdf_site.copy().rename(columns={'site_x':'rec_x','site_y':'rec_y'})
	gdf_rec['ID_rec']=gdf_rec['ID_site']*10
	gdf_rec=fields_load_empty(gdf_rec,'rec')
	save_gdf(gdf_rec,'rec')

	xml_fields=pd.read_csv('MTSM/lib/fields/xml.csv').query('drop!=1')
	gdf_xml=pd.DataFrame()

	gdf_xml.loc[0,'ID_xml']='placeholder'
	gdf_xml.loc[0,'xml_x']=gdf_site.iloc[0]['site_x']
	gdf_xml.loc[0,'xml_y']=gdf_site.iloc[0]['site_y']

	gdf_xml=pd.concat([gdf_xml,pd.DataFrame(columns=xml_fields['field'])])

	save_gdf(gdf_xml.reset_index(drop=True),db_name='xml')

	return gdf_site,gdf_rec,gdf_xml

def fields_load_empty(gdf,db_name,**kwargs):
	'loads empty columns with specified dtype to gdf based on MTSM/lib/fields/{db_name}.csv'
	df_fields=pd.read_csv(f'MTSM/lib/fields/{db_name}.csv')
	if kwargs.get('drop',False)==True:
		df_fields=df_fields.query('drop!=1')

	df_fields=df_fields.loc[~df_fields['field'].isin(gdf.columns)]
	
	for index in df_fields.index:

		gdf[df_fields.loc[index,'field']]=pd.Series(dtype=df_fields.loc[index,'dtype'])
		gdf=gdf.copy()	
	return gdf

	# 'delete unwanted'
	# for folder in ['MTSM/edi']:
	# 	if os.path.exists(folder):
	# 		shutil.rmtree(folder)
	# 		os.makedirs(folder)

if input('To confirm type "y"')=='y':
	gdf_site,gdf_rec,gdf_xml=create_project()
	create_folders()

