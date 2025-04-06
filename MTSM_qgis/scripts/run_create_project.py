import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_python_modules import *
from MTSM_tools import *



os.chdir( Path(__file__).parents[2])

def create_project():
	'creates and saves site_gdf and empty_rec gdf from MTSM/sites.csv'
	gdf_site=save_gdf(pd.read_csv('MTSM_qgis/sites.csv'),'site',geometry=True)
	
	gdf_rec=gdf_site.copy().rename(columns={'site_x':'rec_x','site_y':'rec_y'})
	gdf_rec['ID_rec']=gdf_rec['ID_site']*10
	gdf_rec=fields_load_empty(gdf_rec,'rec')
	save_gdf(gdf_rec,'rec')
	gdf=[]
	for db in ['xml','edi']:
		fields=pd.read_csv('MTSM_qgis/lib/fields/xml.csv').query('drop!=1')
		gdf_tmp=pd.DataFrame()

		gdf_tmp.loc[0,f'ID_{db}']='placeholder'
		gdf_tmp['ID_rec']=0
		gdf_tmp.loc[0,f'{db}_x']=gdf_site.iloc[0]['site_x']
		gdf_tmp.loc[0,f'{db}_y']=gdf_site.iloc[0]['site_y']

		gdf_tmp=pd.concat([gdf_tmp,pd.DataFrame(columns=fields['field'])])
		gdf.append(gdf_tmp)

		save_gdf(gdf_tmp.reset_index(drop=True),db_name=db)

	return gdf_site,gdf_rec,gdf[0],gdf[1]

def fields_load_empty(gdf,db_name,**kwargs):
	'loads empty columns with specified dtype to gdf based on MTSM/lib/fields/{db_name}.csv'
	df_fields=pd.read_csv(f'MTSM_qgis/lib/fields/{db_name}.csv')
	if kwargs.get('drop',False)==True:
		df_fields=df_fields.query('drop!=1')

	df_fields=df_fields.loc[~df_fields['field'].isin(gdf.columns)]
	
	for index in df_fields.index:

		gdf[df_fields.loc[index,'field']]=pd.Series(dtype=df_fields.loc[index,'dtype'])
		gdf=gdf.copy()	
	return gdf


try:
	print("Cleaning databases...")
	gdf_site,gdf_rec,gdf_xml,gdf_edi=create_project()
	print('Creating folder structure..')
	create_folders()
	input('Finished! Press ENTER to exit!')
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')

