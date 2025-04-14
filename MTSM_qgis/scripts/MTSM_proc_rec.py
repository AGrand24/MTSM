import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *

os.chdir( Path(__file__).parents[2])


def groupby_xml():
	gdf_xml=load_gdf('xml').dropna(subset='ID_rec')
	gdf_xml=gdf_xml.sort_values(['ID_rec','ID_xml'])
	gb_fields=pd.read_csv('MTSM_qgis/lib/fields/xml.csv').query('drop!=1').dropna(subset='groupby')

	for index in gb_fields.index:
		gdf_xml[gb_fields.loc[index,'field']]=gdf_xml[gb_fields.loc[index,'field']].astype(gb_fields.loc[index,'dtype'])

	agg_dict=dict(zip(list(gb_fields['field'].loc[gb_fields['groupby']!='list']),list(gb_fields['groupby'].loc[gb_fields['groupby']!='list'])))
	gb=gdf_xml.groupby('ID_rec').agg(agg_dict)

	cols_join=gb_fields.loc[gb_fields['groupby']=='list']['field']
	gdf_xml[cols_join]=gdf_xml[cols_join].astype(str)

	for col in cols_join:
		gb_tmp=gdf_xml.groupby('ID_rec').aggregate({col:(', ').join})
		gb=gb.join(gb_tmp)

	gdf_xml['xml_x']=gdf_xml['xml_x']*gdf_xml['xml_gps_sync']
	gdf_xml['xml_y']=gdf_xml['xml_y']*gdf_xml['xml_gps_sync']
	gdf_xml=gdf_xml.loc[gdf_xml['xml_gps_sync']>1]

	gb_coord=gdf_xml.groupby('ID_rec')[['xml_gps_sync','xml_x','xml_y']]
	gb_coord=gb_coord.agg('sum')

	gb_coord['xml_x']=gb_coord['xml_x']/gb_coord['xml_gps_sync']
	gb_coord['xml_y']=gb_coord['xml_y']/gb_coord['xml_gps_sync']

	gb=pd.merge(gb.drop(columns=['xml_x','xml_y']),gb_coord[['xml_x','xml_y']],left_index=True,right_index=True,how='left')

	gb['xml_gps_num_sats']=np.round(gb['xml_gps_num_sats'],2)
	return gb

def merge_xml2rec(gb):
	rec_fields=pd.read_csv('MTSM_qgis/lib/fields/rec.csv')['field'].to_list()
	gdf_rec=load_gdf('rec')[rec_fields].drop(columns=['ID_xml','xml_rec_start','xml_rec_end'])

	gdf_rec=pd.merge(gdf_rec,gb,how='left',left_on='ID_rec',right_index=True)
	gdf_rec=get_rec_duration_str(gdf_rec,'xml')
	gdf_rec['rec_fl_adu']=gdf_rec['rec_fl_adu'].str.zfill(3)
	gdf_rec=save_gdf(gdf_rec,'rec')
	return gdf_rec

def load_new_sites():
	print('\tLooking for new sites...')


	df_site_csv=pd.read_csv('MTSM_qgis/sites.csv')
	gdf_site_gpkg=load_gdf('site')


	gdf_site_gpkg['site_x']=gdf_site_gpkg.get_coordinates().iloc[:,0]
	gdf_site_gpkg['site_y']=gdf_site_gpkg.get_coordinates().iloc[:,1]
	df_site_gpkg=gdf_site_gpkg.drop(columns='geometry')

	df_site=pd.concat([df_site_gpkg,df_site_csv]).drop_duplicates('ID_site',keep='first').sort_values('ID_site')
	gdf_site=save_gdf(df_site[['ID_site','site_x','site_y']],'site')

	gdf_site=gdf_site.rename(columns={'site_x':'rec_x','site_y':'rec_y'})
	gdf_site['ID_rec']=gdf_site['ID_site']*10
	gdf_rec=load_gdf('rec')

	gdf_rec=pd.concat([gdf_rec,gdf_site]).drop_duplicates(subset='ID_rec',keep='first').reset_index(drop=True)

	new_sites=gdf_site.loc[~gdf_site['ID_site'].isin(gdf_rec['ID_site'])]['ID_site']
	if len(new_sites)>0:
		new_sites=new_sites.sort_values().to_list()
		print(f'\t\t Loaded new sites - {list(set(new_sites))}')
	
	gdf_site=gdf_site.drop(columns='ID_rec')
	
	print('\tReloading site geometries...')
	gdf_rec=pd.merge(gdf_rec.drop(columns=['rec_x','rec_y']),gdf_site.set_index('ID_site'),left_on='ID_site',right_index=True,how='left')
	save_gdf(gdf_rec,'rec')


def get_number_of_jobs():
	gdf=load_gdf('rec')
	n_jobs=[]
	for xml in gdf['ID_xml']:
		# print(xml)
		if xml is not None:
			n_jobs.append(len(xml.split(', ')))
		else:
			n_jobs.append(None)
	gdf['rec_xml_num_of_jobs']=n_jobs
	gdf['rec_xml_num_of_jobs']=gdf['rec_xml_num_of_jobs'].astype(float)-gdf['rec_fl_num_test_jobs'].replace(np.nan,0)
	save_gdf(gdf,'rec')
	return gdf


def rec_mag_dec():
	gdf=load_gdf('rec')
	dates=gdf['xml_rec_start'].dt.date.replace(np.nan,'2025-01-01').astype(str)
	dates=[datetime.strptime(date,'%Y-%m-%d') for date in dates]

	mag_dec=[]
	for lon,lat,date in zip(gdf['rec_x'],gdf['rec_y'],dates):
		mag_dec.append(get_mag_dec(lon,lat,0,date))

	gdf['rec_mag_dec']=mag_dec
	save_gdf(gdf,'rec')
	return gdf


def run_proc_rec():
	print('Processing REC database...')
	print('\tMatchning XML data...')
	gb=groupby_xml()
	gdf_rec=merge_xml2rec(gb)
	
	load_new_sites()

	print('\tCalculating magnetic declination...')
	rec_mag_dec()
	gdf_rec=get_number_of_jobs()