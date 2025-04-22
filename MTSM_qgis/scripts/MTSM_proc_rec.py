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
	gdf_rec['rec_qc_exception']=gdf_rec['rec_qc_exception'].fillna(0)
	gdf_rec=save_gdf(gdf_rec,'rec')
	return gdf_rec

# def load_sites():
# 	print('\tLooking for new sites...')

# 	df_site=load_gdf('site')

# 	df_site['site_x']=df_site.get_coordinates().iloc[:,0]
# 	df_site['site_y']=df_site.get_coordinates().iloc[:,1]
# 	df_site=df_site.drop(columns='geometry')

# 	df_site=df_site.rename(columns={'site_x':'rec_x','site_y':'rec_y'})
# 	df_site['ID_rec']=df_site['ID_site'].astype(int)*10

# 	gdf_rec=load_gdf('rec').dropna(subset='ID_site')
# 	gdf_rec=pd.concat([gdf_rec,df_site],axis=0).drop_duplicates(subset='ID_rec',keep='first').drop_duplicates('ID_rec',keep='first').dropna(subset='ID_site')
# 	gdf_rec['ID_site']=gdf_rec['ID_site'].astype(int)

# 	gdf_new=gdf_rec.loc[~gdf_rec['ID_site'].isin(df_site['ID_site'])]

# 	if len (gdf_new)>0:
# 		gdf_new['rec_x']=gdf_new.get_coordinates().iloc[:,0]
# 		gdf_new['rec_y']=gdf_new.get_coordinates().iloc[:,1]
# 		for site,x,y in zip(gdf_new['ID_site'],gdf_new['rec_x'],gdf_new['rec_y']):
# 			ipt=input(f'\tSite {site} is in REC database but missing in SITE db. To remove this site type "r". To keep press ENTER!')
# 			if ipt=='r':
# 				gdf_rec=gdf_rec.loc[gdf_rec['ID_site']!=site]
# 			else:
# 				df_site_append=pd.DataFrame(data={'ID_site':[site],'rec_x':[x],'rec_y':[y]})
# 				df_site=pd.concat([df_site,df_site_append],axis=0)

# 	df_site=df_site.drop(columns='ID_rec')
# 	df_site['site_x']=df_site['rec_x']
# 	df_site['site_y']=df_site['rec_y']
# 	df_site=df_site.reset_index(drop=True).drop_duplicates('ID_site',keep='last')
# 	df_site=df_site.sort_values('ID_site')
# 	save_gdf(df_site,'site')

# 	print('\tReloading site geometries...')
# 	gdf_rec=pd.merge(gdf_rec.drop(columns=['rec_x','rec_y']),df_site.set_index('ID_site'),left_on='ID_site',right_index=True,how='left')
# 	save_gdf(gdf_rec,'rec')



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

def get_xml_rec_start():
	gdf_rec=load_gdf('rec')
	gdf_xml=load_gdf('xml').dropna(subset='ID_rec')
	gdf_xml['ID_rec']=gdf_xml['ID_rec'].astype(int)

	gdf_filt=gdf_rec.copy().dropna(subset='rec_fl_num_test_jobs').dropna(subset='ID_xml').reset_index(drop=True)
	gdf_xml=gdf_xml[['ID_rec','xml_rec_start']]

	for rec,tj in zip(gdf_filt['ID_rec'],gdf_filt['rec_fl_num_test_jobs'].astype(int)):
		try:
			rec_start=gdf_xml.loc[gdf_xml['ID_rec']==rec].sort_values('xml_rec_start').iloc[tj,1]
			gdf_rec.loc[gdf_rec['ID_rec']==rec,'xml_rec_start']=rec_start
		except:
			print(f'\tError getting rec start - Rec-{rec},test jobs-{tj}')

	save_gdf(gdf_rec,'rec')


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

def rec_unify_coords():
	gdf_rec=load_gdf('rec')
	# load x,y from geometry
	gdf_rec[['rec_x','rec_y']]=gdf_rec.get_coordinates()

	# load x0,y0 coordinates based on first ID_site
	gb=gdf_rec.groupby('ID_site')[['rec_x0','rec_y0']].agg('first')
	gdf_rec=pd.merge(gdf_rec.drop(columns=['rec_x0','rec_y0']),gb,'left',left_on='ID_site',right_index=True)

	# fill empty x0,y0 from x,y (adding new sites)
	gdf_rec.loc[gdf_rec['rec_x0'].isnull(),'rec_x0']=gdf_rec['rec_x']
	gdf_rec.loc[gdf_rec['rec_y0'].isnull(),'rec_y0']=gdf_rec['rec_y']

	# unify final coordinates xml/rec
	gdf_rec['rec_x']=gdf_rec['rec_x0']
	gdf_rec['rec_y']=gdf_rec['rec_y0']
	gdf_rec.loc[~gdf_rec['xml_x'].isnull(),'rec_x']=gdf_rec['xml_x']
	gdf_rec.loc[~gdf_rec['xml_y'].isnull(),'rec_y']=gdf_rec['xml_y']
	save_gdf(gdf_rec,'rec')
	# print(gdf_rec.loc[gdf_rec['ID_rec']==8201,['ID_rec','rec_x','rec_y','rec_x0','rec_y0']])



def rec_edi_coords():
	gdf_rec=load_gdf('rec').drop(columns=['edi_x','edi_y'])
	gdf_edi=load_gdf('edi').set_index('ID_rec')
	gdf_edi
	gdf_rec=pd.merge(gdf_rec,gdf_edi[['edi_x','edi_y']],'left',left_on='ID_rec',right_index=True).reset_index(drop=True)

	save_gdf(gdf_rec,'rec')

def run_proc_rec():
	print('Processing REC database...')
	print('\tMatchning XML data...')
	gb=groupby_xml()
	gdf_rec=merge_xml2rec(gb)
	rec_unify_coords()
	
	print('\tCalculating magnetic declination...')
	rec_mag_dec()
	gdf_rec=get_number_of_jobs()
	get_xml_rec_start()
	rec_edi_coords()
	export_site_db()