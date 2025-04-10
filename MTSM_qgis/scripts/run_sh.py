import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)


from MTSM_python_modules import*
from MTSM_tools import *
os.chdir( Path(__file__).parents[2])

# def view_sh(gdf_xml,rec_id):
# 	gdf_xml=gdf_xml.query(f'ID_rec=={rec_id}')
# 	df_sh=pd.DataFrame()
# 	for id_xml in gdf_xml['ID_xml']:
# 		path=f"ts/Site_{rec_id}/meas_{id_xml[4:]}/"
# 		adu=id_xml[:3]
# 		for file in os.listdir(path):
# 			if file.endswith('xml'):
# 				single_xml=extract_xml(path+file)

# 		single_xml=single_xml.loc[single_xml.index.str.startswith('measurement.SystemHistory')]
# 		single_xml.index=single_xml.index.str.replace('measurement.SystemHistory.message','')

# 		columns=['time','date','component','text','batt1_curr','batt2_curr','batt_volt','temp_','DynamicMode','latitude','longitude','height','num_sats','sync_state']
# 		df_single=pd.DataFrame()
# 		for col in columns:
# 			tmp=single_xml.loc[single_xml.index.str.contains(col)]
# 			single_xml=single_xml.loc[~single_xml.index.str.contains('sensor')]

# 			tmp.index=tmp.index.str.replace('.'+col,'').str.replace('[','').str.replace(']','').astype(int)
# 			tmp.name=col
# 			df_single=pd.concat([df_single,tmp],axis='columns')

# 		df_single['ID_sh']=adu+'_'+df_single['date']+'_'+df_single['time'].str.replace(':','-')
# 		df_single=df_single.drop(columns=['time','date'])
# 		df_sh=pd.concat([df_sh,df_single])

# 	df_sh=df_sh.drop_duplicates(subset='ID_sh',keep='last')
# 	df_sh=df_sh.set_index('ID_sh')
# 	df_sh.to_html('tmp/view_sh.html',na_rep='')
# 	os.startfile(os.getcwd()+'\\tmp\\view_sh.html')
# 	return df_sh

def view_sh(id_rec):

	gdf_xml=load_gdf('xml').query('ID_rec==@id_rec')

	if os.path.exists(gdf_xml.iloc[0]['xml_path']):

		df_sh=pd.DataFrame()
		for fp,adu in zip(gdf_xml['xml_path'],gdf_xml['ID_xml'].str.slice(0,3)):
				single_xml=extract_xml(fp)
				single_xml=single_xml.loc[single_xml.index.str.startswith('measurement.SystemHistory')]
				single_xml.index=single_xml.index.str.replace('measurement.SystemHistory.message','')
				columns=['time','date','component','text','batt1_curr','batt2_curr','batt_volt','temp_','DynamicMode','latitude','longitude','height','num_sats','sync_state']
				df_single=pd.DataFrame()
				for col in columns:
					tmp=single_xml.loc[single_xml.index.str.contains(col)]
					single_xml=single_xml.loc[~single_xml.index.str.contains('sensor')]

					tmp.index=tmp.index.str.replace('.'+col,'').str.replace('[','').str.replace(']','').astype(int)
					tmp.name=col
					df_single=pd.concat([df_single,tmp],axis='columns')
				df_single['ID_sh']=adu+'_'+df_single['date']+'_'+df_single['time'].str.replace(':','-')
				df_single=df_single.drop(columns=['time','date'])
				df_sh=pd.concat([df_sh,df_single])

		df_sh=df_sh.set_index('ID_sh')
		df_sh.to_html('tmp/view_sh.html',na_rep='')
		os.startfile(os.getcwd()+'\\tmp\\view_sh.html')

	else:
		input(f'Missing XML data. Press ENTER to exit!')






try:
	with open('tmp/id_rec.txt','r') as file:
		id_rec=int(file.read())

	df_sh=view_sh(id_rec)
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')
