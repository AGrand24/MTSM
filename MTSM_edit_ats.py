from MTSM_tools import *
from MTSM_ts_sort import *

def run_edit_ats():
	run_ts_sort()

	gdf_rec=load_gdf('rec')
	# gdf_rec=gdf_rec.query('rec_fl_operator=="Adam"')

	sensor_pos_xml=['xml_ch01_sensor_pos_x1','xml_ch01_sensor_pos_x2','xml_ch02_sensor_pos_y2','xml_ch02_sensor_pos_y1']
	sensor_pos_fl=[f"rec_fl_{dir}" for dir in ['ex_n','ex_s','ey_e','ey_w']]
	sensor_diff=['d_n','d_s','d_e','dw']


	gdf_xml=load_gdf('xml')[['ID_rec','ID_xml']+sensor_pos_xml]
	gdf_rec=gdf_rec[['ID_rec','xml_rec_start']+sensor_pos_fl].set_index('ID_rec')

	gdf=pd.merge(gdf_xml,gdf_rec,left_on='ID_rec',right_index=True,how='left')
	gdf=gdf.dropna(subset='ID_rec')

	for sp_fl,sp_xml,d in zip(sensor_pos_fl,sensor_pos_xml,sensor_diff):
		gdf[d]=np.abs(np.abs(gdf[sp_fl])-np.abs(gdf[sp_xml]))
	gdf['d_total']=gdf[sensor_diff].sum(axis=1)
	gdf=gdf.query('d_total>0')



	gdf=gdf[['ID_rec','xml_rec_start']+sensor_pos_fl+sensor_pos_xml].groupby('ID_rec',as_index=False).first()
	gdf=gdf.sort_values('ID_rec')
	gdf['ID_rec']=gdf['ID_rec'].astype(str).str.replace('.0','')
	gdf['rec_fl_ex_n']=-np.abs(gdf['rec_fl_ex_n'])
	gdf['rec_fl_ey_w']=-np.abs(gdf['rec_fl_ey_w'])

	gdf.to_html('tmp/sensor_positions.html',index=False)
	os.startfile(os.getcwd()+'\\tmp\\sensor_positions.html')

	gdf['path_orig']='ts/Site_'+gdf['ID_rec']+'/'
	gdf['path_dest']='ts/3_edit_ats/Site_'+gdf['ID_rec']+'/'

	delete_empty_folders('ts/3_edit_ats/')
	for fp_orig,fp_dest in zip(gdf['path_orig'],gdf['path_dest']):
		print('Copying..',fp_orig,fp_dest)
		shutil.move(fp_orig,fp_dest)


