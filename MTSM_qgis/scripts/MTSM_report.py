import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *

os.chdir( Path(__file__).parents[2])

def format_report_db():
	gdf_rec=load_gdf('rec').sort_values('ID_rec')
	gdf_jl=load_gdf('jl')


	df_jl=gdf_jl.copy().set_index('ID_jl')

	df_jl['jl_alias']=[j.replace(', ','- ') for j in df_jl['jl_freq_sample']]

	gdf_rec=pd.merge(gdf_rec,df_jl['jl_alias'],left_on='rec_fl_joblist',right_index=True,how='left')

	gdf_rec['xml_x']=np.round(gdf_rec['xml_x'],6)
	gdf_rec['xml_y']=np.round(gdf_rec['xml_y'],6)

	gdf_rec['xml_gps_height']=np.round(gdf_rec['xml_gps_height']/100).astype(str).str.replace('.0','')

	gdf_rec['xml_rec_start']=gdf_rec['xml_rec_start'].dt.strftime('%Y-%m-%d\n%H:%M:%S')
	gdf_rec['xml_rec_end']=gdf_rec['xml_rec_end'].dt.strftime('%Y-%m-%d\n%H:%M:%S')
	gdf_rec['rec_qc_date']=gdf_rec['rec_qc_date'].dt.strftime('%Y-%m-%d')
	gdf_rec['rec_fl_rec_start']=gdf_rec['rec_fl_rec_start'].dt.tz_localize(None).dt.strftime('%Y-%m-%d\n%H:%M:%S')


	gdf_rec.loc[gdf_rec['rec_fl_ex_n'].isnull(),'rec_fl_ex_n']=gdf_rec['xml_ch01_sensor_pos_x1']
	gdf_rec.loc[gdf_rec['rec_fl_ex_s'].isnull(),'rec_fl_ex_s']=gdf_rec['xml_ch01_sensor_pos_x2']
	gdf_rec.loc[gdf_rec['rec_fl_ey_e'].isnull(),'rec_fl_ey_e']=gdf_rec['xml_ch02_sensor_pos_y2']
	gdf_rec.loc[gdf_rec['rec_fl_ey_w'].isnull(),'rec_fl_ey_w']=gdf_rec['xml_ch02_sensor_pos_y1']

	for col in ['rec_fl_ex_n','rec_fl_ex_s','rec_fl_ey_e','rec_fl_ey_w']:
		gdf_rec[col]=np.abs(gdf_rec[col])
		gdf_rec[col]=np.abs(gdf_rec[col])
		gdf_rec[col]=gdf_rec[col].astype(str).replace('nan','').str.replace('.0','')

	gdf_rec['rep_sensor_pos']='N '+gdf_rec['rec_fl_ex_n']+' | S '+gdf_rec['rec_fl_ex_s']+'\nE '+gdf_rec['rec_fl_ey_e']+' | W '+gdf_rec['rec_fl_ey_w']

	for ch in range(1,3):
		ch=str(ch)
		gdf_rec[f'xml_ch0{ch}_st_res']=np.round(gdf_rec[f'xml_ch0{ch}_st_res']/1000).astype(str).str.replace('nan','').str.replace('.0','').str.pad(width=2,side='left',fillchar=' ')
		gdf_rec[f'xml_ch0{ch}_st_dc_offset']=np.round(gdf_rec[f'xml_ch0{ch}_st_dc_offset']).astype(str).str.replace('nan','').str.replace('.0','').str.pad(width=3,side='left',fillchar=' ')

	gdf_rec['rep_e_contact']='Ex '+gdf_rec['xml_ch01_st_res']+'kΩ | '+gdf_rec['xml_ch01_st_dc_offset'] +'mv'+ '\nEy '+gdf_rec['xml_ch02_st_res']+'kΩ | '+gdf_rec['xml_ch02_st_dc_offset']+'mv'

	for ch in range(3,6):
		ch=str(ch)
		gdf_rec[f'xml_ch0{ch}_ser_num']=gdf_rec[f'xml_ch0{ch}_ch_type']+'-'+gdf_rec[f'xml_ch0{ch}_sensor_type']+'-'+ gdf_rec[f'xml_ch0{ch}_ser_num'].astype(str).str.zfill(3)

	gdf_rec['rep_ser_num']=gdf_rec[f'xml_ch03_ser_num']+'\n'+gdf_rec[f'xml_ch04_ser_num']+'\n'+gdf_rec[f'xml_ch05_ser_num']

	for col in ['rep_ser_num','rep_sensor_pos','rep_e_contact']:
		gdf_rec.loc[gdf_rec['xml_rec_start'].isnull(),col]=''


	
	gdf_rec['rec_qc_str']=gdf_rec[['rec_qc_status','rec_qc_qcby','rec_qc_date']].astype(str).agg('\n'.join,axis=1)
	cols=['ID_site','ID_rec','xml_x','xml_y','xml_gps_height','rec_fl_adu','rec_fl_operator','jl_alias','rec_fl_rec_start','xml_rec_start','xml_rec_end','rec_fl_note','rep_sensor_pos','rep_e_contact','rep_ser_num','rec_qc_status','rec_qc_note','rec_qc_qcby','rec_qc_str']

	for col in cols:
		gdf_rec[col]=gdf_rec[col].astype(str)
		for s in ['nan','None','NaT']:
			gdf_rec[col]=gdf_rec[col].str.replace(s,'')



	gdf_rep=gpd.GeoDataFrame(gdf_rec[cols])
	gdf_rep.to_file('MTSM_qgis/mtsm_report.gpkg',layer='mtsm_rep_db',engine='pyogrio')


def run_proc_report():
	format_report_db()