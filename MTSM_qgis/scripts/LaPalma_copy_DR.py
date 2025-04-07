from MTSM_tools import *

recs=[7051]

gdf=load_gdf('rec')
gdf=gdf.loc[gdf['ID_rec'].isin(recs)]
print(tabulate(gdf[['ID_rec','xml_rec_start','xml_rec_end','xml_freq_sample']],showindex=False,headers='keys'))
gdf=gdf.dropna(subset='ID_xml')
if len(gdf)>0:
	for coord in ['x','y']:
		gdf[['deg','min']]=gdf[f'xml_{coord}'].astype(str).str.split('.',expand=True)
		gdf['min']=('.'+gdf['min']).astype(float)*60
		gdf[['min','sec']]=gdf['min'].astype(str).str.split('.',expand=True)

		gdf['sec']=np.round(('.'+gdf['sec']).astype(float)*60,3)
		gdf[['sec','sec_dec']]=gdf['sec'].astype(str).str.split('.',expand=True)
		gdf['sec']=gdf['sec'].str.zfill(2)+'.'+gdf['sec_dec'].str.ljust(3,'0')
	# gdf[['sec','sec_dec']]
		gdf[f'xml_out_{coord}']=gdf['deg'].astype(str).str.zfill(2)+'° '+gdf['min'].astype(str).str.zfill(2)+"' "+gdf['sec'].astype(str)+"''"
	gdf['xml_out_x']='W '+ gdf['xml_out_x'].str.replace('-','')
	gdf['xml_out_y']='N '+ gdf['xml_out_y']
	gdf['xml_gps_height']=np.round(gdf['xml_gps_height']/100).astype(float)
	# gdf[['ID_site','xml_out_x','xml_out_y']]
	gdf['xml_rec_start_out']=pd.to_datetime(gdf['xml_rec_start'].dt.date)+pd.Timedelta(hours=13)
	gdf['jl_out']='4096-512-128'
	gdf['ph']=gdf['ID_rec']/10
	gdf[['rec_fl_ex_n','rec_fl_ex_s','rec_fl_ey_e','rec_fl_ey_w']]=gdf[['rec_fl_ex_n','rec_fl_ex_s','rec_fl_ey_e','rec_fl_ey_w']].replace(np.nan,50)
	gdf[['ID_site','ph','xml_out_y','xml_out_x','xml_gps_height','xml_rec_start','xml_rec_start_out','xml_rec_end','rec_fl_operator','rec_fl_adu','jl_out','rec_fl_note','rec_fl_ex_n','rec_fl_ex_s','rec_fl_ey_e','rec_fl_ey_w','xml_ch03_ser_num','xml_ch04_ser_num','xml_ch05_ser_num','xml_ch01_st_res','xml_ch02_st_res']].sort_values('xml_rec_start').to_clipboard(index=False,header=False)
	print('Copied:')
	print([rec for rec in gdf['ID_rec']])
	input('Press enter to close.')
else:
	input('No matching ID rec found... Press enter to close.')
	