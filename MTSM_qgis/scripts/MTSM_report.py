import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *

os.chdir( Path(__file__).parents[2])

def db_format_report_db():
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

def tl_get_df_tl():
	gdf_xml=load_gdf('xml')
	gdf_xml=gdf_xml.dropna(subset='ID_rec').query('ID_rec>0')
	gdf_xml=gdf_xml.rename(columns={'xml_adu':'ID_adu'})

	df_tl=gdf_xml[['ID_rec','ID_adu','xml_rec_start','xml_rec_end']]
	df_tl['ID_rec']=df_tl['ID_rec'].astype(int)

	gdf_rec=load_gdf('rec')

	#add RECs without xml data
	gdf_rec_filt=gdf_rec.copy().loc[gdf_rec['rec_qc_status']=='Recording']
	gdf_rec_filt=gdf_rec_filt.loc[~gdf_rec_filt['rec_fl_rec_start'].isnull()]
	gdf_rec_filt=gdf_rec_filt.rename(columns={'rec_fl_adu':'ID_adu'})[['ID_rec','ID_adu']]

	df_tl=pd.concat([df_tl,gdf_rec_filt]).reset_index(drop=True)
	df_tl['ID_adu']=df_tl['ID_adu'].astype(str).str.zfill(3)

	#merge ID_xml and fl data
	df_tl=pd.merge(df_tl,gdf_rec.set_index('ID_rec')[['ID_xml','rec_fl_rec_start','rec_fl_joblist']],how='left',left_on='ID_rec',right_index=True)
	df_tl['rec_fl_rec_start']=df_tl['rec_fl_rec_start'].dt.tz_convert('UTC').dt.tz_localize(None).max().round('1T')

	#merge jl_rec_duration
	gdf_jl=load_gdf('jl')
	df_tl=pd.merge(df_tl,gdf_jl.set_index('ID_jl')['jl_rec_duration'],how='left',left_on='rec_fl_joblist',right_index=True)

	#convert jl duration to sec
	df_tl['jl_rec_duration']=df_tl['jl_rec_duration'].fillna(0)
	df_tl['jl_rec_duration_sec']=[timedelta(seconds=sec) for sec in df_tl['jl_rec_duration']]

	#unify start, end times
	df_tl['tl_start']=df_tl['xml_rec_start']
	df_tl['tl_end']=df_tl['xml_rec_end']
	df_tl.loc[df_tl['ID_xml'].isnull(),'tl_start']=df_tl['rec_fl_rec_start']
	df_tl.loc[df_tl['ID_xml'].isnull(),'tl_end']=df_tl['rec_fl_rec_start']+df_tl['jl_rec_duration_sec']
	df_tl=df_tl[['ID_adu','ID_xml','ID_rec','tl_start','tl_end']]
	return df_tl


# def tl_get_df_tl():
# 	gdf_xml=load_gdf('xml')
# 	gdf_xml=gdf_xml.dropna(subset='ID_rec').query('ID_rec>0')
# 	gdf_adu=load_gdf('adu').sort_values('ID_adu')

# 	df_eq=gdf_adu.copy()
# 	df_eq['ID_eq']='ADU0'+df_eq['adu_version']+'-'+df_eq['ID_adu']

# 	df_xml_adu=pd.merge(gdf_xml,df_eq.set_index('ID_adu')['ID_eq'],left_on='xml_adu',right_index=True,how='left')[['ID_eq','ID_rec','xml_rec_start','xml_rec_end']]

# 	df_xml_coils=pd.DataFrame()

# 	for ch in list(range(3,6)):
# 		ch=str(ch)
# 		tmp=gdf_xml.loc[gdf_xml[f'xml_ch0{ch}_ser_num'].astype(int)>0]
# 		tmp[f'xml_ch0{ch}_ser_num']=tmp[f'xml_ch0{ch}_ser_num'].astype(str).str.zfill(3)
# 		tmp[f'ID_eq']=tmp[[f'xml_ch0{ch}_sensor_type',f'xml_ch0{ch}_ser_num']].agg('-'.join,axis=1)
# 		df_xml_coils=pd.concat([df_xml_coils,tmp[['ID_eq','ID_rec','xml_rec_start','xml_rec_end']]])

# 	df_tl=pd.concat([df_xml_adu,df_xml_coils])
# 	df_tl['ID_rec']=df_tl['ID_rec'].astype(int)

# 	# merge jl duration and adu version
# 	gdf_rec=load_gdf('rec')
# 	df_tl=pd.merge(df_tl,gdf_rec.set_index('ID_rec')[['rec_fl_rec_start','ID_xml']],how='left',left_on='ID_rec',right_index=True)
# 	gdf_rec_filt=gdf_rec.copy().loc[gdf_rec['ID_xml'].isnull()]
# 	gdf_rec_filt=gdf_rec_filt.loc[~gdf_rec_filt['rec_fl_rec_start'].isnull()]

# 	gdf_rec_filt['rec_fl_rec_start']=gdf_rec_filt['rec_fl_rec_start'].dt.tz_convert('UTC').dt.tz_localize(None).max().round('1T')
# 	df_tl=pd.concat([df_tl,gdf_rec_filt[['ID_rec','rec_fl_rec_start']]]).rename(columns={'xml_rec_start':'tl_start','xml_rec_end':'tl_end'}).reset_index(drop=True)

# 	# merge recording and xml data
# 	gdf_rec_filt=pd.merge(gdf_rec_filt,df_eq.set_index('ID_adu')['ID_eq'],left_on='rec_fl_adu',right_index=True,how='left')
# 	gdf_rec_filt=gdf_rec_filt[['ID_rec','ID_eq','rec_fl_rec_start','rec_fl_adu']]

# 	#merge jl_rec_duration
# 	gdf_jl=load_gdf('jl')
# 	df_tl=pd.merge(df_tl,gdf_jl.set_index('ID_jl')['jl_rec_duration'],how='left',left_on='rec_fl_joblist',right_index=True)
	
# 	print(df_tl)
# 	df_tl['jl_rec_duration']=df_tl['jl_rec_duration'].fillna(0)
# 	df_tl['jl_rec_duration']=[timedelta(seconds=sec) for sec in df_tl['jl_rec_duration']]

# 	# unify start and end
# 	df_tl.loc[df_tl['ID_xml'].isnull(),'tl_start']=df_tl['rec_fl_rec_start']
# 	df_tl.loc[df_tl['ID_xml'].isnull(),'tl_end']=df_tl['rec_fl_rec_start']+df_tl['jl_rec_duration']


# 	df_tl=df_tl[['ID_eq','ID_xml','ID_rec','tl_start','tl_end','jl_rec_duration']]
# 	df_tl=pd.merge(df_tl,gdf_rec.set_index('ID_rec')['rec_fl_adu'],how='left',left_on='ID_rec',right_index=True)
# 	return df_tl

def tl_get_date_max(df_tl):
	gdf_rec=load_gdf('rec')

	dt_fl=(gdf_rec['rec_fl_rec_start'].dt.tz_convert('UTC').dt.tz_localize(None)).max().round('1T')
	dt_xml=(df_tl)['tl_start'].max().round('1T')

	date_max=max([dt_fl,dt_xml]).normalize()+timedelta(days=1)
	return date_max

def tl_get_dt_range(tl_range,df_tl):
	date_max=tl_get_date_max(df_tl)+timedelta(hours=1)
	if tl_range=='full':
		date_min=df_tl['tl_start'].min().normalize()
	else:
		date_min=date_max-timedelta(days=tl_range)

	dt_range=np.arange(date_min-timedelta(hours=1),date_max,timedelta(hours=1))
	return dt_range

def tl_cut_df_tl(df_tl,dt_range):
	df_tl=df_tl.loc[df_tl['tl_end']>=dt_range.min()]
	df_tl.loc[df_tl['tl_start']<=dt_range.min(),'tl_start']=dt_range.min()
	return df_tl


# def tl_get_df_eq(df_tl):
# 	df_eq=df_tl.groupby('ID_eq',as_index=False).agg('first').sort_values('ID_eq').reset_index(drop=True)
# 	df_eq.loc[df_eq['ID_eq'].str.startswith('ADU'),'ID_adu']=df_eq['ID_eq'].str.slice(-3,None)
# 	return df_eq[['ID_eq','ID_adu','y']]

def tl_get_ymax():
	df_adu=load_gdf('adu').sort_values('ID_adu')
	ymax=7*(len(df_adu)+1)/7
	x0,y0=tl_get_x0_y0()
	ymax+=y0
	return ymax

def tl_dt2x(dt,ymax,dt_range):
	x_range_sec=np.ptp(dt_range).astype(int)/10**6
	dt_sec=(dt-dt_range.min()).dt.total_seconds()
	x0,y0=tl_get_x0_y0()
	x=dt_sec*((ymax-y0)/x_range_sec)/.7
	x+=x0
	return x

def tl_adu2_y(df):
	df_adu=load_gdf('adu').sort_values('ID_adu')
	ymax=tl_get_ymax()
	df_adu['y']=ymax-(df_adu.index+1)
	df=pd.merge(df,df_adu.set_index('ID_adu')['y'],how='left',left_on='ID_adu',right_index=True)
	return df

def tl_get_x0_y0():
	coords=load_gdf('rec').to_crs(get_project_crs()).get_coordinates()
	x0,y0=coords.median()
	return x0,y0

def tl_export_gdf_tl(df_tl,ymax,dt_range):
	df_tl['x1']=tl_dt2x(df_tl['tl_start'],ymax,dt_range)
	df_tl['x2']=tl_dt2x(df_tl['tl_end'],ymax,dt_range)
	df_tl=tl_adu2_y(df_tl)

	geom=[]
	for x1,x2,y in zip(df_tl['x1'],df_tl['x2'],df_tl['y']):
		geom.append(LineString(((x1,y),(x2,y))))

	gdf_tl=gpd.GeoDataFrame(df_tl,geometry=geom,crs=get_project_crs())
	gdf_tl.to_file('MTSM_qgis/mtsm_report.gpkg',layer='mtsm_rep_tl_data',engine='pyogrio')
	return gdf_tl

def tl_export_xlabels(dt_range,ymax):
	dt=pd.Series(np.arange(dt_range.min(),dt_range.max()+timedelta(hours=1),timedelta(hours=1)))
	x0,y0=tl_get_x0_y0()
	x=tl_dt2x(dt,ymax,dt_range)
	
	geom=[]
	for xx in x:
		geom.append(LineString(((xx,y0),(xx,ymax))))

	gdf=gpd.GeoDataFrame(pd.DataFrame(data={'dt':dt}),geometry=geom,crs=get_project_crs())
	gdf.to_file('MTSM_qgis/mtsm_report.gpkg',layer='mtsm_rep_tl_x',engine='pyogrio')

def tl_export_ylabels(ymax,dt_range):
	df_adu=load_gdf('adu')
	df_adu['y']=ymax-(df_adu.index+1)

	x=tl_dt2x(pd.Series([dt_range.min(),dt_range.max()]),ymax,dt_range)
	x1=x[0]
	x2=x[1]

	geom=[]
	for y in df_adu['y']:
		geom.append(LineString(((x1,y),(x2,y))))

	gdf_adu=gpd.GeoDataFrame(df_adu,geometry=geom,crs=get_project_crs())

	gdf_adu
	gdf_adu.to_file('MTSM_qgis/mtsm_report.gpkg',layer='mtsm_rep_tl_y',engine='pyogrio')



def tl_export_atlas(dt_range,ymax):
	dt=pd.Series([dt_range.min(),dt_range.max()])
	x1,y1=tl_get_x0_y0()
	x2=tl_dt2x(dt,ymax,dt_range)[1]
	y2=ymax
	geom=Polygon(((x1,y1),(x2,y1),(x2,y2),(x1,y2)))

	gdf_atas=gpd.GeoDataFrame(data={'page':['1']},geometry=[geom],crs=get_project_crs())
	gdf_atas['x1']=x1
	gdf_atas['x2']=x2
	gdf_atas['w']=x2-x1
	gdf_atas['y1']=y1
	gdf_atas['y2']=y2
	gdf_atas['h']=y2-y1

	gdf_atas.to_file('MTSM_qgis/mtsm_report.gpkg',layer='mtsm_rep_tl_atlas',engine='pyogrio')

def tl_export_daylight(dt_range,ymax):
	gdf_site=load_gdf('site')
	lon=gdf_site['site_x'].median()
	lat=gdf_site['site_y'].median()

	dates=pd.Series(dt_range).dt.normalize().unique()
	sun=[]
	for date in dates:
		sun.append(get_sunrise_sunset(lat,lon,date))
	x0,y0=tl_get_x0_y0()

	geom=[]
	for s in sun:
		dt=pd.Series([s[0],s[1]])
		x=tl_dt2x(dt,ymax,dt_range)
		x1=x[0]
		x2=x[1]
		y1=y0
		y2=ymax
		geom.append(Polygon(((x1,y1),(x2,y1),(x2,y2),(x1,y2))))

	gdf=gpd.GeoDataFrame(geometry=geom,crs=get_project_crs())
	gdf.to_file('MTSM_qgis/mtsm_report.gpkg',layer='mtsm_rep_tl_sun',engine='pyogrio')




def run_proc_report():
	db_format_report_db()
