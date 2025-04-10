import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *

os.chdir( Path(__file__).parents[2])

def run_check_sensor_pos():

	gdf_rec=load_gdf('rec')

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

	gdf=gdf.sort_values('ID_rec')
	gdf['ID_rec']=gdf['ID_rec'].astype(str).str.replace('.0','')
	gdf['rec_fl_ex_n']=-np.abs(gdf['rec_fl_ex_n'])
	gdf['rec_fl_ey_w']=-np.abs(gdf['rec_fl_ey_w'])


	cols_out=['n (FL/xml)','s(FL/xml)','e (FL/xml)','w (FL/xml)']
	for i,d in enumerate(cols_out):
		gdf[d]=gdf[sensor_pos_fl[i]].astype(str)+'/'+gdf[sensor_pos_xml[i]].astype(str)

	gdf=gdf[['ID_rec','ID_xml']+cols_out]


	gdf.to_html('tmp/sensor_positions.html',index=False)
	os.startfile(os.getcwd()+'\\tmp\\sensor_positions.html')


def qc_rec_start_span():
	gdf_xml=load_gdf('xml').dropna(subset='ID_rec').query('ID_rec!=0')
	gdf_rec=load_gdf('rec')[['ID_rec','rec_qc_status']].set_index('ID_rec')
	gdf_xml=pd.merge(gdf_xml,gdf_rec,how='left',left_on='ID_rec',right_index=True)

	gdf_xml=gdf_xml.loc[~gdf_xml["rec_qc_status"].isin(["Recording","Accepted","Processed","Remeasured"])]

	gb=gdf_xml.groupby('ID_rec',as_index=False).agg(first=('xml_rec_start','min'),last=('xml_rec_start','max'))
	gb['td']=(gb['last']-gb['first'])

	gb=gb.loc[gb['td']>pd.Timedelta(hours=24)]
	# gdf_rec=load_gdf('rec')
	# gb=pd.merge(gb,gdf_rec[['ID_rec','rec_fl_note','rec_qc_note']].set_index('ID_rec'),how='left',left_on='ID_rec',right_index=True)
	if len(gb)>0:
		print('\nQC WARNING - Following RECs have diference between first and last job starts > 24 hours:\n')
		# print(tabulate(gb,showindex=False,tablefmt='presto',headers=['ID_rec','first job','last job','time delta','note (FL)','note (QC)']))
		print(tabulate(gb,showindex=False,tablefmt='presto',headers=['ID_rec','first job','last job','time delta']))


def qc_missing_data():
	gdf=load_gdf('xml')
	print('QC WARNING! Following RECSs have XML data, but data are missing  in "/ts/" folder\n\t',gdf.loc[gdf['xml_path'].isnull()]['ID_rec'].sort_values().astype(str).str.replace('.0','').astype(int).unique())
	print()

def qc_missing_edi():
		gdf_edi=load_gdf('edi')
		gdf_rec=load_gdf('rec').dropna(subset='ID_xml').sort_values('ID_rec')
		missing_edi=gdf_rec.loc[~gdf_rec['ID_rec'].isin(gdf_edi['ID_rec'])]['ID_rec'].to_list()
		if len(missing_edi)>0:
			print(f"\nQC WARNING - Missing edi for recs: {missing_edi}")

