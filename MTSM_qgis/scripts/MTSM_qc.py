import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *

os.chdir( Path(__file__).parents[2])

def run_check_sensor_pos(**kwargs):

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

	if kwargs.get('html',True)==True:
		gdf.to_html('tmp/sensor_positions.html',index=False)
		os.startfile(os.getcwd()+'\\tmp\\sensor_positions.html')
	else:
		if len(gdf)>0:
			gdf=gdf.drop_duplicates('ID_rec')
			recs=gdf['ID_rec'].astype(int).to_list()
			print(f'\nQC WARNING - Inconsistent sensor positions - {recs}')

def qc_exception(gdf_rec):
	gdf_rec=gdf_rec.loc[gdf_rec['rec_qc_exception']!=1].reset_index(drop=True)
	return gdf_rec

def qc_rec_start_span(gdf_xml,gdf_rec):
	# gdf_rec=qc_exception(gdf_rec)
	gdf_xml=gdf_xml.dropna(subset='ID_rec').query('ID_rec!=0')
	gdf_xml=gdf_xml.loc[gdf_xml['ID_rec'].isin(gdf_rec['ID_rec'])]
		
	gdf_rec=gdf_rec[['ID_rec','rec_qc_status']].set_index('ID_rec')
	gdf_xml=pd.merge(gdf_xml,gdf_rec,how='left',left_on='ID_rec',right_index=True)

	gdf_xml=gdf_xml.loc[~gdf_xml["rec_qc_status"].isin(["Recording","Accepted","Processed","Remeasured"])]

	gb=gdf_xml.groupby('ID_rec',as_index=False).agg(first=('xml_rec_start','min'),last=('xml_rec_start','max'))
	gb['td']=(gb['last']-gb['first'])

	gb=gb.loc[gb['td']>pd.Timedelta(hours=24)]
	if len(gb)>0:
		print('\nQC WARNING - Following RECs have diference between first and last job starts > 24 hours:\n')
		print(tabulate(gb,showindex=False,tablefmt='presto',headers=['ID_rec','first job','last job','time delta']))


def qc_missing_data(gdf):
	missing=gdf.loc[gdf['xml_path'].isnull()]['ID_rec'].sort_values().astype(str).str.replace('.0','').astype(int).unique()
	if len (missing)>0:
		print(f'QC WARNING! Following RECSs have XML data, but data are missing  in "/ts/" folder\n\t',{missing})
		print()

def qc_missing_edi(gdf_edi,gdf_rec):
		gdf_rec=gdf_rec.dropna(subset='ID_xml').sort_values('ID_rec')
		missing_edi=gdf_rec.loc[~gdf_rec['ID_rec'].isin(gdf_edi['ID_rec'])]['ID_rec'].to_list()
		if len(missing_edi)>0:
			print(f"\nQC WARNING - Missing edi for recs: {missing_edi}")

def qc_gps_sync(gdf_rec):
	# gdf_rec=qc_exception(gdf_rec)
	gdf_rec=gdf_rec.loc[gdf_rec['xml_gps_sync']<3.5]
	rec=gdf_rec['ID_rec'].to_list()
	if len (gdf_rec)>0:
		print(f'\nQC WARNING - Unreliable GPS sync (average<3.5) - {rec}')

def qc_n_jobs(gdf_jl,gdf_rec):
	gdf_rec=load_gdf('rec')
	gdf_jl=load_gdf('jl')
	gdf_rec=gdf_rec.dropna(subset='ID_xml')
	# gdf_rec=qc_exception(gdf_rec)
	gdf_jl=gdf_jl.set_index('ID_jl')

	gdf_rec=pd.merge(gdf_rec,gdf_jl['jl_num_of_jobs'],left_on='rec_fl_joblist',right_index=True,how='left')[['ID_rec','rec_xml_num_of_jobs','jl_num_of_jobs','rec_fl_num_test_jobs']]

	cols_jobs=['rec_xml_num_of_jobs','jl_num_of_jobs','rec_fl_num_test_jobs']
	for col in cols_jobs:
		gdf_rec[col]=gdf_rec[col].replace(np.nan,0).astype(int)

	gdf_rec=gdf_rec.loc[gdf_rec['rec_xml_num_of_jobs']!=gdf_rec['jl_num_of_jobs']]
	print('\nQC WARNING - Inconsistent  number of jobs:\n')
	print(tabulate(gdf_rec[['ID_rec']+cols_jobs],showindex=False,headers=['ID_rec','Data','Joblist','Test'],tablefmt="presto"))

def qc_missing_fl_data():
	gdf_rec=load_gdf('rec')
	gdf_qc=gdf_rec.copy().loc[gdf_rec['rec_qc_status']!='Cancelled']

	qc_cols=['ID_rec','rec_fl_rec_start','rec_fl_operator','rec_fl_joblist','rec_fl_adu']

	gdf_qc=gdf_qc.copy()[qc_cols].set_index('ID_rec')
	gdf_qc=gdf_qc.dropna(axis=0,how='all')

	gdf_out=pd.DataFrame(index=gdf_qc.index)

	for col in qc_cols[1:]:
		tmp=gdf_qc[col].fillna(col)
		gdf_out=pd.concat([gdf_out,tmp.loc[tmp==col]],axis=1)

	gdf_out=gdf_out.dropna(axis=0,how='all')
	if len(gdf_out)>0:
		gdf_out['str']=gdf_out[qc_cols[1:]].agg(lambda x:', '.join(x.dropna()), axis=1)
		
		print('\nQC WARNING - Missing FL data:\n')
		for rec,s in zip(gdf_out.index,gdf_out['str']):
			print(f'\t{rec}\t{s}')

def qc_memory():
	gdf=load_gdf('rec')
	gdf['xml_disk_space_free']/=(10**6)
	gdf['xml_disk_space_free']=np.round(gdf['xml_disk_space_free'],1)
	gdf=gdf.loc[gdf['xml_disk_space_free']<1.5]

	print('\nQC WARNING - Low memory:\n')
	if len(gdf)>0:
		for rec,adu,mem in zip(gdf['ID_rec'],gdf['xml_adu'],gdf['xml_disk_space_free']):
			print(f'\t{rec}\t{adu}\t{mem} GB')

def run_qc(ignore_exceptions):
	print('Running qc check...')
	gdf_rec=load_gdf('rec')
	gdf_xml=load_gdf('xml')
	gdf_edi=load_gdf('edi')
	gdf_jl=load_gdf('jl')

	if ignore_exceptions==False:
		gdf_except=qc_exception(gdf_rec)
		exceptions=gdf_rec.loc[~gdf_rec['ID_rec'].isin(gdf_except['ID_rec'])]['ID_rec'].sort_values().to_list()
		if len(exceptions)>0:
			print(f'\nFollowing RECs are excluded from qc checks by user:\n\t{exceptions}')
	else:
		print('\tIgnoring exceptions during qc!')
		gdf_except=gdf_rec

	qc_rec_start_span(gdf_xml,gdf_except)
	qc_missing_edi(gdf_edi,gdf_rec)
	qc_missing_data(gdf_xml)
	qc_gps_sync(gdf_except)
	qc_n_jobs(gdf_except,gdf_jl)
	run_check_sensor_pos(html=False)
	qc_missing_fl_data()
	qc_memory()
