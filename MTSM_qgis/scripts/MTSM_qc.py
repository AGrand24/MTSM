import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *

os.chdir( Path(__file__).parents[2])

def run_check_sensor_pos(recs_list,qc_msg,**kwargs):
	gdf_rec=load_gdf('rec').set_index('ID_rec')
	gdf_xml=load_gdf('xml').set_index('ID_xml')
	gdf_xml=gdf_xml.dropna(subset='ID_rec').query('ID_rec!=0')
	gdf_xml['ID_rec']=gdf_xml['ID_rec'].astype(int)

	directions=['n','s','e','w']
	sensor_pos_xml=['xml_ch01_sensor_pos_x1','xml_ch01_sensor_pos_x2','xml_ch02_sensor_pos_y2','xml_ch02_sensor_pos_y1']
	sensor_pos_fl=[f"rec_fl_{dir}" for dir in ['ex_n','ex_s','ey_e','ey_w']]
	sensor_diff=['d_n','d_s','d_e','dw']
	sensor_str=[f"{d} (FL - XML)" for d in directions]

	gdf_xml=pd.merge(gdf_xml,gdf_rec[sensor_pos_fl],'left',left_on='ID_rec',right_index=True)

	gdf_xml=gdf_xml[['ID_rec']+sensor_pos_fl+sensor_pos_xml]

	for col in [sensor_pos_fl + sensor_pos_xml]:
		gdf_xml[col]=np.abs(gdf_xml[col])

	for diff,fl,xml in zip(sensor_diff,sensor_pos_fl,sensor_pos_xml):
		gdf_xml.loc[gdf_xml[fl].isnull(),fl]=gdf_xml[xml]
		gdf_xml[diff]=gdf_xml[fl]-gdf_xml[xml]

	for diff,fl,xml,ss,d in zip(sensor_diff,sensor_pos_fl,sensor_pos_xml,sensor_str,directions):
		gdf_xml.loc[gdf_xml[diff]!=0,ss]=f"{d} " +gdf_xml[fl].astype(int).astype(str)+' - '+gdf_xml[xml].astype(int).astype(str)

	id_rec=gdf_xml.copy()['ID_rec']

	gdf_xml=gdf_xml[sensor_str]
	gdf_xml=gdf_xml.dropna(axis=1,how='all')
	gdf_xml=gdf_xml.dropna(axis=0,how='all')

	gdf_xml=pd.merge(gdf_xml,id_rec,'left',left_index=True,right_index=True)
	gdf_xml=gdf_xml.reset_index().sort_index(axis=1)

	gb=gdf_xml.drop(columns='ID_xml').groupby('ID_rec').agg('first')
	gb['str']='inconsistent sensor positions (FL/XML)\t ' +gb.agg(lambda x:' | '.join(x.dropna()), axis=1)

	recs_list.extend(gb.index.to_list())
	qc_msg.extend(gb['str'].to_list())

	gdf_xml=gdf_xml.replace(np.nan,'')

	if kwargs.get('html',True)==True:
		gdf_xml.to_html('tmp/sensor_positions.html',index=False)
		os.startfile(os.getcwd()+'\\tmp\\sensor_positions.html')

	return recs_list,qc_msg


def qc_exception(gdf_rec):
	gdf_rec=gdf_rec.loc[gdf_rec['rec_qc_exception']!=1].reset_index(drop=True)
	return gdf_rec

def qc_rec_start_span(gdf_xml,gdf_rec,recs_list,qc_msg):
	gdf_xml=gdf_xml.dropna(subset='ID_rec').query('ID_rec!=0')
	gdf_xml=gdf_xml.loc[gdf_xml['ID_rec'].isin(gdf_rec['ID_rec'])]
		
	gdf_rec=gdf_rec[['ID_rec','rec_qc_status']].set_index('ID_rec')
	gdf_xml=pd.merge(gdf_xml,gdf_rec,how='left',left_on='ID_rec',right_index=True)

	gdf_xml=gdf_xml.loc[~gdf_xml["rec_qc_status"].isin(["Recording","Accepted","Processed","Remeasured"])]

	gb=gdf_xml.groupby('ID_rec',as_index=False).agg(first=('xml_rec_start','min'),last=('xml_rec_start','max'))
	gb['td']=(gb['last']-gb['first'])

	gb=gb.loc[gb['td']>pd.Timedelta(hours=24)]
	if len(gb)>0:
		cols=['first','last','td']
		gb[cols]=gb[cols].astype(str)
		gb['str']=gb[cols].agg(lambda x:' - '.join(x.dropna()), axis=1)

		for rec, s in zip(gb['ID_rec'],gb['str']):
			recs_list.append(rec)
			qc_msg.append(f'rec start span >24 hours - {s}')
	return recs_list,qc_msg




def qc_missing_data(gdf,recs_list,qc_msg):
	missing=gdf.loc[gdf['xml_path'].isnull()]['ID_rec'].sort_values().astype(str).str.replace('.0','').astype(int).unique()
	if len (missing)>0:
		for rec in missing:
			recs_list.append(rec)
			qc_msg.append('missing data in "/ts" folder (xml data only in XML database)')
	return recs_list,qc_msg

def qc_missing_edi(gdf_edi,gdf_rec,recs_list,qc_msg):
	gdf_rec=gdf_rec.dropna(subset='ID_xml').sort_values('ID_rec')
	missing_edi=list(gdf_rec.loc[~gdf_rec['ID_rec'].isin(gdf_edi['ID_rec'])]['ID_rec'].unique())
	if len(missing_edi)>0:
		for rec in missing_edi:
			recs_list.append(rec)
			qc_msg.append('missing edi')
	return recs_list,qc_msg

def qc_gps_sync(gdf_rec,recs_list,qc_msg):
	gdf_rec=gdf_rec.loc[gdf_rec['xml_gps_sync']<3.5]
	if len (gdf_rec)>0:
		for rec,gs in zip(gdf_rec['ID_rec'],gdf_rec['xml_gps_sync']):
			recs_list.append(rec)
			qc_msg.append(f'unreliable gps sync - avg = {np.round(gs,1)}')
	return recs_list,qc_msg

def qc_n_jobs(gdf_rec,recs_list,qc_msg):
	gdf_jl=load_gdf('jl')
	gdf_rec=gdf_rec.dropna(subset='ID_xml')
	gdf_jl=gdf_jl.set_index('ID_jl')

	gdf_rec=pd.merge(gdf_rec,gdf_jl['jl_num_of_jobs'],left_on='rec_fl_joblist',right_index=True,how='left')[['ID_rec','rec_xml_num_of_jobs','jl_num_of_jobs','rec_fl_num_test_jobs']]

	cols_jobs=['rec_xml_num_of_jobs','jl_num_of_jobs','rec_fl_num_test_jobs']
	for col in cols_jobs:
		gdf_rec[col]=gdf_rec[col].replace(np.nan,0).astype(int)

	gdf_rec=gdf_rec.loc[gdf_rec['rec_xml_num_of_jobs']!=gdf_rec['jl_num_of_jobs']]

	if len(gdf_rec)>0:
		for rec,data,jl,test in zip(gdf_rec['ID_rec'],gdf_rec['rec_xml_num_of_jobs'],gdf_rec['jl_num_of_jobs'],gdf_rec['rec_fl_num_test_jobs']):
			recs_list.append(rec)
			qc_msg.append(f'inconsitent number of jobs - {data} (data), {jl} (joblist) {test} (test)')
	return recs_list,qc_msg

def qc_missing_fl_data(recs_list,qc_msg):
	gdf_rec=load_gdf('rec')
	gdf_qc=gdf_rec.copy().loc[gdf_rec['rec_qc_status']!='Cancelled']

	qc_cols=['ID_rec','rec_fl_rec_start','rec_fl_operator','rec_fl_joblist','rec_fl_adu']
	qc_cols_alias=['ID_rec','assembly time','operator','joblist','adu']
	
	for col1,col2 in zip(qc_cols,qc_cols_alias):
		gdf_qc[col2]=gdf_qc[col1]

	gdf_qc=gdf_qc.copy()[qc_cols_alias].set_index('ID_rec')
	gdf_qc=gdf_qc.dropna(axis=0,how='all')

	gdf_out=pd.DataFrame(index=gdf_qc.index)

	for col in qc_cols_alias[1:]:
		tmp=gdf_qc[col].fillna(col)
		gdf_out=pd.concat([gdf_out,tmp.loc[tmp==col]],axis=1)

	gdf_out=gdf_out.dropna(axis=0,how='all')
	if len(gdf_out)>0:
		gdf_out['str']=gdf_out[qc_cols_alias[1:]].agg(lambda x:', '.join(x.dropna()), axis=1)

		for rec,s in zip(gdf_out.index,gdf_out['str']):
			recs_list.append(rec)
			qc_msg.append(f'missing FL data: {s}')
	return recs_list,qc_msg


def qc_memory(recs_list,qc_msg):
	gdf=load_gdf('rec')
	gdf['xml_disk_space_free']/=(10**6)
	gdf['xml_disk_space_free']=np.round(gdf['xml_disk_space_free'],1)
	gdf=gdf.loc[gdf['xml_disk_space_free']<2]

	if len(gdf)>0:
		for rec,adu,mem in zip(gdf['ID_rec'],gdf['xml_adu'],gdf['xml_disk_space_free']):
			recs_list.append(rec)
			qc_msg.append(f'low memory - ADU {adu}\t{mem} GB')
	return recs_list,qc_msg

def qc_dc_offset(recs_list,qc_msg):
	gdf_rec=load_gdf('rec').set_index('ID_rec')

	cols_dc=[f'xml_ch0{ch}_st_dc_offset' for ch in range(1,3)]

	df_out=pd.DataFrame()
	for col,ch in zip(cols_dc,['Ex  ','Ey  ']):
		gdf_rec[col]=gdf_rec[col].round()
		tmp=ch+gdf_rec.loc[gdf_rec[col].abs()>120,col].astype(int).astype(str)
		df_out=pd.concat([df_out,tmp],axis=1)
	if len(df_out)>0:
		df_out['str']='high dc offset - '+df_out.agg(lambda x:' | '.join(x.dropna()),axis=1)

		recs_list.extend(df_out.index.to_list())
		qc_msg.extend(df_out['str'].to_list())
	return recs_list,qc_msg

def qc_res(recs_list,qc_msg):
	gdf_rec=load_gdf('rec').set_index('ID_rec')

	cols_dc=[f'xml_ch0{ch}_st_res' for ch in range(1,3)]

	df_out=pd.DataFrame()
	for col,ch in zip(cols_dc,['Ex  ','Ey  ']):
		tmp=ch+(gdf_rec.loc[gdf_rec[col]>20000,col]//1000).astype(int).astype(str)+'k'
		df_out=pd.concat([df_out,tmp],axis=1)

	if len(df_out)>0:
		df_out['str']='high ground res. - '+df_out.agg(lambda x:' | '.join(x.dropna()),axis=1)

		recs_list.extend(df_out.index.to_list())
		qc_msg.extend(df_out['str'].to_list())
	return recs_list,qc_msg

def qc_ser_num(recs_list,qc_msg):
	gdf_rec=load_gdf('rec').set_index('ID_rec').dropna(subset='ID_xml')
	cols=[f'xml_ch0{ch}_ser_num' for ch in range(3,6)]

	df_out=pd.DataFrame()
	for col,ch in zip(cols,['Ch2  ','Ch3  ','Ch4  ']):
		gdf_rec[col]=gdf_rec[col].str.replace('0',ch)
		tmp=gdf_rec.loc[gdf_rec[col]==ch,col]
		df_out=pd.concat([df_out,tmp],axis=1)

	if len(df_out)>0:
		df_out['str']='missing ser. num. - '+df_out.agg(lambda x:' | '.join(x.dropna()),axis=1)
		recs_list.extend(df_out.index.to_list())
		qc_msg.extend(df_out['str'].to_list())
	return recs_list,qc_msg


def run_qc(ignore_exceptions):
	print('Running qc check...')
	gdf_rec=load_gdf('rec')
	gdf_xml=load_gdf('xml')
	gdf_edi=load_gdf('edi')
	gdf_jl=load_gdf('jl')
	recs_list=[]
	qc_msg=[]

	if ignore_exceptions==False:
		gdf_except=qc_exception(gdf_rec)
		exceptions=gdf_rec.loc[~gdf_rec['ID_rec'].isin(gdf_except['ID_rec'])]['ID_rec'].sort_values().to_list()
		if len(exceptions)>0:
			print(f'\nFollowing RECs are excluded from qc checks by user:\n\t{exceptions}')
	else:
		print('\tIgnoring exceptions during qc!')
		gdf_except=gdf_rec

	recs_list,qc_msg=qc_rec_start_span(gdf_xml,gdf_except,recs_list,qc_msg)
	recs_list,qc_msg=qc_missing_edi(gdf_edi,gdf_xml,recs_list,qc_msg)
	recs_list,qc_msg=qc_missing_data(gdf_xml,recs_list,qc_msg)

	recs_list,qc_msg=qc_gps_sync(gdf_except,recs_list,qc_msg)
	recs_list,qc_msg=qc_n_jobs(gdf_except,recs_list,qc_msg)
	recs_list,qc_msg=run_check_sensor_pos(html=False,recs_list=recs_list,qc_msg=qc_msg)
	recs_list,qc_msg=qc_missing_fl_data(recs_list,qc_msg)
	recs_list,qc_msg=qc_memory(recs_list,qc_msg)
	recs_list,qc_msg=qc_dc_offset(recs_list,qc_msg)
	recs_list,qc_msg=qc_res(recs_list,qc_msg)
	recs_list,qc_msg=qc_ser_num(recs_list,qc_msg)

	df_qc_msg=pd.DataFrame(data={'ID_rec':recs_list,'qc_msg':qc_msg})
	if len(df_qc_msg)>0:
		print('\n\tFound following issues:\n')
		df_qc_msg=df_qc_msg.sort_values('ID_rec')
		df_qc_msg['ID_rec']=df_qc_msg['ID_rec'].astype(int)
		df_qc_msg=df_qc_msg.loc[df_qc_msg['ID_rec']>0]

		for rec in df_qc_msg['ID_rec']:
			gb=df_qc_msg.copy().loc[df_qc_msg['ID_rec']==rec]
			msg=gb['qc_msg'].agg(lambda x:'\n\t\t'.join(x.dropna()), axis=0)
			print(f"\t{rec}:\t{msg}")

		rec_string=f'ID_rec IN ({df_qc_msg['ID_rec'].drop_duplicates().astype(str).agg(lambda x:','.join(x.dropna()), axis=0)})'
		print(f'\n\tFilter string copied to clipboard!')
		pyperclip.copy(rec_string)

	else:
		print('\tQC check finished! Congrats. no issues found!')

