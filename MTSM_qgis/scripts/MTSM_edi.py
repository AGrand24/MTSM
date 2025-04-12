import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *

os.chdir( Path(__file__).parents[2])

def get_edi_priority(ld):
	priority=[]
	for fn in ld['file_name']:
		if '_Site' in fn:
			priority.append(0)
		elif '_stack_all' in fn:
			priority.append(2)
		elif '_ct' in fn:
			priority.append(1)
		elif '_median' in fn:
			priority.append(3)
		else:
			priority.append(4)

	ld['priority']=priority
	return ld

def run_sort_edi():
	print('Looking for new edi files...')
	ld1=get_ld('edi',endswith='edi')


	if len (ld1)>0:
		id_rec=load_gdf('rec')['ID_rec']

		ld1['ID_rec']=ld1['file_name'].copy().str.replace('Site_','').str.replace('.edi','_proc_mini.edi')
		ld1['ID_rec']=ld1['ID_rec'].str.findall(r"^[^_]*")
		ld1['ID_rec']=[int(rec[0]) for rec in ld1['ID_rec']]
		ld1['ID_rec']=ld1['ID_rec'].astype(int)
		ld1=ld1.loc[ld1['ID_rec'].isin(id_rec)]
		

		ld2=get_ld('edi_sorted/',endswith='edi')
		if len(ld2)>0:
			ld2['ID_rec']=ld2['file_name'].str.replace('.edi','').astype(int)
			ld2=ld2.loc[ld2['ID_rec'].isin(id_rec)]

		ld=pd.concat([ld1,ld2])

		if len(ld)>0:
			ld['mod_time']=[time.ctime(os.path.getmtime(file)) for file in ld['file_path']]
			ld['mod_time']=pd.to_datetime(ld['mod_time'])
			ld=get_edi_priority(ld)
			ld=ld.sort_values(['ID_rec','mod_time','priority']).drop_duplicates('ID_rec',keep='last').reset_index(drop=True)

			ld['fp_dest']='edi_sorted/'+ld['ID_rec'].astype(str)+'.edi'

			ld=ld.loc[ld['file_path'].str.replace('\\','/')!=ld['fp_dest']]

			for origin,dest in zip(ld['file_path'],ld['fp_dest']):
				shutil.copy(origin,dest)
				print(f'\tCopied {origin}\tto\t{dest}')
	else:
		print(f"\t'/edi/' folder is empty! Proc data with ProcMT!")
	
def run_read_edi():
	ld=get_ld('edi_sorted/',endswith='.edi')

	if len(ld)>0:
		id_rec=load_gdf('rec')['ID_rec'].astype(str)
		ld=ld.drop(columns='ID_xml')
		ld['ID_edi']=ld['file_name'].str.replace('.edi','')
		ld=ld.loc[ld['ID_edi'].isin(id_rec)]
		ld['ID_rec']=ld['ID_edi'].copy().astype(int)

		df_edi=ld.copy()

		edi_x=[];edi_y=[];edi_z=[]
		for fp in df_edi['file_path']:
			with open(fp) as file:
				lines=file.readlines()
			lines=pd.Series([l.strip() for l in lines])

			edi_x.append(lines.loc[lines.str.startswith('LONG=')].str.replace('LONG=','').iloc[0])
			edi_y.append(lines.loc[lines.str.startswith('LAT=')].str.replace('LAT=','').iloc[0])
			edi_z.append(lines.loc[lines.str.startswith('ELEV=')].str.replace('ELEV=','').iloc[0])

		df_edi['edi_x']=edi_x
		df_edi['edi_y']=edi_y
		df_edi['edi_z']=edi_z
		for axis in ['x','y']:
			coord_in=df_edi[f'edi_{axis}'].str.split(':')
			coord_out=[]
			for c in coord_in:
				coord_out.append(float(c[0])+(float(c[0])/abs(float(c[0])))*(float(c[1])/60+float(c[2])/3600))
			df_edi[f'edi_{axis}']=coord_out

		gdf_edi=save_gdf(df_edi,'edi')
		
		gdf_rec=load_gdf('rec').dropna(subset='ID_xml').query('rec_qc_status!="Recording"').sort_values('ID_rec')
		missing_edi=gdf_rec.loc[~gdf_rec['ID_rec'].isin(gdf_edi['ID_rec'])]['ID_rec'].to_list()
		if len(missing_edi)>0:
			print(f"\tMissing edi for recs: {missing_edi}")

edi_fp1='edi_sorted/10040.edi'
edi_fp2='edi_sorted/10220.edi'

fps_edi=[edi_fp1,edi_fp2]

def calc_phase(df):
	for direction in ['xy','yx','xx','yy']:
		df[f"phi_{direction}"]=np.round(np.degrees(np.atan(df[f'z{direction}_i']/df[f'z{direction}_r'])),2)
	return df

def calc_rho(df):
	for direction in ['xy','yx','xx','yy']:
		fq=2*df['freq']*np.pi
		perm=4*np.pi*(10**-7)

		z=[]
		for r,i in zip(df[f'z{direction}_r'],df[f'z{direction}_i']):
			z.append(np.complex64(r,i))

		df[f'rho_{direction}']=(np.abs(z)**2)/(fq*perm)
		df[f'rho_{direction}']*=1.5792270160903515e-06
	return df


def get_tipper(df,direction):
	t=[]
	for r,i in zip(df[f't{direction}_r'],df[f't{direction}_i']):
		t.append(np.complex64(r,i))
	return t
	
def calc_tipper_magnitude(df):
	t_x=get_tipper(df,'x')
	t_y=get_tipper(df,'y')

	df['t_mag']=np.sqrt(np.abs(t_x)**2+np.absolute(t_y)**2)
	return df

def calc_tipper_phase(df):
	for dir in ['x','y']:
		df[f't_phi_{dir}']=np.degrees(np.atan(df[f't{dir}_i']/df[f't{dir}_r']))
	
	return df

def edi_to_df(edi_fp):

	with open(edi_fp,'r') as file:
		blocks=file.read().strip()

	# lines=[line.strip() for line in lines]

	blocks=blocks.split('>')[1:]

	header=blocks[0]

	blocks=blocks[9:-1]

	cols=['freq','zxx_r','zxx_i','zxx_var','zxy_r','zxy_i','zxy_var','zyx_r','zyx_i','zyx_var','zyy_r','zyy_i','zyy_var','tx_r','tx_i','tx_var','ty_r','ty_i','ty_var','coh_xy','coh_yx','coh_xy','coh_yx','coh_hxy','coh_hxy']
	id_block=['FREQ']
	for dir in  ['XX','XY','YX','YY']:
		for par in ['R','I','.VAR']:
			id_block.append(f'Z{dir}{par}')


	id_block=id_block+['TXR.EXP','TXI.EXP','TXVAR.EXP','TYR.EXP','TYI.EXP','TYVAR.EXP','COH_MEAS1=1005.0001_MEAS2=1008.0001','COH_MEAS1=1006.0001_MEAS2=1007.0001','COH_MEAS1=1000.0001_MEAS2=1003.0001','COH_MEAS1=1001.0001_MEAS2=1002.0001','COH_MEAS1=1003.0001_MEAS2=1002.0001','COH_MEAS1=1008.0001_MEAS2=1007.0001']

	df_data_match=pd.DataFrame(data={'cols':cols,'id_block':id_block})
	df_data_match




	df=pd.DataFrame()
	for block in blocks:
		lines=block.split('\n')
		data_lines=lines[1:-1]
		block_header=lines[0]
		block_header=re.sub(r'\s+', ' ', block_header)
		
		if 'COH' in block_header:
			block_header=block_header.replace(' ','_').split('_ROT')[0]
		else:
			block_header=block_header.split(' ')[0]
			
		# print(block_header)
		col=df_data_match.loc[df_data_match['id_block'].str.startswith(block_header)].iloc[0,0]

		data=[]
		for line in data_lines:
			data.extend(line.split(' '))
		
		data=[d.strip() for d in data]
		data=pd.Series(data,name=col).replace('',None).astype(float).dropna().reset_index(drop=True)
		df=pd.concat([df,data],axis=1)
		
	df=calc_rho(df)
	df=calc_phase(df)
	df=calc_tipper_magnitude(df)
	df=calc_tipper_phase(df)
	return df



# def edi_to_df(edi_fp):
# 	with open(edi_fp,'r') as file:
# 		blocks=file.read().strip()

# 	# lines=[line.strip() for line in lines]

# 	blocks=blocks.split('>')[1:]

# 	header=blocks[0]

# 	blocks=blocks[9:-1]

# 	cols=['freq','zxx_r','zxx_i','zxx_var','zxy_r','zxy_i','zxy_var','zyx_r','zyx_i','zyx_var','zyy_r','zyy_i','zyy_var','tx_r','tx_i','tx_var','ty_r','ty_i','ty_var','coh_xy','coh_yx']


# 	df=pd.DataFrame()
# 	for col,block in zip(cols,blocks):
# 		lines=block.split('\n')[1:-1]
		
# 		data=[]
# 		for line in lines:
# 			data.extend(line.split(' '))
		
# 		data=[d.strip() for d in data]
# 		data=pd.Series(data,name=col).replace('',None).astype(float).dropna().reset_index(drop=True)
# 		df=pd.concat([df,data],axis=1)
		
# 	df=calc_rho(df)
# 	df=calc_phase(df)
# 	df=calc_tipper_magnitude(df)
# 	df=calc_tipper_phase(df)

# 	return df

def plot_edi(dfs,colors1,colors2,recs,alpha):
	print(f'\tPlotting edi {recs[0]}....')
	plt.style.use('dark_background') 
	dpi=100
	fig, axs = plt.subplots(4, 2, sharex=True, figsize=(1600/dpi,900/dpi))
	for df,clr1,clr2,rec,a in zip(dfs,colors1,colors2,recs,alpha):
		
		# axs[0,0].plot(df['freq'],df['rho_xy'],color=color,label=f'{rec} - xy | xx | x',alpha=a)
		# axs[0,0].plot(df['freq'],df['rho_yx'],color=color,linestyle='dotted',label=f'{rec} - yx | yy | y',alpha=a)

		# axs[0,1].plot(df['freq'],df['rho_xx'],color=color,alpha=a)
		# axs[0,1].plot(df['freq'],df['rho_yy'],color=color,linestyle='dotted',alpha=a)

		# axs[1,0].plot(df['freq'],df['phi_xy'],color=color,alpha=a)
		# axs[1,0].plot(df['freq'],df['phi_yx'],color=color,linestyle='dotted',alpha=a)

		# axs[1,1].plot(df['freq'],df['phi_xx'],color=color,alpha=a)
		# axs[1,1].plot(df['freq'],df['phi_yy'],color=color,linestyle='dotted',alpha=a)

		# axs[2,0].plot(df['freq'],df['t_mag'],color=color,alpha=a)

		# axs[2,1].plot(df['freq'],df['t_phi_x'],color=color,alpha=a)
		# axs[2,1].plot(df['freq'],df['t_phi_y'],color=color,linestyle='dotted',alpha=a)

		# axs[3,0].plot(df['freq'],df['coh_xy'],color=color,alpha=a)
		# axs[3,0].plot(df['freq'],df['coh_yx'],color=color,linestyle='dotted',alpha=a)

		axs[0,0].scatter(df['freq'],df['rho_xy'],color=clr1,label=f'{rec} - xy | xx | x',alpha=a,s=12)
		axs[0,0].scatter(df['freq'],df['rho_yx'],color=clr2,label=f'{rec} - yx | yy | y',alpha=a,s=12)

		axs[0,1].scatter(df['freq'],df['rho_xx'],color=clr1,alpha=a,s=12)
		axs[0,1].scatter(df['freq'],df['rho_yy'],color=clr2,alpha=a,s=12)

		axs[1,0].scatter(df['freq'],df['phi_xy'],color=clr1,alpha=a,s=12)
		axs[1,0].scatter(df['freq'],df['phi_yx'],color=clr2,alpha=a,s=12)

		axs[1,1].scatter(df['freq'],df['phi_xx'],color=clr1,alpha=a,s=12)
		axs[1,1].scatter(df['freq'],df['phi_yy'],color=clr2,alpha=a,s=12)

		axs[2,0].scatter(df['freq'],df['t_mag'],color=clr1,alpha=a,s=12)

		axs[2,1].scatter(df['freq'],df['t_phi_x'],color=clr1,alpha=a,s=12)
		axs[2,1].scatter(df['freq'],df['t_phi_y'],color=clr2,alpha=a,s=12)

		axs[3,0].scatter(df['freq'],df['coh_xy'],color=clr1,alpha=a,s=12)
		axs[3,0].scatter(df['freq'],df['coh_yx'],color=clr2,alpha=a,s=12)

	axs[0,0].set_xscale('log')
	axs[0,0].xaxis.set_inverted(True)
	
	axs[0,0].set_yscale('log')
	axs[0,0].set_ylim(1,10**5)
	axs[0,0].set_ylabel("r xy-yx")
	
	axs[0,1].set_yscale('log')
	axs[0,1].set_ylim(1,10**5)
	axs[0,1].set_ylabel("r xx-yy")
	
	axs[0,1].set_yscale('log')
	axs[0,1].set_ylim(1,10**5)
	axs[0,1].set_ylabel("r xx-yy")

	axs[1,0].set_ylabel("phi_xy-yx")
	axs[1,0].set_ylim(0,90)

	axs[1,1].set_ylabel("phi_xy-yx")
	axs[1,1].set_ylim(0,90)

	axs[2,0].set_ylabel("T_mag")
	axs[2,0].set_yscale('log')

	axs[2,1].set_ylabel("T_phi x-y")

	axs[3,0].set_ylabel("coh")
	axs[3,0].set_ylim(0.5,1.1)

	axs[3,1].set_visible(False)

	for i in list(range(4)):
		for j in list(range(2)):
			axs[i,j].grid(True)
			axs[i, j].tick_params(axis='both', which='both', direction='in', length=5)
			axs[i,j].grid(axis='x',which='both',color='gray', linestyle='--', linewidth=0.5)
			axs[i,j].grid(axis='y',which='major',color='gray', linestyle='--', linewidth=0.5)


	axs[3,0].set_xlabel('f [Hz]')
	axs[2,1].set_xlabel('f [Hz]')
	axs[2,1].tick_params(axis='x', labelbottom=True)
	axs[0,0].tick_params(axis='x', labeltop=True)
	axs[0,1].tick_params(axis='x', labeltop=True)

	plt.tight_layout()
	fig.legend(loc='lower right')
	plt.savefig(f'edi_sorted/img/{recs[0]}.png')
	plt.close()

def run_plot_edi():
	colors1=['#9538ff']
	colors2=['#f5e569']
	alpha=[.8,.8]

	gdf_rec=load_gdf('rec')
	gdf_rec=pd.concat([gdf_rec.copy().loc[gdf_rec['rec_qc_status'].isnull()],gdf_rec.copy().loc[gdf_rec['rec_qc_status']=='Recording']])
	rec_proc=gdf_rec['ID_rec'].to_list()
	
	gdf_edi=load_gdf('edi').dropna(subset='file_path').to_crs(3857)

	ld=get_ld('edi_sorted/img/',endswith='png')
	if len(ld)>0:
		recs_ld=[int(Path(fp).name.replace('.png','')) for fp in ld['file_path']]
		recs_ld=[rec for rec in gdf_edi['ID_rec'] if rec not in recs_ld]
	else:
		recs_ld=gdf_edi['ID_rec'].to_list()
	
	recs=list(set(recs_ld+rec_proc))

	gdf_edi=gdf_edi.loc[gdf_edi['ID_rec'].isin(recs)].sort_values('ID_rec')
	gdf_edi[['x','y']]=gdf_edi.get_coordinates()

	for edi,rec in zip(gdf_edi['file_path'],gdf_edi['ID_rec']):
		try:
			df=edi_to_df(edi)
			plot_edi([df],colors1,colors2,[rec],alpha)
		except:
			print(f'\tCannot read {edi}')
