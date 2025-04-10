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
			priority.append(1)
		elif '_ct' in fn:
			priority.append(2)
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


