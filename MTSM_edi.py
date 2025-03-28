from MTSM_python_modules import *
from MTSM_tools import *

def run_sort_edi():
	gdf_rec=load_gdf('rec')
	id_rec=gdf_rec['ID_rec']

	ld=get_ld('edi',endswith='.edi')
	if len(ld)>0:
		ld['ID_rec']=ld['file_name'].str.findall(r"^[^_]*")
		ld['ID_rec']=[int(rec[0]) for rec in ld['ID_rec']]

		for index in ld.index:
			for type in ['_median','_stack_all','_ct']:
				if type in ld.loc[index,'file_name']:
					ld.loc[index,'edi_type']=type

		ld['ID_edi']=ld['ID_rec'].astype(str)+ld['edi_type']

		ld['dest']='MTSM/edi/'+ld['ID_edi'].astype(str)+'.edi'

		ld=ld.loc[ld['ID_rec'].isin(id_rec)]
		for origin,dest in zip(list(ld['file_path']),list(ld['dest'])):
			shutil.copy(origin,dest)
			# print(f'\tCopied {origin}\t-\t{dest}')

def run_read_edi():
	ld=get_ld('MTSM/edi/')
	if len(ld)>0:
		ld=ld.drop(columns='ID_xml')
		ld['ID_rec']=ld['file_name'].str.findall(r"^[^_]*")
		ld['ID_rec']=[int(lst[0]) for lst in ld['ID_rec']]
		ld['ID_edi']=ld['file_name'].str.replace('.edi','')

		df_edi=ld.copy()
		for index in df_edi.index:
			with open(df_edi.loc[index,'file_path']) as file:
				lines=file.readlines()

			df_edi.loc[index,'edi_y']=lines[10][:-1].replace('  LAT=','')
			df_edi.loc[index,'edi_x']=lines[11][:-1].replace('  LONG=','')
			df_edi.loc[index,'edi_z']=lines[12][:-1].replace('  ELEV=','')

		for axis in ['x','y']:
			coord_in=df_edi[f'edi_{axis}'].str.split(':')
			coord_out=[]
			for c in coord_in:
				coord_out.append(float(c[0])+(float(c[0])/abs(float(c[0])))*(float(c[1])/60+float(c[2])/3600))
			df_edi[f'edi_{axis}']=coord_out

		df_edi=df_edi.groupby('ID_rec',as_index=False).agg('first')

		gdf_edi=save_gdf(df_edi,'edi')
		
		gdf_rec=load_gdf('rec').dropna(subset='ID_xml').query('rec_qc_status!="Recording"').sort_values('ID_rec')
		missing_edi=gdf_rec.loc[~gdf_rec['ID_rec'].isin(gdf_edi['ID_rec'])]['ID_rec'].to_list()
		if len(missing_edi)>0:
			print(f"\tMissing edi for recs: {missing_edi}")

	return df_edi
