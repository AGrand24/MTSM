import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *
# from MTSM_python_modules import *

os.chdir( Path(__file__).parents[2])


def read_job_data(ld):
	'reads xml data baset on ld dataframe into single dataframe'
	df_fields=pd.read_csv(f'MTSM_qgis/lib/fields/jl.csv').query('drop!=1').dropna(subset='orig_name')
	df_xml_read=pd.DataFrame()
	for i,index in enumerate(ld.index):
		xml_read=extract_xml(ld.loc[index,'file_path'])
		xml_single=pd.DataFrame(xml_read.loc[xml_read.index.isin(df_fields['orig_name'])]).T
		xml_single['ID_jl']=ld.loc[index,'ID_jl']
		df_xml_read=pd.concat([df_xml_read,xml_single])

	df_xml_read=df_xml_read.rename(columns=dict(zip(df_fields['orig_name'],df_fields['field'])))
	
	df_xml_read['jl_rec_start']=pd.to_datetime(df_xml_read['jl_start_date']+'T'+df_xml_read['jl_start_time'])
	df_xml_read['jl_rec_end']=pd.to_datetime(df_xml_read['jl_end_date']+'T'+df_xml_read['jl_end_time'])

	df_xml_read['jl_rec_duration']=(df_xml_read['jl_rec_end']-df_xml_read['jl_rec_start']).dt.total_seconds()

	freq=[]
	for fq in df_xml_read['jl_freq_sample'].astype(float):
		if fq>999:
			freq.append(str(int(np.round(fq/1000)))+'k')
		else:
			freq.append(str(int(np.round(fq))))
	df_xml_read['jl_freq_sample']=freq
	df_xml_read=df_xml_read.sort_values(['ID_jl','jl_rec_start'])
	return df_xml_read.reset_index(drop=True)


def groupby_jl_data(df_xml):
	df_jl=df_xml.groupby('ID_jl').aggregate({'jl_rec_start':'min','jl_rec_end':'max','jl_rec_duration':'sum','jl_num_of_jobs':'count'})
	df_jl['jl_rec_downtime']=((df_jl['jl_rec_end']-df_jl['jl_rec_start']).dt.total_seconds()-df_jl['jl_rec_duration'])/60


	for col in ['jl_freq_sample','jl_freq_base']:
		gb_tmp=df_xml.groupby('ID_jl').aggregate({col:(', ').join})
		df_jl=pd.concat([df_jl,gb_tmp],axis=1)

	return df_jl

def run_proc_jl():
	print('Reading jolbist data from "MTSM/joblists/" folder...')
	ld=get_ld('MTSM_qgis/joblists/')
	if len(ld)>0:
		ld=ld.replace('ADUConf.xml','').replace('HwConfig.xml','').replace('HwDatabase.xml','').replace('',None).dropna(subset='file_name')
		print()
		id_jl=ld['file_path'].str.findall(r"(?<=joblists\/)(.*)(?=\\JLE_Template)")
		ld['ID_jl']=[id[0] for id in id_jl]

		df_job=read_job_data(ld)
		df_job['jl_num_of_jobs']='ID_jl'
		df_job=df_job[['ID_jl','jl_rec_start','jl_rec_end','jl_rec_duration','jl_num_of_jobs','jl_freq_sample','jl_freq_base']]
		df_jl=groupby_jl_data(df_job).reset_index()
		df_jl=get_rec_duration_str(df_jl,'jl')

		gdf_jl=gpd.GeoDataFrame(data=df_jl)
		gdf_jl.to_file(f'MTSM_qgis/mtsm_jl.gpkg',engine='pyogrio')
		print(tabulate(gdf_jl[['ID_jl','jl_num_of_jobs','jl_rec_start','jl_rec_end','jl_rec_duration_str']],showindex=False,headers=['ID_jl','Jobs','Start','End','Duration'],tablefmt="presto"))

		print()

		return gdf_jl
	else:
		print("\tWarning! 'MTSM_qgis/joblists' folder is empty! Copy project's joblist for full JL functionality. ")