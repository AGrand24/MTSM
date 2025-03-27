from MTSM_tools import *
from MTSM_python_modules import *


def read_job_data(ld):
	'reads xml data baset on ld dataframe into single dataframe'
	df_fields=pd.read_csv(f'MTSM/lib/fields/jl.csv').query('drop!=1').dropna(subset='orig_name')
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
	
	return df_xml_read.reset_index(drop=True)

def groupby_jl_data(df_xml):
	df_jl=df_xml.groupby('ID_jl').aggregate({'jl_rec_start':'min','jl_rec_end':'max','jl_rec_duration':'sum','jl_num_of_jobs':'count'})
	df_jl['jl_rec_downtime']=((df_jl['jl_rec_end']-df_jl['jl_rec_start']).dt.total_seconds()-df_jl['jl_rec_duration'])/60
	return df_jl

def run_proc_jl():
	print('Reading jolbist data from "MTSM/joblists/" folder...')
	ld=get_ld('MTSM/joblists/')
	ld=ld.replace('ADUConf.xml','').replace('HwConfig.xml','').replace('HwDatabase.xml','').replace('',None).dropna(subset='file_name')
	if len(ld)>0:
		print(f"\t Found:")
		id_jl=ld['file_path'].str.findall(r"(?<=joblists\/)(.*)(?=\\JLE_Template)")
		ld['ID_jl']=[id[0] for id in id_jl]

		df_job=read_job_data(ld)
		df_job['jl_num_of_jobs']='ID_jl'
		df_job=df_job[['ID_jl','jl_rec_start','jl_rec_end','jl_rec_duration','jl_num_of_jobs']]
		df_jl=groupby_jl_data(df_job).reset_index()
		df_jl=get_rec_duration_str(df_jl,'jl')

		gdf_jl=gpd.GeoDataFrame(data=df_jl)
		gdf_jl.to_file(f'MTSM_qgis/mtsm_jl.gpkg',engine='pyogrio')
		gdf_jl['ph1']='\t'
		gdf_jl['ph2']='\t'
		print(tabulate(gdf_jl[['ph1','ph2','ID_jl','jl_num_of_jobs','jl_rec_start','jl_rec_end','jl_rec_duration_str']],showindex=False,headers=['\t','\t','ID_jl','Jobs','Start','End','Duration']))

		print()

		return gdf_jl
