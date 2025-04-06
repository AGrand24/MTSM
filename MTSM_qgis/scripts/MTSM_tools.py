import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_python_modules import *

os.chdir( Path(__file__).parents[2])


def df_to_gdf(df,db_name,geometry,**kwargs):
	gdf=gpd.GeoDataFrame(data=df)
	crs=kwargs.get('crs',4326)
	
	if geometry==True:
		gdf=gdf.set_geometry(gpd.points_from_xy(df[f'{db_name}_x'],df[f'{db_name}_y']))
		gdf=gdf.set_crs(crs)
	gdf=gdf.sort_index(axis='columns')
	return gdf

def save_gdf(df,db_name,**kwargs):
	df=set_dtypes(df,db_name)
	gdf=df_to_gdf(df,db_name,kwargs.get('geometry',True))
	gdf=gdf.drop_duplicates(subset=f'ID_{db_name}',keep='last')
	gdf=gdf.replace('None',None).replace('nan',None)
	gdf.to_file(f'MTSM_qgis/mtsm_{db_name}.gpkg',engine='pyogrio')
	return gdf

def load_gdf(db_name):
	gdf=gpd.read_file(f'MTSM_qgis/mtsm_{db_name}.gpkg',engine='pyogrio')
	return gdf


def get_ld(dir_path,**kwargs):

	file_list = []
	# Walk through the directory and its subdirectories
	for root, dirs, files in os.walk(dir_path):
		for file in files:
			# Get the file path
			file_path = os.path.join(root, file)
			# Append the file name and path to the list
			file_list.append({'file_name': file, 'file_path': file_path})

	# Create a Pandas DataFrame from the list
	df_ld = pd.DataFrame(file_list)

	if len(df_ld)>1:
		if kwargs.get('endswith',None)!=None:
			df_ld=df_ld.loc[df_ld['file_name'].str.endswith(kwargs.get('endswith',None))]
		try:
			df_ld['ID_xml']=df_ld['file_name'].str.slice(0,23)
			if kwargs.get('filter_id_ts',False)==True:
				df_ld=df_ld.drop_duplicates(subset='ID_ts',keep='last').reset_index(drop=True)
		except:
			pass
	return df_ld.reset_index(drop=True)

def extract_xml(path):
	tree = etree.parse(path)
	lstKey = []
	lstValue = []
	for p in tree.iter() :
		lstKey.append(tree.getpath(p).replace("/",".")[1:])
		lstValue.append(p.text)
	
	xml_out={lstKey[i]: lstValue[i] for i in range(len(lstKey))}
	return pd.Series(xml_out)


def lower_case_columns(data):
	'lower case columns'
	for col in data.columns:
		if str(col).startswith('ID_')==False:
			data=data.rename(columns={col:col.lower()})
	# data.columns = [x.lower() for x in data.columns if not x.startswith('ID')]
	return data

def set_dtypes(df,db_name):
	'set dtype for column based on field csv file'
	df_fields=pd.read_csv(f'MTSM_qgis/lib/fields/{db_name}.csv')
	
	if db_name=='rec':
		df_fields2=pd.read_csv(f'MTSM_qgis/lib/fields/xml.csv').query('drop==0')
		df_fields=pd.concat([df_fields,df_fields2],axis=0).drop_duplicates('field',keep='first').reset_index(drop=True)
	if db_name=='xml':
		df_fields=df_fields.query('drop==0').reset_index(drop=True)
		


	cols=[col for col in df.columns.to_list() if col in df_fields['field'].to_list() ]
	df=df[cols]

	df_empty=pd.DataFrame(columns=df_fields['field'].to_list())
	for field,dtype in zip(df_fields['field'],df_fields['dtype']):
		df_empty[field]=df_empty[field].astype(dtype)



	df_new=pd.concat([df_empty,df.reset_index(drop=True)])
	for field,dtype in zip(df_empty.dtypes.index,df_empty.dtypes):
		# print(field,dtype)
		if dtype=='datetime64[ms]':
			df_new[field]=pd.to_datetime(df_new[field],format='mixed').dt.round('1s')
		else:
			df_new[field]=df_new[field].astype(dtype)
	
	return df_new


def get_rec_duration_str(gdf_xml,prefix):
	h=(gdf_xml[f'{prefix}_rec_duration']//3600)
	m=((gdf_xml[f'{prefix}_rec_duration']-h*3600)//60)
	s=((gdf_xml[f'{prefix}_rec_duration']-(h*3600+m*60)))
	h,m,s=h.replace(np.nan,0),m.replace(np.nan,0),s.replace(np.nan,0)

	h,m,s=h.astype(int).astype(str).str.zfill(2),m.astype(int).astype(str).str.zfill(2),s.astype(int).astype(str).str.zfill(2)

	gdf_xml[f'{prefix}_rec_duration_str']=h+':'+m+':'+s
	gdf_xml[f'{prefix}_rec_duration_str']
	return gdf_xml


def create_folders(**kwargs):
	print('\tChcecking folder structure..')
	folders=['ts/1_unmatched/','ts/2_discarded/','tmp/','MTSM_qgis/rec_import_export/','MTSM_qgis/id_xml_bckp/']

	adus=load_gdf('adu')['ID_adu'].to_list()

	new_folders=[]
	for folder in folders[:2]:
		[new_folders.append(folder+adu) for adu in adus]

	new_folders=folders+new_folders

	for folder in new_folders:
		if not os.path.exists(folder):
			os.makedirs(folder)
			if kwargs.get('print',False)==True:
				print(f'\tCreated folder-\t {folder}')
	
def handleRemoveReadonly(func, path, exc):
  excvalue = exc[1]
  if func in (os.rmdir, os.remove) and excvalue.errno == errno.EACCES:
      os.chmod(path, stat.S_IRWXU| stat.S_IRWXG| stat.S_IRWXO) # 0777
      func(path)
  else:
      raise


def delete_folder(path):
	shutil.rmtree(path, ignore_errors=False, onerror=handleRemoveReadonly)


def delete_empty_folders(path):
	# print('\tDeleting empty folders...')
	empty_dirs = []
	for dirpath, dirnames, filenames in os.walk(path):
		if not dirnames and not filenames:
			empty_dirs.append(dirpath)

	[delete_folder(x) for x in empty_dirs]

def get_mag_dec(lon,lat,h,date):
	Be, Bn, Bu = ppigrf.igrf(lon, lat, h, date)
	mag_dec=ppigrf.get_inclination_declination(Be,Bn,Bu)
	return float(mag_dec[1][0])