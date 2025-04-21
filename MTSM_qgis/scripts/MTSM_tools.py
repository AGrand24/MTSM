import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_python_modules import *

os.chdir( Path(__file__).parents[2])


def df_to_gdf(df,db_name,geometry,**kwargs):
	gdf=gpd.GeoDataFrame(data=df.copy())
	crs=kwargs.get('crs',4326)
	
	if geometry==True:
		if db_name=='rec':
			df.loc[~df['xml_x'].isnull(),'rec_x']=df['xml_x']
			df.loc[~df['xml_y'].isnull(),'rec_y']=df['xml_y']
			gdf=gdf.set_geometry(gpd.points_from_xy(df['rec_x'],df['rec_y']))
		else:
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

def load_gdf(db_name,**kwargs):
	layer= kwargs.get('layer',None)
	if layer==None:
		gdf=gpd.read_file(f'MTSM_qgis/mtsm_{db_name}.gpkg',engine='pyogrio')
	else:
		gdf=gpd.read_file(f'MTSM_qgis/mtsm_{db_name}.gpkg',engine='pyogrio',layer=layer)
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
	folders=['ts/1_unmatched/','ts/2_discarded/','tmp/','edi/','edi_sorted/img','backups/','reports']

	# adus=load_gdf('adu')['ID_adu'].to_list()

	# new_folders=[]
	# for folder in folders[:2]:
	# 	[new_folders.append(folder+adu) for adu in adus]

	# new_folders=folders+new_folders
	new_folders=folders

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

def warning_missing_data():
	gdf=load_gdf('xml')
	print('WARNING! Following RECSs have XML data, but are missing data in "/ts/" folder\n\t',gdf.loc[gdf['xml_path'].isnull()]['ID_rec'].sort_values().astype(str).str.replace('.0','').unique())

def export_qc_geometry():
	gdf_project_extents=load_gdf('set',layer='project_extents')
	coord_extents=gdf_project_extents.get_coordinates()
	crs=gdf_project_extents.crs

	x0=coord_extents['x'].min()
	x100=coord_extents['x'].max()
	xrange=x100-x0

	y0=coord_extents['y'].min()
	y100=coord_extents['y'].max()
	yrange=y100-y0

	df_qc=load_gdf('qc')
	df_qc.sort_values('ID_qc').reset_index(drop=True)

	total_sites=len(load_gdf('site'))
	df_qc['total_sites']=total_sites


	df_qc['qc_x']=x0+.05*xrange
	df_qc['qc_y']=y0+((df_qc.index.astype(int)+1)*0.03*xrange)

	gdf_qc=gpd.GeoDataFrame(df_qc,geometry=gpd.points_from_xy(df_qc['qc_x'],df_qc['qc_y']),crs=crs)
	gdf_qc.to_file('MTSM_qgis/mtsm_qc.gpkg',engine='pyogrio',layer='qc')

def get_project_crs():
	with open('MTSM_qgis/lib/epsg.txt') as file:
		crs=file.read()
	return crs

def get_sunrise_sunset(latitude, longitude, date):
	location = LocationInfo(latitude=latitude, longitude=longitude)
	s = sun(location.observer, date=date)
	return pd.Timestamp(s['sunrise']).round('1T').tz_localize(None),pd.Timestamp(s['sunset']).round('1T').tz_localize(None)

def import_sites_csv():
	with open('./fp_sites_csv.txt') as file:
		fp=file.read().strip()
	print(f'Reading new sites from {fp}\n')
	df=pd.read_csv(fp,header=None,names=['ID_site','site_x','site_y'])
	df=df.sort_values('ID_site')
	if len(df)>0:
		print('Found sites:')
		print(tabulate(df,showindex=False,headers=['ID_site','LON','LAT']))

		gdf_site=load_gdf('site')

		df_new=df.copy().loc[~df['ID_site'].isin(gdf_site['ID_site'])]

		if len(df_new)>0:
			print('\nFound new sites:')
			print(tabulate(df_new,showindex=False,headers=['ID_site','LON','LAT']))
			ipt=input('To load new sites type "y"!')
			if ipt=='y':
				gdf_site_out=pd.concat([gdf_site,df],axis=0)
				ipt=input('To overwrite coordinates of duplicate sites, with csv coordinate type "y"!')
				if ipt=='y':
					gdf_site_out=gdf_site_out=gdf_site_out.drop_duplicates(subset='ID_site',keep='last')
				else:
					gdf_site_out=gdf_site_out=gdf_site_out.drop_duplicates(subset='ID_site',keep='first')
				gdf_site_out=gdf_site_out.sort_values('ID_site')
				save_gdf(gdf_site_out,'site')
		else:
			ipt=input('No new site found! To replace coordinates in SITE db with coordinates from csv type "y"!')
			if ipt=='y':
				gdf_site_out=df
				save_gdf(gdf_site_out,'site')
		print('Loading finished press ENTER to exit!')

