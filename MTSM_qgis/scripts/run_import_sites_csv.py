import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *
from MTSM_proc_rec import rec_unify_coords

os.chdir( Path(__file__).parents[2])

def load_csv_data():
	with open('tmp/fp_sites_csv.txt') as file:
		fp=file.read().strip()
	print(f'Reading new sites from {fp}')
	df=pd.read_csv(fp,header=None,names=['ID_site','rec_x0','rec_y0']).dropna()
	df=df.sort_values('ID_site')
	df['version']='csv'

	try:
		df['ID_site']=df['ID_site'].astype(int)
		df['rec_x0']=df['rec_x0'].astype(float)
		df['rec_y0']=df['rec_y0'].astype(float)
		data_check=True
	except:
		print('Incorrect format. Correct format is:\nID_site (integer), LON(decimal degrees), LAT (decimal degrees) No header, separated by comma')
		data_check=False
		df=pd.DataFrame()
	
	return df, data_check

def import_sites_csv():

	df,data_check=load_csv_data()

	gdf_rec=load_gdf('rec')
	gdf_rec['version']='database'

	if len(df)>0 and data_check==True:
		print(f'Found {len(df)} sites!')
		df_new=df.loc[~df['ID_site'].isin(gdf_rec['ID_site'].unique())]
		if len (df_new)>0:
			print('\nNew sites:\n')
			print(tabulate(df_new,showindex=False,headers=['ID_site','LON','LAT']))
			if input('\n To add new sites to database type "y". To skip press ENTER!')=='y':
				df_new['ID_rec']=df_new['ID_site']*10
				gdf_rec=pd.concat([gdf_rec,df_new],axis=0).sort_values('ID_rec').reset_index(drop=True)
				print('Imported new sites from csv!')
		df_conflict=pd.concat([df,gdf_rec[['ID_site','rec_x0','rec_y0','version']]]).reset_index(drop=True)

		if len(df_conflict)>0:
			df_conflict=df_conflict.loc[(df_conflict['ID_site'].isin(gdf_rec['ID_site'].unique()))&(df_conflict['ID_site'].isin(df['ID_site'].unique()))]
			df_conflict['compare']=df_conflict['rec_x0'].astype(str)+df_conflict['rec_y0'].astype(str)
			df_conflict=df_conflict.drop_duplicates(subset='compare',keep=False)

			if len(df_conflict)>0:
				df_conflict=df_conflict.sort_values('ID_site')
				print('\nFound conflicting coordinates:\n')
				print(tabulate(df_conflict[['ID_site','version','rec_x0','rec_y0']],showindex=False,headers=['ID_site','Version','LON','LAT']))
				ipt=input('To keep coordinates from database press ENTER! To overwrite them with coordinates from csv type "c"!')
				
				if ipt=='c':
					df_conflict=df_conflict.loc[df_conflict['version']=='csv']
					for site,x,y in zip(df_conflict['ID_site'],df_conflict['rec_x0'],df_conflict['rec_y0']):
						gdf_rec.loc[gdf_rec['ID_site']==site,'rec_x0']=x
						gdf_rec.loc[gdf_rec['ID_site']==site,'rec_y0']=y
						gdf_rec=gdf_rec.sort_values('ID_rec').reset_index(drop=True)
						print('Overwritten coordinates!')
		
		save_gdf(gdf_rec,'rec')
		rec_unify_coords()
		export_site_db()

try:
	import_sites_csv()
	input('\nImport finished! Press ENTER to continue!')
except Exception as error:
	traceback.print_exc()
	input('Press ENTER to continue!')

