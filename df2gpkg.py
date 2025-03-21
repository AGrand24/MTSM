import geopandas as gpd
import pandas as pd
from shapely import MultiPolygon,Polygon,Point,LineString

def df2gdf(df,path,layer,**kwargs):
	crs=kwargs.get('crs',5514)
	gt=kwargs.get('gt','pt')

	if kwargs.get('z',False)==False:
		df['z']=0

	if gt=='pt':
		geom=gpd.points_from_xy(df['x'],df['y'],df['z'])
	else:
		geom=[]
		gb=df.groupby('ID',as_index=False).agg(list)[['x','y','z']]
		if gt=='ls':
			for index in gb.index:
				geom.append(LineString(zip(gb.loc[index,'x'],gb.loc[index,'y'],gb.loc[index,'z'])))
				df=gb.drop(columns=['x','y','z'])
		elif gt=='pl':
			for index in gb.index:
				geom.append(Polygon(zip(gb.loc[index,'x'],gb.loc[index,'y'],gb.loc[index,'z'])))
				df=gb.drop(columns=['x','y','z'])

	gdf=gpd.GeoDataFrame(df)
	gdf=gdf.set_geometry(geom).set_crs(crs)
	gdf.to_file(path,layer=layer+f'_{gt}',engine='pyogrio')
	return gdf

def load_gdf(path,layer):
	gdf=gpd.read_file(path,layer=layer,engine='pyogrio')
	return gdf