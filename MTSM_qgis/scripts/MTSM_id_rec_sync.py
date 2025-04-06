from MTSM_python_modules import *
from MTSM_tools import *

def id_rec_by_distance(tolerance):
	with open('MTSM_qgis/lib/epsg.txt') as file:
		crs=file.read()

	gdf_xml=load_gdf('xml').to_crs(crs)
	gdf_xml_sync=gdf_xml.loc[gdf_xml['ID_rec'].isnull()]
	gdf_rec=load_gdf('rec').to_crs(crs)
	gdf_rec=gdf_rec.sort_values('ID_rec').drop_duplicates('ID_site',keep='last')

	gdf_rec[['x','y']]=gdf_rec.get_coordinates()
	gdf_xml_sync[['x','y']]=gdf_xml_sync.get_coordinates()


	id_rec_sync=pd.DataFrame()
	for index in gdf_rec.index:
		tmp=gdf_xml_sync[['ID_rec','ID_xml','x','y']].copy()
		tmp['distance']=((tmp['x']-gdf_rec.loc[index,'x'])**2+(tmp['y']-gdf_rec.loc[index,'y'])**2)**.5
		tmp=tmp.query(f'distance<{tolerance}')
		tmp['ID_rec']=gdf_rec.loc[index,'ID_rec']
		id_rec_sync=pd.concat([id_rec_sync,tmp])

	id_rec_sync=id_rec_sync[['ID_rec','ID_xml']].set_index('ID_xml')
	gdf_xml_sync=gdf_xml_sync.drop(columns=['x','y'])
	gdf_xml_sync=gdf_xml_sync.drop(columns='ID_rec')
	gdf_xml_sync=pd.merge(gdf_xml_sync,id_rec_sync,how='left',left_on='ID_xml',right_index=True)

	gdf_xml_sync.sort_index(axis='columns')

	gdf_xml=pd.concat([gdf_xml.drop(columns='geometry'),gdf_xml_sync.drop(columns='geometry')]).drop_duplicates(subset='ID_xml',keep='last')
	save_gdf(gdf_xml,'xml')
