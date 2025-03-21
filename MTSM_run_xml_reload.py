import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_tools import *
from MTSM_proc_rec import *
from MTSM_read_xml import *

gdf_xml=load_gdf_xml()
gdf_xml[['ID_xml','ID_rec']].to_csv('MTSM/lib/id_rec_xml_match.csv',index=False)


gdf_xml=gdf_xml.drop(index=gdf_xml.index.to_list())
save_gdf(gdf_xml,'xml')
gdf_xml=run_xml_read()
gdf_xml=gdf_xml.drop(columns=['ID_rec'])
gdf_xml=pd.merge(gdf_xml,pd.read_csv('MTSM/lib/id_rec_xml_match.csv').set_index('ID_xml'),left_on='ID_xml',right_index=True,how='left').sort_index(axis=1)
save_gdf(gdf_xml,'xml')


# print('Reloading xml data...')

run_proc_rec()

# input('Proccesing finished! Enter to exit!')