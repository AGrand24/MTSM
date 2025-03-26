import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)


from MTSM_tools import *
from MTSM_python_modules import *

print('Exporting to excel..')
gdf_rec=load_gdf('rec').drop(columns='geometry').sort_index(axis=1)

fp='tmp/rec_data_export_'+time.strftime('%y%m%d_%H%M%S')+'.csv'

gdf_rec.to_csv(fp,index=False)
input(f'Rec database exported to - \t {fp}')