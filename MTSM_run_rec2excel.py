import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)


from MTSM_tools import *
from MTSM_python_modules import *

print('Exporting to excel..')
gdf_rec=load_gdf('rec')

fields=pd.read_csv('MTSM/lib/fields/rec_excel.tsv',sep='\t').dropna(subset='name_new')

df_export=gdf_rec[fields['name_orig']]

for col_old,col_new in zip(list(fields['name_orig']),list(fields['name_new'])):
	df_export[col_new]=df_export[col_old]

df_export=df_export[fields['name_new']]
try:
	df_export.to_excel('MTSM/export/rec_export.xlsx',index=False)
except:
	input('Error - Close MTSM/export/rec_export.xlsx!')