import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)


from MTSM_import_export import *

fpath=export_rec()

input(f'Rec FL and QC data exported to {fpath}')
