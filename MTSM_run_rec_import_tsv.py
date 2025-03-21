import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)


from MTSM_import_export import *

fpath=import_rec()

input(f'Rec FL and QC data imported from {fpath}!')