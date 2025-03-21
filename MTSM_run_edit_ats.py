import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

from MTSM_edit_ats import *

run_edit_ats()

# input('Copying finished! Enter to exit!')