import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)


from MTSM_id_rec_sync import *

id_rec_by_distance()

input('Syncing finished! Press enter to exit!')

