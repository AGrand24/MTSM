import os

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

import sys
import subprocess
subprocess.call([sys.executable,'MTSM_main_proc.py', 'full'])