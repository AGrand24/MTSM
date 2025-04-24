import os
from pathlib import Path
import subprocess
import sys

abspath=os.path.abspath(__file__)
dname=os.path.dirname(abspath)
os.chdir(dname)

def install_requirements(requirements_file):
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_file])
        input(f"\n\nSuccessfully installed packages from {requirements_file}! Press ENTER to exit!")
    except subprocess.CalledProcessError as e:
        input(f"\n\nFailed to install packages from {requirements_file}: {e}\nPress ENTER to exit!")

# Example usage
install_requirements("req.txt")