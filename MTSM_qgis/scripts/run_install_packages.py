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
        print(f"Successfully installed packages from {requirements_file}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install packages from {requirements_file}: {e}")

# Example usage
install_requirements("req.txt")