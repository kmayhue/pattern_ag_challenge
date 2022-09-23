#Steps to install and run etl.py
1. This installation requires that Python3 be installed on your workspace. Step 1 is to install Python
2. Create new folder 
  mkdir <foldername>
3. Created venv environment 
  python3 -m venv pattern_ag_venv
4. Activate venv environment
  source pattern_ag_venv/bin/activate
5. clone repository
  git clone git@github.com:kmayhue/pattern_ag_challenge.git (using ssh)
6. install the required packages
  pip install -r requirements.txt
7. Run the etl pipeline  
  python scripts/etl.py
