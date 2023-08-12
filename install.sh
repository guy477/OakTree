#!/bin/bash

# Navigate to the directory of the package
cd ot_db_manager

echo "OT_WSERV"

# Activate the Anaconda environment named OT_WSERV
source activate OT_WSERV

# Install the ot_logging package
pip install .

cd ..

cd ot_environment

pip install .

cd ..

cd ot_logging

pip install .

# Pause and wait for user input before closing
read -p "Press [Enter] key to continue..."
