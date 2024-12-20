#!/bin/bash

# Make sure you have your conda environment activated before running this script

# Check if running as root and set base directory accordingly
if [ "$EUID" -eq 0 ]; then
    cd /root/OakTree/
fi

# Navigate to the directory of the package
cd ot_db_manager

# Install the ot_logging package
pip install .

cd ..

cd ot_environment

pip install .

cd ..

cd ot_logging

pip install .

cd ..