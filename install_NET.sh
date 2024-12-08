#!/bin/bash

# Make sure you have your conda environment activated before running this script

# Navigate to the directory of the package
cd ot_db_manager

# conda init zsh

# source ~/.bashrc
# Activate CONDA ENV BEFORE RUNNING
# conda activate OT_NET
# conda activate OT_NET

# Install the ot_logging package
pip install .
#conda install -n OT_NET .
#conda run -n OT_NET python -m install .

cd ..

cd ot_environment

pip install .

cd ..

cd ot_logging

pip install .

cd ..