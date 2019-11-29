#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=24
#SBATCH --threads-per-core=1
#SBATCH --constraint=HSW24
#SBATCH -J test-LS
#SBATCH -e test-LS.e%j
#SBATCH -o test-LS.o%j
#SBATCH --time=08:30:00
#SBATCH --exclusive

set -x
ulimit -s
ulimit -s unlimited

python 2019-10-15-extractions-filtering-1variable-1boxLS-ipython.py
