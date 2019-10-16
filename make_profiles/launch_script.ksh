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

python extractions-gradients-all-variables-boxes.py 'LS' 1 '20090701'
python extractions-gradients-all-variables-boxes.py 'GS' 93 '20090701'
#python calcul-wprimebprime-boxes.py 'LS' 1 '20090701'

#python extractions-gradients-all-variables-boxes-GS.py 0 '20090701'

#python 2019-09-30-AA-calcul-wprimebprime-boxes-LS.py

