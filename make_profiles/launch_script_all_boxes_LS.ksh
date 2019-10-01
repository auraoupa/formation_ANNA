#!/bin/bash
#SBATCH --nodes=8
#SBATCH --ntasks=65
#SBATCH --threads-per-core=1
#SBATCH --constraint=BDW28
#SBATCH -J box-LS
#SBATCH -e box-LS.e%j
#SBATCH -o box-LS.o%j
#SBATCH --time=02:30:00
#SBATCH --exclusive

NB_NPROC=65 ##

runcode() { srun --mpi=pmi2 -m cyclic -n $@ ; }
liste=''

date='20090701'

set -x
ulimit -s
ulimit -s unlimited

for k in $(seq 1 65); do 
        km=$(echo $(( $k - 1)))
	echo "python extractions-gradients-all-variables-boxes.py 'LS' $km $date" > extract-boxLS${km}-${date}.ksh
	chmod +x extract-boxLS${km}-${date}.ksh
	liste="$liste extract-boxLS${km}-${date}.ksh"
done

runcode  $NB_NPROC /scratch/cnt0024/hmg2840/albert7a/bin/mpi_shell $liste



