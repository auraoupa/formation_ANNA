#!/bin/bash
#SBATCH --nodes=10
#SBATCH --ntasks=102
#SBATCH --threads-per-core=1
#SBATCH --constraint=BDW28
#SBATCH -J boxGS
#SBATCH -e boxGS.e%j
#SBATCH -o boxGS.o%j
#SBATCH --time=02:30:00
#SBATCH --exclusive

NB_NPROC=102 ##

runcode() { srun --mpi=pmi2 -m cyclic -n $@ ; }
liste=''

date='20090701'

set -x
ulimit -s
ulimit -s unlimited

for k in $(seq 1 102); do 
        km=$(echo $(( $k - 1)))
	echo "python extractions-gradients-all-variables-boxes.py 'GS' $km $date > output_extractions-gradients-all-variables-boxes-GS-box-${km}-${date}.txt" > extract-boxGS${km}-${date}.ksh
	chmod +x extract-boxGS${km}-${date}.ksh
	liste="$liste extract-boxGS${km}-${date}.ksh"
done

runcode  $NB_NPROC /scratch/cnt0024/hmg2840/albert7a/bin/mpi_shell $liste



