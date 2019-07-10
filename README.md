# formation ANNA

## Ultimate goal : computation of T, S, dxT, dxS, dyT, dyS, w'b' profiles on occigen in 2°x2° boxes spanning North Atlantic

## Steps

  - How we work at MEOM : https://github.com/meom-group/tutos/blob/master/software.md
    - machines
      - pc
      - cal1
      - HPC
      - cloud
    - python
      - conda
      - environnement
      - xarray
      - dask
    - notebook
      - exemple : en local http://localhost:8888/notebooks/lupa/vorticity-variance/cmems-glo-hr_demo-fine-scale-metrics_01_vorticity-variance_v1.0.ipynb
      - exemple sur github :  https://github.com/auraoupa/NATL60-diags/blob/master/figure_rmsssh/2018-05-07-AA-maps-rmsssh-NATL60-AVISO.ipynb
    - github
      - compte
      - git clone/pull
	- git clone https://github.com/auraoupa/formation_ANNA.git
	- git pull 
      - git add/commit/push
        - 1. git add * or git add . or git add file
        - 2. git commit -m "comment"
        - 3. git push (login + psswd)
      
  - occigen
    - architecture
      - home/scratch/store
      - quotas
      - visu
    - simulation & files
      - eNATL60 brut
      - fichiers
    - ipython/notebook
      - conda/pip
    - cdftools
      - installation

# Building the machine learning dataset from eNATL60

- [ ] check filtrage boxcar from xscale
- [ ] tester différents chunks
- [ ] préparer profiles w’b’ pour 1 jour et 1 niveau vertical
- [ ] tester optimisation avec numba pour une boucle sur les niveaux verticaux
- [ ] boucle sur les fichiers et les boîtes
