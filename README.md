# formation ANNA

## Ultimate goal : computation of T, S, dxT, dxS, dyT, dyS, w'b' profiles on occigen in 2°x2° boxes spanning North Atlantic

## Work on machine learning dataset from eNATL60

### Premiers tests sur occigen (visu + ipython)
- calcul w’b’ sur 1 niveau et 1h
	- tester différents chunks
		* 1000x1000 pour x et y c le mieux
	- tester plusieurs boites pour un ficher
		* on ne multiplie pas par le nombre de boîtes, ouf !
		* 1 boîte = 1mn : https://github.com/auraoupa/formation_ANNA/blob/master/scripts/2019-07-10-AA-compute_wprimevprime_1day1box.py
		* 2 boîtes = 2mn : https://github.com/auraoupa/formation_ANNA/blob/master/scripts/2019-07-10-AA-compute_wprimevprime_1day2box.py
		* 14 boîtes = 3mn : https://github.com/auraoupa/formation_ANNA/blob/master/scripts/2019-07-10-AA-compute_wprimevprime_1day14box.py
		
### La suite 

- [ ] Anna : work on the boxes
- [ ] Aurélie/Julien : check filtrage boxcar from xscale/choose window type in https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.windows.get_window.html#scipy.signal.windows.get_window
- [ ] Aurélie : ecrire netcdf au fur et a mesure
- [ ] Aurélie : contacter cines pour mise en place notebook et dask-jobqueue
- [ ] Aurélie : tester optimisation avec numba pour une boucle sur les niveaux verticaux et/ou les 24h
- [ ] Anna : installation sur occigen
	- [ ] bashrc
	- [ ] conda
	- [ ] git clone formation_ANNA

et enfin :
- [ ] Anna : boucle sur les fichiers et les boîtes, 1 fichier par boîte ? 365x24x300 par profils = ~3Go
- [ ] Anna : idem pour les autres profils T, S, U, V dxT etc ...

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

