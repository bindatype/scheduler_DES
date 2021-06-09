Set up a conda env:
conda create --name scheduler_DES -y
conda activate scheduler_DES
conda install -c conda-forge -y simpy
conda install -c conda-forge -y mpi4py
conda install -c conda-forge -y pandas
conda update -n base -c defaults conda




Usage: 
All arguments are required
	-m Mode: auto or user
	-f Input file name
	-d Number of defq nodes
	-s Number of short nodes
	-t Number of tiny nodes
	-n Number of nano nodes
Example: mpiexec -n 4 python3 scheduler_DES.py -m auto -f $NAME -d 54 -s 26 -n 30  -t 50
	There should be as many mpi processes as there are partitions. In the current implementation mpiexec -n


